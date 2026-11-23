"""核心逻辑与 API 测试。"""
from __future__ import annotations

import random
from datetime import datetime

import pytest

from app import core
from app.db import read_conn, write_txn
from app.topology import SWITCHES

INITIAL = {s[0]: s[4] for s in SWITCHES}
LINKS = {s[0]: (s[2], s[3]) for s in SWITCHES}
SOURCES = ["P1", "P2"]
AREAS = ["T1", "T2", "T3", "T4"]

DAY = "2026-11-23"


def E(iso: str) -> float:
    return core.to_epoch(datetime.fromisoformat(iso.replace("Z", "+00:00")))


def create(no, sw, ts, st):
    with write_txn() as conn:
        return core.create_order(conn, no, sw, E(ts), st)


def update(no, ts, st):
    with write_txn() as conn:
        return core.update_order(conn, no, E(ts), st)


def delete(no):
    with write_txn() as conn:
        return core.delete_order(conn, no)


def loss(area):
    with read_conn() as conn:
        rows = conn.execute(
            "SELECT start_epoch, end_epoch FROM loss_periods "
            "WHERE area_id = ? ORDER BY start_epoch",
            (area,),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


# ------------------------------------------------------------ 冲突约束

def test_same_switch_same_time_rejected(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    with pytest.raises(core.ConflictError):
        create("D2", "K1", f"{DAY}T10:00:00Z", "通")
    # 不同开关同一时刻允许
    create("D2", "K5", f"{DAY}T10:00:00Z", "断")
    # 同一单号不允许重复
    with pytest.raises(core.ConflictError):
        create("D1", "K2", f"{DAY}T11:00:00Z", "断")


def test_update_conflict_rejected(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    create("D2", "K1", f"{DAY}T11:00:00Z", "通")
    with pytest.raises(core.ConflictError):
        update("D2", f"{DAY}T10:00:00Z", "通")  # 撞到 D1 的时刻


# ------------------------------------------------------------ 失电判定与联络转供

def test_tie_switch_resupply(db):
    # 拉开 K1：T1、T2 失电；T3、T4 正常
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    assert loss("T1") == [(E(f"{DAY}T10:00:00Z"), None)]
    assert loss("T2") == [(E(f"{DAY}T10:00:00Z"), None)]
    assert loss("T3") == [] and loss("T4") == []
    # 合上联络开关 K3：T1、T2 由 P2 转供，失电时段在 10:05 封口
    create("D2", "K3", f"{DAY}T10:05:00Z", "通")
    assert loss("T1") == [(E(f"{DAY}T10:00:00Z"), E(f"{DAY}T10:05:00Z"))]
    assert loss("T2") == [(E(f"{DAY}T10:00:00Z"), E(f"{DAY}T10:05:00Z"))]
    assert loss("T3") == [] and loss("T4") == []


def test_same_timestamp_changes_together(db):
    # 同一时刻：K1 断开 + K3 合上，一起核算 → T1/T2 由 P2 转供，全程不失电
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    create("D2", "K3", f"{DAY}T10:00:00Z", "通")
    for area in AREAS:
        assert loss(area) == []


def test_area_lost_only_when_no_source(db):
    # K1、K5 同时断开：四个台区都连不到任何电源
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    create("D2", "K5", f"{DAY}T10:00:00Z", "断")
    for area in AREAS:
        assert loss(area) == [(E(f"{DAY}T10:00:00Z"), None)]


# ------------------------------------------------------------ 更正与撤销

def test_update_moves_and_changes(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    assert loss("T1") == [(E(f"{DAY}T10:00:00Z"), None)]
    # 更正时刻：10:00 -> 10:30
    update("D1", f"{DAY}T10:30:00Z", "断")
    assert loss("T1") == [(E(f"{DAY}T10:30:00Z"), None)]
    # 更正通断：断 -> 通（相当于没拉开）
    update("D1", f"{DAY}T10:30:00Z", "通")
    assert loss("T1") == []
    # 无变化的更正是幂等 no-op
    with write_txn() as conn:
        assert core.update_order(conn, "D1", E(f"{DAY}T10:30:00Z"), "通") == []


def test_delete_order(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    delete("D1")
    assert loss("T1") == [] and loss("T2") == []
    with pytest.raises(core.NotFoundError):
        delete("D1")


def test_redundant_order_no_recompute(db):
    # K1 本来就是通，补一张“通”单：状态无差异，不重算任何时段
    with write_txn() as conn:
        spans = core.create_order(conn, "D1", "K1", E(f"{DAY}T10:00:00Z"), "通")
    assert spans == []
    # 撤销这张冗余单同样无需重算
    with write_txn() as conn:
        assert core.delete_order(conn, "D1") == []


# ------------------------------------------------------------ 事务一致性

def test_failed_write_keeps_original(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    before = loss("T1")
    with pytest.raises(RuntimeError):
        with write_txn() as conn:
            core.create_order(conn, "D2", "K3", E(f"{DAY}T10:05:00Z"), "通")
            raise RuntimeError("模拟重算失败")
    # 变位单与失电时段一起回滚，仍可按原结果核查
    assert loss("T1") == before
    with read_conn() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM orders WHERE order_no = 'D2'"
        ).fetchone()[0] == 0


# ------------------------------------------------------------ 核查

def test_verify_report_and_remove(db):
    create("D1", "K1", f"{DAY}T10:00:00Z", "断")
    create("D2", "K3", f"{DAY}T10:05:00Z", "通")
    rs, re_ = E(f"{DAY}T00:00:00Z"), E("2026-11-24T00:00:00Z")
    with read_conn() as conn:
        # 人工报停 09:30-10:03：多报 09:30-10:00，漏报 10:03-10:05
        loss_p, add, remove = core.verify(
            conn, "T1", rs, re_, [(E(f"{DAY}T09:30:00Z"), E(f"{DAY}T10:03:00Z"))]
        )
    assert loss_p == [(E(f"{DAY}T10:00:00Z"), E(f"{DAY}T10:05:00Z"))]
    assert add == [(E(f"{DAY}T10:03:00Z"), E(f"{DAY}T10:05:00Z"))]
    assert remove == [(E(f"{DAY}T09:30:00Z"), E(f"{DAY}T10:00:00Z"))]
    with read_conn() as conn:
        # 报停与失电完全一致：无补报、无剔除
        _, add2, remove2 = core.verify(
            conn, "T1", rs, re_, [(E(f"{DAY}T10:00:00Z"), E(f"{DAY}T10:05:00Z"))]
        )
        # 报停区间会被裁剪到核查范围内（起点计入、终点不计）
        _, add3, _ = core.verify(
            conn, "T1",
            E(f"{DAY}T10:01:00Z"), E(f"{DAY}T10:04:00Z"),
            [(E(f"{DAY}T09:00:00Z"), E(f"{DAY}T11:00:00Z"))],
        )
    assert add2 == [] and remove2 == []
    assert add3 == []


def test_verify_rejects_bad_input(db):
    with read_conn() as conn:
        with pytest.raises(core.ValidationError):
            core.verify(conn, "T9", 0.0, 1.0, [])  # 台区不存在
        with pytest.raises(core.ValidationError):
            core.verify(conn, "T1", 2.0, 1.0, [])  # 范围起终点颠倒


# ------------------------------------------------------------ 增量重算 vs 暴力全量重算

def brute_force_loss(orders, t0, t1):
    """参考实现：由初始状态 + 全部变位单直接推算 [t0, t1) 内各台区失电时段。"""
    times = sorted({v[1] for v in orders.values() if t0 < v[1] < t1})
    bounds = [t0] + times + [t1]
    result = {a: [] for a in AREAS}
    open_since = {}
    for i in range(len(bounds) - 1):
        t = bounds[i]
        states = dict(INITIAL)
        for sw, ts, st in sorted(orders.values(), key=lambda v: v[1]):
            if ts <= t:
                states[sw] = st
        parent = {n: n for n in SOURCES + AREAS}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for sw, st in states.items():
            if st == "通":
                a, b = LINKS[sw]
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[ra] = rb
        fed = {find(s) for s in SOURCES}
        lost = {a for a in AREAS if find(a) not in fed}
        for a in AREAS:
            if a in lost:
                open_since.setdefault(a, t)
            elif a in open_since:
                result[a].append((open_since.pop(a), t))
    for a, s in open_since.items():
        result[a].append((s, t1))
    return result


def test_incremental_matches_bruteforce(db):
    rng = random.Random(20261123)
    base = E("2026-11-20T00:00:00Z")
    orders = {}
    for seq in range(80):
        op = rng.choice(["create", "create", "update", "delete"])
        sw = rng.choice(list(INITIAL))
        t = base + rng.randrange(0, 3 * 24 * 60) * 60.0
        st = rng.choice(["通", "断"])
        no = f"O{seq:03d}"
        try:
            if op == "create":
                with write_txn() as conn:
                    core.create_order(conn, no, sw, t, st)
                orders[no] = (sw, t, st)
            elif op == "update" and orders:
                victim = rng.choice(list(orders))
                with write_txn() as conn:
                    core.update_order(conn, victim, t, st)
                orders[victim] = (orders[victim][0], t, st)
            elif op == "delete" and orders:
                victim = rng.choice(list(orders))
                with write_txn() as conn:
                    core.delete_order(conn, victim)
                del orders[victim]
        except core.ConflictError:
            continue
    t0, t1 = base - 3600.0, base + 4 * 86400.0
    expected = brute_force_loss(orders, t0, t1)
    with read_conn() as conn:
        for area in AREAS:
            actual = core.list_loss_periods(conn, area, t0, t1)
            assert actual == expected[area], f"{area}: {actual} != {expected[area]}"


# ------------------------------------------------------------ API 端到端

def test_api_flow(client):
    assert client.get("/docs").status_code == 200
    topo = client.get("/topology").json()
    assert {s["switch_id"] for s in topo["switches"]} == {"K1", "K2", "K3", "K4", "K5"}
    k3 = next(s for s in topo["switches"] if s["switch_id"] == "K3")
    assert k3["initial_state"] == "断" and k3["current_state"] == "断"

    # 补录
    r = client.post("/orders", json={
        "order_no": "D1", "switch_id": "K1",
        "change_time": "2026-11-23T10:00:00Z", "state": "断",
    })
    assert r.status_code == 201
    assert r.json()["recomputed_ranges"] == [
        {"start": "2026-11-23T10:00:00.000000Z", "end": None}
    ]
    # 同一开关同一时刻另一张单 → 409
    r = client.post("/orders", json={
        "order_no": "D2", "switch_id": "K1",
        "change_time": "2026-11-23T10:00:00+00:00", "state": "通",
    })
    assert r.status_code == 409
    # 合上联络开关（不带时区按 UTC 处理）
    r = client.post("/orders", json={
        "order_no": "D3", "switch_id": "K3",
        "change_time": "2026-11-23T10:05:00", "state": "通",
    })
    assert r.status_code == 201
    assert r.json()["order"]["change_time"] == "2026-11-23T10:05:00.000000Z"

    # 核查
    r = client.post("/verify", json={
        "area_id": "T1",
        "range_start": "2026-11-23T00:00:00Z",
        "range_end": "2026-11-24T00:00:00Z",
        "reported": [{"start": "2026-11-23T09:30:00Z", "end": "2026-11-23T10:03:00Z"}],
    })
    body = r.json()
    assert body["loss_periods"] == [
        {"start": "2026-11-23T10:00:00.000000Z", "end": "2026-11-23T10:05:00.000000Z"}
    ]
    assert body["should_report"] == [
        {"start": "2026-11-23T10:03:00.000000Z", "end": "2026-11-23T10:05:00.000000Z"}
    ]
    assert body["should_remove"] == [
        {"start": "2026-11-23T09:30:00.000000Z", "end": "2026-11-23T10:00:00.000000Z"}
    ]

    # 更正：K3 合闸时刻 10:05 -> 10:10
    r = client.put("/orders/D3", json={
        "change_time": "2026-11-23T10:10:00Z", "state": "通",
    })
    assert r.status_code == 200
    periods = client.get(
        "/loss-periods",
        params={
            "area_id": "T1",
            "start": "2026-11-23T00:00:00Z",
            "end": "2026-11-24T00:00:00Z",
        },
    ).json()["periods"]
    assert periods == [
        {"start": "2026-11-23T10:00:00.000000Z", "end": "2026-11-23T10:10:00.000000Z"}
    ]

    # 撤销
    assert client.delete("/orders/D3").status_code == 200
    assert client.delete("/orders/D3").status_code == 404
    periods = client.get(
        "/loss-periods",
        params={
            "area_id": "T1",
            "start": "2026-11-23T00:00:00Z",
            "end": "2026-11-24T00:00:00Z",
        },
    ).json()["periods"]
    # 失电仍在持续，查询结果按范围终点裁剪
    assert periods == [
        {"start": "2026-11-23T10:00:00.000000Z", "end": "2026-11-24T00:00:00.000000Z"}
    ]

    # 开关时间轴：初始段 start 为 null
    timeline = client.get("/switches/K1/timeline").json()
    assert timeline["segments"][0] == {"start": None, "state": "通"}
    assert timeline["segments"][1] == {
        "start": "2026-11-23T10:00:00.000000Z", "state": "断",
    }
