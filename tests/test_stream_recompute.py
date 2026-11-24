"""流式增量重算的规模化与正确性测试。

验证：数千变位（跨 REWRITE_BATCH 批边界）下重算结果与暴力全量推算一致、
范围外相邻半段正确接合、持续段（NULL 终点）处理，以及写单回执的截断保护。
"""
from __future__ import annotations

import random

from app import core
from app.db import read_conn, write_txn

INITIAL = {"K1": "通", "K2": "通", "K3": "断", "K4": "通", "K5": "通"}
LINKS = {
    "K1": ("P1", "T1"), "K2": ("T1", "T2"), "K3": ("T2", "T3"),
    "K4": ("T3", "T4"), "K5": ("T4", "P2"),
}
SOURCES = ["P1", "P2"]
AREAS = ["T1", "T2", "T3", "T4"]


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


def test_large_recompute_matches_bruteforce(db):
    rng = random.Random(99)
    base = 1_700_000_000.0
    orders = {}
    seq = 0
    # 约 3000 张单，失电段数远超 REWRITE_BATCH(2048)，覆盖跨批 flush 与相接延长
    for _ in range(3000):
        sw = rng.choice(list(INITIAL))
        t = base + rng.randrange(0, 4000) * 10.0
        st = rng.choice(["通", "断"])
        no = f"O{seq:05d}"
        seq += 1
        try:
            with write_txn() as conn:
                core.create_order(conn, no, sw, t, st)
            orders[no] = (sw, t, st)
        except core.ConflictError:
            continue
    t0, t1 = base - 100.0, base + 4000 * 10.0 + 100.0
    expected = brute_force_loss(orders, t0, t1)
    with read_conn() as conn:
        for area in AREAS:
            actual = core.list_loss_periods(conn, area, t0, t1)
            assert actual == expected[area], f"{area} mismatch"


def test_mid_range_recompute_keeps_outside_segments(db):
    base = 1_700_000_000.0
    # 造 K1 反复断/通，产生多段失电
    with write_txn() as conn:
        for i in range(10):
            core.create_order(conn, f"OFF{i}", "K1", base + i * 100, "断")
            core.create_order(conn, f"ON{i}", "K1", base + i * 100 + 40, "通")
    with read_conn() as conn:
        before = core.list_loss_periods(conn, "T1", base - 10, base + 2000)
    assert len(before) == 10
    # 更正中间一张单（i=5 的断单后移 20s），重算子范围；范围外段必须保留并接合
    with write_txn() as conn:
        core.update_order(conn, "OFF5", base + 500 + 20, "断")
    with read_conn() as conn:
        after = core.list_loss_periods(conn, "T1", base - 10, base + 2000)
    # 段仍按 start 排序、互不重叠且相接处正确；首尾段不受影响
    assert after[0] == before[0]
    assert after[-1] == before[-1]
    for (s1, e1), (s2, e2) in zip(after, after[1:]):
        assert e1 <= s2  # 有序不重叠
    # 被移动的第 5 段起点应后移 20s
    moved = [iv for iv in after if iv[0] == base + 520]
    assert moved and moved[0][1] == base + 540


def test_open_ended_loss_period_roundtrip(db):
    base = 1_700_000_000.0
    with write_txn() as conn:
        core.create_order(conn, "D1", "K1", base, "断")  # T1/T2 持续失电
    with read_conn() as conn:
        rows = [tuple(r) for r in conn.execute(
            "SELECT start_epoch, end_epoch FROM loss_periods WHERE area_id='T1'"
        ).fetchall()]
    assert rows == [(base, None)]
    # 合 K3 转供，封口为有限段（流式重算须正确处理持续段的右保留/封口）
    with write_txn() as conn:
        core.create_order(conn, "D2", "K3", base + 60, "通")
    with read_conn() as conn:
        rows = [tuple(r) for r in conn.execute(
            "SELECT start_epoch, end_epoch FROM loss_periods WHERE area_id='T1'"
        ).fetchall()]
    assert rows == [(base, base + 60)]


def test_receipt_truncation(db):
    base = 1_700_000_000.0
    n = 600  # T1、T2 各 600 段，合计 1200 行 > RECEIPT_LOSS_LIMIT(1000)
    with write_txn() as conn:
        # 批量落变位单与时间轴，再做一次全量流式重算（覆盖跨批 flush）
        order_rows, seg_rows = [], []
        for i in range(n):
            t = base + i * 100
            order_rows += [(f"OFF{i}", "K1", t, "断"), (f"ON{i}", "K1", t + 40, "通")]
            seg_rows += [("K1", t, "断"), ("K1", t + 40, "通")]
        conn.executemany(
            "INSERT INTO orders (order_no, switch_id, change_epoch, state) "
            "VALUES (?,?,?,?)", order_rows)
        conn.executemany(
            "INSERT INTO switch_segments (switch_id, start_epoch, state) "
            "VALUES (?,?,?)", seg_rows)
        spans = core.recompute_spans(conn, [(base, None)])
        # 回执查询的跨度覆盖全部历史段
        detail, total, truncated = core.loss_in_spans(conn, [(base - 50, None)])
    assert spans == [(base, None)]
    assert truncated is True
    assert total > core.RECEIPT_LOSS_LIMIT
    assert len(detail) == core.RECEIPT_LOSS_LIMIT
