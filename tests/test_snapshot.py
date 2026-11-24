"""核查快照与流式输出测试。

覆盖：流式区间算法与原内存版等价、同一已提交快照隔离、NULL 终点、人工报停
裁剪合并、临时文件预算（96 MiB 上限）、传完与断开资源释放，以及大规模核查的
完整正确性。
"""
from __future__ import annotations

import json
import random

import pytest

from app import core, snapshot
from app.db import read_conn, write_txn

DAY = "2026-11-23"


def E(iso: str) -> float:
    from datetime import datetime

    return core.to_epoch(datetime.fromisoformat(iso.replace("Z", "+00:00")))


# ------------------------------------------------------------ 流式区间算法

def test_merge_stream_matches_core():
    rng = random.Random(1)
    cases = [
        [],
        [(1.0, 2.0)],
        [(1.0, 5.0), (5.0, 9.0)],          # 相接
        [(1.0, 5.0), (3.0, 7.0)],          # 重叠
        [(1.0, 1.0)],                       # 零长丢弃
        [(1.0, 4.0), (4.0, 8.0), (8.0, 10.0), (20.0, 21.0)],
    ]
    for _ in range(50):
        ivs = []
        cur = 0.0
        for _ in range(rng.randrange(1, 12)):
            s = cur + rng.randrange(0, 3)
            e = s + rng.randrange(0, 4)
            ivs.append((float(s), float(e)))
            cur = s
        cases.append(ivs)
    for ivs in cases:
        ordered = sorted(ivs, key=lambda iv: iv[0])
        assert list(snapshot.merge_stream(iter(ordered))) == core.merge_intervals(ordered)


def test_subtract_stream_matches_core():
    rng = random.Random(2)
    base = merge = None
    for _ in range(80):
        def gen():
            ivs, cur = [], 0.0
            for _ in range(rng.randrange(1, 8)):
                s = cur + rng.randrange(0, 3)
                e = s + rng.randrange(1, 4)
                ivs.append((float(s), float(e)))
                cur = s + rng.randrange(0, 2)
            return core.merge_intervals(ivs)
        a, b = gen(), gen()
        assert list(snapshot.subtract_stream(iter(a), iter(b))) == \
            core.subtract_intervals(a, b)


# ------------------------------------------------------------ 快照任务

def _make_job(tmp_path, monkeypatch, area="T1", rs=0.0, re_=1000.0, reported=None):
    monkeypatch.setenv("VERIFY_TMPDIR", str(tmp_path))
    return snapshot.VerifyJob.create(area, rs, re_, reported or [])


def _drain(job):
    return "".join(job.stream_and_cleanup())


def test_job_validates_range_and_area(db, tmp_path, monkeypatch):
    with pytest.raises(core.ValidationError):
        _make_job(tmp_path, monkeypatch, rs=10.0, re_=10.0)
    with pytest.raises(core.ValidationError):
        _make_job(tmp_path, monkeypatch, area="T9")


def test_snapshot_isolates_later_changes(db, tmp_path, monkeypatch):
    rs, re_ = E(f"{DAY}T00:00:00Z"), E(f"{DAY}T23:59:59Z")
    # 造两条失电段
    with write_txn() as conn:
        conn.executemany(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
            [("T1", rs + 10, rs + 20), ("T1", rs + 30, rs + 40)],
        )
    job = _make_job(tmp_path, monkeypatch, rs=rs, re_=re_)
    # 快照建立后，主库再改：删除全部失电段
    with write_txn() as conn:
        conn.execute("DELETE FROM loss_periods WHERE area_id='T1'")
    payload = json.loads(_drain(job))
    # 本次核查仍是更正前内容
    assert len(payload["loss_periods"]) == 2
    # 下次核查反映更正（已无失电段）
    job2 = _make_job(tmp_path, monkeypatch, rs=rs, re_=re_)
    payload2 = json.loads(_drain(job2))
    assert payload2["loss_periods"] == []


def test_null_end_period_is_clipped(db, tmp_path, monkeypatch):
    rs, re_ = E(f"{DAY}T00:00:00Z"), E(f"{DAY}T12:00:00Z")
    with write_txn() as conn:
        conn.execute(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,NULL)",
            ("T1", rs + 100),
        )
    job = _make_job(tmp_path, monkeypatch, rs=rs, re_=re_)
    payload = json.loads(_drain(job))
    # 持续至今段被裁剪到范围终点，快照里不保留 NULL
    assert payload["loss_periods"] == [
        {"start": core.to_iso(rs + 100), "end": core.to_iso(re_)}
    ]


def test_reported_clipped_and_merged(db, tmp_path, monkeypatch):
    rs, re_ = 0.0, 100.0
    with write_txn() as conn:
        conn.execute(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
            ("T1", 40.0, 60.0),
        )
    # 报停横跨范围且含相接两段：[-10,30) 与 [30,70)，裁剪+合并后 [0,70)
    job = _make_job(tmp_path, monkeypatch, rs=rs, re_=re_,
                    reported=[(-10.0, 30.0), (30.0, 70.0)])
    payload = json.loads(_drain(job))
    # 应剔除 = 报停 - 失电 = [0,40) 与 [60,70)
    assert payload["should_remove"] == [
        {"start": core.to_iso(0.0), "end": core.to_iso(40.0)},
        {"start": core.to_iso(60.0), "end": core.to_iso(70.0)},
    ]
    # 失电 [40,60) 被报停完全覆盖 → 无应补报
    assert payload["should_report"] == []


def test_matches_core_verify_on_random_data(db, tmp_path, monkeypatch):
    rng = random.Random(7)
    rs, re_ = 0.0, 5000.0
    loss, reported = [], []
    with write_txn() as conn:
        cur = 10.0
        for _ in range(60):
            s = cur + rng.randrange(0, 20)
            e = s + rng.randrange(1, 15)
            loss.append((s, e))
            conn.execute(
                "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
                ("T1", s, e),
            )
            cur = e
        for _ in range(40):
            s = rng.uniform(-20, 4900)
            reported.append((s, s + rng.uniform(1, 30)))
    monkeypatch.setenv("VERIFY_TMPDIR", str(tmp_path))
    job = snapshot.VerifyJob.create("T1", rs, re_, reported)
    payload = json.loads(_drain(job))
    with read_conn() as conn:
        exp_loss, exp_add, exp_rem = core.verify(conn, "T1", rs, re_, reported)
    assert payload["loss_periods"] == [core.interval_json(*iv) for iv in exp_loss]
    assert payload["should_report"] == [core.interval_json(*iv) for iv in exp_add]
    assert payload["should_remove"] == [core.interval_json(*iv) for iv in exp_rem]


# ------------------------------------------------------------ 资源释放与预算

def test_cleanup_after_full_stream(db, tmp_path, monkeypatch):
    job = _make_job(tmp_path, monkeypatch)
    assert tmp_path.exists() and any(tmp_path.iterdir())
    _drain(job)
    assert job._closed
    assert not any(p.name.startswith("verify-") for p in tmp_path.iterdir())


def test_cleanup_on_early_close(db, tmp_path, monkeypatch):
    job = _make_job(tmp_path, monkeypatch)
    gen = job.stream_and_cleanup()
    next(gen)  # 只取首块即模拟客户端断开
    gen.close()
    assert job._closed
    assert not any(p.name.startswith("verify-") for p in tmp_path.iterdir())


def test_async_stream_cleanup_on_disconnect(db, tmp_path, monkeypatch):
    import asyncio

    job = _make_job(tmp_path, monkeypatch)

    async def disconnected():
        return True  # 首块前即判定断开

    async def run():
        chunks = []
        async for chunk in job.astream_and_cleanup(disconnected):
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())
    assert chunks == []  # 断开后不产出正文
    assert job._closed


def test_storage_limit_enforced(db, tmp_path, monkeypatch):
    # 灌足够多数据，把临时上限压到极小，触发 507 对应异常并清理
    with write_txn() as conn:
        conn.executemany(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
            [("T1", float(i), float(i) + 1) for i in range(5000)],
        )
    monkeypatch.setenv("VERIFY_TMPDIR", str(tmp_path))
    old = snapshot.VERIFY_TMP_LIMIT
    snapshot.VERIFY_TMP_LIMIT = 4096  # 4 KiB，必然超限
    try:
        with pytest.raises(snapshot.StorageLimitError):
            snapshot.VerifyJob.create("T1", 0.0, 100000.0, [])
    finally:
        snapshot.VERIFY_TMP_LIMIT = old
    # 超限时已产生的临时目录被清理
    assert not any(p.name.startswith("verify-") for p in tmp_path.iterdir())


def test_temp_files_within_budget_large_scale(db, tmp_path, monkeypatch):
    n = 50_000
    with write_txn() as conn:
        conn.executemany(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
            [("T1", float(i * 10), float(i * 10) + 5) for i in range(n)],
        )
    monkeypatch.setenv("VERIFY_TMPDIR", str(tmp_path))
    job = snapshot.VerifyJob.create("T1", 0.0, float(n * 10), [])
    total = sum(
        f.stat().st_size for p in tmp_path.iterdir() if p.is_dir() for f in p.iterdir()
    )
    assert total <= snapshot.VERIFY_TMP_LIMIT == 96 * 1024 * 1024
    payload = json.loads(_drain(job))
    assert len(payload["loss_periods"]) == n
    assert len(payload["should_report"]) == n
    assert payload["should_remove"] == []


# ------------------------------------------------------------ API 端到端

def test_verify_endpoint_streams_json(client):
    r = client.post("/verify", json={
        "area_id": "T1",
        "range_start": f"{DAY}T00:00:00Z",
        "range_end": "2026-11-24T00:00:00Z",
        "reported": [],
    })
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    assert set(body) == {"area_id", "range", "loss_periods", "should_report", "should_remove"}


def test_verify_endpoint_validation(client):
    r = client.post("/verify", json={
        "area_id": "T9",
        "range_start": f"{DAY}T00:00:00Z",
        "range_end": "2026-11-24T00:00:00Z",
        "reported": [],
    })
    assert r.status_code == 400
    r = client.post("/verify", json={
        "area_id": "T1",
        "range_start": f"{DAY}T00:00:00Z",
        "range_end": f"{DAY}T00:00:00Z",
        "reported": [],
    })
    assert r.status_code == 400


def test_verify_endpoint_returns_507_on_budget(client, monkeypatch, tmp_path):
    # 数据量超过被压低的临时预算 → API 返回 507
    base = E(f"{DAY}T00:00:00Z")
    with write_txn() as conn:
        conn.executemany(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) VALUES (?,?,?)",
            [("T1", base + i, base + i + 1) for i in range(3000)],
        )
    monkeypatch.setenv("VERIFY_TMPDIR", str(tmp_path / "verify-tmp"))
    monkeypatch.setattr(snapshot, "VERIFY_TMP_LIMIT", 4096)
    r = client.post("/verify", json={
        "area_id": "T1",
        "range_start": f"{DAY}T00:00:00Z",
        "range_end": "2026-11-24T00:00:00Z",
        "reported": [],
    })
    assert r.status_code == 507
    assert "MiB" in r.json()["detail"]
    # 失败后无遗留临时目录（VERIFY_TMPDIR 自动创建后也应被清空）
    assert not list((tmp_path / "verify-tmp").glob("verify-*"))
