"""核查快照与流式输出。

一次 POST /verify 的三个数组（失电、应补报、应剔除）必须取自**同一个已提交
状态**，且在结果规模很大（单台区数十万失电时段）时不能把数据整体搬进内存。

做法：

1. 在主库上开启一个显式只读事务，把该台区核查范围内的失电时段分批复制到
   独立的临时 SQLite 文件（快照库），随后立即结束事务并关闭主库连接——
   本次核查不再持有主库读锁，传输暂停期间主库仍可提交变位更正、WAL 也可
   被截断；
2. 人工报停区间先裁剪到核查范围、按原规则合并重叠或相接区间，写入快照库；
3. 应补报 = 失电 − 报停、应剔除 = 报停 − 失电，全部在排序游标上以流式
   区间算法完成（O(n+m) 时间、O(1) 额外空间），JSON 分块经
   StreamingResponse 输出，进程内存与结果规模无关；
4. 单个核查的临时文件总量受 96 MiB 约束（VERIFY_TMP_LIMIT），写入过程中
   逐批检查，超限抛 StorageLimitError 并清理；传完或客户端断开都会删除
   本次核查的临时目录，应用关闭时再兜底清理一次。

区间语义统一为 [start, end)：起点计入、终点不计。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
from typing import Iterator, Optional

from . import core
from .db import read_conn

# 单次核查允许的临时文件合计上限（字节）：96 MiB
VERIFY_TMP_LIMIT = 96 * 1024 * 1024
# 每批从 SQLite 取/写的行数；稳态内存只与批大小相关
BATCH = 2048
# 单个 JSON 分块的目标字节数
CHUNK_BYTES = 64 * 1024


class StorageLimitError(Exception):
    """本次核查的临时文件超过 96 MiB 上限。"""


# 仍在进行中的核查任务，供应用关闭时兜底清理
_jobs: set["VerifyJob"] = set()
_jobs_lock = threading.Lock()


def shutdown_cleanup() -> None:
    """应用关闭：清理所有未释放的核查临时目录。"""
    with _jobs_lock:
        jobs = list(_jobs)
        _jobs.clear()
    for job in jobs:
        job.cleanup()


# ------------------------------------------------------------ 流式区间运算

def merge_stream(intervals: Iterator[tuple[float, float]]) -> Iterator[tuple[float, float]]:
    """流式合并重叠或相接的区间（区间语义 [start, end)，丢弃零长区间）。

    输入须按 start 排序；与 core.merge_intervals 同规则，但不整体落内存。
    """
    cur_s: Optional[float] = None
    cur_e: Optional[float] = None
    for s, e in intervals:
        if e is not None and not s < e:
            continue  # 丢弃零长区间
        if cur_s is None:
            cur_s, cur_e = s, e
        elif cur_e is not None and s <= cur_e:
            if e is None or e > cur_e:
                cur_e = e
        else:
            yield cur_s, cur_e
            cur_s, cur_e = s, e
    if cur_s is not None:
        yield cur_s, cur_e


def subtract_stream(
    base_it: Iterator[tuple[float, float]],
    minus_it: Iterator[tuple[float, float]],
) -> Iterator[tuple[float, float]]:
    """流式区间集合差 base − minus。

    两侧均为按 start 排序、已合并、互不重叠的区间流；产出自然合并的差集，
    规则与 core.subtract_intervals 一致（[start, end) 语义）。
    """
    m = next(minus_it, None)
    for s, e in base_it:
        cur = s
        # 跳过完全落在当前位置之前的减区间
        while m is not None and m[1] <= cur:
            m = next(minus_it, None)
        while m is not None and m[0] < e:
            ms, me = m
            if me <= cur:
                m = next(minus_it, None)
                continue
            if ms > cur:
                yield cur, ms
            if me > cur:
                cur = me
            if cur >= e:
                break  # m 可能还与后续 base 区间相交，保留到下一轮再判
            m = next(minus_it, None)
        if cur < e:
            yield cur, e


# ------------------------------------------------------------ 核查快照任务

class VerifyJob:
    """一次核查的快照生命周期：建快照 → 流式输出 → 释放临时文件。"""

    def __init__(self, job_dir: str, db_file: str, area_id: str, rs: float, re_: float):
        self._dir = job_dir
        self._db_file = db_file
        self._conn: Optional[sqlite3.Connection] = None
        self._closed = False
        self.area_id = area_id
        self.rs = rs
        self.re_ = re_

    # ---------- 创建快照

    @classmethod
    def create(
        cls,
        area_id: str,
        rs: float,
        re_: float,
        reported: list[tuple[float, float]],
    ) -> "VerifyJob":
        """在单个只读事务内完成快照，返回可多次流式读取的核查任务。

        失电时段来自主库同一已提交快照；人工报停裁剪到 [rs, re_) 后按原规则
        合并。校验失败抛 core.ValidationError，临时文件超 96 MiB 抛
        StorageLimitError，两种情况都会清理已产生的临时文件。
        """
        if not rs < re_:
            raise core.ValidationError("时间范围起点必须早于终点")
        tmp_root = os.environ.get("VERIFY_TMPDIR")
        if tmp_root:
            os.makedirs(tmp_root, exist_ok=True)
        job_dir = tempfile.mkdtemp(prefix="verify-", dir=tmp_root)
        db_file = os.path.join(job_dir, "snapshot.db")
        job = cls(job_dir, db_file, area_id, rs, re_)
        try:
            # StreamingResponse 的分块生成器经 anyio 线程池逐块执行，暂停后续
            # 读可能落在另一个工作线程上；快照库为本次核查独占、整个生命周期
            # 由 VerifyJob 串行管理（同一时刻只有一个读取者），关闭跨线程检查。
            snap = sqlite3.connect(db_file, check_same_thread=False)
            job._conn = snap
            # 一次性快照库无需持久化回滚日志，落盘的只有主库文件
            snap.execute("PRAGMA journal_mode=MEMORY")
            snap.execute("PRAGMA synchronous=OFF")
            snap.execute("PRAGMA temp_store=MEMORY")
            # WITHOUT ROWID：两个 REAL 即完整记录，35 万行约 10 MiB 量级
            snap.execute(
                "CREATE TABLE loss ("
                "start_epoch REAL NOT NULL, end_epoch REAL NOT NULL, "
                "PRIMARY KEY (start_epoch, end_epoch)) WITHOUT ROWID"
            )
            snap.execute("CREATE TABLE rep_merged (start_epoch REAL NOT NULL, end_epoch REAL NOT NULL)")

            # 显式开启只读事务：下面所有主库读取共用同一个已提交快照
            with read_conn() as conn:
                conn.execute("BEGIN")
                try:
                    if conn.execute(
                        "SELECT 1 FROM areas WHERE area_id = ?", (area_id,)
                    ).fetchone() is None:
                        raise core.ValidationError(f"台区 {area_id} 不存在")
                    cur = conn.execute(
                        "SELECT start_epoch, end_epoch FROM loss_periods "
                        "WHERE area_id = ? AND start_epoch < ? "
                        "AND (end_epoch IS NULL OR end_epoch > ?) ORDER BY start_epoch",
                        (area_id, re_, rs),
                    )
                    ins = snap.cursor()
                    while True:
                        batch = cur.fetchmany(BATCH)
                        if not batch:
                            break
                        # end_epoch 为 NULL 表示持续至今：裁剪到核查范围后
                        # 终点即 re_（快照表不再保留 NULL）
                        clipped = [
                            (max(s, rs), re_ if e is None else min(e, re_))
                            for s, e in batch
                        ]
                        ins.executemany(
                            "INSERT INTO loss (start_epoch, end_epoch) VALUES (?, ?)", clipped
                        )
                        snap.commit()
                        job._check_budget()
                    cur.close()
                finally:
                    conn.rollback()  # 结束只读事务，立即释放主库 WAL 读锁

            # 人工报停：裁到核查范围 → 合并重叠或相接区间，落快照库
            clipped_rep = core.clip_intervals(list(reported), rs, re_)
            rows = list(merge_stream(iter(sorted(clipped_rep, key=lambda iv: iv[0]))))
            snap.executemany(
                "INSERT INTO rep_merged (start_epoch, end_epoch) VALUES (?, ?)", rows
            )
            snap.commit()
            job._check_budget()
        except BaseException:
            job.cleanup()
            raise
        with _jobs_lock:
            _jobs.add(job)
        return job

    def _check_budget(self) -> None:
        """本次核查的临时文件合计不得超过 96 MiB。"""
        total = 0
        for name in os.listdir(self._dir):
            total += os.path.getsize(os.path.join(self._dir, name))
        if total > VERIFY_TMP_LIMIT:
            raise StorageLimitError(
                f"核查临时文件合计 {total / 1024 / 1024:.1f} MiB 超过 "
                f"{VERIFY_TMP_LIMIT / 1024 / 1024:.0f} MiB 上限"
            )

    # ---------- 流式读取

    def _rows(self, table: str) -> Iterator[tuple[float, float]]:
        """快照表上的分批排序游标，稳态内存只与 BATCH 相关。"""
        cur = self._conn.cursor()
        cur.execute(
            f"SELECT start_epoch, end_epoch FROM {table} ORDER BY start_epoch"
        )
        try:
            while True:
                batch = cur.fetchmany(BATCH)
                if not batch:
                    break
                for row in batch:
                    yield row
        finally:
            cur.close()

    @staticmethod
    def _interval_chunk(intervals: Iterator[tuple[float, float]]) -> Iterator[str]:
        """把区间流编码为 JSON 数组元素分块（不含外层方括号）。"""
        buf: list[str] = []
        size = 0
        first = True
        for s, e in intervals:
            if not first:
                buf.append(",")
                size += 1
            first = False
            payload = json.dumps(
                {"start": core.to_iso(s), "end": core.to_iso(e)},
                separators=(",", ":"),
                ensure_ascii=False,
            )
            buf.append(payload)
            size += len(payload)
            if size >= CHUNK_BYTES:
                yield "".join(buf)
                buf, size = [], 0
        if buf:
            yield "".join(buf)

    def stream_payload(self) -> Iterator[str]:
        """产出与原 /verify 完全同构的 JSON 正文分块。"""
        yield (
            "{"
            f'"area_id": {json.dumps(self.area_id, ensure_ascii=False)}, '
            f'"range": {{"start": {json.dumps(core.to_iso(self.rs))}, '
            f'"end": {json.dumps(core.to_iso(self.re_))}}}, '
            '"loss_periods": ['
        )
        yield from self._interval_chunk(self._rows("loss"))
        yield '], "should_report": ['
        yield from self._interval_chunk(
            subtract_stream(self._rows("loss"), self._rows("rep_merged"))
        )
        yield '], "should_remove": ['
        yield from self._interval_chunk(
            subtract_stream(self._rows("rep_merged"), self._rows("loss"))
        )
        yield "]}"

    def stream_and_cleanup(self) -> Iterator[str]:
        """StreamingResponse 入口：传完或断开（GeneratorExit）都释放资源。"""
        try:
            yield from self.stream_payload()
        finally:
            self.cleanup()

    async def astream_and_cleanup(self, is_disconnected) -> Iterator[str]:
        """异步 StreamingResponse 入口。

        同步 StreamingResponse 的分块生成器跑在 anyio 工作线程里，客户端断开
        后线程无法被取消、生成器挂起，其 finally 不会执行，临时文件无法释放。
        这里改为异步生成器：每产出一块就检查连接，断开（is_disconnected()）
        立即显式 cleanup 并停止；正常传完由 finally 兜底释放。
        """
        try:
            for chunk in self.stream_payload():
                if await is_disconnected():
                    return
                yield chunk
        finally:
            self.cleanup()

    # ---------- 资源释放

    def cleanup(self) -> None:
        """关闭快照连接并删除本次核查的全部临时文件，可重复调用。"""
        if self._closed:
            return
        self._closed = True
        with _jobs_lock:
            _jobs.discard(self)
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            self._conn = None
        try:
            for name in os.listdir(self._dir):
                os.unlink(os.path.join(self._dir, name))
            os.rmdir(self._dir)
        except FileNotFoundError:
            pass
