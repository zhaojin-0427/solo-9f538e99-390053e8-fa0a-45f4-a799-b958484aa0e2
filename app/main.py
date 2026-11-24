"""停电单核查 API（FastAPI + SQLite）。

打开 /docs 即可按单号补录、更正或撤销开关变位，并对人工报停区间做核查。
"""
from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal, Optional

import anyio
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import core, snapshot
from .db import init_db, read_conn, write_txn
from .topology import seed_topology

# 进程地址空间受限（容器 224 MiB 量级）时的两道保险：
# 1) 主修复在运行环境：glibc ptmalloc 默认给每个线程预留 64 MiB 虚拟地址
#    空间作为独立 malloc arena（HEAP_MAX_SIZE，零 RSS 但计入 RLIMIT_AS），
#    多个暂停中的流式连接会把地址空间顶满。容器通过 MALLOC_ARENA_MAX=1
#    收敛为单一 arena（见 Dockerfile / docker-compose.yml）。
# 2) 这里再收窄 anyio 工作线程数与每线程栈：anyio 默认 40 个工作线程、
#    每线程 8 MiB 栈预留同样可观。核查为流式同步接口，每个暂停中的连接占用
#    一个工作线程，池大小与线程栈可由环境变量覆盖。
_MAX_THREADS = int(os.environ.get("MAX_WORKER_THREADS", "8"))
try:
    threading.stack_size(int(os.environ.get("THREAD_STACK_BYTES", str(2 * 1024 * 1024))))
except (ValueError, OverflowError):
    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 须在事件循环内获取默认线程限流器
    anyio.to_thread.current_default_thread_limiter().total_tokens = _MAX_THREADS
    init_db()
    with write_txn() as conn:
        seed_topology(conn)  # 加载固定的电源、台区与双向开关连线
    try:
        yield
    finally:
        # 兜底：服务关闭时释放所有仍在传输中的核查临时文件
        snapshot.shutdown_cleanup()


app = FastAPI(
    title="停电单核查 API",
    description=(
        "加载固定的电源、台区与双向开关连线（注明各开关初始通断）。\n\n"
        "- 按单号补录 / 更正 / 撤销开关变位：每张单固定开关，填写 UTC 时刻和通断；\n"
        "- 同一开关同一时刻已有另一张单时拒绝提交；同刻变位一起核算；\n"
        "- 台区连不到任何电源才算失电（联络开关转供后不算失电）；\n"
        "- 核查时填台区、时间范围和人工报停区间（起点计入、终点不计），"
        "直接列出应补报与应剔除的区间。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------ 异常映射

@app.exception_handler(core.ConflictError)
async def conflict_handler(request: Request, exc: core.ConflictError):
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(core.NotFoundError)
async def not_found_handler(request: Request, exc: core.NotFoundError):
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(core.ValidationError)
async def validation_handler(request: Request, exc: core.ValidationError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(snapshot.StorageLimitError)
async def storage_limit_handler(request: Request, exc: snapshot.StorageLimitError):
    return JSONResponse(status_code=507, content={"detail": str(exc)})


# ------------------------------------------------------------ 请求模型

State = Literal["通", "断"]


class OrderCreate(BaseModel):
    """补录变位单：每张单固定开关，填写 UTC 时刻和通断。"""

    order_no: str = Field(..., description="变位单号（全局唯一）", examples=["PB20261123001"])
    switch_id: str = Field(..., description="开关号（每张单固定开关）", examples=["K1"])
    change_time: datetime = Field(
        ..., description="变位生效时刻（UTC，ISO 8601）", examples=["2026-11-23T10:00:00Z"]
    )
    state: State = Field(..., description="变位后通断：通=闭合，断=分开")


class OrderUpdate(BaseModel):
    """更正变位单：开关固定，只能改 UTC 时刻与通断。"""

    change_time: datetime = Field(
        ..., description="更正后的变位生效时刻（UTC）", examples=["2026-11-23T10:30:00Z"]
    )
    state: State = Field(..., description="更正后的通断")


class ReportedInterval(BaseModel):
    """人工报停区间，[start, end)：起点计入、终点不计。"""

    start: datetime = Field(..., examples=["2026-11-23T09:30:00Z"])
    end: datetime = Field(..., examples=["2026-11-23T10:10:00Z"])


class VerifyRequest(BaseModel):
    area_id: str = Field(..., description="台区号", examples=["T1"])
    range_start: datetime = Field(
        ..., description="核查范围起点（计入）", examples=["2026-11-23T00:00:00Z"]
    )
    range_end: datetime = Field(
        ..., description="核查范围终点（不计）", examples=["2026-11-24T00:00:00Z"]
    )
    reported: list[ReportedInterval] = Field(
        default_factory=list, description="人工报停区间列表（起点计入、终点不计）"
    )


# ------------------------------------------------------------ 基础

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/topology", tags=["拓扑"], summary="查看固定的电源、台区与双向开关连线")
def get_topology():
    """返回固定拓扑：电源、台区、双向开关连线及各开关初始通断与当前状态。"""
    now = time.time()
    with read_conn() as conn:
        sources = [
            {"source_id": r[0], "name": r[1]}
            for r in conn.execute("SELECT source_id, name FROM sources ORDER BY source_id")
        ]
        areas = [
            {"area_id": r[0], "name": r[1]}
            for r in conn.execute("SELECT area_id, name FROM areas ORDER BY area_id")
        ]
        switches = [
            {
                "switch_id": r[0],
                "name": r[1],
                "node_a": r[2],
                "node_b": r[3],
                "initial_state": r[4],
                "current_state": core.state_at(conn, r[0], now),
            }
            for r in conn.execute(
                "SELECT switch_id, name, node_a, node_b, initial_state "
                "FROM switches ORDER BY switch_id"
            )
        ]
    return {"sources": sources, "areas": areas, "switches": switches}


@app.get(
    "/switches/{switch_id}/timeline",
    tags=["拓扑"],
    summary="查看开关状态时间轴（物化段）",
)
def get_switch_timeline(switch_id: str):
    """开关状态时间轴：定位任意时刻状态只需索引点查，无需扫描变位历史。"""
    with read_conn() as conn:
        rows = conn.execute(
            "SELECT start_epoch, state FROM switch_segments "
            "WHERE switch_id = ? ORDER BY start_epoch",
            (switch_id,),
        ).fetchall()
    if not rows:
        raise core.NotFoundError(f"开关 {switch_id} 不存在")
    return {
        "switch_id": switch_id,
        "segments": [
            {
                "start": None if r[0] == core.NEG_INF else core.to_iso(r[0]),
                "state": r[1],
            }
            for r in rows
        ],
        "note": "start 为 null 的段表示初始状态段",
    }


# ------------------------------------------------------------ 变位单

def _order_json(row) -> dict:
    return {
        "order_no": row[0],
        "switch_id": row[1],
        "change_time": core.to_iso(row[2]),
        "state": row[3],
    }


def _op_result(spans, loss_info) -> dict:
    loss, total, truncated = loss_info
    return {
        "recomputed_ranges": [core.interval_json(a, b) for a, b in spans],
        "loss_periods": [
            {"area_id": area, **core.interval_json(s, e)} for area, s, e in loss
        ],
        # 回执明细有上限（core.RECEIPT_LOSS_LIMIT），总数与截断标记供客户端判断；
        # 完整失电段请走 /loss-periods 或 /verify
        "loss_period_count": total,
        "loss_periods_truncated": truncated,
    }


@app.post(
    "/orders",
    status_code=201,
    tags=["变位单"],
    summary="补录开关变位单",
)
def create_order(body: OrderCreate):
    """补录一张变位单。同一开关同一时刻已有另一张单时拒绝提交（409）。

    变位单与重算后的失电时段在同一事务中生效；失败回滚，核查仍用原结果。
    """
    t = core.to_epoch(body.change_time)
    with write_txn() as conn:
        spans = core.create_order(conn, body.order_no, body.switch_id, t, body.state)
        loss = core.loss_in_spans(conn, spans)
    return {
        "order": {
            "order_no": body.order_no,
            "switch_id": body.switch_id,
            "change_time": core.to_iso(t),
            "state": body.state,
        },
        **_op_result(spans, loss),
    }


@app.get("/orders", tags=["变位单"], summary="变位单列表")
def list_orders(switch_id: Optional[str] = Query(None, description="按开关过滤")):
    with read_conn() as conn:
        if switch_id is None:
            rows = conn.execute(
                "SELECT order_no, switch_id, change_epoch, state FROM orders "
                "ORDER BY switch_id, change_epoch"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT order_no, switch_id, change_epoch, state FROM orders "
                "WHERE switch_id = ? ORDER BY change_epoch",
                (switch_id,),
            ).fetchall()
    return {"orders": [_order_json(r) for r in rows]}


@app.get("/orders/{order_no}", tags=["变位单"], summary="按单号查询变位单")
def get_order(order_no: str):
    with read_conn() as conn:
        row = conn.execute(
            "SELECT order_no, switch_id, change_epoch, state FROM orders "
            "WHERE order_no = ?",
            (order_no,),
        ).fetchone()
    if row is None:
        raise core.NotFoundError(f"变位单 {order_no} 不存在")
    return _order_json(row)


@app.put("/orders/{order_no}", tags=["变位单"], summary="更正开关变位单")
def update_order(order_no: str, body: OrderUpdate):
    """按单号更正变位单的 UTC 时刻与通断（开关固定不可改）。

    只重算新旧开关状态不同的时段。
    """
    t = core.to_epoch(body.change_time)
    with write_txn() as conn:
        spans = core.update_order(conn, order_no, t, body.state)
        loss = core.loss_in_spans(conn, spans)
    return {"order_no": order_no, **_op_result(spans, loss)}


@app.delete("/orders/{order_no}", tags=["变位单"], summary="撤销开关变位单")
def delete_order(order_no: str):
    """按单号撤销变位单，只重算新旧开关状态不同的时段。"""
    with write_txn() as conn:
        spans = core.delete_order(conn, order_no)
        loss = core.loss_in_spans(conn, spans)
    return {"deleted": order_no, **_op_result(spans, loss)}


# ------------------------------------------------------------ 失电时段与核查

@app.get("/loss-periods", tags=["失电时段"], summary="查询已算好的台区失电时段")
def get_loss_periods(
    area_id: str = Query(..., description="台区号", examples=["T1"]),
    start: datetime = Query(..., description="范围起点（计入）"),
    end: datetime = Query(..., description="范围终点（不计）"),
):
    rs, re_ = core.to_epoch(start), core.to_epoch(end)
    if not rs < re_:
        raise core.ValidationError("时间范围起点必须早于终点")
    with read_conn() as conn:
        if conn.execute(
            "SELECT 1 FROM areas WHERE area_id = ?", (area_id,)
        ).fetchone() is None:
            raise core.ValidationError(f"台区 {area_id} 不存在")
        periods = core.list_loss_periods(conn, area_id, rs, re_)
    return {
        "area_id": area_id,
        "periods": [core.interval_json(s, e) for s, e in periods],
    }


@app.post("/verify", tags=["核查"], summary="核查人工报停区间（流式返回）")
async def verify_endpoint(request: Request, body: VerifyRequest):
    """核查：直接用已算好的失电时段，列出应补报和应剔除的区间。

    - 失电判定：台区连不到任何电源才算失电；
    - 区间语义：起点计入、终点不计；
    - 应补报 = 失电时段 − 人工报停区间；应剔除 = 人工报停 − 失电时段；
    - 人工报停先裁剪到核查范围，再按原规则合并重叠或相接的区间；
    - 三个数组取自核查开始时同一个已提交状态：先把该状态快照到独立临时
      SQLite 文件再流式输出，传输期间变位更正不影响本次结果，下次核查生效；
    - 结果规模再大也不整体入内存（分批游标 + 分块 JSON）；本次核查的临时
      文件合计不超过 96 MiB，传完或客户端断开都会释放。
    """
    rs, re_ = core.to_epoch(body.range_start), core.to_epoch(body.range_end)
    reported = []
    for iv in body.reported:
        s, e = core.to_epoch(iv.start), core.to_epoch(iv.end)
        if not s < e:
            raise core.ValidationError("人工报停区间起点必须早于终点")
        reported.append((s, e))
    # 快照创建含同步 SQLite 读写，放到工作线程，避免阻塞事件循环
    job = await anyio.to_thread.run_sync(
        lambda: snapshot.VerifyJob.create(body.area_id, rs, re_, reported)
    )
    return StreamingResponse(
        job.astream_and_cleanup(request.is_disconnected),
        media_type="application/json",
        headers={"Cache-Control": "no-store"},
    )
