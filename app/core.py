"""停电核查核心逻辑。

设计要点：
- 变位单（orders）与开关状态时间轴（switch_segments）、台区失电时段
  （loss_periods）在同一事务中生效；任何一步失败整体回滚，核查仍用原结果。
- 补录 / 更正 / 撤销只重算新旧开关状态不同的时段 [t, 下一变位时刻)；
  段首状态通过对 switch_segments 的索引点查定位，无需扫描此前的变位历史。
- 同一时刻多个开关的变位落在同一 epoch，核算时一起生效。
- 台区只有连不到任何电源才算失电（联络开关转供后不算失电）。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

NEG_INF = -1.0e300  # 初始状态段起点（负无穷哨兵，不参与业务时间比较）

STATE_CLOSED = "通"
STATE_OPEN = "断"

# 大规模重算/写回时的批大小：稳态内存只与批大小相关，与失电段总数无关
REWRITE_BATCH = 2048


class ConflictError(Exception):
    """同一开关同一时刻已存在另一张变位单，或单号冲突。"""


class NotFoundError(Exception):
    """单号不存在。"""


class ValidationError(Exception):
    """输入不合法（开关 / 台区不存在、时间范围错误等）。"""


# ---------------------------------------------------------------- 时间

def to_epoch(dt: datetime) -> float:
    """datetime -> UTC epoch 秒；不带时区时按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


def to_iso(epoch: Optional[float]) -> Optional[str]:
    """UTC epoch 秒 -> ISO 8601 字符串；None（持续至今）原样返回。"""
    if epoch is None:
        return None
    return (
        datetime.fromtimestamp(epoch, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def interval_json(start: float, end: Optional[float]) -> dict:
    return {"start": to_iso(start), "end": to_iso(end)}


# ------------------------------------------------------------ 区间集合运算

def merge_intervals(intervals):
    """合并相交或相接的区间；end 为 None 表示持续至今。区间语义 [start, end)。"""
    out = []
    for s, e in sorted(intervals, key=lambda iv: iv[0]):
        if e is not None and not s < e:
            continue  # 丢弃零长区间
        if out and out[-1][1] is not None and s <= out[-1][1]:
            if e is None or e > out[-1][1]:
                out[-1] = (out[-1][0], e)
        else:
            out.append((s, e))
    return out


def clip_intervals(intervals, rs, re_):
    """把区间裁剪到 [rs, re_)，丢弃空段。"""
    out = []
    for s, e in intervals:
        s2 = max(s, rs)
        e2 = re_ if e is None else min(e, re_)
        if s2 < e2:
            out.append((s2, e2))
    return out


def subtract_intervals(base, minus):
    """区间集合差 base − minus。"""
    minus = merge_intervals(minus)
    res = []
    for s, e in merge_intervals(base):
        cur = s
        for ms, me in minus:
            if me <= cur:
                continue
            if ms >= e:
                break
            if ms > cur:
                res.append((cur, ms))
            cur = max(cur, me)
            if cur >= e:
                break
        if cur < e:
            res.append((cur, e))
    return res


# ------------------------------------------------------------ 拓扑与连通性

def load_topology(conn):
    """返回 (电源列表, 台区列表, {开关: (端点A, 端点B)})。"""
    sources = [r[0] for r in conn.execute("SELECT source_id FROM sources ORDER BY source_id")]
    areas = [r[0] for r in conn.execute("SELECT area_id FROM areas ORDER BY area_id")]
    links = {
        r[0]: (r[1], r[2])
        for r in conn.execute("SELECT switch_id, node_a, node_b FROM switches ORDER BY switch_id")
    }
    return sources, areas, links


def lost_areas(topo, states):
    """按各开关通断状态计算失电台区：连不到任何电源才算失电。"""
    sources, areas, links = topo
    parent = {n: n for n in sources + areas}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for sw_id, st in states.items():
        if st == STATE_CLOSED:
            a, b = links[sw_id]
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

    fed_roots = {find(s) for s in sources}
    return {a for a in areas if find(a) not in fed_roots}


# -------------------------------------------------------- 开关状态时间轴

def state_at(conn, switch_id, t):
    """定位 t 时刻开关状态：对物化时间轴做索引点查，无需扫描此前的变位历史。"""
    row = conn.execute(
        "SELECT state FROM switch_segments WHERE switch_id = ? AND start_epoch <= ? "
        "ORDER BY start_epoch DESC LIMIT 1",
        (switch_id, t),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"开关 {switch_id} 缺少初始状态段")
    return row[0]


def _next_segment_start(conn, switch_id, t):
    row = conn.execute(
        "SELECT start_epoch FROM switch_segments WHERE switch_id = ? AND start_epoch > ? "
        "ORDER BY start_epoch LIMIT 1",
        (switch_id, t),
    ).fetchone()
    return row[0] if row else None


def apply_insert(conn, switch_id, t, state):
    """在时间轴上落一次变位，返回新旧状态不同的时段 [t, 下一变位时刻)；无差异返回 None。"""
    before = state_at(conn, switch_id, t)
    conn.execute(
        "INSERT INTO switch_segments (switch_id, start_epoch, state) VALUES (?, ?, ?)",
        (switch_id, t, state),
    )
    if state == before:
        return None
    return (t, _next_segment_start(conn, switch_id, t))


def apply_revoke(conn, switch_id, t):
    """从时间轴撤掉一次变位，返回新旧状态不同的时段；无差异返回 None。"""
    row = conn.execute(
        "SELECT state FROM switch_segments WHERE switch_id = ? AND start_epoch = ?",
        (switch_id, t),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"开关 {switch_id} 在 {t} 时刻无变位段可撤销")
    old_state = row[0]
    before = conn.execute(
        "SELECT state FROM switch_segments WHERE switch_id = ? AND start_epoch < ? "
        "ORDER BY start_epoch DESC LIMIT 1",
        (switch_id, t),
    ).fetchone()[0]
    conn.execute(
        "DELETE FROM switch_segments WHERE switch_id = ? AND start_epoch = ?",
        (switch_id, t),
    )
    if old_state == before:
        return None
    return (t, _next_segment_start(conn, switch_id, t))


# -------------------------------------------------------- 失电时段增量重算

def _merge_spans(ranges):
    """合并相交或相接的待重算时段；b 为 None 表示到正无穷。"""
    spans = []
    for a, b in sorted(ranges, key=lambda r: r[0]):
        if spans:
            pa, pb = spans[-1]
            if pb is None or a <= pb:
                spans[-1] = (pa, None if (pb is None or b is None) else max(pb, b))
                continue
        spans.append((a, b))
    return spans


def recompute_spans(conn, ranges):
    """只重算新旧开关状态不同的时段，返回实际重算的时段列表。"""
    spans = _merge_spans([r for r in ranges if r is not None])
    for a, b in spans:
        _recompute_range(conn, a, b)
    return spans


class _LossWriter:
    """单台区失电段的流式写入器：按 start 递增喂入、按批入库。

    稳态内存只与批大小 REWRITE_BATCH 相关，与失电段总数无关。新段与上一段
    相接（[s,e) 语义下前一段 end == 下一段 start）时直接延长末段，等价于
    merge_intervals 的相接合并。末段可能仍在缓冲中，也可能已入库，两种情况
    都能正确延长。
    """

    def __init__(self, conn, area_id):
        self.conn = conn
        self.area_id = area_id
        self._buf: list[tuple[float, float]] = []
        self._last_end: Optional[float] = None  # 已喂入末段的终点
        self._last_start: Optional[float] = None
        self._last_in_db = False  # 末段是否已入库（决定延长用 UPDATE 还是改缓冲）

    def add(self, s, e):
        if e is not None and not s < e:
            return  # 丢弃零长区间
        if self._last_end is not None and s == self._last_end:
            # 与末段相接：延长它
            if self._last_in_db:
                self.conn.execute(
                    "UPDATE loss_periods SET end_epoch = ? "
                    "WHERE area_id = ? AND start_epoch = ? AND end_epoch = ?",
                    (e, self.area_id, self._last_start, self._last_end),
                )
            else:
                self._buf[-1] = (self._last_start, e)
            self._last_end = e
            return
        self._buf.append((s, e))
        self._last_start, self._last_end, self._last_in_db = s, e, False
        if len(self._buf) >= REWRITE_BATCH:
            self.flush()

    def flush(self):
        if not self._buf:
            return
        self.conn.executemany(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) "
            "VALUES (?, ?, ?)",
            [(self.area_id, s, e) for s, e in self._buf],
        )
        self._last_start, self._last_end = self._buf[-1]
        self._last_in_db = True
        self._buf = []


def _recompute_range(conn, a, b):
    """重算 [a, b) 内各台区失电段并写回 loss_periods（流式，内存与规模无关）。

    段首（a 时刻）各开关状态用 state_at 索引点查定位，不扫描此前变位历史；
    同一时刻的多个变位落在同一 epoch 边界，一起生效。epoch 边界用游标逐个
    拉取，失电段封口即经 _LossWriter 批量入库，不再把全部 epoch / 失电段
    堆进内存；范围外至多两个相邻半段单独保留并与新段接合。
    """
    topo = load_topology(conn)
    _, areas, links = topo
    if b is None:
        # 重算 [a, 正无穷)：命中所有终点落在 a 之后（含持续至今）的旧段
        def old_where(area):
            return (
                "area_id = ? AND (end_epoch IS NULL OR end_epoch >= ?)",
                (area, a),
            )
        cut_sql = (
            "SELECT DISTINCT start_epoch FROM switch_segments "
            "WHERE start_epoch > ? ORDER BY 1"
        )
        cut_params = (a,)
    else:
        # 重算 [a, b)：命中与该范围相交的旧段
        def old_where(area):
            return (
                "area_id = ? AND (end_epoch IS NULL OR end_epoch >= ?) "
                "AND start_epoch <= ?",
                (area, a, b),
            )
        cut_sql = (
            "SELECT DISTINCT start_epoch FROM switch_segments "
            "WHERE start_epoch > ? AND start_epoch < ? ORDER BY 1"
        )
        cut_params = (a, b)

    # 1) 先收集范围外需要保留的相邻半段（每个台区至多左、右各一段），并删除旧记录
    kept: dict[str, list[tuple[float, float]]] = {area: [] for area in areas}
    for area in areas:
        where, params = old_where(area)
        rows_cur = conn.execute(
            f"SELECT start_epoch, end_epoch FROM loss_periods WHERE {where}", params
        )
        while True:
            batch = rows_cur.fetchmany(REWRITE_BATCH)
            if not batch:
                break
            for s, e in batch:
                if s < a:
                    kept[area].append((s, a))  # 范围外左半段
                if b is not None and (e is None or e > b):
                    kept[area].append((b, e))  # 范围外右半段
        rows_cur.close()
        # 删除谓词与上面的读取完全一致，避免超长变量列表
        conn.execute(f"DELETE FROM loss_periods WHERE {where}", params)

    # 2) 左保留段在最前，先喂入写入器；随后游标逐个 epoch 推进、封口即写。
    #    open_since 每台区至多一条；右保留段在所有新段之后喂入。
    writers = {area: _LossWriter(conn, area) for area in areas}
    open_since: dict[str, float] = {}
    for area in areas:
        for s, e in kept[area]:
            if s < a:
                writers[area].add(s, e)  # 范围外左半段，位于重算范围之前

    def emit(t):
        states = {sw: state_at(conn, sw, t) for sw in links}
        current = lost_areas(topo, states)
        for area in areas:
            if area in current:
                open_since.setdefault(area, t)
            elif area in open_since:
                writers[area].add(open_since.pop(area), t)

    emit(a)  # 段首状态
    cut_cur = conn.execute(cut_sql, cut_params)
    while True:
        cut_batch = cut_cur.fetchmany(REWRITE_BATCH)
        if not cut_batch:
            break
        for (t,) in cut_batch:
            emit(t)
    cut_cur.close()
    # 到重算范围终点仍未封口的失电段
    for area, s in open_since.items():
        writers[area].add(s, b)
    # 范围外右半段在所有新段之后；相接时由写入器自动延长，否则另起一段
    for area in areas:
        for s, e in kept[area]:
            if b is not None and s >= b:
                writers[area].add(s, e)
    # 3) 落盘剩余缓冲
    for area in areas:
        writers[area].flush()


# -------------------------------------------------------- 变位单操作（须在写事务内调用）

def _ensure_switch(conn, switch_id):
    if conn.execute(
        "SELECT 1 FROM switches WHERE switch_id = ?", (switch_id,)
    ).fetchone() is None:
        raise ValidationError(f"开关 {switch_id} 不存在")


def create_order(conn, order_no, switch_id, change_epoch, state):
    """补录变位单。同一开关同一时刻已有另一张单时拒绝提交。"""
    _ensure_switch(conn, switch_id)
    if conn.execute(
        "SELECT 1 FROM orders WHERE order_no = ?", (order_no,)
    ).fetchone() is not None:
        raise ConflictError(f"变位单号 {order_no} 已存在，拒绝提交")
    dup = conn.execute(
        "SELECT order_no FROM orders WHERE switch_id = ? AND change_epoch = ?",
        (switch_id, change_epoch),
    ).fetchone()
    if dup is not None:
        raise ConflictError(
            f"开关 {switch_id} 在该时刻已有变位单 {dup[0]}，拒绝提交"
        )
    try:
        conn.execute(
            "INSERT INTO orders (order_no, switch_id, change_epoch, state) "
            "VALUES (?, ?, ?, ?)",
            (order_no, switch_id, change_epoch, state),
        )
    except sqlite3.IntegrityError as exc:  # 兜底：唯一索引约束
        raise ConflictError(f"提交冲突：{exc}") from exc
    return recompute_spans(conn, [apply_insert(conn, switch_id, change_epoch, state)])


def update_order(conn, order_no, change_epoch, state):
    """更正变位单：每张单开关固定，只能改 UTC 时刻与通断。"""
    row = conn.execute(
        "SELECT switch_id, change_epoch, state FROM orders WHERE order_no = ?",
        (order_no,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"变位单 {order_no} 不存在")
    switch_id, old_t, old_state = row[0], row[1], row[2]
    if old_t == change_epoch and old_state == state:
        return []  # 无变化，无需重算
    dup = conn.execute(
        "SELECT order_no FROM orders "
        "WHERE switch_id = ? AND change_epoch = ? AND order_no <> ?",
        (switch_id, change_epoch, order_no),
    ).fetchone()
    if dup is not None:
        raise ConflictError(
            f"开关 {switch_id} 在该时刻已有变位单 {dup[0]}，拒绝提交"
        )
    conn.execute(
        "UPDATE orders SET change_epoch = ?, state = ? WHERE order_no = ?",
        (change_epoch, state, order_no),
    )
    ranges = [
        apply_revoke(conn, switch_id, old_t),
        apply_insert(conn, switch_id, change_epoch, state),
    ]
    return recompute_spans(conn, ranges)


def delete_order(conn, order_no):
    """撤销变位单。"""
    row = conn.execute(
        "SELECT switch_id, change_epoch FROM orders WHERE order_no = ?",
        (order_no,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"变位单 {order_no} 不存在")
    conn.execute("DELETE FROM orders WHERE order_no = ?", (order_no,))
    return recompute_spans(conn, [apply_revoke(conn, row[0], row[1])])


# ------------------------------------------------------------ 查询与核查

def list_loss_periods(conn, area_id, rs, re_):
    """已算好的台区失电时段，裁剪到 [rs, re_)。"""
    rows = conn.execute(
        "SELECT start_epoch, end_epoch FROM loss_periods "
        "WHERE area_id = ? AND start_epoch < ? AND (end_epoch IS NULL OR end_epoch > ?) "
        "ORDER BY start_epoch",
        (area_id, re_, rs),
    ).fetchall()
    return clip_intervals([(r[0], r[1]) for r in rows], rs, re_)


def verify(conn, area_id, rs, re_, reported):
    """核查：直接用已算好的失电时段比对人工报停区间（起点计入、终点不计）。

    返回 (失电时段, 应补报, 应剔除)。
    """
    if not rs < re_:
        raise ValidationError("时间范围起点必须早于终点")
    if conn.execute(
        "SELECT 1 FROM areas WHERE area_id = ?", (area_id,)
    ).fetchone() is None:
        raise ValidationError(f"台区 {area_id} 不存在")
    loss = list_loss_periods(conn, area_id, rs, re_)
    rep = clip_intervals(list(reported), rs, re_)
    return loss, subtract_intervals(loss, rep), subtract_intervals(rep, loss)


# 变位单操作回执中失电段明细的最大条数：回执只用于确认本次重算结果，完整
# 历史失电段应通过 /loss-periods 或 /verify 获取；否则一次写单的响应会随
# 历史规模无限增长而 OOM。
RECEIPT_LOSS_LIMIT = 1000


def loss_in_spans(conn, spans):
    """汇总若干时段内各台区的失电时段（用于变位单操作回执）。

    读取按批进行、明细最多保留 RECEIPT_LOSS_LIMIT 条，但会统计命中总数，
    使回执内存与历史规模解耦。返回 (明细列表, 命中总数, 是否截断)。
    """
    out = []
    total = 0
    truncated = False
    for a, b in spans:
        if b is None:
            sql = (
                "SELECT area_id, start_epoch, end_epoch FROM loss_periods "
                "WHERE end_epoch IS NULL OR end_epoch > ? "
                "ORDER BY area_id, start_epoch"
            )
            params = (a,)
        else:
            sql = (
                "SELECT area_id, start_epoch, end_epoch FROM loss_periods "
                "WHERE start_epoch < ? AND (end_epoch IS NULL OR end_epoch > ?) "
                "ORDER BY area_id, start_epoch"
            )
            params = (b, a)
        cur = conn.execute(sql, params)
        while True:
            batch = cur.fetchmany(2048)
            if not batch:
                break
            for area_id, s, e in batch:
                s2 = max(s, a)
                e2 = e
                if e2 is None or (b is not None and e2 > b):
                    e2 = b
                if e2 is None or s2 < e2:
                    total += 1
                    if len(out) < RECEIPT_LOSS_LIMIT:
                        out.append((area_id, s2, e2))
                    else:
                        truncated = True
        cur.close()
    return out, total, truncated
