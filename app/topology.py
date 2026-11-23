"""固定拓扑：电源、台区与双向开关连线（含各开关初始通断）。

    P1 ─K1─ T1 ─K2─ T2 ─K3─ T3 ─K4─ T4 ─K5─ P2
    （K3 为联络开关，初始断开）

正常运行时 K3 断开，T1/T2 由 P1 供电、T3/T4 由 P2 供电；
一侧失电时可合上 K3，由另一侧电源继续供电（转供）。
"""
from __future__ import annotations

from .core import NEG_INF, STATE_CLOSED, STATE_OPEN, load_topology, lost_areas

SOURCES = [
    ("P1", "1号电源（甲变电站）"),
    ("P2", "2号电源（乙变电站）"),
]

AREAS = [
    ("T1", "台区T1"),
    ("T2", "台区T2"),
    ("T3", "台区T3"),
    ("T4", "台区T4"),
]

# (开关号, 名称, 端点A, 端点B, 初始通断)
SWITCHES = [
    ("K1", "甲站出线开关", "P1", "T1", STATE_CLOSED),
    ("K2", "T1-T2分段开关", "T1", "T2", STATE_CLOSED),
    ("K3", "T2-T3联络开关", "T2", "T3", STATE_OPEN),
    ("K4", "T3-T4分段开关", "T3", "T4", STATE_CLOSED),
    ("K5", "乙站出线开关", "T4", "P2", STATE_CLOSED),
]


def seed_topology(conn) -> bool:
    """首次启动写入固定拓扑；已存在则跳过。返回是否写入。"""
    if conn.execute("SELECT COUNT(*) FROM switches").fetchone()[0]:
        return False
    conn.executemany("INSERT INTO sources (source_id, name) VALUES (?, ?)", SOURCES)
    conn.executemany("INSERT INTO areas (area_id, name) VALUES (?, ?)", AREAS)
    conn.executemany(
        "INSERT INTO switches (switch_id, name, node_a, node_b, initial_state) "
        "VALUES (?, ?, ?, ?, ?)",
        SWITCHES,
    )
    # 每个开关的初始状态段（起点为负无穷哨兵）
    conn.executemany(
        "INSERT INTO switch_segments (switch_id, start_epoch, state) VALUES (?, ?, ?)",
        [(sw_id, NEG_INF, st) for sw_id, _, _, _, st in SWITCHES],
    )
    # 初始状态下已失电的台区（本拓扑没有，防御性处理）：从负无穷起记失电
    topo = load_topology(conn)
    initial_states = {sw_id: st for sw_id, _, _, _, st in SWITCHES}
    for area in sorted(lost_areas(topo, initial_states)):
        conn.execute(
            "INSERT INTO loss_periods (area_id, start_epoch, end_epoch) "
            "VALUES (?, ?, NULL)",
            (area, NEG_INF),
        )
    return True
