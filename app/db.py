"""SQLite 连接、建表与事务管理。"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

_DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "outage.db"
)

SCHEMA = """
-- 电源
CREATE TABLE IF NOT EXISTS sources (
    source_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

-- 台区
CREATE TABLE IF NOT EXISTS areas (
    area_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

-- 双向开关连线：两端可接电源或台区，initial_state 为初始通断
CREATE TABLE IF NOT EXISTS switches (
    switch_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    node_a          TEXT NOT NULL,
    node_b          TEXT NOT NULL,
    initial_state   TEXT NOT NULL CHECK (initial_state IN ('通', '断'))
);

-- 开关变位单：每张单固定一个开关，记录 UTC 变位时刻与变位后通断
CREATE TABLE IF NOT EXISTS orders (
    order_no        TEXT PRIMARY KEY,
    switch_id       TEXT NOT NULL REFERENCES switches (switch_id),
    change_epoch    REAL NOT NULL,              -- UTC epoch 秒
    state           TEXT NOT NULL CHECK (state IN ('通', '断')),
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
-- 同一开关同一时刻只允许一张变位单
CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_switch_time
    ON orders (switch_id, change_epoch);

-- 物化的开关状态时间轴：定位任意时刻状态只需索引点查，无需回放变位历史
CREATE TABLE IF NOT EXISTS switch_segments (
    switch_id       TEXT NOT NULL,
    start_epoch     REAL NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('通', '断')),
    PRIMARY KEY (switch_id, start_epoch)
);

-- 已算好的台区失电时段；end_epoch 为 NULL 表示持续至今
CREATE TABLE IF NOT EXISTS loss_periods (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    area_id     TEXT NOT NULL,
    start_epoch REAL NOT NULL,
    end_epoch   REAL
);
CREATE INDEX IF NOT EXISTS ix_loss_area_start ON loss_periods (area_id, start_epoch);
"""

_write_lock = threading.RLock()


def db_path() -> str:
    return os.environ.get("DB_PATH", _DEFAULT_DB)


def connect() -> sqlite3.Connection:
    path = db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def read_conn():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_txn():
    """写事务：变位单与失电时段在同一事务中生效；

    任何一步失败整体回滚，核查仍使用原结果。
    """
    with _write_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
