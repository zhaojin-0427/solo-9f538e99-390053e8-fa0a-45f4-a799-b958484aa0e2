"""pytest 公共夹具：每个用例使用独立的临时 SQLite 库。"""
from __future__ import annotations

import pytest

from app.db import init_db, write_txn
from app.topology import seed_topology


@pytest.fixture()
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    init_db()
    with write_txn() as conn:
        seed_topology(conn)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "api.db"))
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c
