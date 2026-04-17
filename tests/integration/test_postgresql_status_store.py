from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from typing import Any
from uuid import uuid4

import pytest

from graph_engine.models import Neo4jGraphStatus
from graph_engine.status import GraphStatusManager
from graph_engine.status.store import PostgreSQLStatusStore

pytestmark = pytest.mark.skipif(
    os.getenv("DATABASE_URL") is None,
    reason="DATABASE_URL is not set; PostgreSQL status store tests require a database.",
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class BarrierStatusStore(PostgreSQLStatusStore):
    def __init__(self, *args: Any, barrier: Barrier, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._barrier = barrier

    def read_current_status(self) -> Neo4jGraphStatus | None:
        status = super().read_current_status()
        if status is not None and status.graph_status == "ready":
            self._barrier.wait(timeout=10)
        return status


def test_postgresql_status_store_bootstraps_empty_row_and_persists_transitions() -> None:
    pytest.importorskip("psycopg")

    database_url = _database_url()
    table_name = _table_name()
    store = PostgreSQLStatusStore(database_url, table_name=table_name)

    try:
        assert store.read_current_status() is None

        rebuilding = GraphStatusManager(store, clock=lambda: NOW).mark_rebuilding()
        assert rebuilding.graph_status == "rebuilding"
        assert store.read_current_status() == rebuilding

        ready = GraphStatusManager(store, clock=lambda: NOW).mark_ready(
            node_count=3,
            edge_count=2,
            key_label_counts={"Entity": 2, "Sector": 1},
            checksum="ready-checksum",
            reload_completed=True,
        )

        assert store.read_current_status() == ready
        assert ready.graph_status == "ready"
        assert ready.graph_generation_id == 1
        assert ready.last_verified_at == NOW
        assert ready.last_reload_at == NOW
    finally:
        _drop_table(database_url, table_name)


def test_postgresql_status_store_rejects_competing_reload_and_sync_transitions() -> None:
    pytest.importorskip("psycopg")

    database_url = _database_url()
    table_name = _table_name()
    store = PostgreSQLStatusStore(database_url, table_name=table_name)
    store.write_current_status(_status())
    barrier = Barrier(2)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(_begin_sync, database_url, table_name, barrier),
                executor.submit(_mark_rebuilding, database_url, table_name, barrier),
            ]
            outcomes = [future.result(timeout=20) for future in futures]

        assert outcomes.count("stale") == 1
        assert outcomes.count("syncing") + outcomes.count("rebuilding") == 1
        final_status = store.read_current_status()
        assert final_status is not None
        assert final_status.graph_status in {"syncing", "rebuilding"}
    finally:
        _drop_table(database_url, table_name)


def _begin_sync(database_url: str, table_name: str, barrier: Barrier) -> str:
    store = BarrierStatusStore(database_url, table_name=table_name, barrier=barrier)
    manager = GraphStatusManager(store, clock=lambda: NOW)
    try:
        _, status = manager.begin_sync()
    except RuntimeError as exc:
        assert "stale graph_status transition" in str(exc)
        return "stale"
    return status.graph_status


def _mark_rebuilding(database_url: str, table_name: str, barrier: Barrier) -> str:
    store = BarrierStatusStore(database_url, table_name=table_name, barrier=barrier)
    manager = GraphStatusManager(store, clock=lambda: NOW)
    try:
        status = manager.mark_rebuilding()
    except RuntimeError as exc:
        assert "stale graph_status transition" in str(exc)
        return "stale"
    return status.graph_status


def _status() -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=8,
        node_count=2,
        edge_count=1,
        key_label_counts={"Entity": 2},
        checksum="ready-checksum",
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    assert database_url is not None
    return database_url


def _table_name() -> str:
    return f"neo4j_graph_status_test_{uuid4().hex}"


def _drop_table(database_url: str, table_name: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
