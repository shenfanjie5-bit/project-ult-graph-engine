from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.status import (
    GraphStatusManager,
    check_live_graph_consistency,
    require_ready_status,
)
from graph_engine.status.consistency import _read_live_graph_metrics

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)
SNAPSHOT_CREATED_AT = datetime(2026, 4, 17, 1, 0, 0, tzinfo=timezone.utc)


class InMemoryStatusStore:
    def __init__(self, status: Neo4jGraphStatus | None = None) -> None:
        self.status = status
        self.writes: list[Neo4jGraphStatus] = []

    def read_current_status(self) -> Neo4jGraphStatus | None:
        return self.status

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        self.status = status
        self.writes.append(status)

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        if self.status != expected_status:
            return False
        self.write_current_status(next_status)
        return True


class InterleavingStatusStore(InMemoryStatusStore):
    def __init__(
        self,
        status: Neo4jGraphStatus,
        interleaved_status: Neo4jGraphStatus,
    ) -> None:
        super().__init__(status)
        self.interleaved_status = interleaved_status
        self.compare_calls = 0

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        self.compare_calls += 1
        self.status = self.interleaved_status
        return super().compare_and_write_current_status(
            expected_status=expected_status,
            next_status=next_status,
        )


class StaticSnapshotReader:
    def __init__(
        self,
        snapshot: GraphSnapshot | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.error = error
        self.calls: list[str] = []

    def read_graph_snapshot(self, snapshot_ref: str) -> GraphSnapshot:
        self.calls.append(snapshot_ref)
        if self.error is not None:
            raise self.error
        if self.snapshot is None:
            raise RuntimeError("snapshot not configured")
        return self.snapshot


class FakeLiveGraphClient:
    def __init__(
        self,
        rows: list[dict[str, Any]] | object,
        *,
        error: Exception | None = None,
    ) -> None:
        self.rows = rows
        self.error = error
        self.calls: list[str] = []

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(query)
        if self.error is not None:
            raise self.error
        return self.rows  # type: ignore[return-value]


class ExplodingReadyManager(GraphStatusManager):
    def require_ready(self) -> Neo4jGraphStatus:
        raise AssertionError("require_ready should not be called")


def test_get_status_raises_for_missing_status() -> None:
    manager = GraphStatusManager(InMemoryStatusStore())

    with pytest.raises(LookupError, match="not been initialized"):
        manager.get_status()


def test_mark_rebuilding_bootstraps_empty_status() -> None:
    store = InMemoryStatusStore()

    status = GraphStatusManager(store, clock=lambda: NOW).mark_rebuilding()

    assert status.graph_status == "rebuilding"
    assert status.graph_generation_id == 0
    assert status.node_count == 0
    assert status.edge_count == 0
    assert status.key_label_counts == {}
    assert status.checksum == "rebuilding"
    assert status.last_verified_at is None
    assert status.last_reload_at is None
    assert store.writes == [status]


@pytest.mark.parametrize("source_status", ["ready", "failed"])
def test_mark_rebuilding_allows_ready_and_failed_states(
    source_status: Literal["ready", "failed"],
) -> None:
    store = InMemoryStatusStore(_status(graph_status=source_status, graph_generation_id=4))

    status = GraphStatusManager(store, clock=lambda: NOW).mark_rebuilding()

    assert status.graph_status == "rebuilding"
    assert status.graph_generation_id == 4
    assert status.node_count == 0
    assert status.edge_count == 0
    assert status.checksum == "rebuilding"


def test_mark_rebuilding_rejects_existing_rebuilding_state() -> None:
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_status="rebuilding")))

    with pytest.raises(ValueError, match="rebuilding"):
        manager.mark_rebuilding()


def test_mark_rebuilding_rejects_active_syncing_state() -> None:
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_status="syncing")))

    with pytest.raises(ValueError, match="syncing"):
        manager.mark_rebuilding()


@pytest.mark.parametrize("source_status", ["ready", "syncing", "failed"])
def test_mark_ready_only_allows_rebuilding(
    source_status: Literal["ready", "syncing", "failed"],
) -> None:
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_status=source_status)))

    with pytest.raises(ValueError, match="to 'ready'"):
        manager.mark_ready(
            node_count=2,
            edge_count=1,
            key_label_counts={"Entity": 2},
            checksum="abc123",
        )


def test_mark_ready_rejects_missing_status() -> None:
    manager = GraphStatusManager(InMemoryStatusStore())

    with pytest.raises(ValueError, match="None"):
        manager.mark_ready(
            node_count=2,
            edge_count=1,
            key_label_counts={"Entity": 2},
            checksum="abc123",
        )


def test_mark_ready_writes_metrics_and_increments_generation() -> None:
    store = InMemoryStatusStore(_status(graph_status="rebuilding", graph_generation_id=3))

    status = GraphStatusManager(store, clock=lambda: NOW).mark_ready(
        node_count=5,
        edge_count=4,
        key_label_counts={"Entity": 3, "Sector": 2},
        checksum="ready-checksum",
    )

    assert status.graph_status == "ready"
    assert status.graph_generation_id == 4
    assert status.node_count == 5
    assert status.edge_count == 4
    assert status.key_label_counts == {"Entity": 3, "Sector": 2}
    assert status.checksum == "ready-checksum"
    assert status.last_verified_at == NOW
    assert status.last_reload_at is None


def test_mark_ready_sets_reload_timestamp_when_reload_completed() -> None:
    store = InMemoryStatusStore(_status(graph_status="rebuilding", graph_generation_id=8))

    status = GraphStatusManager(store, clock=lambda: NOW).mark_ready(
        node_count=1,
        edge_count=0,
        key_label_counts={"Entity": 1},
        checksum="ready-checksum",
        graph_generation_id=8,
        reload_completed=True,
    )

    assert status.graph_generation_id == 8
    assert status.last_verified_at == NOW
    assert status.last_reload_at == NOW


def test_mark_ready_rejects_generation_backtracking() -> None:
    manager = GraphStatusManager(
        InMemoryStatusStore(_status(graph_status="rebuilding", graph_generation_id=8)),
    )

    with pytest.raises(ValueError, match="cannot move backwards"):
        manager.mark_ready(
            node_count=1,
            edge_count=0,
            key_label_counts={"Entity": 1},
            checksum="ready-checksum",
            graph_generation_id=7,
        )


def test_mark_ready_rejects_stale_rebuilding_status_before_write() -> None:
    interleaved_status = _status(graph_status="failed", graph_generation_id=8)
    store = InterleavingStatusStore(
        _status(graph_status="rebuilding", graph_generation_id=8),
        interleaved_status,
    )

    with pytest.raises(RuntimeError, match="stale graph_status transition"):
        GraphStatusManager(store, clock=lambda: NOW).mark_ready(
            node_count=1,
            edge_count=0,
            key_label_counts={"Entity": 1},
            checksum="ready-checksum",
        )

    assert store.status == interleaved_status
    assert store.writes == []
    assert store.compare_calls == 1


def test_sync_writer_token_blocks_rebuilding_until_released() -> None:
    store = InMemoryStatusStore(_status(graph_status="ready", graph_generation_id=8))
    manager = GraphStatusManager(store, clock=lambda: NOW)

    ready_status, syncing_status = manager.begin_sync()

    assert ready_status == _status(graph_status="ready", graph_generation_id=8)
    assert store.status == syncing_status
    assert syncing_status.graph_status == "syncing"
    with pytest.raises(ValueError, match="syncing"):
        manager.mark_rebuilding()

    released = manager.finish_sync(
        expected_status=syncing_status,
        ready_status=ready_status,
    )

    assert released == ready_status
    assert store.status == ready_status


def test_sync_writer_token_failure_marks_status_failed() -> None:
    store = InMemoryStatusStore(_status(graph_status="ready", graph_generation_id=8))
    manager = GraphStatusManager(store, clock=lambda: NOW)
    _, syncing_status = manager.begin_sync()

    failed = manager.mark_sync_failed(
        expected_status=syncing_status,
        checksum="sync-failed",
    )

    assert failed.graph_status == "failed"
    assert failed.graph_generation_id == syncing_status.graph_generation_id
    assert failed.checksum == "sync-failed"
    assert store.status == failed


def test_mark_rebuilding_rejects_stale_ready_status_before_write() -> None:
    interleaved_status = _status(graph_status="failed", graph_generation_id=8)
    store = InterleavingStatusStore(
        _status(graph_status="ready", graph_generation_id=8),
        interleaved_status,
    )

    with pytest.raises(RuntimeError, match="stale graph_status transition"):
        GraphStatusManager(store, clock=lambda: NOW).mark_rebuilding()

    assert store.status == interleaved_status
    assert store.writes == []
    assert store.compare_calls == 1


@pytest.mark.parametrize("source_status", ["ready", "syncing", "rebuilding"])
def test_mark_failed_allows_ready_syncing_and_rebuilding(
    source_status: Literal["ready", "syncing", "rebuilding"],
) -> None:
    store = InMemoryStatusStore(_status(graph_status=source_status, graph_generation_id=6))

    status = GraphStatusManager(store, clock=lambda: NOW).mark_failed(
        node_count=2,
        edge_count=1,
        key_label_counts={"Entity": 2},
        checksum="failed-checksum",
    )

    assert status.graph_status == "failed"
    assert status.graph_generation_id == 6
    assert status.node_count == 2
    assert status.edge_count == 1
    assert status.key_label_counts == {"Entity": 2}
    assert status.checksum == "failed-checksum"
    assert status.last_verified_at is None


def test_mark_failed_rejects_failed_state() -> None:
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_status="failed")))

    with pytest.raises(ValueError, match="to 'failed'"):
        manager.mark_failed()


def test_mark_failed_rejects_missing_status() -> None:
    manager = GraphStatusManager(InMemoryStatusStore())

    with pytest.raises(ValueError, match="None"):
        manager.mark_failed()


def test_mark_failed_rejects_stale_ready_status_before_write() -> None:
    interleaved_status = _status(graph_status="rebuilding", graph_generation_id=8)
    store = InterleavingStatusStore(
        _status(graph_status="ready", graph_generation_id=8),
        interleaved_status,
    )

    with pytest.raises(RuntimeError, match="stale graph_status transition"):
        GraphStatusManager(store, clock=lambda: NOW).mark_failed()

    assert store.status == interleaved_status
    assert store.writes == []
    assert store.compare_calls == 1


def test_require_ready_returns_ready_status() -> None:
    ready_status = _status()

    assert require_ready_status(ready_status) is ready_status
    assert GraphStatusManager(InMemoryStatusStore(ready_status)).require_ready() is ready_status


@pytest.mark.parametrize("graph_status", ["syncing", "rebuilding", "failed"])
def test_require_ready_rejects_non_ready_status(
    graph_status: Literal["syncing", "rebuilding", "failed"],
) -> None:
    status = _status(graph_status=graph_status)

    with pytest.raises(PermissionError, match="ready"):
        require_ready_status(status)
    with pytest.raises(PermissionError, match="ready"):
        GraphStatusManager(InMemoryStatusStore(status)).require_ready()


def test_mark_verified_updates_metrics_without_bumping_generation() -> None:
    store = InMemoryStatusStore(_status(graph_generation_id=9))
    snapshot = _snapshot(graph_generation_id=9, node_count=4, edge_count=3)

    status = GraphStatusManager(store, clock=lambda: NOW).mark_verified(snapshot)

    assert status.graph_status == "ready"
    assert status.graph_generation_id == 9
    assert status.node_count == 4
    assert status.edge_count == 3
    assert status.checksum == snapshot.checksum
    assert status.last_verified_at == NOW


def test_mark_verified_rejects_generation_mismatch() -> None:
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_generation_id=9)))

    with pytest.raises(ValueError, match="graph_generation_id"):
        manager.mark_verified(_snapshot(graph_generation_id=8))


def test_mark_verified_rejects_stale_ready_status_before_write() -> None:
    interleaved_status = _status(graph_status="failed", graph_generation_id=9)
    store = InterleavingStatusStore(
        _status(graph_status="ready", graph_generation_id=9),
        interleaved_status,
    )

    with pytest.raises(RuntimeError, match="stale graph_status transition"):
        GraphStatusManager(store, clock=lambda: NOW).mark_verified(
            _snapshot(graph_generation_id=9),
        )

    assert store.status == interleaved_status
    assert store.writes == []
    assert store.compare_calls == 1


def test_check_live_graph_consistency_returns_true_for_matching_snapshot() -> None:
    client = FakeLiveGraphClient(_metrics_rows())
    snapshot = _snapshot_from_live_client(client, graph_generation_id=3)
    store = InMemoryStatusStore(_status_from_snapshot(snapshot))

    result = check_live_graph_consistency(
        "snapshot-1",
        client=client,  # type: ignore[arg-type]
        snapshot_reader=StaticSnapshotReader(snapshot),
        status_manager=GraphStatusManager(store, clock=lambda: NOW),
    )

    assert result is True
    assert store.writes == []


def test_check_live_graph_consistency_requires_status_manager_by_default() -> None:
    client = MagicMock()

    with pytest.raises(ValueError, match="status_manager"):
        check_live_graph_consistency(
            "snapshot-1",
            client=client,
            snapshot_reader=StaticSnapshotReader(_snapshot()),
        )

    client.execute_read.assert_not_called()


def test_check_live_graph_consistency_blocks_non_ready_before_reads() -> None:
    client = FakeLiveGraphClient(_metrics_rows())
    snapshot_reader = StaticSnapshotReader(_snapshot())
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_status="rebuilding")))

    with pytest.raises(PermissionError, match="ready"):
        check_live_graph_consistency(
            "snapshot-1",
            client=client,  # type: ignore[arg-type]
            snapshot_reader=snapshot_reader,
            status_manager=manager,
        )

    assert client.calls == []
    assert snapshot_reader.calls == []


def test_check_live_graph_consistency_can_skip_ready_guard_for_maintenance() -> None:
    client = FakeLiveGraphClient(_metrics_rows())
    snapshot = _snapshot_from_live_client(client, graph_generation_id=3)
    manager = ExplodingReadyManager(
        InMemoryStatusStore(_status(graph_status="rebuilding", graph_generation_id=3)),
    )
    client.calls = []

    result = check_live_graph_consistency(
        "snapshot-1",
        client=client,  # type: ignore[arg-type]
        snapshot_reader=StaticSnapshotReader(snapshot),
        status_manager=manager,
        require_ready=False,
    )

    assert result is True
    assert len(client.calls) == 1


@pytest.mark.parametrize(
    "snapshot_updates",
    [
        {"graph_generation_id": 99},
        {"node_count": 99},
        {"edge_count": 99},
        {"key_label_counts": {"Entity": 99}},
        {"checksum": "wrong-checksum"},
    ],
)
def test_check_live_graph_consistency_returns_false_for_mismatches(
    snapshot_updates: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeLiveGraphClient(_metrics_rows())
    snapshot = _snapshot_from_live_client(client, graph_generation_id=3).model_copy(
        update=snapshot_updates,
    )
    manager = GraphStatusManager(
        InMemoryStatusStore(_status(graph_generation_id=3)),
        clock=lambda: NOW,
    )
    client.calls = []
    caplog.set_level(logging.WARNING, logger="graph_engine.status.consistency")

    assert (
        check_live_graph_consistency(
            "snapshot-1",
            client=client,  # type: ignore[arg-type]
            snapshot_reader=StaticSnapshotReader(snapshot),
            status_manager=manager,
        )
        is False
    )
    assert any(
        getattr(record, "snapshot_ref", None) == "snapshot-1"
        and getattr(record, "failure_stage", None) == "result_validation"
        for record in caplog.records
    )


def test_check_live_graph_consistency_reader_error_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeLiveGraphClient(_metrics_rows())
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_generation_id=3)))
    caplog.set_level(logging.ERROR, logger="graph_engine.status.consistency")

    result = check_live_graph_consistency(
        "snapshot-1",
        client=client,  # type: ignore[arg-type]
        snapshot_reader=StaticSnapshotReader(error=RuntimeError("reader failed")),
        status_manager=manager,
    )

    assert result is False
    assert client.calls == []
    assert any(
        getattr(record, "snapshot_ref", None) == "snapshot-1"
        and getattr(record, "failure_stage", None) == "snapshot_read"
        for record in caplog.records
    )


def test_check_live_graph_consistency_client_error_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeLiveGraphClient(_metrics_rows(), error=RuntimeError("neo4j failed"))
    snapshot = _snapshot_from_rows(graph_generation_id=3)
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_generation_id=3)))
    caplog.set_level(logging.ERROR, logger="graph_engine.status.consistency")

    result = check_live_graph_consistency(
        "snapshot-1",
        client=client,  # type: ignore[arg-type]
        snapshot_reader=StaticSnapshotReader(snapshot),
        status_manager=manager,
    )

    assert result is False
    assert any(
        getattr(record, "snapshot_ref", None) == "snapshot-1"
        and getattr(record, "failure_stage", None) == "live_graph_read"
        for record in caplog.records
    )


def test_check_live_graph_consistency_malformed_client_result_fails_closed() -> None:
    client = FakeLiveGraphClient([{"node_count": 2}])
    snapshot = _snapshot_from_rows(graph_generation_id=3)
    manager = GraphStatusManager(InMemoryStatusStore(_status(graph_generation_id=3)))

    result = check_live_graph_consistency(
        "snapshot-1",
        client=client,  # type: ignore[arg-type]
        snapshot_reader=StaticSnapshotReader(snapshot),
        status_manager=manager,
    )

    assert result is False


def _status(
    *,
    graph_status: Literal["ready", "syncing", "rebuilding", "failed"] = "ready",
    graph_generation_id: int = 3,
    node_count: int = 2,
    edge_count: int = 1,
    key_label_counts: dict[str, int] | None = None,
    checksum: str = "abc123",
) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status=graph_status,
        graph_generation_id=graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts={"Entity": 2} if key_label_counts is None else key_label_counts,
        checksum=checksum,
        last_verified_at=NOW if graph_status == "ready" else None,
        last_reload_at=None,
    )


def _status_from_snapshot(snapshot: GraphSnapshot) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=snapshot.graph_generation_id,
        node_count=snapshot.node_count,
        edge_count=snapshot.edge_count,
        key_label_counts=snapshot.key_label_counts,
        checksum=snapshot.checksum,
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _snapshot(
    *,
    graph_generation_id: int = 3,
    node_count: int = 2,
    edge_count: int = 1,
    key_label_counts: dict[str, int] | None = None,
    checksum: str = "abc123",
) -> GraphSnapshot:
    return GraphSnapshot(
        cycle_id="cycle-1",
        snapshot_id="snapshot-1",
        graph_generation_id=graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts={"Entity": node_count} if key_label_counts is None else key_label_counts,
        checksum=checksum,
        created_at=SNAPSHOT_CREATED_AT,
    )


def _snapshot_from_live_client(
    client: FakeLiveGraphClient,
    *,
    graph_generation_id: int,
) -> GraphSnapshot:
    node_count, edge_count, key_label_counts, checksum = _read_live_graph_metrics(
        client,  # type: ignore[arg-type]
    )
    return _snapshot(
        graph_generation_id=graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )


def _snapshot_from_rows(*, graph_generation_id: int) -> GraphSnapshot:
    return _snapshot_from_live_client(
        FakeLiveGraphClient(_metrics_rows()),
        graph_generation_id=graph_generation_id,
    )


def _metrics_rows() -> list[dict[str, Any]]:
    nodes = [
        {
            "labels": ["Entity"],
            "node_id": "node-a",
            "canonical_entity_id": "entity-a",
            "properties": {"node_id": "node-a", "ticker": "AAA"},
        },
        {
            "labels": ["Entity"],
            "node_id": "node-b",
            "canonical_entity_id": "entity-b",
            "properties": {"node_id": "node-b", "ticker": "BBB"},
        },
    ]
    relationships = [
        {
            "source_node_id": "node-a",
            "target_node_id": "node-b",
            "relationship_type": "SUPPLY_CHAIN",
            "edge_id": "edge-1",
            "properties": {"edge_id": "edge-1", "weight": 0.7},
        }
    ]
    return [
        {
            "node_count": 2,
            "edge_count": 1,
            "label_counts": [{"label": "Entity", "count": 2}],
            "nodes": nodes,
            "relationships": relationships,
        }
    ]
