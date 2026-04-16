from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

import graph_engine.snapshots.generator as snapshot_generator
from graph_engine.models import (
    GraphImpactSnapshot,
    GraphSnapshot,
    Neo4jGraphStatus,
    PropagationContext,
    PropagationResult,
)
from graph_engine.snapshots import (
    build_graph_impact_snapshot,
    build_graph_snapshot,
    compute_graph_snapshots,
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class FakeSnapshotClient:
    def __init__(
        self,
        *,
        nodes: list[dict[str, Any]],
        relationships: list[dict[str, Any]],
    ) -> None:
        self.nodes = nodes
        self.relationships = relationships
        self.read_calls: list[str] = []

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.read_calls.append(query)
        if "RETURN node_count, edge_count, label_counts, nodes, relationships" in query:
            counts: dict[str, int] = {}
            for node in self.nodes:
                for label in node["labels"]:
                    counts[label] = counts.get(label, 0) + 1
            return [
                {
                    "node_count": len(self.nodes),
                    "edge_count": len(self.relationships),
                    "label_counts": [
                        {"label": label, "count": count}
                        for label, count in sorted(counts.items())
                    ],
                    "nodes": list(self.nodes),
                    "relationships": list(self.relationships),
                }
            ]
        return []


class StaticRegimeReader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def read_regime_context(self, world_state_ref: str) -> dict[str, Any]:
        self.calls.append(world_state_ref)
        return {}


class StaticGraphStatusReader:
    def __init__(
        self,
        graph_status: Neo4jGraphStatus | list[Neo4jGraphStatus],
    ) -> None:
        if isinstance(graph_status, list):
            self.graph_statuses = graph_status
        else:
            self.graph_statuses = [graph_status]
        self.calls = 0

    def read_graph_status(self) -> Neo4jGraphStatus:
        self.calls += 1
        index = min(self.calls - 1, len(self.graph_statuses) - 1)
        return self.graph_statuses[index]

    def validate_ready_status_for_snapshot_publication(
        self,
        expected_status: Neo4jGraphStatus,
    ) -> bool:
        current_status = self.read_graph_status()
        return (
            current_status.graph_status == "ready"
            and current_status.graph_generation_id == expected_status.graph_generation_id
            and current_status.node_count == expected_status.node_count
            and current_status.edge_count == expected_status.edge_count
            and current_status.key_label_counts == expected_status.key_label_counts
            and current_status.checksum == expected_status.checksum
        )


class RecordingSnapshotWriter:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events
        self.calls: list[tuple[GraphSnapshot, GraphImpactSnapshot]] = []

    def write_snapshots(
        self,
        graph_snapshot: GraphSnapshot,
        impact_snapshot: GraphImpactSnapshot,
    ) -> None:
        if self.events is not None:
            self.events.append("write")
        self.calls.append((graph_snapshot, impact_snapshot))


class RaisingSnapshotWriter:
    def write_snapshots(
        self,
        graph_snapshot: GraphSnapshot,
        impact_snapshot: GraphImpactSnapshot,
    ) -> None:
        raise RuntimeError("writer failed")


def test_build_graph_snapshot_reads_metrics_and_stable_checksum() -> None:
    client = FakeSnapshotClient(
        nodes=[
            {
                "labels": ["Entity"],
                "node_id": "node-b",
                "canonical_entity_id": "entity-b",
                "properties": {"node_id": "node-b", "ticker": "BBB"},
            },
            {
                "labels": ["Entity", "Sector"],
                "node_id": "node-a",
                "canonical_entity_id": "entity-a",
                "properties": {"node_id": "node-a", "ticker": "AAA"},
            },
        ],
        relationships=[
            {
                "source_node_id": "node-a",
                "target_node_id": "node-b",
                "relationship_type": "SUPPLY_CHAIN",
                "edge_id": "edge-1",
                "properties": {"weight": 0.7},
            }
        ],
    )

    first = build_graph_snapshot("cycle-1", 3, client)  # type: ignore[arg-type]
    second = build_graph_snapshot("cycle-1", 3, client)  # type: ignore[arg-type]

    assert first.node_count == 2
    assert first.edge_count == 1
    assert first.key_label_counts == {"Entity": 2, "Sector": 1}
    assert first.checksum == second.checksum
    assert first.snapshot_id == second.snapshot_id
    assert len(client.read_calls) == 2

    client.relationships[0] = {
        **client.relationships[0],
        "properties": {"weight": 0.9},
    }
    changed = build_graph_snapshot("cycle-1", 3, client)  # type: ignore[arg-type]
    assert changed.checksum != first.checksum
    assert len(client.read_calls) == 3


def test_build_graph_impact_snapshot_preserves_propagation_payload() -> None:
    propagation_result = _propagation_result()

    impact_snapshot = build_graph_impact_snapshot(
        "cycle-1",
        "world-state-1",
        propagation_result,
    )

    assert impact_snapshot.regime_context_ref == "world-state-1"
    assert impact_snapshot.activated_paths == propagation_result.activated_paths
    assert impact_snapshot.impacted_entities == propagation_result.impacted_entities
    assert impact_snapshot.channel_breakdown == propagation_result.channel_breakdown


def test_compute_graph_snapshots_requires_status_without_side_effects() -> None:
    client = MagicMock()
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()

    with pytest.raises(ValueError, match="requires graph_status"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            graph_generation_id=1,
            regime_reader=reader,
            snapshot_writer=writer,
        )

    assert reader.calls == []
    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


def test_compute_graph_snapshots_rejects_non_ready_status_without_side_effects() -> None:
    client = MagicMock()
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()
    graph_status = Neo4jGraphStatus(
        graph_status="failed",
        graph_generation_id=1,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="failed",
        last_verified_at=NOW,
        last_reload_at=None,
    )

    with pytest.raises(PermissionError, match="ready"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            graph_generation_id=1,
            regime_reader=reader,
            snapshot_writer=writer,
            graph_status=graph_status,
        )

    assert reader.calls == []
    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


def test_compute_graph_snapshots_rejects_generation_mismatch_without_side_effects() -> None:
    client = MagicMock()
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()

    with pytest.raises(ValueError, match="graph_generation_id disagrees"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            graph_generation_id=9,
            regime_reader=reader,
            snapshot_writer=writer,
            graph_status_reader=StaticGraphStatusReader(
                _ready_status(_graph_snapshot(graph_generation_id=1)),
            ),
        )

    assert reader.calls == []
    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


@pytest.mark.parametrize(
    ("status_update", "message"),
    [
        ({"node_count": 99}, "node_count"),
        ({"edge_count": 99}, "edge_count"),
        ({"key_label_counts": {"Entity": 999}}, "key_label_counts"),
        ({"checksum": "wrong-checksum"}, "checksum"),
    ],
)
def test_compute_graph_snapshots_rejects_live_metric_mismatch_before_propagation(
    status_update: dict[str, Any],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeSnapshotClient(
        nodes=[
            {
                "labels": ["Entity"],
                "node_id": "node-a",
                "canonical_entity_id": "entity-a",
                "properties": {"node_id": "node-a"},
            }
        ],
        relationships=[],
    )
    graph_snapshot = build_graph_snapshot("cycle-1", 3, client)  # type: ignore[arg-type]
    client.read_calls.clear()
    graph_status = _ready_status(graph_snapshot).model_copy(update=status_update)
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()

    def fail_propagation(*args: Any, **kwargs: Any) -> PropagationResult:
        raise AssertionError("propagation must not run")

    monkeypatch.setattr(snapshot_generator, "run_fundamental_propagation", fail_propagation)

    with pytest.raises(ValueError, match=message):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,  # type: ignore[arg-type]
            regime_reader=reader,
            snapshot_writer=writer,
            graph_status_reader=StaticGraphStatusReader(graph_status),
        )

    assert reader.calls == []
    assert writer.calls == []
    assert len(client.read_calls) == 1


def test_compute_graph_snapshots_writes_once_after_both_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    graph_snapshot = _graph_snapshot(graph_generation_id=3)
    impact_snapshot = _impact_snapshot()

    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_snapshot",
        lambda *args, **kwargs: events.append("graph") or graph_snapshot,
    )

    def build_context(*args: Any, **kwargs: Any) -> PropagationContext:
        assert args[2] == 3
        events.append("context")
        return _context(graph_generation_id=3)

    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        build_context,
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: events.append("propagation")
        or _propagation_result(graph_generation_id=3),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: events.append("impact") or impact_snapshot,
    )
    writer = RecordingSnapshotWriter(events)
    status_reader = StaticGraphStatusReader(_ready_status(graph_snapshot))

    result = compute_graph_snapshots(
        "cycle-1",
        "world-state-1",
        client=MagicMock(),
        regime_reader=StaticRegimeReader(),
        snapshot_writer=writer,
        graph_status_reader=status_reader,
    )

    assert status_reader.calls == 2
    assert result == (graph_snapshot, impact_snapshot)
    assert events == ["graph", "context", "propagation", "impact", "write"]
    assert writer.calls == [(graph_snapshot, impact_snapshot)]


def test_compute_graph_snapshots_rechecks_status_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    graph_snapshot = _graph_snapshot(graph_generation_id=3)
    ready_status = _ready_status(graph_snapshot)
    rebuilding_status = ready_status.model_copy(update={"graph_status": "rebuilding"})

    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_snapshot",
        lambda *args, **kwargs: events.append("graph") or graph_snapshot,
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: events.append("context") or _context(graph_generation_id=3),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: events.append("propagation")
        or _propagation_result(graph_generation_id=3),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: events.append("impact") or _impact_snapshot(),
    )
    writer = RecordingSnapshotWriter(events)
    status_reader = StaticGraphStatusReader([ready_status, rebuilding_status])

    with pytest.raises(RuntimeError, match="changed before snapshot publication"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            graph_status_reader=status_reader,
        )

    assert status_reader.calls == 2
    assert events == ["graph", "context", "propagation", "impact"]
    assert writer.calls == []


def test_compute_graph_snapshots_rejects_direct_ready_status_without_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    writer = RecordingSnapshotWriter()

    def fail_snapshot_build(*args: Any, **kwargs: Any) -> GraphSnapshot:
        raise AssertionError("snapshot build must not run")

    monkeypatch.setattr(snapshot_generator, "build_graph_snapshot", fail_snapshot_build)

    with pytest.raises(ValueError, match="graph_status_reader"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            graph_status=_ready_status(),
        )

    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


def test_compute_graph_snapshots_rejects_ambiguous_status_inputs() -> None:
    client = MagicMock()
    writer = RecordingSnapshotWriter()
    graph_status = _ready_status()

    with pytest.raises(ValueError, match="either graph_status or graph_status_reader"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            graph_status=graph_status,
            graph_status_reader=StaticGraphStatusReader(graph_status),
        )

    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


def test_compute_graph_snapshots_does_not_write_if_snapshot_build_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = RecordingSnapshotWriter()

    monkeypatch.setattr(snapshot_generator, "build_propagation_context", lambda *args, **kwargs: _context())
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: _propagation_result(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_snapshot",
        lambda *args, **kwargs: _graph_snapshot(),
    )

    def raise_impact_error(*args: Any, **kwargs: Any) -> GraphImpactSnapshot:
        raise ValueError("impact failed")

    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        raise_impact_error,
    )

    with pytest.raises(ValueError, match="impact failed"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            graph_status_reader=StaticGraphStatusReader(_ready_status()),
        )

    assert writer.calls == []


def test_compute_graph_snapshots_surfaces_writer_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snapshot_generator, "build_propagation_context", lambda *args, **kwargs: _context())
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: _propagation_result(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_snapshot",
        lambda *args, **kwargs: _graph_snapshot(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: _impact_snapshot(),
    )

    with pytest.raises(RuntimeError, match="writer failed"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            regime_reader=StaticRegimeReader(),
            snapshot_writer=RaisingSnapshotWriter(),
            graph_status_reader=StaticGraphStatusReader(_ready_status()),
        )


def _context(*, graph_generation_id: int = 1) -> PropagationContext:
    return PropagationContext(
        cycle_id="cycle-1",
        world_state_ref="world-state-1",
        graph_generation_id=graph_generation_id,
        enabled_channels=["fundamental"],
        channel_multipliers={"fundamental": 1.0},
        regime_multipliers={"fundamental": 1.0},
        decay_policy={},
        regime_context={},
    )


def _propagation_result(*, graph_generation_id: int = 1) -> PropagationResult:
    return PropagationResult(
        cycle_id="cycle-1",
        graph_generation_id=graph_generation_id,
        activated_paths=[
            {
                "source_node_id": "node-1",
                "target_node_id": "node-2",
                "score": 0.7,
            }
        ],
        impacted_entities=[{"node_id": "node-2", "score": 0.4}],
        channel_breakdown={"fundamental": {"path_count": 1}},
    )


def _graph_snapshot(
    *,
    graph_generation_id: int = 1,
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
        key_label_counts=key_label_counts or {"Entity": 2},
        checksum=checksum,
        created_at=NOW,
    )


def _ready_status(graph_snapshot: GraphSnapshot | None = None) -> Neo4jGraphStatus:
    if graph_snapshot is None:
        graph_snapshot = _graph_snapshot()
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=graph_snapshot.graph_generation_id,
        node_count=graph_snapshot.node_count,
        edge_count=graph_snapshot.edge_count,
        key_label_counts=graph_snapshot.key_label_counts,
        checksum=graph_snapshot.checksum,
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _impact_snapshot() -> GraphImpactSnapshot:
    return GraphImpactSnapshot(
        cycle_id="cycle-1",
        impact_snapshot_id="impact-1",
        regime_context_ref="world-state-1",
        activated_paths=[],
        impacted_entities=[],
        channel_breakdown={"fundamental": {}},
    )
