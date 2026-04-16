from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
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


class StaticStatusReader:
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


def test_compute_graph_snapshots_requires_status_source_without_side_effects() -> None:
    client = MagicMock()
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()

    with pytest.raises(ValueError, match="graph_status or status_reader"):
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

    with pytest.raises(PermissionError, match="ready"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=client,
            graph_generation_id=1,
            regime_reader=reader,
            snapshot_writer=writer,
            graph_status=_ready_status(
                graph_status="failed",
                node_count=0,
                edge_count=0,
                key_label_counts={},
                checksum="failed",
            ),
        )

    assert reader.calls == []
    assert writer.calls == []
    client.execute_read.assert_not_called()
    client.execute_write.assert_not_called()


def test_compute_graph_snapshots_uses_status_reader_and_derives_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    status_reader = StaticStatusReader(_ready_status(graph_generation_id=7))
    writer = RecordingSnapshotWriter(events)

    _patch_metrics(monkeypatch, events)

    def build_context(
        cycle_id: str,
        world_state_ref: str,
        graph_generation_id: int,
        **kwargs: Any,
    ) -> PropagationContext:
        events.append(f"context:{graph_generation_id}")
        return _context(graph_generation_id=graph_generation_id)

    def run_propagation(
        context: PropagationContext,
        *args: Any,
        **kwargs: Any,
    ) -> PropagationResult:
        events.append(f"propagation:{context.graph_generation_id}")
        return _propagation_result(graph_generation_id=context.graph_generation_id)

    monkeypatch.setattr(snapshot_generator, "build_propagation_context", build_context)
    monkeypatch.setattr(snapshot_generator, "run_fundamental_propagation", run_propagation)

    graph_snapshot, impact_snapshot = compute_graph_snapshots(
        "cycle-1",
        "world-state-1",
        client=MagicMock(),
        regime_reader=StaticRegimeReader(),
        snapshot_writer=writer,
        status_reader=status_reader,
    )

    assert status_reader.calls == 2
    assert graph_snapshot.graph_generation_id == 7
    assert graph_snapshot.node_count == 2
    assert graph_snapshot.edge_count == 1
    assert graph_snapshot.key_label_counts == {"Entity": 2}
    assert graph_snapshot.checksum == "abc123"
    assert impact_snapshot.regime_context_ref == "world-state-1"
    assert events == ["metrics", "context:7", "propagation:7", "metrics", "write"]
    assert writer.calls == [(graph_snapshot, impact_snapshot)]


@pytest.mark.parametrize(
    ("field_name", "graph_generation_id", "status_updates", "expected_events"),
    [
        ("graph_generation_id", 2, {}, []),
        ("node_count", 1, {"node_count": 999}, ["metrics"]),
        ("edge_count", 1, {"edge_count": 999}, ["metrics"]),
        ("key_label_counts", 1, {"key_label_counts": {"Entity": 999}}, ["metrics"]),
        ("checksum", 1, {"checksum": "stale"}, ["metrics"]),
    ],
)
def test_compute_graph_snapshots_rejects_status_disagreements_before_propagation(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    graph_generation_id: int,
    status_updates: dict[str, Any],
    expected_events: list[str],
) -> None:
    events: list[str] = []
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()
    graph_status = _ready_status(**status_updates)

    _patch_metrics(monkeypatch, events)

    def run_propagation(*args: Any, **kwargs: Any) -> PropagationResult:
        events.append("propagation")
        return _propagation_result()

    monkeypatch.setattr(snapshot_generator, "run_fundamental_propagation", run_propagation)

    with pytest.raises(ValueError, match=field_name):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            graph_generation_id=graph_generation_id,
            regime_reader=reader,
            snapshot_writer=writer,
            status_reader=StaticStatusReader(graph_status),
        )

    assert events == expected_events
    assert reader.calls == []
    assert writer.calls == []


def test_compute_graph_snapshots_rejects_direct_ready_status_without_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    reader = StaticRegimeReader()
    writer = RecordingSnapshotWriter()

    _patch_metrics(monkeypatch, events)

    with pytest.raises(ValueError, match="status_reader"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            graph_generation_id=1,
            regime_reader=reader,
            snapshot_writer=writer,
            graph_status=_ready_status(),
        )

    assert events == []
    assert reader.calls == []
    assert writer.calls == []


def test_compute_graph_snapshots_writes_once_after_both_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    impact_snapshot = _impact_snapshot()

    _patch_metrics(monkeypatch, events)
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: events.append("context") or _context(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: events.append("propagation") or _propagation_result(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: events.append("impact") or impact_snapshot,
    )
    writer = RecordingSnapshotWriter(events)

    result = compute_graph_snapshots(
        "cycle-1",
        "world-state-1",
        client=MagicMock(),
        graph_generation_id=1,
        regime_reader=StaticRegimeReader(),
        snapshot_writer=writer,
        status_reader=StaticStatusReader(_ready_status()),
    )

    graph_snapshot = result[0]
    assert result == (graph_snapshot, impact_snapshot)
    assert graph_snapshot.node_count == 2
    assert graph_snapshot.edge_count == 1
    assert graph_snapshot.checksum == "abc123"
    assert events == ["metrics", "context", "propagation", "impact", "metrics", "write"]
    assert writer.calls == [(graph_snapshot, impact_snapshot)]


def test_compute_graph_snapshots_rechecks_status_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    ready_status = _ready_status()
    rebuilding_status = _ready_status(graph_status="rebuilding")
    status_reader = StaticStatusReader([ready_status, rebuilding_status])
    writer = RecordingSnapshotWriter(events)

    _patch_metrics(monkeypatch, events)
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: events.append("context") or _context(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: events.append("propagation") or _propagation_result(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: events.append("impact") or _impact_snapshot(),
    )

    with pytest.raises(RuntimeError, match="changed before snapshot publication"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            graph_generation_id=1,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            status_reader=status_reader,
        )

    assert status_reader.calls == 2
    assert events == ["metrics", "context", "propagation", "impact"]
    assert writer.calls == []


def test_compute_graph_snapshots_rechecks_live_metrics_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    writer = RecordingSnapshotWriter(events)

    _patch_metrics(
        monkeypatch,
        events,
        metrics_sequence=[
            (2, 1, {"Entity": 2}, "abc123"),
            (3, 1, {"Entity": 3}, "changed"),
        ],
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: events.append("context") or _context(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: events.append("propagation") or _propagation_result(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "build_graph_impact_snapshot",
        lambda *args, **kwargs: events.append("impact") or _impact_snapshot(),
    )

    with pytest.raises(ValueError, match="live graph metrics disagree"):
        compute_graph_snapshots(
            "cycle-1",
            "world-state-1",
            client=MagicMock(),
            graph_generation_id=1,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            status_reader=StaticStatusReader(_ready_status()),
        )

    assert events == ["metrics", "context", "propagation", "impact", "metrics"]
    assert writer.calls == []


def test_compute_graph_snapshots_does_not_write_if_snapshot_build_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = RecordingSnapshotWriter()

    _patch_metrics(monkeypatch)
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: _context(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: _propagation_result(),
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
            graph_generation_id=1,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=writer,
            status_reader=StaticStatusReader(_ready_status()),
        )

    assert writer.calls == []


def test_compute_graph_snapshots_surfaces_writer_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_metrics(monkeypatch)
    monkeypatch.setattr(
        snapshot_generator,
        "build_propagation_context",
        lambda *args, **kwargs: _context(),
    )
    monkeypatch.setattr(
        snapshot_generator,
        "run_fundamental_propagation",
        lambda *args, **kwargs: _propagation_result(),
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
            graph_generation_id=1,
            regime_reader=StaticRegimeReader(),
            snapshot_writer=RaisingSnapshotWriter(),
            status_reader=StaticStatusReader(_ready_status()),
        )


def _patch_metrics(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str] | None = None,
    metrics_sequence: list[tuple[int, int, dict[str, int], str]] | None = None,
) -> None:
    metrics = metrics_sequence or [(2, 1, {"Entity": 2}, "abc123")]
    calls = 0

    def read_metrics(client: Any) -> tuple[int, int, dict[str, int], str]:
        nonlocal calls
        if events is not None:
            events.append("metrics")
        index = min(calls, len(metrics) - 1)
        calls += 1
        return metrics[index]

    monkeypatch.setattr(snapshot_generator, "_read_graph_metrics", read_metrics)


def _ready_status(
    *,
    graph_status: Literal["ready", "rebuilding", "failed"] = "ready",
    graph_generation_id: int = 1,
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
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _context(graph_generation_id: int = 1) -> PropagationContext:
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


def _propagation_result(graph_generation_id: int = 1) -> PropagationResult:
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


def _impact_snapshot() -> GraphImpactSnapshot:
    return GraphImpactSnapshot(
        cycle_id="cycle-1",
        impact_snapshot_id="impact-1",
        regime_context_ref="world-state-1",
        activated_paths=[],
        impacted_entities=[],
        channel_breakdown={"fundamental": {}},
    )
