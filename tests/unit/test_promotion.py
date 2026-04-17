from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

import graph_engine.promotion.service as service_module
from graph_engine.client import Neo4jClient
from graph_engine.models import CandidateGraphDelta, Neo4jGraphStatus, PromotionPlan
from graph_engine.promotion import build_promotion_plan, promote_graph_deltas
from graph_engine.promotion.planner import validate_entity_anchors
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.status import GraphStatusManager

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class FakeCandidateReader:
    def __init__(self, deltas: list[CandidateGraphDelta]) -> None:
        self.deltas = deltas
        self.calls: list[tuple[str, str]] = []

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list[CandidateGraphDelta]:
        self.calls.append((cycle_id, selection_ref))
        return self.deltas


class FakeEntityReader:
    def __init__(self, existing_ids: set[str]) -> None:
        self.existing_ids = existing_ids
        self.calls: list[set[str]] = []

    def existing_entity_ids(self, entity_ids: set[str]) -> set[str]:
        self.calls.append(entity_ids)
        return self.existing_ids


class FakeCanonicalWriter:
    def __init__(self, calls: list[str] | None = None, fail: bool = False) -> None:
        self.calls = calls if calls is not None else []
        self.fail = fail
        self.plans: list[PromotionPlan] = []

    def write_canonical_records(self, plan: PromotionPlan) -> None:
        self.calls.append("canonical")
        self.plans.append(plan)
        if self.fail:
            raise RuntimeError("canonical write failed")


class InMemoryStatusStore:
    def __init__(self, status: Neo4jGraphStatus | None = None) -> None:
        self.status = status
        self.compare_calls: list[tuple[Neo4jGraphStatus | None, Neo4jGraphStatus]] = []

    def read_current_status(self) -> Neo4jGraphStatus | None:
        return self.status

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        self.compare_calls.append((expected_status, next_status))
        if self.status != expected_status:
            return False
        self.write_current_status(next_status)
        return True


class StaleBarrierStatusStore(InMemoryStatusStore):
    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        self.status = _status(graph_status="rebuilding")
        return super().compare_and_write_current_status(
            expected_status=expected_status,
            next_status=next_status,
        )


def _status_manager_from_store(store: InMemoryStatusStore) -> GraphStatusManager:
    return GraphStatusManager(store)


def test_validate_entity_anchors_checks_all_source_entities_once() -> None:
    deltas = [
        _delta("delta-1", "node_add", {"node": _node_payload()}, ["entity-1", "entity-2"]),
        _delta("delta-2", "node_add", {"node": _node_payload("node-2")}, ["entity-2"]),
    ]
    entity_reader = FakeEntityReader({"entity-1"})

    with pytest.raises(ValueError, match="entity-2"):
        validate_entity_anchors(deltas, entity_reader)

    assert entity_reader.calls == [{"entity-1", "entity-2"}]


def test_build_promotion_plan_parses_frozen_deltas_in_stable_order() -> None:
    deltas = [
        _delta("delta-4", "assertion_add", {"assertion": _assertion_payload()}),
        _delta("delta-1", "node_add", {"node": _node_payload()}),
        _delta("delta-3", "edge_update", {"edge": _edge_payload("edge-2")}),
        _delta("delta-2", "edge_add", {"edge": _edge_payload()}),
    ]

    plan = build_promotion_plan("cycle-1", "selection-1", deltas)

    assert plan.cycle_id == "cycle-1"
    assert plan.selection_ref == "selection-1"
    assert plan.delta_ids == ["delta-1", "delta-2", "delta-3", "delta-4"]
    assert [record.node_id for record in plan.node_records] == ["node-1"]
    assert [record.edge_id for record in plan.edge_records] == ["edge-1", "edge-2"]
    assert [record.assertion_id for record in plan.assertion_records] == ["assertion-1"]
    assert plan.created_at.tzinfo is not None


@pytest.mark.parametrize(
    ("delta_factory", "match"),
    [
        (
            lambda: _delta(
                "delta-1",
                "node_add",
                {"node": _node_payload()},
                validation_status="validated",
            ),
            "not frozen",
        ),
        (
            lambda: _delta(
                "delta-1",
                "node_add",
                {"node": _node_payload()},
                cycle_id="cycle-2",
            ),
            "cycle-2",
        ),
        (
            lambda: _delta("delta-1", "node_add", {"wrong": _node_payload()}),
            "payload must include 'node'",
        ),
        (
            lambda: _delta(
                "delta-1",
                "node_add",
                {"node": _node_payload(label="Unknown")},
            ),
            "unsupported node label",
        ),
        (
            lambda: _delta(
                "delta-1",
                "edge_add",
                {"edge": _edge_payload(relationship_type="UNKNOWN")},
            ),
            "unsupported relationship type",
        ),
        (
            lambda: _delta(
                "delta-1",
                "node_add",
                {"node": _node_payload(properties={"raw_text": "not allowed"})},
            ),
            "forbidden field 'raw_text'",
        ),
    ],
)
def test_build_promotion_plan_rejects_invalid_delta_contracts(
    delta_factory: Callable[[], CandidateGraphDelta],
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        build_promotion_plan("cycle-1", "selection-1", [delta_factory()])


def test_promote_graph_deltas_writes_canonical_before_live_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    store = InMemoryStatusStore(_status())

    def fake_sync_live_graph(plan: PromotionPlan, client: Neo4jClient) -> None:
        assert plan.delta_ids == ["delta-1"]
        assert client is mock_client
        calls.append("sync")

    mock_client = MagicMock(spec=Neo4jClient)
    monkeypatch.setattr(service_module, "sync_live_graph", fake_sync_live_graph)
    writer = FakeCanonicalWriter(calls)

    plan = promote_graph_deltas(
        "cycle-1",
        "selection-1",
        candidate_reader=FakeCandidateReader([
            _delta("delta-1", "node_add", {"node": _node_payload()}),
        ]),
        entity_reader=FakeEntityReader({"entity-1"}),
        canonical_writer=writer,
        client=mock_client,
        status_manager=_status_manager_from_store(store),
    )

    assert calls == ["canonical", "sync"]
    assert writer.plans == [plan]
    assert store.compare_calls == [(_status(), _status())]


def test_promote_graph_deltas_does_not_sync_when_canonical_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_live_graph = MagicMock()
    monkeypatch.setattr(service_module, "sync_live_graph", sync_live_graph)

    with pytest.raises(RuntimeError, match="canonical write failed"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=FakeCanonicalWriter(fail=True),
            client=MagicMock(spec=Neo4jClient),
            status_manager=_status_manager(),
        )

    sync_live_graph.assert_not_called()


def test_promote_graph_deltas_requires_status_manager_for_live_sync() -> None:
    writer = FakeCanonicalWriter()
    reader = FakeCandidateReader([
        _delta("delta-1", "node_add", {"node": _node_payload()}),
    ])

    with pytest.raises(ValueError, match="status_manager"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=reader,
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=writer,
            client=MagicMock(spec=Neo4jClient),
        )

    assert reader.calls == []
    assert writer.plans == []


def test_promote_graph_deltas_blocks_live_sync_when_graph_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_live_graph = MagicMock()
    monkeypatch.setattr(service_module, "sync_live_graph", sync_live_graph)
    writer = FakeCanonicalWriter()

    with pytest.raises(PermissionError, match="ready"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=writer,
            client=MagicMock(spec=Neo4jClient),
            status_manager=_status_manager(graph_status="rebuilding"),
        )

    assert writer.plans == []
    sync_live_graph.assert_not_called()


def test_promote_graph_deltas_rejects_stale_status_before_live_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_live_graph = MagicMock()
    monkeypatch.setattr(service_module, "sync_live_graph", sync_live_graph)
    writer = FakeCanonicalWriter()
    status_manager = _status_manager_from_store(StaleBarrierStatusStore(_status()))

    with pytest.raises(RuntimeError, match="writer barrier"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=writer,
            client=MagicMock(spec=Neo4jClient),
            status_manager=status_manager,
        )

    assert len(writer.plans) == 1
    sync_live_graph.assert_not_called()


def test_promote_graph_deltas_detects_status_change_during_live_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    store = InMemoryStatusStore(_status())

    def fake_sync_live_graph(plan: PromotionPlan, client: Neo4jClient) -> None:
        assert plan.delta_ids == ["delta-1"]
        calls.append("sync")
        store.status = _status(graph_status="rebuilding")

    monkeypatch.setattr(service_module, "sync_live_graph", fake_sync_live_graph)
    writer = FakeCanonicalWriter()

    with pytest.raises(RuntimeError, match="graph_status changed during promotion live sync"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=writer,
            client=MagicMock(spec=Neo4jClient),
            status_manager=_status_manager_from_store(store),
        )

    assert len(writer.plans) == 1
    assert calls == ["sync"]
    assert store.status == _status(graph_status="rebuilding")


def test_promote_graph_deltas_can_skip_live_graph_sync() -> None:
    writer = FakeCanonicalWriter()

    plan = promote_graph_deltas(
        "cycle-1",
        "selection-1",
        candidate_reader=FakeCandidateReader([
            _delta("delta-1", "node_add", {"node": _node_payload()}),
        ]),
        entity_reader=FakeEntityReader({"entity-1"}),
        canonical_writer=writer,
        sync_to_live_graph=False,
    )

    assert writer.plans == [plan]


def _status_manager(
    *,
    graph_status: str = "ready",
) -> GraphStatusManager:
    return GraphStatusManager(InMemoryStatusStore(_status(graph_status=graph_status)))


def _status(
    *,
    graph_status: str = "ready",
) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status=graph_status,
        graph_generation_id=3,
        node_count=2,
        edge_count=1,
        key_label_counts={"Entity": 2},
        checksum="abc123",
        last_verified_at=NOW if graph_status == "ready" else None,
        last_reload_at=None,
    )


def _delta(
    delta_id: str,
    delta_type: str,
    payload: dict[str, Any],
    source_entity_ids: list[str] | None = None,
    *,
    cycle_id: str = "cycle-1",
    validation_status: str = "frozen",
) -> CandidateGraphDelta:
    return CandidateGraphDelta(
        delta_id=delta_id,
        cycle_id=cycle_id,
        delta_type=delta_type,
        source_entity_ids=source_entity_ids or ["entity-1"],
        payload=payload,
        validation_status=validation_status,
    )


def _node_payload(
    node_id: str = "node-1",
    *,
    canonical_entity_id: str = "entity-1",
    label: str = NodeLabel.ENTITY.value,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "canonical_entity_id": canonical_entity_id,
        "label": label,
        "properties": properties or {"ticker": "ULT"},
        "created_at": NOW,
        "updated_at": NOW,
    }


def _edge_payload(
    edge_id: str = "edge-1",
    *,
    relationship_type: str = RelationshipType.SUPPLY_CHAIN.value,
) -> dict[str, Any]:
    return {
        "edge_id": edge_id,
        "source_node_id": "node-1",
        "target_node_id": "node-2",
        "relationship_type": relationship_type,
        "properties": {"source": "filing"},
        "weight": 0.7,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _assertion_payload() -> dict[str, Any]:
    return {
        "assertion_id": "assertion-1",
        "source_node_id": "node-1",
        "target_node_id": "node-2",
        "assertion_type": "risk",
        "evidence": {"source": "contract"},
        "confidence": 0.8,
        "created_at": NOW,
    }
