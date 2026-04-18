from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

import graph_engine.promotion.service as service_module
from graph_engine.client import Neo4jClient
from graph_engine.models import (
    CandidateGraphDelta,
    FrozenGraphDelta,
    Neo4jGraphStatus,
    PropagationResult,
    PromotionPlan,
)
from graph_engine.promotion import build_promotion_plan, promote_graph_deltas
from graph_engine.promotion.planner import validate_entity_anchors
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.snapshots import build_graph_impact_snapshot
from graph_engine.status import GraphStatusManager
from tests.fakes import InMemoryStatusStore

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class FakeCandidateReader:
    def __init__(self, deltas: list[FrozenGraphDelta]) -> None:
        self.deltas = deltas
        self.calls: list[tuple[str, str]] = []

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list[FrozenGraphDelta]:
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


class RebuildAfterSyncAcquireStore(InMemoryStatusStore):
    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        result = super().compare_and_write_current_status(
            expected_status=expected_status,
            next_status=next_status,
        )
        if (
            result
            and expected_status is not None
            and expected_status.graph_status == "ready"
            and next_status.writer_lock_token is not None
        ):
            self.status = _status(graph_status="rebuilding")
        return result


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


def test_build_promotion_plan_maps_delta_evidence_into_edge_and_assertion_records() -> None:
    deltas = [
        _delta(
            "delta-1",
            "edge_add",
            {
                "edge": _edge_payload(properties={"source": "filing"}),
                "evidence": ["fact-edge"],
            },
        ),
        _delta(
            "delta-2",
            "assertion_add",
            {
                "assertion": _assertion_payload(evidence={"source": "contract"}),
                "evidence": ["fact-assertion"],
            },
        ),
    ]

    plan = build_promotion_plan("cycle-1", "selection-1", deltas)

    assert plan.edge_records[0].properties["evidence_refs"] == ["fact-edge"]
    assert plan.assertion_records[0].evidence == {
        "source": "contract",
        "evidence_refs": ["fact-assertion"],
    }


def test_contract_delta_evidence_flows_through_promotion_to_impact_snapshot() -> None:
    contract_delta = CandidateGraphDelta(
        delta_id="contract-edge-1",
        delta_type="upsert_edge",
        source_node="node-1",
        target_node="node-2",
        relation_type=RelationshipType.SUPPLY_CHAIN.value,
        properties={
            "evidence_confidence": 0.9,
            "recency_decay": 1.0,
            "weight": 0.7,
        },
        evidence=["fact-contract-1"],
        subsystem_id="subsystem-news",
    )
    writer = FakeCanonicalWriter()

    plan = promote_graph_deltas(
        "cycle-1",
        "selection-1",
        candidate_reader=FakeCandidateReader([
            _delta(
                "delta-1",
                "edge_add",
                contract_delta.model_dump(),
                source_entity_ids=["entity-1", "entity-2"],
            ),
        ]),
        entity_reader=FakeEntityReader({"entity-1", "entity-2"}),
        canonical_writer=writer,
        sync_to_live_graph=False,
    )

    edge_record = plan.edge_records[0]
    assert writer.plans == [plan]
    assert edge_record.edge_id == contract_delta.delta_id
    assert edge_record.source_node_id == contract_delta.source_node
    assert edge_record.target_node_id == contract_delta.target_node
    assert edge_record.relationship_type == contract_delta.relation_type
    assert edge_record.properties["evidence_refs"] == ["fact-contract-1"]

    impact_snapshot = build_graph_impact_snapshot(
        "cycle-1",
        "world-state-1",
        PropagationResult(
            cycle_id="cycle-1",
            graph_generation_id=3,
            activated_paths=[
                {
                    "source_node_id": edge_record.source_node_id,
                    "source_labels": ["Entity"],
                    "target_node_id": edge_record.target_node_id,
                    "target_labels": ["Entity"],
                    "edge_id": edge_record.edge_id,
                    "relationship_type": edge_record.relationship_type,
                    "evidence_refs": edge_record.properties["evidence_refs"],
                    "score": 0.7,
                }
            ],
            impacted_entities=[
                {"node_id": edge_record.target_node_id, "labels": ["Entity"], "score": 0.7}
            ],
            channel_breakdown={"fundamental": {"path_count": 1}},
        ),
    )

    assert impact_snapshot.evidence_refs == ["fact-contract-1"]


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
    delta_factory: Callable[[], FrozenGraphDelta],
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
    assert len(store.compare_calls) == 2
    expected_ready, locked_status = store.compare_calls[0]
    finish_expected, finish_ready = store.compare_calls[1]
    assert expected_ready == _status()
    assert locked_status.graph_status == "ready"
    assert locked_status.writer_lock_token is not None
    assert finish_expected == locked_status
    assert finish_ready == _status()
    assert store.status == _status()


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

    with pytest.raises(RuntimeError, match="stale graph_status transition"):
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


def test_promote_graph_deltas_does_not_sync_if_rebuild_starts_after_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_live_graph = MagicMock()
    monkeypatch.setattr(service_module, "sync_live_graph", sync_live_graph)
    store = RebuildAfterSyncAcquireStore(_status())

    with pytest.raises(RuntimeError, match="before promotion live sync"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=FakeCanonicalWriter(),
            client=MagicMock(spec=Neo4jClient),
            status_manager=_status_manager_from_store(store),
        )

    assert sync_live_graph.call_count == 0
    assert store.status == _status(graph_status="rebuilding")


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

    with pytest.raises(RuntimeError, match="after promotion live sync"):
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


def test_promote_graph_deltas_marks_status_failed_when_live_sync_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_sync_live_graph(plan: PromotionPlan, client: Neo4jClient) -> None:
        raise RuntimeError("sync failed")

    store = InMemoryStatusStore(_status())
    monkeypatch.setattr(service_module, "sync_live_graph", fake_sync_live_graph)

    with pytest.raises(RuntimeError, match="sync failed"):
        promote_graph_deltas(
            "cycle-1",
            "selection-1",
            candidate_reader=FakeCandidateReader([
                _delta("delta-1", "node_add", {"node": _node_payload()}),
            ]),
            entity_reader=FakeEntityReader({"entity-1"}),
            canonical_writer=FakeCanonicalWriter(),
            client=MagicMock(spec=Neo4jClient),
            status_manager=_status_manager_from_store(store),
        )

    assert store.status is not None
    assert store.status.graph_status == "failed"


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
    writer_lock_token: str | None = None,
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
        writer_lock_token=writer_lock_token,
    )


def _delta(
    delta_id: str,
    delta_type: str,
    payload: dict[str, Any],
    source_entity_ids: list[str] | None = None,
    *,
    cycle_id: str = "cycle-1",
    validation_status: str = "frozen",
) -> FrozenGraphDelta:
    return FrozenGraphDelta(
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
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "edge_id": edge_id,
        "source_node_id": "node-1",
        "target_node_id": "node-2",
        "relationship_type": relationship_type,
        "properties": (
            {"source": "filing", "evidence_refs": [f"fact-{edge_id}"]}
            if properties is None
            else properties
        ),
        "weight": 0.7,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _assertion_payload(*, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "assertion_id": "assertion-1",
        "source_node_id": "node-1",
        "target_node_id": "node-2",
        "assertion_type": "risk",
        "evidence": {"source": "contract"} if evidence is None else evidence,
        "confidence": 0.8,
        "created_at": NOW,
    }
