from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from graph_engine.models import (
    CandidateGraphDelta,
    GraphAssertionRecord,
    GraphEdgeRecord,
    GraphImpactSnapshot,
    GraphNodeRecord,
    GraphSnapshot,
    Neo4jGraphStatus,
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


def _model_payloads() -> list[tuple[type[BaseModel], dict[str, Any]]]:
    return [
        (
            GraphNodeRecord,
            {
                "node_id": "node-1",
                "canonical_entity_id": "entity-1",
                "label": "Entity",
                "properties": {"ticker": "ULT"},
                "created_at": NOW,
                "updated_at": NOW,
            },
        ),
        (
            GraphEdgeRecord,
            {
                "edge_id": "edge-1",
                "source_node_id": "node-1",
                "target_node_id": "node-2",
                "relationship_type": "SUPPLY_CHAIN",
                "properties": {"source": "filing"},
                "weight": 0.7,
                "created_at": NOW,
                "updated_at": NOW,
            },
        ),
        (
            GraphAssertionRecord,
            {
                "assertion_id": "assertion-1",
                "source_node_id": "node-1",
                "target_node_id": None,
                "assertion_type": "risk",
                "evidence": {"source": "contract"},
                "confidence": 0.8,
                "created_at": NOW,
            },
        ),
        (
            CandidateGraphDelta,
            {
                "delta_id": "delta-1",
                "cycle_id": "cycle-1",
                "delta_type": "node_add",
                "source_entity_ids": ["entity-1"],
                "payload": {"node_id": "node-1"},
                "validation_status": "frozen",
            },
        ),
        (
            GraphSnapshot,
            {
                "cycle_id": "cycle-1",
                "snapshot_id": "snapshot-1",
                "graph_generation_id": 1,
                "node_count": 10,
                "edge_count": 12,
                "key_label_counts": {"Entity": 10},
                "checksum": "abc123",
                "created_at": NOW,
            },
        ),
        (
            GraphImpactSnapshot,
            {
                "cycle_id": "cycle-1",
                "impact_snapshot_id": "impact-1",
                "regime_context_ref": "world-state-1",
                "activated_paths": [{"path": ["node-1", "node-2"]}],
                "impacted_entities": [{"entity_id": "entity-2", "score": 0.5}],
                "channel_breakdown": {"fundamental": {"score": 0.5}},
            },
        ),
        (
            Neo4jGraphStatus,
            {
                "graph_status": "ready",
                "graph_generation_id": 1,
                "node_count": 10,
                "edge_count": 12,
                "key_label_counts": {"Entity": 10},
                "checksum": "abc123",
                "last_verified_at": NOW,
                "last_reload_at": None,
            },
        ),
    ]


@pytest.mark.parametrize(("model_type", "payload"), _model_payloads())
def test_models_round_trip_json(model_type: type[BaseModel], payload: dict[str, Any]) -> None:
    model = model_type.model_validate(payload)

    restored = model_type.model_validate_json(model.model_dump_json())

    assert restored == model


def test_edge_weight_defaults_to_one() -> None:
    edge = GraphEdgeRecord(
        edge_id="edge-1",
        source_node_id="node-1",
        target_node_id="node-2",
        relationship_type="OWNERSHIP",
        properties={},
        created_at=NOW,
        updated_at=NOW,
    )

    assert edge.weight == 1.0


def test_candidate_delta_rejects_invalid_validation_status() -> None:
    with pytest.raises(ValidationError):
        CandidateGraphDelta(
            delta_id="delta-1",
            cycle_id="cycle-1",
            delta_type="node_add",
            source_entity_ids=["entity-1"],
            payload={},
            validation_status="invalid",
        )


def test_candidate_delta_rejects_invalid_delta_type() -> None:
    with pytest.raises(ValidationError):
        CandidateGraphDelta(
            delta_id="delta-1",
            cycle_id="cycle-1",
            delta_type="node_delete",
            source_entity_ids=["entity-1"],
            payload={},
            validation_status="validated",
        )


def test_assertion_confidence_is_bounded() -> None:
    with pytest.raises(ValidationError):
        GraphAssertionRecord(
            assertion_id="assertion-1",
            source_node_id="node-1",
            target_node_id="node-2",
            assertion_type="risk",
            evidence={},
            confidence=1.5,
            created_at=NOW,
        )


def test_graph_snapshot_rejects_negative_counts() -> None:
    with pytest.raises(ValidationError):
        GraphSnapshot(
            cycle_id="cycle-1",
            snapshot_id="snapshot-1",
            graph_generation_id=1,
            node_count=-1,
            edge_count=0,
            key_label_counts={},
            checksum="abc123",
            created_at=NOW,
        )


def test_models_forbid_unexpected_fields() -> None:
    with pytest.raises(ValidationError):
        GraphNodeRecord(
            node_id="node-1",
            canonical_entity_id="entity-1",
            label="Entity",
            properties={},
            created_at=NOW,
            updated_at=NOW,
            raw_text="not allowed",
        )
