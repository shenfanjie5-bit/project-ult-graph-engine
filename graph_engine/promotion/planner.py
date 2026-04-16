"""Build canonical graph promotion plans from frozen candidate deltas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from graph_engine.models import (
    CandidateGraphDelta,
    GraphAssertionRecord,
    GraphEdgeRecord,
    GraphNodeRecord,
    PromotionPlan,
)
from graph_engine.promotion.interfaces import EntityAnchorReader
from graph_engine.schema.definitions import NodeLabel, RelationshipType

_FORBIDDEN_PAYLOAD_FIELDS = {
    "chunk",
    "light_rag_artifact",
    "lightrag_artifact",
    "raw_text",
}
_VALID_NODE_LABELS = {label.value for label in NodeLabel}
_VALID_RELATIONSHIP_TYPES = {relationship.value for relationship in RelationshipType}


def validate_entity_anchors(
    deltas: Sequence[CandidateGraphDelta],
    entity_reader: EntityAnchorReader,
) -> None:
    """Fail if any source entity ids referenced by deltas are missing."""

    entity_ids = {
        entity_id
        for delta in deltas
        for entity_id in delta.source_entity_ids
    }
    existing_entity_ids = entity_reader.existing_entity_ids(entity_ids)
    missing_entity_ids = sorted(entity_ids - existing_entity_ids)
    if missing_entity_ids:
        raise ValueError(
            "missing entity anchors: " + ", ".join(missing_entity_ids),
        )


def build_promotion_plan(
    cycle_id: str,
    selection_ref: str,
    deltas: Sequence[CandidateGraphDelta],
) -> PromotionPlan:
    """Parse frozen candidate deltas into a stable promotion plan."""

    sorted_deltas = sorted(deltas, key=lambda delta: delta.delta_id)
    node_records: list[GraphNodeRecord] = []
    edge_records: list[GraphEdgeRecord] = []
    assertion_records: list[GraphAssertionRecord] = []

    for delta in sorted_deltas:
        _validate_delta_header(delta, cycle_id)
        _reject_forbidden_payload_fields(delta.payload)

        if delta.delta_type == "node_add":
            node_records.append(_parse_node_record(delta))
        elif delta.delta_type in {"edge_add", "edge_update"}:
            edge_records.append(_parse_edge_record(delta))
        elif delta.delta_type == "assertion_add":
            assertion_records.append(_parse_assertion_record(delta))

    return PromotionPlan(
        cycle_id=cycle_id,
        selection_ref=selection_ref,
        delta_ids=[delta.delta_id for delta in sorted_deltas],
        node_records=node_records,
        edge_records=edge_records,
        assertion_records=assertion_records,
        created_at=datetime.now(timezone.utc),
    )


def _validate_delta_header(delta: CandidateGraphDelta, cycle_id: str) -> None:
    if delta.cycle_id != cycle_id:
        raise ValueError(
            f"delta {delta.delta_id} belongs to cycle {delta.cycle_id!r}, "
            f"not {cycle_id!r}",
        )
    if delta.validation_status != "frozen":
        raise ValueError(f"delta {delta.delta_id} is not frozen")


def _parse_node_record(delta: CandidateGraphDelta) -> GraphNodeRecord:
    payload = _required_payload_section(delta, "node")
    node_record = GraphNodeRecord.model_validate(payload)
    if node_record.label not in _VALID_NODE_LABELS:
        raise ValueError(
            f"delta {delta.delta_id} uses unsupported node label {node_record.label!r}",
        )
    return node_record


def _parse_edge_record(delta: CandidateGraphDelta) -> GraphEdgeRecord:
    payload = _required_payload_section(delta, "edge")
    edge_record = GraphEdgeRecord.model_validate(payload)
    if edge_record.relationship_type not in _VALID_RELATIONSHIP_TYPES:
        raise ValueError(
            "delta "
            f"{delta.delta_id} uses unsupported relationship type "
            f"{edge_record.relationship_type!r}",
        )
    return edge_record


def _parse_assertion_record(delta: CandidateGraphDelta) -> GraphAssertionRecord:
    return GraphAssertionRecord.model_validate(
        _required_payload_section(delta, "assertion"),
    )


def _required_payload_section(
    delta: CandidateGraphDelta,
    key: str,
) -> Mapping[str, Any]:
    payload_section = delta.payload.get(key)
    if not isinstance(payload_section, Mapping):
        raise ValueError(f"delta {delta.delta_id} payload must include {key!r}")
    return payload_section


def _reject_forbidden_payload_fields(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested_value in value.items():
            if isinstance(key, str) and key in _FORBIDDEN_PAYLOAD_FIELDS:
                raise ValueError(f"candidate payload includes forbidden field {key!r}")
            _reject_forbidden_payload_fields(nested_value)
        return

    if isinstance(value, list):
        for item in value:
            _reject_forbidden_payload_fields(item)
