"""Adapt contract candidate deltas into canonical graph promotion plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Literal

from contracts.schemas import CandidateGraphDelta

from graph_engine.models import (
    FrozenGraphDelta,
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
_PromotionDeltaType = Literal["node_add", "edge_add", "edge_update", "assertion_add"]
_CONTRACT_DELTA_TYPES: dict[str, _PromotionDeltaType] = {
    "add_relation": "edge_add",
    "create_relation": "edge_add",
    "edge_add": "edge_add",
    "edge_update": "edge_update",
    "update_relation": "edge_update",
    "upsert_edge": "edge_update",
    "upsert_relation": "edge_update",
}
_CONTRACT_RELATIONSHIP_TYPES = {
    "assertion_link": RelationshipType.ASSERTION_LINK.value,
    "belongs_to_sector": RelationshipType.SECTOR_MEMBERSHIP.value,
    "event_impact": RelationshipType.EVENT_IMPACT.value,
    "industry_chain": RelationshipType.INDUSTRY_CHAIN.value,
    "ownership": RelationshipType.OWNERSHIP.value,
    "sector_membership": RelationshipType.SECTOR_MEMBERSHIP.value,
    "supply_chain": RelationshipType.SUPPLY_CHAIN.value,
}


def adapt_candidate_graph_delta(
    delta: CandidateGraphDelta,
    cycle_id: str,
) -> FrozenGraphDelta:
    """Translate a contracts-defined delta into the internal promotion record."""

    promotion_delta_type = _contract_promotion_delta_type(delta)
    relationship_type = _contract_relationship_type(delta)
    created_at = datetime.now(timezone.utc)
    properties = _contract_edge_properties(delta)

    return FrozenGraphDelta(
        delta_id=delta.delta_id,
        cycle_id=cycle_id,
        delta_type=promotion_delta_type,
        source_entity_ids=_contract_endpoint_entity_ids(delta),
        payload={
            "edge": {
                "edge_id": _contract_edge_id(delta, relationship_type),
                "source_node_id": delta.source_node,
                "target_node_id": delta.target_node,
                "relationship_type": relationship_type,
                "properties": properties,
                "weight": _contract_edge_weight(delta, properties),
                "created_at": created_at,
                "updated_at": created_at,
            },
        },
        validation_status="frozen",
    )


def validate_entity_anchors(
    deltas: Sequence[FrozenGraphDelta],
    entity_reader: EntityAnchorReader,
) -> None:
    """Fail if any entity anchors referenced by deltas are missing."""

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


def _contract_promotion_delta_type(delta: CandidateGraphDelta) -> _PromotionDeltaType:
    delta_type = _CONTRACT_DELTA_TYPES.get(delta.delta_type.strip().lower())
    if delta_type is None:
        raise ValueError(
            f"contract delta {delta.delta_id} uses unsupported delta_type "
            f"{delta.delta_type!r}",
        )
    return delta_type


def _contract_relationship_type(delta: CandidateGraphDelta) -> str:
    relation_type = delta.relation_type.strip()
    if relation_type in _VALID_RELATIONSHIP_TYPES:
        return relation_type

    mapped_relation_type = _CONTRACT_RELATIONSHIP_TYPES.get(relation_type.lower())
    if mapped_relation_type is not None:
        return mapped_relation_type

    return relation_type


def _contract_edge_id(delta: CandidateGraphDelta, relationship_type: str) -> str:
    return "|".join((delta.source_node, relationship_type, delta.target_node))


def _contract_endpoint_entity_ids(delta: CandidateGraphDelta) -> list[str]:
    if delta.source_node == delta.target_node:
        return [delta.source_node]
    return [delta.source_node, delta.target_node]


def _contract_edge_properties(delta: CandidateGraphDelta) -> dict[str, Any]:
    properties = dict(delta.properties)
    properties["contract_delta_id"] = delta.delta_id
    properties["contract_delta_type"] = delta.delta_type
    properties["evidence_refs"] = list(delta.evidence)
    properties["subsystem_id"] = delta.subsystem_id
    return properties


def _contract_edge_weight(
    delta: CandidateGraphDelta,
    properties: Mapping[str, Any],
) -> float:
    weight = properties.get("weight", 1.0)
    if isinstance(weight, bool) or not isinstance(weight, int | float):
        raise ValueError(
            f"contract delta {delta.delta_id} property 'weight' must be numeric",
        )
    return float(weight)


def build_promotion_plan(
    cycle_id: str,
    selection_ref: str,
    deltas: Sequence[FrozenGraphDelta],
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


def _validate_delta_header(delta: FrozenGraphDelta, cycle_id: str) -> None:
    if delta.cycle_id != cycle_id:
        raise ValueError(
            f"delta {delta.delta_id} belongs to cycle {delta.cycle_id!r}, "
            f"not {cycle_id!r}",
        )
    if delta.validation_status != "frozen":
        raise ValueError(f"delta {delta.delta_id} is not frozen")


def _parse_node_record(delta: FrozenGraphDelta) -> GraphNodeRecord:
    payload = _required_payload_section(delta, "node")
    node_record = GraphNodeRecord.model_validate(payload)
    if node_record.label not in _VALID_NODE_LABELS:
        raise ValueError(
            f"delta {delta.delta_id} uses unsupported node label {node_record.label!r}",
        )
    return node_record


def _parse_edge_record(delta: FrozenGraphDelta) -> GraphEdgeRecord:
    payload = _required_payload_section(delta, "edge")
    edge_record = GraphEdgeRecord.model_validate(payload)
    if edge_record.relationship_type not in _VALID_RELATIONSHIP_TYPES:
        raise ValueError(
            "delta "
            f"{delta.delta_id} uses unsupported relationship type "
            f"{edge_record.relationship_type!r}",
        )
    return edge_record


def _parse_assertion_record(delta: FrozenGraphDelta) -> GraphAssertionRecord:
    return GraphAssertionRecord.model_validate(
        _required_payload_section(delta, "assertion"),
    )


def _required_payload_section(
    delta: FrozenGraphDelta,
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
