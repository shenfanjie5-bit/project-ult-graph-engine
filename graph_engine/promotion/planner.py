"""Build canonical graph promotion plans from frozen candidate deltas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from graph_engine.models import (
    CandidateGraphDelta,
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


def validate_entity_anchors(
    deltas: Sequence[FrozenGraphDelta],
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
    contract_delta = _contract_delta_from_payload(delta.payload)
    payload = _edge_payload(delta, contract_delta=contract_delta)
    edge_record = GraphEdgeRecord.model_validate(payload)
    if edge_record.relationship_type not in _VALID_RELATIONSHIP_TYPES:
        raise ValueError(
            "delta "
            f"{delta.delta_id} uses unsupported relationship type "
            f"{edge_record.relationship_type!r}",
        )
    return edge_record


def _parse_assertion_record(delta: FrozenGraphDelta) -> GraphAssertionRecord:
    contract_delta = _contract_delta_from_payload(delta.payload)
    payload = dict(_required_payload_section(delta, "assertion"))
    evidence_refs = _delta_evidence_refs(delta, contract_delta)
    if evidence_refs:
        evidence = payload.get("evidence")
        evidence_mapping = dict(evidence) if isinstance(evidence, Mapping) else {}
        existing_refs = set(_evidence_refs_from_mapping(evidence_mapping))
        evidence_mapping["evidence_refs"] = sorted(existing_refs | set(evidence_refs))
        payload["evidence"] = evidence_mapping
    return GraphAssertionRecord.model_validate(payload)


def _edge_payload(
    delta: FrozenGraphDelta,
    *,
    contract_delta: CandidateGraphDelta | None,
) -> Mapping[str, Any]:
    payload_section = _optional_payload_section(delta, "edge")
    evidence_refs = _delta_evidence_refs(delta, contract_delta)
    if payload_section is not None:
        return _edge_payload_with_evidence_refs(payload_section, evidence_refs)
    if contract_delta is None:
        raise ValueError(f"delta {delta.delta_id} payload must include 'edge'")
    return _edge_payload_from_contract_delta(contract_delta, evidence_refs=evidence_refs)


def _edge_payload_from_contract_delta(
    contract_delta: CandidateGraphDelta,
    *,
    evidence_refs: list[str],
) -> dict[str, Any]:
    created_at = datetime.now(timezone.utc)
    properties = dict(contract_delta.properties)
    existing_refs = set(_evidence_refs_from_mapping(properties))
    properties["evidence_refs"] = sorted(existing_refs | set(evidence_refs))
    return {
        "edge_id": contract_delta.delta_id,
        "source_node_id": contract_delta.source_node,
        "target_node_id": contract_delta.target_node,
        "relationship_type": contract_delta.relation_type,
        "properties": properties,
        "weight": _edge_weight(properties),
        "created_at": created_at,
        "updated_at": created_at,
    }


def _edge_payload_with_evidence_refs(
    payload_section: Mapping[str, Any],
    evidence_refs: list[str],
) -> Mapping[str, Any]:
    if not evidence_refs:
        return payload_section
    payload = dict(payload_section)
    properties = payload.get("properties")
    if not isinstance(properties, Mapping):
        return payload
    existing_refs = set(_evidence_refs_from_mapping(properties))
    payload["properties"] = {
        **dict(properties),
        "evidence_refs": sorted(existing_refs | set(evidence_refs)),
    }
    return payload


def _required_payload_section(
    delta: FrozenGraphDelta,
    key: str,
) -> Mapping[str, Any]:
    payload_section = delta.payload.get(key)
    if not isinstance(payload_section, Mapping):
        raise ValueError(f"delta {delta.delta_id} payload must include {key!r}")
    return payload_section


def _optional_payload_section(
    delta: FrozenGraphDelta,
    key: str,
) -> Mapping[str, Any] | None:
    payload_section = delta.payload.get(key)
    if payload_section is None:
        return None
    if not isinstance(payload_section, Mapping):
        raise ValueError(f"delta {delta.delta_id} payload section {key!r} must be a mapping")
    return payload_section


def _contract_delta_from_payload(payload: Mapping[str, Any]) -> CandidateGraphDelta | None:
    for key in ("contract_delta", "candidate_delta", "graph_delta"):
        candidate = payload.get(key)
        if isinstance(candidate, CandidateGraphDelta):
            return candidate
        if isinstance(candidate, Mapping):
            return CandidateGraphDelta.model_validate(candidate)

    if _looks_like_contract_delta(payload):
        return CandidateGraphDelta.model_validate(payload)
    return None


def _looks_like_contract_delta(payload: Mapping[str, Any]) -> bool:
    return {
        "delta_id",
        "delta_type",
        "source_node",
        "target_node",
        "relation_type",
        "properties",
        "evidence",
        "subsystem_id",
    } <= set(payload)


def _delta_evidence_refs(
    delta: FrozenGraphDelta,
    contract_delta: CandidateGraphDelta | None,
) -> list[str]:
    refs: set[str] = set()
    refs.update(_evidence_refs_from_value(delta.payload.get("evidence_refs")))
    refs.update(_evidence_refs_from_value(delta.payload.get("evidence_ref")))
    refs.update(_evidence_refs_from_value(delta.payload.get("evidence")))
    if contract_delta is not None:
        refs.update(_evidence_refs_from_value(contract_delta.evidence))
    return sorted(refs)


def _evidence_refs_from_mapping(mapping: Mapping[str, Any]) -> list[str]:
    refs = set(_evidence_refs_from_value(mapping.get("evidence_refs")))
    refs.update(_evidence_refs_from_value(mapping.get("evidence_ref")))
    return sorted(refs)


def _evidence_refs_from_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return sorted(str(item) for item in value if str(item))
    return [str(value)] if str(value) else []


def _edge_weight(properties: Mapping[str, Any]) -> float:
    weight = properties.get("weight")
    if weight is None:
        return 1.0
    return float(weight)


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
