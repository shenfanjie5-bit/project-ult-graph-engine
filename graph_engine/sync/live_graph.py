"""Incremental Neo4j live graph synchronization for promotion plans."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.models import GraphAssertionRecord, GraphEdgeRecord, GraphNodeRecord, PromotionPlan
from graph_engine.schema.definitions import NodeLabel, RelationshipType

_SAFE_PROPERTY_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_PROPERTY_NAMES = {
    "assertion_id",
    "assertion_type",
    "canonical_entity_id",
    "confidence",
    "created_at",
    "edge_id",
    "evidence",
    "label",
    "node_id",
    "properties",
    "relationship_type",
    "source_node_id",
    "target_node_id",
    "updated_at",
    "weight",
}


def sync_live_graph(
    promotion_batch: PromotionPlan,
    client: Neo4jClient,
    *,
    batch_size: int = 1000,
) -> None:
    """Mirror promoted canonical records into Neo4j with idempotent MERGE queries."""

    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")

    _validate_dynamic_identifiers(promotion_batch)
    _sync_node_records(promotion_batch.node_records, client, batch_size)
    _sync_edge_records(promotion_batch.edge_records, client, batch_size)
    _sync_assertion_records(promotion_batch.assertion_records, client, batch_size)


def _sync_node_records(
    node_records: Iterable[GraphNodeRecord],
    client: Neo4jClient,
    batch_size: int,
) -> None:
    rows_by_label: dict[NodeLabel, list[dict[str, Any]]] = defaultdict(list)
    for node_record in node_records:
        label = _node_label(node_record.label)
        rows_by_label[label].append(_node_row(node_record))

    for label, rows in rows_by_label.items():
        label_identifier = _quote_identifier(label.value)
        query = f"""
UNWIND $rows AS row
MERGE (n:{label_identifier} {{node_id: row.node_id}})
SET n.canonical_entity_id = row.canonical_entity_id,
    n.label = row.label,
    n.properties = row.properties,
    n.created_at = row.created_at,
    n.updated_at = row.updated_at
SET n += row.safe_properties
"""
        for batch in _batched(rows, batch_size):
            client.execute_write(query, {"rows": batch})


def _sync_edge_records(
    edge_records: Iterable[GraphEdgeRecord],
    client: Neo4jClient,
    batch_size: int,
) -> None:
    rows_by_type: dict[RelationshipType, list[dict[str, Any]]] = defaultdict(list)
    for edge_record in edge_records:
        relationship_type = _relationship_type(edge_record.relationship_type)
        rows_by_type[relationship_type].append(_edge_row(edge_record))

    for relationship_type, rows in rows_by_type.items():
        relationship_identifier = _quote_identifier(relationship_type.value)
        query = f"""
UNWIND $rows AS row
MATCH (source {{node_id: row.source_node_id}})
MATCH (target {{node_id: row.target_node_id}})
MERGE (source)-[r:{relationship_identifier} {{edge_id: row.edge_id}}]->(target)
SET r.relationship_type = row.relationship_type,
    r.weight = row.weight,
    r.properties = row.properties,
    r.created_at = row.created_at,
    r.updated_at = row.updated_at
SET r += row.safe_properties
"""
        for batch in _batched(rows, batch_size):
            client.execute_write(query, {"rows": batch})


def _sync_assertion_records(
    assertion_records: Iterable[GraphAssertionRecord],
    client: Neo4jClient,
    batch_size: int,
) -> None:
    rows = [_assertion_row(assertion_record) for assertion_record in assertion_records]
    if not rows:
        return

    assertion_label = _quote_identifier(NodeLabel.ASSERTION.value)
    assertion_link_type = _quote_identifier(RelationshipType.ASSERTION_LINK.value)
    source_query = f"""
UNWIND $rows AS row
MATCH (source {{node_id: row.source_node_id}})
MERGE (assertion:{assertion_label} {{node_id: row.assertion_id}})
SET assertion.assertion_id = row.assertion_id,
    assertion.assertion_type = row.assertion_type,
    assertion.confidence = row.confidence,
    assertion.evidence = row.evidence,
    assertion.source_node_id = row.source_node_id,
    assertion.target_node_id = row.target_node_id,
    assertion.created_at = row.created_at
MERGE (source)-[link:{assertion_link_type} {{
    assertion_id: row.assertion_id,
    role: "source"
}}]->(assertion)
SET link.assertion_id = row.assertion_id,
    link.role = "source",
    link.created_at = row.created_at
"""
    target_query = f"""
UNWIND $rows AS row
MATCH (assertion:{assertion_label} {{node_id: row.assertion_id}})
MATCH (target {{node_id: row.target_node_id}})
MERGE (assertion)-[link:{assertion_link_type} {{
    assertion_id: row.assertion_id,
    role: "target"
}}]->(target)
SET link.assertion_id = row.assertion_id,
    link.role = "target",
    link.created_at = row.created_at
"""

    for batch in _batched(rows, batch_size):
        client.execute_write(source_query, {"rows": batch})

    target_rows = [row for row in rows if row["target_node_id"] is not None]
    for batch in _batched(target_rows, batch_size):
        client.execute_write(target_query, {"rows": batch})


def _node_row(node_record: GraphNodeRecord) -> dict[str, Any]:
    return {
        "node_id": node_record.node_id,
        "canonical_entity_id": node_record.canonical_entity_id,
        "label": node_record.label,
        "properties": node_record.properties,
        "safe_properties": _safe_properties(node_record.properties),
        "created_at": node_record.created_at,
        "updated_at": node_record.updated_at,
    }


def _edge_row(edge_record: GraphEdgeRecord) -> dict[str, Any]:
    return {
        "edge_id": edge_record.edge_id,
        "source_node_id": edge_record.source_node_id,
        "target_node_id": edge_record.target_node_id,
        "relationship_type": edge_record.relationship_type,
        "weight": edge_record.weight,
        "properties": edge_record.properties,
        "safe_properties": _safe_properties(edge_record.properties),
        "created_at": edge_record.created_at,
        "updated_at": edge_record.updated_at,
    }


def _assertion_row(assertion_record: GraphAssertionRecord) -> dict[str, Any]:
    return {
        "assertion_id": assertion_record.assertion_id,
        "source_node_id": assertion_record.source_node_id,
        "target_node_id": assertion_record.target_node_id,
        "assertion_type": assertion_record.assertion_type,
        "evidence": assertion_record.evidence,
        "confidence": assertion_record.confidence,
        "created_at": assertion_record.created_at,
    }


def _safe_properties(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in properties.items()
        if _is_safe_property_name(key)
    }


def _is_safe_property_name(property_name: str) -> bool:
    return (
        property_name not in _RESERVED_PROPERTY_NAMES
        and _SAFE_PROPERTY_NAME_PATTERN.fullmatch(property_name) is not None
    )


def _validate_dynamic_identifiers(promotion_batch: PromotionPlan) -> None:
    for node_record in promotion_batch.node_records:
        _node_label(node_record.label)
    for edge_record in promotion_batch.edge_records:
        _relationship_type(edge_record.relationship_type)


def _node_label(value: str) -> NodeLabel:
    try:
        return NodeLabel(value)
    except ValueError as exc:
        raise ValueError(f"unsupported node label: {value!r}") from exc


def _relationship_type(value: str) -> RelationshipType:
    try:
        return RelationshipType(value)
    except ValueError as exc:
        raise ValueError(f"unsupported relationship type: {value!r}") from exc


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _batched(rows: list[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]
