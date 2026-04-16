"""Phase 0 consistency checks between canonical snapshots and the live graph."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from graph_engine.client import Neo4jClient
from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.snapshots.generator import _checksum_payload, _sorted_payload_list
from graph_engine.status.manager import GraphStatusManager

_LIVE_GRAPH_METRICS_QUERY = """
CALL {
    MATCH (node)
    WITH node
    ORDER BY coalesce(node.node_id, elementId(node)) ASC
    RETURN count(node) AS node_count,
           collect({
               labels: labels(node),
               node_id: node.node_id,
               canonical_entity_id: node.canonical_entity_id,
               properties: properties(node)
           }) AS nodes
}
CALL {
    MATCH (node)
    UNWIND labels(node) AS label
    WITH label, count(*) AS count
    ORDER BY label ASC
    RETURN collect({label: label, count: count}) AS label_counts
}
CALL {
    MATCH (source)-[relationship]->(target)
    WITH source, relationship, target
    ORDER BY coalesce(relationship.edge_id, elementId(relationship)) ASC
    RETURN count(relationship) AS edge_count,
           collect({
               source_node_id: source.node_id,
               target_node_id: target.node_id,
               relationship_type: type(relationship),
               edge_id: relationship.edge_id,
               properties: properties(relationship)
           }) AS relationships
}
RETURN node_count, edge_count, label_counts, nodes, relationships
"""


class CanonicalSnapshotReader(Protocol):
    """Read canonical graph snapshots from the data-platform/Iceberg boundary."""

    def read_graph_snapshot(self, snapshot_ref: str) -> GraphSnapshot:
        """Return the canonical snapshot referenced by snapshot_ref."""


def check_live_graph_consistency(
    snapshot_ref: str,
    *,
    client: Neo4jClient,
    snapshot_reader: CanonicalSnapshotReader,
    status_manager: GraphStatusManager | None = None,
    require_ready: bool = True,
) -> bool:
    """Return whether live Neo4j metrics match the canonical graph snapshot."""

    status = _resolve_status(status_manager, require_ready=require_ready)

    try:
        snapshot = snapshot_reader.read_graph_snapshot(snapshot_ref)
        node_count, edge_count, key_label_counts, checksum = _read_live_graph_metrics(client)
    except Exception:  # noqa: BLE001 - consistency checks fail closed.
        return False

    if status is not None and snapshot.graph_generation_id != status.graph_generation_id:
        return False
    return (
        snapshot.node_count == node_count
        and snapshot.edge_count == edge_count
        and snapshot.key_label_counts == key_label_counts
        and snapshot.checksum == checksum
    )


def _resolve_status(
    status_manager: GraphStatusManager | None,
    *,
    require_ready: bool,
) -> Neo4jGraphStatus | None:
    if require_ready:
        if status_manager is None:
            raise ValueError("status_manager is required when require_ready=True")
        return status_manager.require_ready()

    if status_manager is None:
        return None
    try:
        return status_manager.get_status()
    except LookupError:
        return None


def _read_live_graph_metrics(client: Neo4jClient) -> tuple[int, int, dict[str, int], str]:
    rows = client.execute_read(_LIVE_GRAPH_METRICS_QUERY)
    row = _single_metrics_row(rows)
    node_count = _int_field(row, "node_count")
    edge_count = _int_field(row, "edge_count")
    key_label_counts = _label_counts(row["label_counts"])
    nodes = _list_field(row, "nodes")
    relationships = _list_field(row, "relationships")

    payload = {
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes": _sorted_payload_list(nodes),
        "relationships": _sorted_payload_list(relationships),
    }
    return node_count, edge_count, key_label_counts, _checksum_payload(payload)


def _single_metrics_row(rows: object) -> Mapping[str, Any]:
    if not isinstance(rows, list) or len(rows) != 1:
        raise ValueError("Neo4j graph metrics query returned malformed rows")
    row = rows[0]
    if not isinstance(row, Mapping):
        raise ValueError("Neo4j graph metrics query returned a malformed row")
    required_fields = {"node_count", "edge_count", "label_counts", "nodes", "relationships"}
    if not required_fields <= row.keys():
        raise ValueError("Neo4j graph metrics query returned an incomplete row")
    return row


def _int_field(row: Mapping[str, Any], field_name: str) -> int:
    value = row[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Neo4j graph metrics field {field_name!r} is malformed")
    if value < 0:
        raise ValueError(f"Neo4j graph metrics field {field_name!r} cannot be negative")
    return value


def _list_field(row: Mapping[str, Any], field_name: str) -> list[Any]:
    value = row[field_name]
    if not isinstance(value, list):
        raise ValueError(f"Neo4j graph metrics field {field_name!r} is malformed")
    return value


def _label_counts(value: object) -> dict[str, int]:
    if not isinstance(value, list):
        raise ValueError("Neo4j graph metrics field 'label_counts' is malformed")

    counts: dict[str, int] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError("Neo4j graph metrics field 'label_counts' is malformed")
        label = item.get("label")
        count = item.get("count")
        if not isinstance(label, str):
            raise ValueError("Neo4j graph metrics label count is missing a string label")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("Neo4j graph metrics label count is malformed")
        counts[label] = count
    return counts
