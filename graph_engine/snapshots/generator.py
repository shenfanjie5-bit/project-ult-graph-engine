"""Graph and impact snapshot generation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.models import (
    GraphImpactSnapshot,
    GraphSnapshot,
    Neo4jGraphStatus,
    PropagationResult,
)
from graph_engine.propagation.context import RegimeContextReader, build_propagation_context
from graph_engine.propagation.fundamental import run_fundamental_propagation
from graph_engine.snapshots.writer import SnapshotWriter


def build_graph_snapshot(
    cycle_id: str,
    graph_generation_id: int,
    client: Neo4jClient,
) -> GraphSnapshot:
    """Read live graph metrics and return a deterministic structural snapshot."""

    node_count, edge_count = _read_graph_counts(client)
    key_label_counts = _read_key_label_counts(client)
    checksum = _read_graph_checksum(client, node_count, edge_count)
    return GraphSnapshot(
        cycle_id=cycle_id,
        snapshot_id=f"graph-snapshot-{cycle_id}-{graph_generation_id}-{checksum[:12]}",
        graph_generation_id=graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
        created_at=datetime.now(timezone.utc),
    )


def build_graph_impact_snapshot(
    cycle_id: str,
    world_state_ref: str,
    propagation_result: PropagationResult,
) -> GraphImpactSnapshot:
    """Build the downstream impact snapshot from a propagation result."""

    payload = {
        "cycle_id": cycle_id,
        "graph_generation_id": propagation_result.graph_generation_id,
        "world_state_ref": world_state_ref,
        "activated_paths": propagation_result.activated_paths,
        "impacted_entities": propagation_result.impacted_entities,
        "channel_breakdown": propagation_result.channel_breakdown,
    }
    impact_hash = _checksum_payload(payload)
    return GraphImpactSnapshot(
        cycle_id=cycle_id,
        impact_snapshot_id=f"graph-impact-{cycle_id}-{impact_hash[:12]}",
        regime_context_ref=world_state_ref,
        activated_paths=propagation_result.activated_paths,
        impacted_entities=propagation_result.impacted_entities,
        channel_breakdown=propagation_result.channel_breakdown,
    )


def compute_graph_snapshots(
    cycle_id: str,
    world_state_ref: str,
    *,
    client: Neo4jClient,
    graph_generation_id: int,
    regime_reader: RegimeContextReader,
    snapshot_writer: SnapshotWriter,
    graph_status: Neo4jGraphStatus | None = None,
    graph_name: str = "graph_engine_fundamental",
) -> tuple[GraphSnapshot, GraphImpactSnapshot]:
    """Run fundamental propagation and write graph plus impact snapshots."""

    context = build_propagation_context(
        cycle_id,
        world_state_ref,
        graph_generation_id,
        regime_reader=regime_reader,
        graph_status=graph_status,
    )
    propagation_result = run_fundamental_propagation(
        context,
        client,
        graph_name=graph_name,
    )
    graph_snapshot = build_graph_snapshot(cycle_id, graph_generation_id, client)
    impact_snapshot = build_graph_impact_snapshot(
        cycle_id,
        world_state_ref,
        propagation_result,
    )
    snapshot_writer.write_snapshots(graph_snapshot, impact_snapshot)
    return graph_snapshot, impact_snapshot


def _read_graph_counts(client: Neo4jClient) -> tuple[int, int]:
    rows = client.execute_read(
        """
MATCH (node)
WITH count(node) AS node_count
OPTIONAL MATCH ()-[relationship]->()
RETURN node_count, count(relationship) AS edge_count
""",
    )
    if not rows:
        return 0, 0
    row = rows[0]
    return int(row.get("node_count", 0)), int(row.get("edge_count", 0))


def _read_key_label_counts(client: Neo4jClient) -> dict[str, int]:
    rows = client.execute_read(
        """
MATCH (node)
UNWIND labels(node) AS label
RETURN label, count(*) AS count
ORDER BY label ASC
""",
    )
    return {
        str(row["label"]): int(row["count"])
        for row in rows
        if row.get("label") is not None
    }


def _read_graph_checksum(
    client: Neo4jClient,
    node_count: int,
    edge_count: int,
) -> str:
    node_rows = client.execute_read(
        """
MATCH (node)
WITH node
ORDER BY coalesce(node.node_id, elementId(node)) ASC
RETURN collect({
    labels: labels(node),
    node_id: node.node_id,
    canonical_entity_id: node.canonical_entity_id,
    properties: properties(node)
}) AS nodes
""",
    )
    relationship_rows = client.execute_read(
        """
MATCH (source)-[relationship]->(target)
WITH source, relationship, target
ORDER BY coalesce(relationship.edge_id, elementId(relationship)) ASC
RETURN collect({
    source_node_id: source.node_id,
    target_node_id: target.node_id,
    relationship_type: type(relationship),
    edge_id: relationship.edge_id,
    properties: properties(relationship)
}) AS relationships
""",
    )
    payload = {
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes": _sorted_payload_list(_first_list(node_rows, "nodes")),
        "relationships": _sorted_payload_list(
            _first_list(relationship_rows, "relationships"),
        ),
    }
    return _checksum_payload(payload)


def _first_list(rows: list[dict[str, Any]], key: str) -> list[Any]:
    if not rows:
        return []
    value = rows[0].get(key)
    if not isinstance(value, list):
        return []
    return value


def _sorted_payload_list(values: list[Any]) -> list[Any]:
    normalized_values = [_jsonable(value) for value in values]
    return sorted(normalized_values, key=_stable_json)


def _checksum_payload(payload: Any) -> str:
    encoded = _stable_json(_jsonable(payload)).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested_value in sorted(value.items(), key=lambda item: str(item[0])):
            string_key = str(key)
            if string_key == "labels" and isinstance(nested_value, list):
                normalized[string_key] = sorted(
                    (_jsonable(item) for item in nested_value),
                    key=_stable_json,
                )
            else:
                normalized[string_key] = _jsonable(nested_value)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted((_jsonable(item) for item in value), key=_stable_json)

    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)
