"""Graph and impact snapshot generation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Protocol

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


class GraphStatusReader(Protocol):
    """Read the current live graph status from the status boundary."""

    def read_graph_status(self) -> Neo4jGraphStatus:
        """Return the current Neo4j graph status."""


def build_graph_snapshot(
    cycle_id: str,
    graph_generation_id: int,
    client: Neo4jClient,
) -> GraphSnapshot:
    """Read live graph metrics and return a deterministic structural snapshot."""

    node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
    return _graph_snapshot_from_metrics(
        cycle_id,
        graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
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
    graph_generation_id: int | None = None,
    regime_reader: RegimeContextReader,
    snapshot_writer: SnapshotWriter,
    graph_status: Neo4jGraphStatus | None = None,
    status_reader: GraphStatusReader | None = None,
    graph_name: str | None = None,
) -> tuple[GraphSnapshot, GraphImpactSnapshot]:
    """Run fundamental propagation and write graph plus impact snapshots."""

    ready_status = _resolve_ready_graph_status(graph_status, status_reader)
    _require_publication_status_reader(status_reader)
    _validate_generation_input(graph_generation_id, ready_status)
    node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
    _validate_status_metrics(
        ready_status,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )

    context = build_propagation_context(
        cycle_id,
        world_state_ref,
        ready_status.graph_generation_id,
        regime_reader=regime_reader,
        graph_status=ready_status,
    )
    propagation_result = run_fundamental_propagation(
        context,
        client,
        graph_name=graph_name,
    )
    graph_snapshot = _graph_snapshot_from_metrics(
        cycle_id,
        ready_status.graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )
    impact_snapshot = build_graph_impact_snapshot(
        cycle_id,
        world_state_ref,
        propagation_result,
    )
    _validate_publication_status_and_metrics(
        ready_status,
        status_reader,
        client,
    )
    snapshot_writer.write_snapshots(graph_snapshot, impact_snapshot)
    return graph_snapshot, impact_snapshot


def _resolve_ready_graph_status(
    graph_status: Neo4jGraphStatus | None,
    status_reader: GraphStatusReader | None,
) -> Neo4jGraphStatus:
    if graph_status is None:
        if status_reader is None:
            raise ValueError(
                "formal snapshot computation requires graph_status or status_reader",
            )
        graph_status = status_reader.read_graph_status()

    if graph_status.graph_status != "ready":
        raise PermissionError(
            "snapshot computation requires graph_status='ready'; "
            f"received {graph_status.graph_status!r}",
        )
    return graph_status


def _require_publication_status_reader(
    status_reader: GraphStatusReader | None,
) -> None:
    if status_reader is None:
        raise ValueError(
            "formal snapshot publication requires status_reader "
            "for write-boundary validation",
        )


def _validate_generation_input(
    graph_generation_id: int | None,
    graph_status: Neo4jGraphStatus,
) -> None:
    if graph_generation_id is None:
        return
    if graph_generation_id != graph_status.graph_generation_id:
        raise ValueError(
            "graph_generation_id disagrees with Neo4jGraphStatus: "
            f"received {graph_generation_id}, "
            f"status has {graph_status.graph_generation_id}",
        )


def _validate_status_metrics(
    graph_status: Neo4jGraphStatus,
    *,
    node_count: int,
    edge_count: int,
    key_label_counts: dict[str, int],
    checksum: str,
) -> None:
    mismatches: list[str] = []
    if graph_status.node_count != node_count:
        mismatches.append(
            f"node_count status={graph_status.node_count} live={node_count}",
        )
    if graph_status.edge_count != edge_count:
        mismatches.append(
            f"edge_count status={graph_status.edge_count} live={edge_count}",
        )
    if graph_status.key_label_counts != key_label_counts:
        mismatches.append(
            "key_label_counts "
            f"status={graph_status.key_label_counts!r} live={key_label_counts!r}",
        )
    if graph_status.checksum != checksum:
        mismatches.append(
            f"checksum status={graph_status.checksum!r} live={checksum!r}",
        )
    if mismatches:
        raise ValueError(
            "live graph metrics disagree with Neo4jGraphStatus: "
            + "; ".join(mismatches),
        )


def _validate_publication_status_and_metrics(
    ready_status: Neo4jGraphStatus,
    status_reader: GraphStatusReader | None,
    client: Neo4jClient,
) -> None:
    if status_reader is None:
        raise ValueError(
            "formal snapshot publication requires status_reader "
            "for write-boundary validation",
        )

    current_status = status_reader.read_graph_status()
    if current_status.graph_status != "ready":
        raise RuntimeError(
            "ready graph status changed before snapshot publication",
        )
    _validate_status_matches_ready_status(current_status, ready_status)

    node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
    _validate_status_metrics(
        current_status,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )


def _validate_status_matches_ready_status(
    current_status: Neo4jGraphStatus,
    ready_status: Neo4jGraphStatus,
) -> None:
    checks = (
        (
            "graph_generation_id",
            current_status.graph_generation_id,
            ready_status.graph_generation_id,
        ),
        ("node_count", current_status.node_count, ready_status.node_count),
        ("edge_count", current_status.edge_count, ready_status.edge_count),
        (
            "key_label_counts",
            current_status.key_label_counts,
            ready_status.key_label_counts,
        ),
        ("checksum", current_status.checksum, ready_status.checksum),
    )
    mismatches = [
        f"{field} current={current_value!r} ready={ready_value!r}"
        for field, current_value, ready_value in checks
        if current_value != ready_value
    ]
    if mismatches:
        raise RuntimeError(
            "ready graph status changed before snapshot publication: "
            + "; ".join(mismatches),
        )


def _graph_snapshot_from_metrics(
    cycle_id: str,
    graph_generation_id: int,
    *,
    node_count: int,
    edge_count: int,
    key_label_counts: dict[str, int],
    checksum: str,
) -> GraphSnapshot:
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


def _read_graph_metrics(client: Neo4jClient) -> tuple[int, int, dict[str, int], str]:
    rows = client.execute_read(
        """
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
""",
    )
    if not rows:
        node_count = 0
        edge_count = 0
        key_label_counts: dict[str, int] = {}
        nodes: list[Any] = []
        relationships: list[Any] = []
    else:
        row = rows[0]
        node_count = int(row.get("node_count", 0))
        edge_count = int(row.get("edge_count", 0))
        key_label_counts = _label_counts(row.get("label_counts"))
        nodes = _list_value(row.get("nodes"))
        relationships = _list_value(row.get("relationships"))

    payload = {
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes": _sorted_payload_list(nodes),
        "relationships": _sorted_payload_list(relationships),
    }
    return node_count, edge_count, key_label_counts, _checksum_payload(payload)


def _list_value(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []
    return value


def _label_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, list):
        return {}
    counts: dict[str, int] = {}
    for row in value:
        if not isinstance(row, Mapping):
            continue
        label = row.get("label")
        if label is None:
            continue
        counts[str(label)] = int(row.get("count", 0))
    return counts


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
