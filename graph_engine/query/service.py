"""Status-gated read-only query APIs for the live Neo4j graph."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, get_args

from graph_engine.client import Neo4jClient
from graph_engine.models import GraphQueryResult, PropagationChannel, ReadonlySimulationRequest
from graph_engine.propagation.channels import effective_channel_expression
from graph_engine.status import GraphStatusManager

MAX_QUERY_DEPTH = 4
_ALLOWED_CHANNELS: frozenset[str] = frozenset(get_args(PropagationChannel))
_NODE_STRUCTURAL_PROPERTY_KEYS = frozenset(
    {
        "canonical_entity_id",
        "created_at",
        "label",
        "node_id",
        "properties_json",
        "updated_at",
    },
)
_EDGE_STRUCTURAL_PROPERTY_KEYS = frozenset(
    {
        "created_at",
        "edge_id",
        "properties_json",
        "relationship_type",
        "source_node_id",
        "target_node_id",
        "updated_at",
        "weight",
    },
)


def query_subgraph(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
    result_limit: int = 500,
    max_depth: int = MAX_QUERY_DEPTH,
) -> GraphQueryResult:
    """Return a deterministic bounded subgraph from a ready live graph."""

    seed_list = _validated_seed_entities(seed_entities)
    validated_depth = _validated_depth(depth, max_depth=max_depth)
    validated_limit = _validated_result_limit(result_limit)
    if status_manager is None:
        raise ValueError("query_subgraph requires status_manager")

    with status_manager.ready_read() as graph_status:
        nodes, edges = _read_subgraph(
            seed_list,
            validated_depth,
            client=client,
            result_limit=validated_limit,
        )
        return GraphQueryResult(
            graph_generation_id=graph_status.graph_generation_id,
            subgraph_nodes=nodes,
            subgraph_edges=edges,
            status="ready",
        )


def query_propagation_paths(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
    channels: list[str] | None = None,
    result_limit: int = 100,
    max_depth: int = MAX_QUERY_DEPTH,
) -> list[dict[str, Any]]:
    """Return read-only propagation path summaries from a ready live graph."""

    seed_list = _validated_seed_entities(seed_entities)
    validated_depth = _validated_depth(depth, max_depth=max_depth)
    validated_limit = _validated_result_limit(result_limit)
    channel_filter = _validated_channels(channels)
    if status_manager is None:
        raise ValueError("query_propagation_paths requires status_manager")

    with status_manager.ready_read():
        rows = client.execute_read(
            _path_query(validated_depth),
            {
                "channels": channel_filter,
                "result_limit": validated_limit,
                "seed_entities": seed_list,
            },
        )
    row = rows[0] if isinstance(rows, list) and rows else {}
    raw_paths = row.get("paths") if isinstance(row, dict) else None
    return _normalize_paths(raw_paths, channels=channel_filter, result_limit=validated_limit)


def simulate_readonly_impact(
    seed_entities: list[str],
    context: ReadonlySimulationRequest,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
) -> dict[str, Any]:
    """Run the existing read-only local impact adapter without mutating Neo4j."""

    seed_list = _validated_seed_entities(seed_entities)
    validated_depth = _validated_depth(context.depth, max_depth=MAX_QUERY_DEPTH)
    validated_limit = _validated_result_limit(context.result_limit)
    if status_manager is None:
        raise ValueError("simulate_readonly_impact requires status_manager")

    with status_manager.ready_read() as ready_status:
        if ready_status.graph_generation_id != context.graph_generation_id:
            raise ValueError(
                "ReadonlySimulationRequest graph_generation_id disagrees with Neo4jGraphStatus: "
                f"context={context.graph_generation_id}, "
                f"status={ready_status.graph_generation_id}",
            )
        nodes, edges = _read_subgraph(
            seed_list,
            validated_depth,
            client=client,
            result_limit=validated_limit,
        )

    subgraph = {
        "seed_entities": seed_list,
        "depth": validated_depth,
        "nodes": nodes,
        "relationships": edges,
    }
    impacted_entities = _impacted_entities_from_subgraph(
        nodes,
        set(seed_list),
        result_limit=validated_limit,
    )
    return {
        "cycle_id": context.cycle_id,
        "world_state_ref": context.world_state_ref,
        "graph_generation_id": context.graph_generation_id,
        "seed_entities": seed_list,
        "subgraph": subgraph,
        "impacted_entities": impacted_entities,
        "activated_paths": list(edges),
        "channel_breakdown": {
            "readonly": {
                "depth": validated_depth,
                "edge_count": len(edges),
                "node_count": len(nodes),
            },
        },
    }


def _read_subgraph(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    result_limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = client.execute_read(
        _subgraph_query(depth),
        {
            "result_limit": result_limit,
            "seed_entities": seed_entities,
        },
    )
    row = rows[0] if isinstance(rows, list) and rows else {}
    if not isinstance(row, dict):
        row = {}

    raw_nodes = row.get("nodes", row.get("subgraph_nodes"))
    raw_edges = row.get("relationships", row.get("subgraph_edges", row.get("edges")))
    return (
        _normalize_nodes(raw_nodes, result_limit=result_limit),
        _normalize_edges(raw_edges, result_limit=result_limit),
    )


def _subgraph_query(depth: int) -> str:
    return f"""
MATCH (seed)
WHERE seed.canonical_entity_id IN $seed_entities
   OR seed.node_id IN $seed_entities
OPTIONAL MATCH path = (seed)-[*0..{depth}]-(reachable)
WITH collect(path) AS paths
CALL {{
    WITH paths
    UNWIND paths AS graph_path
    WITH graph_path
    WHERE graph_path IS NOT NULL
    UNWIND nodes(graph_path) AS graph_node
    WITH DISTINCT graph_node
    ORDER BY coalesce(graph_node.node_id, ""),
             coalesce(graph_node.canonical_entity_id, "")
    LIMIT $result_limit
    RETURN collect({{
        node_id: graph_node.node_id,
        canonical_entity_id: graph_node.canonical_entity_id,
        labels: labels(graph_node),
        properties: properties(graph_node)
    }}) AS nodes
}}
CALL {{
    WITH paths
    UNWIND paths AS graph_path
    WITH graph_path
    WHERE graph_path IS NOT NULL
    UNWIND relationships(graph_path) AS relationship
    WITH DISTINCT relationship
    ORDER BY coalesce(relationship.edge_id, ""),
             coalesce(startNode(relationship).node_id, ""),
             type(relationship),
             coalesce(endNode(relationship).node_id, "")
    LIMIT $result_limit
    RETURN collect({{
        edge_id: relationship.edge_id,
        source_node_id: startNode(relationship).node_id,
        target_node_id: endNode(relationship).node_id,
        relationship_type: type(relationship),
        properties: properties(relationship),
        weight: coalesce(relationship.weight, 1.0)
    }}) AS relationships
}}
RETURN nodes, relationships
"""


def _path_query(depth: int) -> str:
    if depth == 0:
        return """
MATCH (seed)
WHERE seed.canonical_entity_id IN $seed_entities
   OR seed.node_id IN $seed_entities
WITH count(seed) AS seed_count
RETURN [] AS paths
"""

    channel_expression = effective_channel_expression("relationship")
    return f"""
MATCH (seed)
WHERE seed.canonical_entity_id IN $seed_entities
   OR seed.node_id IN $seed_entities
MATCH path = (seed)-[*1..{depth}]-(reachable)
UNWIND relationships(path) AS relationship
WITH relationship,
     {channel_expression} AS channel,
     length(path) AS path_length
WHERE channel IS NOT NULL
  AND ($channels IS NULL OR channel IN $channels)
WITH relationship,
     channel,
     min(path_length) AS path_length,
     toFloat(coalesce(relationship.weight, 1.0)) AS score
ORDER BY score DESC,
         path_length ASC,
         coalesce(relationship.edge_id, ""),
         coalesce(startNode(relationship).node_id, ""),
         type(relationship),
         coalesce(endNode(relationship).node_id, "")
LIMIT $result_limit
RETURN collect({{
    channel: channel,
    edge_id: relationship.edge_id,
    source_node_id: startNode(relationship).node_id,
    target_node_id: endNode(relationship).node_id,
    relationship_type: type(relationship),
    score: score,
    path_length: path_length,
    properties: properties(relationship)
}}) AS paths
"""


def _node_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    raw_properties = _raw_properties(value)
    return {
        "node_id": _text_or_none(value.get("node_id", raw_properties.get("node_id"))),
        "canonical_entity_id": _text_or_none(
            value.get("canonical_entity_id", raw_properties.get("canonical_entity_id")),
        ),
        "labels": _sorted_text_list(value.get("labels", raw_properties.get("labels", []))),
        "properties": _properties_payload(raw_properties, _NODE_STRUCTURAL_PROPERTY_KEYS),
    }


def _edge_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    raw_properties = _raw_properties(value)
    weight = _float_or_default(value.get("weight", raw_properties.get("weight")), 1.0)
    return {
        "edge_id": _text_or_none(value.get("edge_id", raw_properties.get("edge_id"))),
        "source_node_id": _text_or_none(
            value.get("source_node_id", raw_properties.get("source_node_id")),
        ),
        "target_node_id": _text_or_none(
            value.get("target_node_id", raw_properties.get("target_node_id")),
        ),
        "relationship_type": _text_or_none(
            value.get("relationship_type", raw_properties.get("relationship_type")),
        ),
        "properties": _properties_payload(raw_properties, _EDGE_STRUCTURAL_PROPERTY_KEYS),
        "weight": weight,
    }


def _path_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    raw_properties = _raw_properties(value)
    score = _float_or_default(value.get("score", raw_properties.get("weight")), 1.0)
    path_length = _int_or_default(value.get("path_length"), 1)
    return {
        "channel": _text_or_none(value.get("channel", raw_properties.get("channel"))),
        "edge_id": _text_or_none(value.get("edge_id", raw_properties.get("edge_id"))),
        "source_node_id": _text_or_none(
            value.get("source_node_id", raw_properties.get("source_node_id")),
        ),
        "target_node_id": _text_or_none(
            value.get("target_node_id", raw_properties.get("target_node_id")),
        ),
        "relationship_type": _text_or_none(
            value.get("relationship_type", raw_properties.get("relationship_type")),
        ),
        "score": score,
        "path_length": path_length,
        "properties": _properties_payload(raw_properties, _EDGE_STRUCTURAL_PROPERTY_KEYS),
    }


def _validated_seed_entities(seed_entities: list[str]) -> list[str]:
    if not isinstance(seed_entities, list) or not seed_entities:
        raise ValueError("seed_entities must not be empty")

    seen: set[str] = set()
    normalized: list[str] = []
    for seed_entity in seed_entities:
        if not isinstance(seed_entity, str):
            raise ValueError("seed_entities must contain strings")
        stripped = seed_entity.strip()
        if not stripped:
            raise ValueError("seed_entities must contain non-empty ids")
        if stripped not in seen:
            seen.add(stripped)
            normalized.append(stripped)
    return normalized


def _validated_depth(depth: int, *, max_depth: int) -> int:
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
        raise ValueError("max_depth must be a non-negative integer")
    if isinstance(depth, bool) or not isinstance(depth, int):
        raise ValueError("depth must be an integer")
    if depth < 0:
        raise ValueError("depth must be greater than or equal to zero")
    if depth > max_depth:
        raise ValueError(f"depth must be less than or equal to {max_depth}")
    return depth


def _validated_result_limit(result_limit: int) -> int:
    if isinstance(result_limit, bool) or not isinstance(result_limit, int):
        raise ValueError("result_limit must be an integer")
    if result_limit < 1:
        raise ValueError("result_limit must be greater than or equal to one")
    return result_limit


def _validated_channels(channels: list[str] | None) -> list[str] | None:
    if channels is None:
        return None
    if not isinstance(channels, list) or not channels:
        raise ValueError("channels must be a non-empty list when provided")

    seen: set[str] = set()
    normalized: list[str] = []
    for channel in channels:
        if not isinstance(channel, str):
            raise ValueError("channels must contain strings")
        stripped = channel.strip()
        if not stripped:
            raise ValueError("channels must contain non-empty values")
        if stripped not in _ALLOWED_CHANNELS:
            raise ValueError(f"unknown propagation channel: {stripped}")
        if stripped not in seen:
            seen.add(stripped)
            normalized.append(stripped)
    return normalized


def _normalize_nodes(value: Any, *, result_limit: int) -> list[dict[str, Any]]:
    nodes_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in _mapping_rows(value):
        node = _node_payload(item)
        key = _node_identity(node)
        if key not in nodes_by_key:
            nodes_by_key[key] = node
    nodes = list(nodes_by_key.values())
    nodes.sort(key=_node_sort_key)
    return nodes[:result_limit]


def _normalize_edges(value: Any, *, result_limit: int) -> list[dict[str, Any]]:
    edges_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in _mapping_rows(value):
        edge = _edge_payload(item)
        key = _edge_identity(edge)
        if key not in edges_by_key:
            edges_by_key[key] = edge
    edges = list(edges_by_key.values())
    edges.sort(key=_edge_sort_key)
    return edges[:result_limit]


def _normalize_paths(
    value: Any,
    *,
    channels: list[str] | None,
    result_limit: int,
) -> list[dict[str, Any]]:
    channel_set = set(channels) if channels is not None else None
    paths_by_key: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for item in _mapping_rows(value):
        path = _path_payload(item)
        channel = path.get("channel")
        if channel is None:
            continue
        if channel_set is not None and channel not in channel_set:
            continue
        key = _path_identity(path)
        if key not in paths_by_key:
            paths_by_key[key] = path
    paths = list(paths_by_key.values())
    paths.sort(key=_path_sort_key)
    return paths[:result_limit]


def _mapping_rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _raw_properties(value: Mapping[str, Any]) -> dict[str, Any]:
    raw_properties = value.get("properties")
    if isinstance(raw_properties, Mapping):
        return dict(raw_properties)
    return {}


def _properties_payload(
    raw_properties: Mapping[str, Any],
    structural_property_keys: frozenset[str],
) -> dict[str, Any]:
    properties = dict(raw_properties)
    decoded_properties = _decode_json_object(properties.get("properties_json"))
    payload: dict[str, Any] = dict(decoded_properties or {})
    for key, item in properties.items():
        if key not in structural_property_keys:
            payload.setdefault(str(key), item)
    return payload


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _node_identity(node: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _sort_text(node.get("node_id")),
        _sort_text(node.get("canonical_entity_id")),
    )


def _edge_identity(edge: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        _sort_text(edge.get("edge_id")),
        _sort_text(edge.get("source_node_id")),
        _sort_text(edge.get("relationship_type")),
        _sort_text(edge.get("target_node_id")),
    )


def _path_identity(path: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        _sort_text(path.get("edge_id")),
        _sort_text(path.get("channel")),
        _sort_text(path.get("source_node_id")),
        _sort_text(path.get("relationship_type")),
        _sort_text(path.get("target_node_id")),
    )


def _node_sort_key(node: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _sort_text(node.get("node_id")),
        _sort_text(node.get("canonical_entity_id")),
    )


def _edge_sort_key(edge: Mapping[str, Any]) -> tuple[bool, str, str, str, str]:
    edge_id = _sort_text(edge.get("edge_id"))
    return (
        edge_id == "",
        edge_id,
        _sort_text(edge.get("source_node_id")),
        _sort_text(edge.get("relationship_type")),
        _sort_text(edge.get("target_node_id")),
    )


def _path_sort_key(path: Mapping[str, Any]) -> tuple[float, int, bool, str, str, str, str]:
    edge_id = _sort_text(path.get("edge_id"))
    return (
        -_float_or_default(path.get("score"), 0.0),
        _int_or_default(path.get("path_length"), 0),
        edge_id == "",
        edge_id,
        _sort_text(path.get("source_node_id")),
        _sort_text(path.get("relationship_type")),
        _sort_text(path.get("target_node_id")),
    )


def _impacted_entities_from_subgraph(
    nodes: list[dict[str, Any]],
    seed_entities: set[str],
    *,
    result_limit: int,
) -> list[dict[str, Any]]:
    impacted_entities: list[dict[str, Any]] = []
    for node in nodes:
        node_id = node.get("node_id")
        canonical_entity_id = node.get("canonical_entity_id")
        is_seed = node_id in seed_entities or canonical_entity_id in seed_entities
        impacted_entities.append(
            {
                "canonical_entity_id": canonical_entity_id,
                "is_seed": is_seed,
                "labels": _sorted_text_list(node.get("labels", [])),
                "node_id": node_id,
                "score": 1.0 if is_seed else 0.0,
            }
        )
    impacted_entities.sort(
        key=lambda entity: (
            not bool(entity["is_seed"]),
            _sort_text(entity.get("node_id")),
            _sort_text(entity.get("canonical_entity_id")),
        ),
    )
    return impacted_entities[:result_limit]


def _sorted_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted(str(item) for item in value if item is not None)


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _sort_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _float_or_default(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_or_default(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
