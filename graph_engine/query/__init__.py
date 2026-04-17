"""Status-gated read APIs for live graph subgraph queries and local simulations."""

from __future__ import annotations

from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.models import ReadonlySimulationRequest
from graph_engine.status import GraphStatusManager, ready_read

MAX_QUERY_DEPTH = 10


def query_subgraph(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
) -> dict[str, Any]:
    """Return a bounded live subgraph after verifying the graph is ready."""

    with ready_read(status_manager, "subgraph query"):
        return _query_subgraph_after_ready(seed_entities, depth, client=client)


def simulate_readonly_impact(
    seed_entities: list[str],
    context: ReadonlySimulationRequest,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
) -> dict[str, Any]:
    """Run a read-only local impact simulation without modifying live graph state."""

    with ready_read(status_manager, "readonly impact simulation") as ready_status:
        if ready_status.graph_generation_id != context.graph_generation_id:
            raise ValueError(
                "ReadonlySimulationRequest graph_generation_id disagrees with Neo4jGraphStatus: "
                f"context={context.graph_generation_id}, "
                f"status={ready_status.graph_generation_id}",
            )

        subgraph = _query_subgraph_after_ready(seed_entities, context.depth, client=client)
        impacted_entities = _impacted_entities_from_subgraph(
            subgraph,
            set(seed_entities),
            result_limit=context.result_limit,
        )
        return {
            "cycle_id": context.cycle_id,
            "world_state_ref": context.world_state_ref,
            "graph_generation_id": ready_status.graph_generation_id,
            "seed_entities": list(seed_entities),
            "subgraph": subgraph,
            "impacted_entities": impacted_entities,
            "activated_paths": list(subgraph["relationships"]),
            "channel_breakdown": {
                "readonly": {
                    "depth": subgraph["depth"],
                    "node_count": len(subgraph["nodes"]),
                    "edge_count": len(subgraph["relationships"]),
                },
            },
        }


def _query_subgraph_after_ready(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
) -> dict[str, Any]:
    seed_list = _validated_seed_entities(seed_entities)
    validated_depth = _validated_depth(depth)
    rows = client.execute_read(
        _subgraph_query(validated_depth),
        {"seed_entities": seed_list},
    )
    row = rows[0] if isinstance(rows, list) and rows else {}
    if not isinstance(row, dict):
        row = {}
    return {
        "seed_entities": seed_list,
        "depth": validated_depth,
        "nodes": _dict_rows(row.get("nodes")),
        "relationships": _dict_rows(row.get("relationships")),
    }


def _subgraph_query(depth: int) -> str:
    return f"""
MATCH (seed)
WHERE seed.canonical_entity_id IN $seed_entities
   OR seed.node_id IN $seed_entities
OPTIONAL MATCH path = (seed)-[*0..{depth}]-(node)
WITH collect(DISTINCT seed) + collect(DISTINCT node) AS raw_nodes,
     collect(path) AS paths
WITH [graph_node IN raw_nodes WHERE graph_node IS NOT NULL] AS raw_nodes,
     paths
UNWIND raw_nodes AS graph_node
WITH collect(DISTINCT graph_node) AS nodes, paths
CALL {{
    WITH paths
    UNWIND paths AS graph_path
    WITH graph_path
    WHERE graph_path IS NOT NULL
    UNWIND relationships(graph_path) AS relationship
    RETURN collect(DISTINCT relationship) AS relationships
}}
RETURN [graph_node IN nodes WHERE graph_node IS NOT NULL | {{
           node_id: graph_node.node_id,
           canonical_entity_id: graph_node.canonical_entity_id,
           labels: labels(graph_node),
           properties: properties(graph_node)
       }}] AS nodes,
       [relationship IN relationships WHERE relationship IS NOT NULL | {{
           edge_id: relationship.edge_id,
           relationship_type: type(relationship),
           source_node_id: startNode(relationship).node_id,
           target_node_id: endNode(relationship).node_id,
           properties: properties(relationship)
       }}] AS relationships
"""


def _validated_seed_entities(seed_entities: list[str]) -> list[str]:
    if not seed_entities:
        raise ValueError("seed_entities must not be empty")
    normalized = [seed_entity.strip() for seed_entity in seed_entities]
    if any(not seed_entity for seed_entity in normalized):
        raise ValueError("seed_entities must contain non-empty ids")
    return normalized


def _validated_depth(depth: int) -> int:
    if isinstance(depth, bool) or not isinstance(depth, int):
        raise ValueError("depth must be an integer")
    if depth < 0:
        raise ValueError("depth must be greater than or equal to zero")
    if depth > MAX_QUERY_DEPTH:
        raise ValueError(f"depth must be less than or equal to {MAX_QUERY_DEPTH}")
    return depth


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _impacted_entities_from_subgraph(
    subgraph: dict[str, Any],
    seed_entities: set[str],
    *,
    result_limit: int,
) -> list[dict[str, Any]]:
    impacted_entities: list[dict[str, Any]] = []
    for node in subgraph["nodes"]:
        node_id = node.get("node_id")
        canonical_entity_id = node.get("canonical_entity_id")
        is_seed = node_id in seed_entities or canonical_entity_id in seed_entities
        impacted_entities.append(
            {
                "node_id": node_id,
                "canonical_entity_id": canonical_entity_id,
                "labels": sorted(str(label) for label in node.get("labels", [])),
                "score": 1.0 if is_seed else 0.0,
                "is_seed": is_seed,
            }
        )
    impacted_entities.sort(
        key=lambda entity: (
            not bool(entity["is_seed"]),
            str(entity.get("node_id") or ""),
            str(entity.get("canonical_entity_id") or ""),
        ),
    )
    return impacted_entities[:result_limit]


__all__ = [
    "MAX_QUERY_DEPTH",
    "query_subgraph",
    "simulate_readonly_impact",
]
