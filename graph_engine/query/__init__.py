"""Status-gated public Neo4j read APIs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.models import Neo4jGraphStatus
from graph_engine.status import GraphStatusManager

_SUBGRAPH_QUERY_TEMPLATE = """
MATCH (seed)
WHERE seed.canonical_entity_id IN $seed_entities
   OR seed.node_id IN $seed_entities
OPTIONAL MATCH path = (seed)-[*0..__DEPTH__]-(connected)
WITH collect(DISTINCT seed) AS seeds,
     collect(path) AS paths
WITH seeds,
     reduce(nodes_acc = [], path IN paths | nodes_acc + nodes(path)) AS path_nodes,
     reduce(edges_acc = [], path IN paths | edges_acc + relationships(path)) AS path_edges
WITH seeds + path_nodes AS raw_nodes,
     path_edges AS raw_edges
UNWIND raw_nodes AS node
WITH collect(DISTINCT node) AS nodes,
     raw_edges
UNWIND CASE WHEN raw_edges = [] THEN [NULL] ELSE raw_edges END AS relationship
WITH nodes,
     [item IN collect(DISTINCT relationship) WHERE item IS NOT NULL] AS relationships
RETURN [node IN nodes WHERE node IS NOT NULL | {
           node_id: node.node_id,
           canonical_entity_id: node.canonical_entity_id,
           labels: labels(node),
           properties: properties(node)
       }] AS subgraph_nodes,
       [relationship IN relationships | {
           edge_id: relationship.edge_id,
           source_node_id: startNode(relationship).node_id,
           target_node_id: endNode(relationship).node_id,
           relationship_type: type(relationship),
           properties: properties(relationship)
       }] AS subgraph_edges
"""


def query_subgraph(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager | None = None,
) -> dict[str, Any]:
    """Return a local subgraph only when the live graph is ready."""

    ready_status = _require_ready(status_manager, "subgraph query")
    return _query_subgraph_after_ready(
        seed_entities,
        depth,
        client=client,
        ready_status=ready_status,
    )


def simulate_readonly_impact(
    seed_entities: list[str],
    context: object,
    *,
    client: Neo4jClient,
    status_manager: GraphStatusManager | None = None,
) -> dict[str, Any]:
    """Run the read-only local simulation boundary behind the ready gate."""

    ready_status = _require_ready(status_manager, "readonly simulation")
    depth = _context_int(context, "depth", default=1)
    subgraph = _query_subgraph_after_ready(
        seed_entities,
        depth,
        client=client,
        ready_status=ready_status,
    )
    return {
        "graph_generation_id": ready_status.graph_generation_id,
        "seed_entities": list(seed_entities),
        "impacted_subgraph": subgraph,
        "status": "ready",
    }


def _query_subgraph_after_ready(
    seed_entities: list[str],
    depth: int,
    *,
    client: Neo4jClient,
    ready_status: Neo4jGraphStatus,
) -> dict[str, Any]:
    seeds = _validated_seed_entities(seed_entities)
    query_depth = _validated_depth(depth)
    rows = client.execute_read(
        _SUBGRAPH_QUERY_TEMPLATE.replace("__DEPTH__", str(query_depth)),
        {"seed_entities": seeds},
    )
    row = _first_row(rows)
    return {
        "graph_generation_id": ready_status.graph_generation_id,
        "subgraph_nodes": _list_field(row, "subgraph_nodes"),
        "subgraph_edges": _list_field(row, "subgraph_edges"),
        "status": "ready",
    }


def _require_ready(
    status_manager: GraphStatusManager | None,
    operation_name: str,
) -> Neo4jGraphStatus:
    if status_manager is None:
        raise ValueError(f"{operation_name} requires status_manager")
    return status_manager.require_ready()


def _validated_seed_entities(seed_entities: Sequence[str]) -> list[str]:
    seeds = [seed for seed in seed_entities if isinstance(seed, str) and seed.strip()]
    if not seeds:
        raise ValueError("seed_entities must contain at least one entity id")
    return seeds


def _validated_depth(depth: int) -> int:
    if isinstance(depth, bool) or not isinstance(depth, int):
        raise ValueError("depth must be an integer")
    if depth < 0:
        raise ValueError("depth must be non-negative")
    return depth


def _first_row(rows: object) -> Mapping[str, Any]:
    if not isinstance(rows, list) or not rows:
        return {}
    row = rows[0]
    if not isinstance(row, Mapping):
        return {}
    return row


def _list_field(row: Mapping[str, Any], field_name: str) -> list[Any]:
    value = row.get(field_name)
    if not isinstance(value, list):
        return []
    return value


def _context_int(context: object, field_name: str, *, default: int) -> int:
    value: object
    if isinstance(context, Mapping):
        value = context.get(field_name, default)
    else:
        value = getattr(context, field_name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        return default
    return value


__all__ = ["query_subgraph", "simulate_readonly_impact"]
