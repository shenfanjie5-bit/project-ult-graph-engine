"""Synthetic graph generation and loading helpers for Neo4j benchmark runs."""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from graph_engine.client import Neo4jClient

_RANDOM_SEED = 42
_IDENTIFIER_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_"
)


def generate_synthetic_nodes(count: int, labels: list[str]) -> list[dict[str, Any]]:
    """Generate benchmark nodes with stable ids and pseudo-random labels."""

    if count < 0:
        raise ValueError("count must be non-negative")
    _validate_non_empty_strings(labels, "labels")

    rng = random.Random(_RANDOM_SEED)
    return [
        {
            "node_id": f"synthetic-node-{index:08d}",
            "canonical_entity_id": f"synthetic-entity-{index:08d}",
            "label": rng.choice(labels),
            "properties": {
                "synthetic_benchmark": True,
                "ordinal": index,
                "name": f"Synthetic Entity {index}",
            },
        }
        for index in range(count)
    ]


def generate_synthetic_edges(
    nodes: list[dict[str, Any]],
    edge_count: int,
    relationship_types: list[str],
) -> list[dict[str, Any]]:
    """Generate benchmark edges with valid source and target node references."""

    if edge_count < 0:
        raise ValueError("edge_count must be non-negative")
    if edge_count == 0:
        return []
    if not nodes:
        raise ValueError("nodes must be non-empty when edge_count is positive")
    _validate_non_empty_strings(relationship_types, "relationship_types")

    node_ids = [_node_id(node) for node in nodes]
    rng = random.Random(_RANDOM_SEED + 1)
    node_count = len(node_ids)

    edges: list[dict[str, Any]] = []
    for index in range(edge_count):
        source_index = index % node_count
        target_index = rng.randrange(node_count)
        if node_count > 1 and target_index == source_index:
            target_index = (target_index + 1) % node_count

        relationship_type = rng.choice(relationship_types)
        weight = round(rng.uniform(0.1, 1.0), 6)
        edges.append(
            {
                "edge_id": f"synthetic-edge-{index:09d}",
                "source_node_id": node_ids[source_index],
                "target_node_id": node_ids[target_index],
                "relationship_type": relationship_type,
                "weight": weight,
                "properties": {
                    "synthetic_benchmark": True,
                    "ordinal": index,
                    "weight": weight,
                },
            }
        )

    return edges


def load_synthetic_graph(
    client: Neo4jClient,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    batch_size: int = 5000,
) -> None:
    """Load synthetic nodes and relationships into Neo4j using batched MERGE."""

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    for label, rows in _group_by(nodes, "label").items():
        query = (
            f"UNWIND $rows AS row\n"
            f"MERGE (n:{_quote_identifier(label)} {{node_id: row.node_id}})\n"
            "SET n.canonical_entity_id = row.canonical_entity_id,\n"
            "    n.label = row.label,\n"
            "    n.properties = row.properties,\n"
            "    n += row.properties"
        )
        for batch in _batched(rows, batch_size):
            client.execute_write(query, {"rows": batch})

    for relationship_type, rows in _group_by(edges, "relationship_type").items():
        query = (
            "UNWIND $rows AS row\n"
            "MATCH (source {node_id: row.source_node_id})\n"
            "MATCH (target {node_id: row.target_node_id})\n"
            f"MERGE (source)-[r:{_quote_identifier(relationship_type)} "
            "{edge_id: row.edge_id}]->(target)\n"
            "SET r.relationship_type = row.relationship_type,\n"
            "    r.weight = row.weight,\n"
            "    r.properties = row.properties,\n"
            "    r += row.properties"
        )
        for batch in _batched(rows, batch_size):
            client.execute_write(query, {"rows": batch})


def clear_graph(client: Neo4jClient) -> None:
    """Delete all nodes and relationships from the configured Neo4j database."""

    client.execute_write("MATCH (n) DETACH DELETE n")


def _validate_non_empty_strings(values: list[str], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must be non-empty")
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError(f"{field_name} must only contain non-empty strings")


def _node_id(node: dict[str, Any]) -> str:
    value = node.get("node_id")
    if not isinstance(value, str) or not value:
        raise ValueError("every node must include a non-empty node_id")
    return value


def _group_by(rows: Iterable[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"every row must include a non-empty {key}")
        grouped[value].append(row)
    return dict(grouped)


def _batched(
    rows: Sequence[dict[str, Any]],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    for start in range(0, len(rows), batch_size):
        yield list(rows[start : start + batch_size])


def _quote_identifier(identifier: str) -> str:
    if not identifier or any(character not in _IDENTIFIER_CHARS for character in identifier):
        raise ValueError(f"invalid Neo4j identifier: {identifier!r}")
    return f"`{identifier}`"

