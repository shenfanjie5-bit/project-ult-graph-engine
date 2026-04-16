"""Manual Neo4j GDS benchmark runner for graph-engine scale budgets."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Any

from benchmarks.generate_synthetic import (
    clear_graph,
    generate_synthetic_edges,
    generate_synthetic_nodes,
    load_synthetic_graph,
)
from graph_engine.client import Neo4jClient
from graph_engine.config import load_config_from_env
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.manager import SchemaManager

_DEFAULT_GRAPH_NAME = "graph_engine_benchmark"
_CONNECTION_FAILURE_MARKERS = (
    "connection",
    "connect",
    "service unavailable",
    "failed to obtain",
    "unable to retrieve routing",
    "authentication",
)
_GDS_UNAVAILABLE_MARKERS = (
    "no procedure",
    "not found",
    "not available",
    "procedure not found",
    "procedurenotfound",
    "unknown procedure",
    "unknown function",
)


@dataclass(frozen=True)
class BenchmarkResult:
    """Standard result emitted by every benchmark operation."""

    operation: str
    node_count: int
    edge_count: int
    duration_seconds: float
    memory_mb: float | None
    passed: bool


def benchmark_gds_projection_create(client: Neo4jClient, graph_name: str) -> BenchmarkResult:
    """Create a GDS named projection and return wall-clock timing."""

    try:
        _drop_gds_projection_if_exists(client, graph_name)
        start = time.perf_counter()
        rows = _execute_write(
            client,
            """
            CALL gds.graph.project($graph_name, $node_labels, $relationship_projection)
            YIELD graphName, nodeCount, relationshipCount
            RETURN graphName, nodeCount, relationshipCount
            """,
            {
                "graph_name": graph_name,
                "node_labels": [label.value for label in NodeLabel],
                "relationship_projection": _relationship_projection(),
            },
        )
        duration_seconds = time.perf_counter() - start
    except Exception as exc:
        _raise_if_connection_failure(exc)
        if _is_gds_unavailable(exc):
            raise RuntimeError("GDS plugin not available") from exc
        raise

    row = rows[0] if rows else {}
    return BenchmarkResult(
        operation="gds_projection_create",
        node_count=_int_field(row, "nodeCount"),
        edge_count=_int_field(row, "relationshipCount"),
        duration_seconds=duration_seconds,
        memory_mb=None,
        passed=True,
    )


def benchmark_pagerank(
    client: Neo4jClient,
    graph_name: str,
    max_iterations: int = 20,
) -> BenchmarkResult:
    """Run GDS PageRank against an existing named projection."""

    if max_iterations <= 0:
        raise ValueError("max_iterations must be positive")

    node_count, edge_count = _projected_graph_counts(client, graph_name)
    start = time.perf_counter()
    _execute_write(
        client,
        """
        CALL gds.pageRank.stats(
            $graph_name,
            {
                maxIterations: $max_iterations,
                relationshipWeightProperty: 'weight'
            }
        )
        YIELD ranIterations, didConverge
        RETURN ranIterations, didConverge
        """,
        {"graph_name": graph_name, "max_iterations": max_iterations},
    )
    duration_seconds = time.perf_counter() - start

    return BenchmarkResult(
        operation="pagerank",
        node_count=node_count,
        edge_count=edge_count,
        duration_seconds=duration_seconds,
        memory_mb=None,
        passed=duration_seconds < _default_budget("propagation"),
    )


def benchmark_path_traversal(
    client: Neo4jClient,
    seed_node_id: str,
    max_depth: int = 5,
) -> BenchmarkResult:
    """Run a bounded breadth-first traversal from one synthetic seed node."""

    if not seed_node_id:
        raise ValueError("seed_node_id must be non-empty")
    if max_depth <= 0:
        raise ValueError("max_depth must be positive")

    node_count, edge_count = _live_graph_counts(client)
    visited = {seed_node_id}
    frontier = {seed_node_id}

    start = time.perf_counter()
    for _depth in range(max_depth):
        if not frontier:
            break
        rows = _execute_read(
            client,
            """
            MATCH (source)
            WHERE source.node_id IN $frontier
            MATCH (source)-[]->(target)
            WHERE target.node_id IS NOT NULL
            RETURN collect(DISTINCT target.node_id) AS target_node_ids
            """,
            {"frontier": list(frontier)},
        )
        target_node_ids = set(rows[0].get("target_node_ids", [])) if rows else set()
        frontier = {node_id for node_id in target_node_ids if node_id not in visited}
        visited.update(frontier)

    duration_seconds = time.perf_counter() - start
    return BenchmarkResult(
        operation="path_traversal",
        node_count=node_count,
        edge_count=edge_count,
        duration_seconds=duration_seconds,
        memory_mb=None,
        passed=duration_seconds < _default_budget("propagation"),
    )


def benchmark_cold_reload_simulation(
    client: Neo4jClient,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> BenchmarkResult:
    """Measure clear, full reload, and schema rebuild timing."""

    try:
        start = time.perf_counter()
        clear_graph(client)
        load_synthetic_graph(client, nodes, edges)
        SchemaManager(client).apply_schema()
        duration_seconds = time.perf_counter() - start
    except Exception as exc:
        _raise_if_connection_failure(exc)
        raise

    return BenchmarkResult(
        operation="cold_reload",
        node_count=len(nodes),
        edge_count=len(edges),
        duration_seconds=duration_seconds,
        memory_mb=None,
        passed=duration_seconds < _default_budget("cold_reload"),
    )


def benchmark_consistency_check(
    client: Neo4jClient,
    expected_node_count: int,
    expected_edge_count: int,
) -> BenchmarkResult:
    """Measure live graph node and relationship count verification."""

    start = time.perf_counter()
    node_count, edge_count = _live_graph_counts(client)
    duration_seconds = time.perf_counter() - start
    counts_match = node_count == expected_node_count and edge_count == expected_edge_count

    return BenchmarkResult(
        operation="consistency_check",
        node_count=node_count,
        edge_count=edge_count,
        duration_seconds=duration_seconds,
        memory_mb=None,
        passed=counts_match and duration_seconds < _default_budget("consistency_check"),
    )


def run_full_benchmark_suite(
    client: Neo4jClient,
    target_nodes: int = 100_000,
    target_edge_factor: int = 8,
) -> list[BenchmarkResult]:
    """Generate, load, and benchmark the full Lite-scale synthetic graph."""

    if target_nodes <= 0:
        raise ValueError("target_nodes must be positive")
    if target_edge_factor <= 0:
        raise ValueError("target_edge_factor must be positive")

    node_labels = [label.value for label in NodeLabel]
    relationship_types = [relationship_type.value for relationship_type in RelationshipType]
    nodes = generate_synthetic_nodes(target_nodes, node_labels)
    edges = generate_synthetic_edges(nodes, target_nodes * target_edge_factor, relationship_types)

    try:
        clear_graph(client)
        SchemaManager(client).apply_schema()
        load_synthetic_graph(client, nodes, edges)
    except Exception as exc:
        _raise_if_connection_failure(exc)
        raise

    results = [
        benchmark_consistency_check(client, len(nodes), len(edges)),
        benchmark_gds_projection_create(client, _DEFAULT_GRAPH_NAME),
        benchmark_pagerank(client, _DEFAULT_GRAPH_NAME),
        benchmark_path_traversal(client, nodes[0]["node_id"]),
        benchmark_cold_reload_simulation(client, nodes, edges),
    ]
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-nodes", type=int, default=100_000)
    parser.add_argument("--target-edge-factor", type=int, default=8)
    args = parser.parse_args(argv)

    from benchmarks.report import DEFAULT_BUDGETS, check_budgets, generate_text_report

    with Neo4jClient(load_config_from_env()) as client:
        results = run_full_benchmark_suite(
            client,
            target_nodes=args.target_nodes,
            target_edge_factor=args.target_edge_factor,
        )

    print(generate_text_report(results, DEFAULT_BUDGETS))
    return 0 if check_budgets(results, DEFAULT_BUDGETS) else 1


def _relationship_projection() -> dict[str, dict[str, Any]]:
    return {
        relationship_type.value: {
            "type": relationship_type.value,
            "orientation": "NATURAL",
            "properties": ["weight"],
        }
        for relationship_type in RelationshipType
    }


def _drop_gds_projection_if_exists(client: Neo4jClient, graph_name: str) -> None:
    rows = _execute_write(
        client,
        "CALL gds.graph.exists($graph_name) YIELD exists RETURN exists",
        {"graph_name": graph_name},
    )
    if rows and rows[0].get("exists") is True:
        _execute_write(
            client,
            "CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName",
            {"graph_name": graph_name},
        )


def _projected_graph_counts(client: Neo4jClient, graph_name: str) -> tuple[int, int]:
    rows = _execute_write(
        client,
        """
        CALL gds.graph.list($graph_name)
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        {"graph_name": graph_name},
    )
    row = rows[0] if rows else {}
    return _int_field(row, "nodeCount"), _int_field(row, "relationshipCount")


def _live_graph_counts(client: Neo4jClient) -> tuple[int, int]:
    rows = _execute_read(
        client,
        """
        MATCH (n)
        WITH count(n) AS node_count
        OPTIONAL MATCH ()-[r]->()
        RETURN node_count, count(r) AS edge_count
        """,
    )
    row = rows[0] if rows else {}
    return _int_field(row, "node_count"), _int_field(row, "edge_count")


def _execute_read(
    client: Neo4jClient,
    query: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    try:
        return client.execute_read(query, parameters)
    except Exception as exc:
        _raise_if_connection_failure(exc)
        raise


def _execute_write(
    client: Neo4jClient,
    query: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    try:
        return client.execute_write(query, parameters)
    except Exception as exc:
        _raise_if_connection_failure(exc)
        raise


def _raise_if_connection_failure(exc: Exception) -> None:
    if isinstance(exc, ConnectionError):
        raise exc

    message = str(exc).lower()
    if any(marker in message for marker in _CONNECTION_FAILURE_MARKERS):
        raise ConnectionError(f"Neo4j connection failed: {exc}") from exc


def _is_gds_unavailable(exc: Exception) -> bool:
    message = str(exc).lower()
    return "gds" in message and any(marker in message for marker in _GDS_UNAVAILABLE_MARKERS)


def _int_field(row: dict[str, Any], field_name: str) -> int:
    value = row.get(field_name, 0)
    return value if isinstance(value, int) else int(value)


def _default_budget(name: str) -> float:
    from benchmarks.report import DEFAULT_BUDGETS

    return DEFAULT_BUDGETS[name]


if __name__ == "__main__":
    raise SystemExit(main())
