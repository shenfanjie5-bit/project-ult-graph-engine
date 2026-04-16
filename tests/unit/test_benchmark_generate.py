from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from benchmarks.generate_synthetic import (
    clear_graph,
    generate_synthetic_edges,
    generate_synthetic_nodes,
    load_synthetic_graph,
)
from benchmarks.report import DEFAULT_BUDGETS, check_budgets, generate_text_report
from benchmarks.run_benchmark import (
    BenchmarkResult,
    benchmark_consistency_check,
    benchmark_gds_projection_create,
    run_full_benchmark_suite,
)
from graph_engine.schema.definitions import NodeLabel, RelationshipType


def test_generate_synthetic_nodes_returns_expected_shape() -> None:
    nodes = generate_synthetic_nodes(3, [NodeLabel.ENTITY.value, NodeLabel.SECTOR.value])

    assert len(nodes) == 3
    assert nodes[0]["node_id"] == "synthetic-node-00000000"
    assert nodes[0]["canonical_entity_id"] == "synthetic-entity-00000000"
    assert nodes[0]["label"] in {NodeLabel.ENTITY.value, NodeLabel.SECTOR.value}
    assert nodes[0]["properties"]["synthetic_benchmark"] is True


def test_generate_synthetic_nodes_distributes_labels() -> None:
    labels = [NodeLabel.ENTITY.value, NodeLabel.SECTOR.value, NodeLabel.INDUSTRY.value]

    nodes = generate_synthetic_nodes(300, labels)
    observed_labels = {node["label"] for node in nodes}

    assert observed_labels == set(labels)


def test_generate_synthetic_nodes_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="count"):
        generate_synthetic_nodes(-1, [NodeLabel.ENTITY.value])

    with pytest.raises(ValueError, match="labels"):
        generate_synthetic_nodes(1, [])


def test_generate_synthetic_edges_returns_valid_references_and_weights() -> None:
    nodes = generate_synthetic_nodes(10, [NodeLabel.ENTITY.value])
    relationship_types = [
        RelationshipType.SUPPLY_CHAIN.value,
        RelationshipType.EVENT_IMPACT.value,
    ]

    edges = generate_synthetic_edges(nodes, 50, relationship_types)
    node_labels_by_id = {node["node_id"]: node["label"] for node in nodes}

    assert len(edges) == 50
    assert all(edge["source_node_id"] in node_labels_by_id for edge in edges)
    assert all(edge["target_node_id"] in node_labels_by_id for edge in edges)
    assert all(
        edge["source_label"] == node_labels_by_id[edge["source_node_id"]]
        for edge in edges
    )
    assert all(
        edge["target_label"] == node_labels_by_id[edge["target_node_id"]]
        for edge in edges
    )
    assert all(edge["relationship_type"] in relationship_types for edge in edges)
    assert all(0.1 <= edge["weight"] <= 1.0 for edge in edges)


def test_generate_synthetic_edges_rejects_positive_count_without_nodes() -> None:
    with pytest.raises(ValueError, match="nodes"):
        generate_synthetic_edges([], 1, [RelationshipType.SUPPLY_CHAIN.value])


def test_generate_synthetic_edges_allows_empty_edges_without_relationship_types() -> None:
    nodes = generate_synthetic_nodes(2, [NodeLabel.ENTITY.value])

    assert generate_synthetic_edges(nodes, 0, []) == []


def test_load_synthetic_graph_batches_nodes_and_edges_by_type() -> None:
    client = MagicMock()
    nodes = [
        {
            "node_id": "n1",
            "canonical_entity_id": "e1",
            "label": NodeLabel.ENTITY.value,
            "properties": {},
        },
        {
            "node_id": "n2",
            "canonical_entity_id": "e2",
            "label": NodeLabel.SECTOR.value,
            "properties": {},
        },
    ]
    edges = [
        {
            "edge_id": "r1",
            "source_node_id": "n1",
            "target_node_id": "n2",
            "relationship_type": RelationshipType.SUPPLY_CHAIN.value,
            "weight": 0.5,
            "properties": {},
        }
    ]

    load_synthetic_graph(client, nodes, edges, batch_size=1)

    queries = [call.args[0] for call in client.execute_write.call_args_list]
    assert len(queries) == 3
    assert any("MERGE (n:`Entity`" in query for query in queries)
    assert any("MERGE (n:`Sector`" in query for query in queries)
    edge_queries = [query for query in queries if "[r:`SUPPLY_CHAIN`" in query]
    assert len(edge_queries) == 1
    assert "MATCH (source:`Entity`" in edge_queries[0]
    assert "MATCH (target:`Sector`" in edge_queries[0]


def test_load_synthetic_graph_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        load_synthetic_graph(MagicMock(), [], [], batch_size=0)


def test_clear_graph_deletes_only_synthetic_benchmark_data_in_batches() -> None:
    client = MagicMock()
    client.execute_write.side_effect = [
        [{"deleted": 10_000}],
        [{"deleted": 3}],
        [{"deleted": 2}],
    ]

    clear_graph(client, batch_size=10_000)

    queries = [call.args[0] for call in client.execute_write.call_args_list]
    parameters = [call.args[1] for call in client.execute_write.call_args_list]

    assert len(queries) == 3
    assert all("synthetic_benchmark = true" in query for query in queries)
    assert all("LIMIT $batch_size" in query for query in queries)
    assert all("MATCH (n) DETACH DELETE n" not in query for query in queries)
    assert parameters == [
        {"batch_size": 10_000},
        {"batch_size": 10_000},
        {"batch_size": 10_000},
    ]


def test_clear_graph_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        clear_graph(MagicMock(), batch_size=0)


def test_generate_text_report_includes_status_and_operation_names() -> None:
    results = [
        BenchmarkResult("pagerank", 100, 500, 1.2, None, True),
        BenchmarkResult("cold_reload", 100, 500, 120.0, None, False),
    ]

    report = generate_text_report(results, DEFAULT_BUDGETS)

    assert "PASS pagerank" in report
    assert "FAIL cold_reload" in report
    assert "Overall: FAIL" in report


def test_check_budgets_returns_true_only_when_all_budgeted_results_pass() -> None:
    passing_results = [
        BenchmarkResult("pagerank", 100, 500, 1.0, None, True),
        BenchmarkResult("consistency_check", 100, 500, 0.1, None, True),
        BenchmarkResult("cold_reload", 100, 500, 2.0, None, True),
    ]
    failing_results = [
        BenchmarkResult("pagerank", 100, 500, 61.0, None, True),
    ]

    assert check_budgets(passing_results, DEFAULT_BUDGETS) is True
    assert check_budgets(failing_results, DEFAULT_BUDGETS) is False


def test_benchmark_gds_projection_create_reports_missing_gds_plugin() -> None:
    client = MagicMock()
    client.execute_write.side_effect = RuntimeError(
        "There is no procedure with the name gds.graph.exists registered"
    )

    with pytest.raises(RuntimeError, match="GDS plugin not available"):
        benchmark_gds_projection_create(client, "missing_gds")


def test_benchmark_gds_projection_create_does_not_hide_gds_runtime_errors() -> None:
    client = MagicMock()
    client.execute_write.side_effect = RuntimeError("gds PageRank execution failed")

    with pytest.raises(RuntimeError, match="PageRank execution failed"):
        benchmark_gds_projection_create(client, "runtime_failure")


def test_benchmark_consistency_check_normalizes_connection_failures() -> None:
    client = MagicMock()
    client.execute_read.side_effect = RuntimeError("connection refused")

    with pytest.raises(ConnectionError, match="Neo4j connection failed"):
        benchmark_consistency_check(client, 1, 1)


def test_run_full_benchmark_suite_normalizes_initial_connection_failures() -> None:
    client = MagicMock()
    client.execute_write.side_effect = RuntimeError("service unavailable")

    with pytest.raises(ConnectionError, match="Neo4j connection failed"):
        run_full_benchmark_suite(client, target_nodes=1, target_edge_factor=1)
