from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.models import GraphAssertionRecord, GraphEdgeRecord, GraphNodeRecord, PromotionPlan
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.sync import sync_live_graph

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


def test_sync_live_graph_uses_idempotent_merge_queries() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        node_records=[
            _node_record("node-1", "entity-1"),
            _node_record("node-2", "entity-2"),
        ],
        edge_records=[_edge_record()],
        assertion_records=[_assertion_record()],
    )

    sync_live_graph(plan, client)

    queries = [call.args[0] for call in client.execute_write.call_args_list]
    assert len(queries) == 1
    assert any("MERGE" in query for query in queries)
    assert any("MERGE (n:`Entity` {node_id: row.node_id})" in query for query in queries)
    assert any(
        "MERGE (source)-[r:`SUPPLY_CHAIN` {edge_id: row.edge_id}]->(target)" in query
        for query in queries
    )
    assert any(
        "MERGE (assertion:`Assertion` {node_id: row.assertion_id})" in query
        for query in queries
    )
    assert queries[0].count("`ASSERTION_LINK`") == 2


def test_sync_live_graph_batches_rows_by_node_label() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        node_records=[
            _node_record("node-1", "entity-1"),
            _node_record("node-2", "entity-2"),
        ],
    )

    sync_live_graph(plan, client, batch_size=1)

    assert client.execute_write.call_count == 1
    parameters = client.execute_write.call_args.args[1]
    row_batches = [parameters["node_rows_0"], parameters["node_rows_1"]]
    assert row_batches == [
        [_expected_node_row("node-1", "entity-1")],
        [_expected_node_row("node-2", "entity-2")],
    ]


def test_sync_live_graph_batches_edges_by_relationship_type() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        edge_records=[
            _edge_record("edge-1", relationship_type=RelationshipType.SUPPLY_CHAIN.value),
            _edge_record("edge-2", relationship_type=RelationshipType.OWNERSHIP.value),
        ],
    )

    sync_live_graph(plan, client)

    queries = [call.args[0] for call in client.execute_write.call_args_list]
    assert len(queries) == 1
    assert any("[r:`SUPPLY_CHAIN`" in query for query in queries)
    assert any("[r:`OWNERSHIP`" in query for query in queries)
    assert client.execute_write.call_args.args[1]["required_endpoint_node_ids"] == [
        "node-1",
        "node-2",
    ]
    client.execute_read.assert_not_called()


def test_sync_live_graph_rejects_missing_edge_endpoints_from_write_transaction() -> None:
    client = MagicMock(spec=Neo4jClient)
    client.execute_write.return_value = [
        {"missing_endpoint_node_ids": ["node-1"], "mutation_applied": 0},
    ]
    plan = _promotion_plan(edge_records=[_edge_record()])

    with pytest.raises(ValueError, match="node-1"):
        sync_live_graph(plan, client)

    client.execute_read.assert_not_called()
    client.execute_write.assert_called_once()
    assert "MATCH (n {node_id: node_id})" in client.execute_write.call_args.args[0]


def test_sync_live_graph_rejects_missing_external_endpoints_before_mutation_branch() -> None:
    client = MagicMock(spec=Neo4jClient)
    client.execute_write.return_value = [
        {"missing_endpoint_node_ids": ["node-2"], "mutation_applied": 0},
    ]
    plan = _promotion_plan(
        node_records=[_node_record("node-1", "entity-1")],
        edge_records=[_edge_record()],
    )

    with pytest.raises(ValueError, match="node-2"):
        sync_live_graph(plan, client)

    client.execute_read.assert_not_called()
    client.execute_write.assert_called_once()
    query = client.execute_write.call_args.args[0]
    parameters = client.execute_write.call_args.args[1]
    assert parameters["required_endpoint_node_ids"] == ["node-2"]
    assert query.index("WHERE size(missing_endpoint_node_ids) = 0") < query.index(
        "MERGE (n:`Entity`",
    )


def test_sync_live_graph_rejects_missing_assertion_endpoints_from_write_transaction() -> None:
    client = MagicMock(spec=Neo4jClient)
    client.execute_write.return_value = [
        {"missing_endpoint_node_ids": ["node-2"], "mutation_applied": 0},
    ]
    plan = _promotion_plan(assertion_records=[_assertion_record()])

    with pytest.raises(ValueError, match="node-2"):
        sync_live_graph(plan, client)

    client.execute_read.assert_not_called()
    client.execute_write.assert_called_once()


def test_sync_live_graph_deletes_stale_edge_endpoint_before_merge() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        edge_records=[
            _edge_record("edge-1", target_node_id="node-3"),
        ],
    )

    sync_live_graph(plan, client)

    queries = [call.args[0] for call in client.execute_write.call_args_list]
    assert len(queries) == 1
    assert "MATCH ()-[stale {edge_id: row.edge_id}]->()" in queries[0]
    assert "DELETE stale" in queries[0]
    assert (
        "MERGE (source)-[r:`SUPPLY_CHAIN` {edge_id: row.edge_id}]->(target)"
        in queries[0]
    )
    assert queries[0].index("DELETE stale") < queries[0].index(
        "MERGE (source)-[r:`SUPPLY_CHAIN`",
    )
    client.execute_read.assert_not_called()


def test_sync_live_graph_does_not_split_stale_delete_and_replacement_writes() -> None:
    client = MagicMock(spec=Neo4jClient)
    client.execute_write.side_effect = RuntimeError("replacement failed")
    plan = _promotion_plan(
        edge_records=[
            _edge_record("edge-1", target_node_id="node-3"),
        ],
    )

    with pytest.raises(RuntimeError, match="replacement failed"):
        sync_live_graph(plan, client)

    assert client.execute_write.call_count == 1
    query = client.execute_write.call_args.args[0]
    assert "DELETE stale" in query
    assert "MERGE (source)-[r:`SUPPLY_CHAIN` {edge_id: row.edge_id}]->(target)" in query
    client.execute_read.assert_not_called()


def test_sync_live_graph_expands_only_safe_properties() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        node_records=[
            _node_record(
                "node-1",
                "entity-1",
                properties={
                    "ticker": "ULT",
                    "bad-key": "ignored",
                    "node_id": "not-overwritten",
                },
            )
        ],
    )

    sync_live_graph(plan, client)

    rows = client.execute_write.call_args.args[1]["node_rows_0"]
    assert rows[0]["properties"] == {
        "ticker": "ULT",
        "bad-key": "ignored",
        "node_id": "not-overwritten",
    }
    assert rows[0]["safe_properties"] == {"ticker": "ULT"}


def test_sync_live_graph_rejects_invalid_node_label_before_writing() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        node_records=[_node_record("node-1", "entity-1", label="BadLabel")],
    )

    with pytest.raises(ValueError, match="unsupported node label"):
        sync_live_graph(plan, client)

    client.execute_write.assert_not_called()


def test_sync_live_graph_rejects_invalid_relationship_type_before_writing() -> None:
    client = MagicMock(spec=Neo4jClient)
    plan = _promotion_plan(
        node_records=[_node_record("node-1", "entity-1")],
        edge_records=[_edge_record(relationship_type="BAD_TYPE")],
    )

    with pytest.raises(ValueError, match="unsupported relationship type"):
        sync_live_graph(plan, client)

    client.execute_write.assert_not_called()


def test_sync_live_graph_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        sync_live_graph(_promotion_plan(), MagicMock(spec=Neo4jClient), batch_size=0)


def _promotion_plan(
    *,
    node_records: list[GraphNodeRecord] | None = None,
    edge_records: list[GraphEdgeRecord] | None = None,
    assertion_records: list[GraphAssertionRecord] | None = None,
) -> PromotionPlan:
    return PromotionPlan(
        cycle_id="cycle-1",
        selection_ref="selection-1",
        delta_ids=["delta-1"],
        node_records=node_records or [],
        edge_records=edge_records or [],
        assertion_records=assertion_records or [],
        created_at=NOW,
    )


def _node_record(
    node_id: str,
    canonical_entity_id: str,
    *,
    label: str = NodeLabel.ENTITY.value,
    properties: dict[str, object] | None = None,
) -> GraphNodeRecord:
    return GraphNodeRecord(
        node_id=node_id,
        canonical_entity_id=canonical_entity_id,
        label=label,
        properties=properties or {"ticker": "ULT"},
        created_at=NOW,
        updated_at=NOW,
    )


def _edge_record(
    edge_id: str = "edge-1",
    *,
    source_node_id: str = "node-1",
    target_node_id: str = "node-2",
    relationship_type: str = RelationshipType.SUPPLY_CHAIN.value,
) -> GraphEdgeRecord:
    return GraphEdgeRecord(
        edge_id=edge_id,
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        relationship_type=relationship_type,
        properties={"source": "filing"},
        weight=0.7,
        created_at=NOW,
        updated_at=NOW,
    )


def _assertion_record() -> GraphAssertionRecord:
    return GraphAssertionRecord(
        assertion_id="assertion-1",
        source_node_id="node-1",
        target_node_id="node-2",
        assertion_type="risk",
        evidence={"source": "contract"},
        confidence=0.8,
        created_at=NOW,
    )


def _expected_node_row(node_id: str, canonical_entity_id: str) -> dict[str, object]:
    return {
        "node_id": node_id,
        "canonical_entity_id": canonical_entity_id,
        "label": NodeLabel.ENTITY.value,
        "properties": {"ticker": "ULT"},
        "safe_properties": {"ticker": "ULT"},
        "created_at": NOW,
        "updated_at": NOW,
    }
