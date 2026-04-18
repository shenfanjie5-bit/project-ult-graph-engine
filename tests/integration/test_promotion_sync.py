from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.config import load_config_from_env
from graph_engine.models import (
    CandidateGraphDelta,
    GraphEdgeRecord,
    GraphNodeRecord,
    Neo4jGraphStatus,
    PromotionPlan,
)
from graph_engine.promotion import promote_graph_deltas
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.manager import SchemaManager
from graph_engine.status import GraphStatusManager
from graph_engine.sync import sync_live_graph
from tests.fakes import InMemoryStatusStore

pytestmark = pytest.mark.skipif(
    os.getenv("NEO4J_PASSWORD") is None,
    reason="NEO4J_PASSWORD is not set; Neo4j promotion integration tests require a database.",
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class StaticCandidateReader:
    def __init__(self, deltas: list[CandidateGraphDelta]) -> None:
        self.deltas = deltas

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list[CandidateGraphDelta]:
        return self.deltas


class StaticEntityReader:
    def __init__(self, entity_ids: set[str]) -> None:
        self.entity_ids = entity_ids

    def existing_entity_ids(self, entity_ids: set[str]) -> set[str]:
        return entity_ids & self.entity_ids


class CapturingCanonicalWriter:
    def __init__(self) -> None:
        self.plans: list[PromotionPlan] = []

    def write_canonical_records(self, plan: PromotionPlan) -> None:
        self.plans.append(plan)


def test_repeated_promotion_sync_is_idempotent_in_live_graph() -> None:
    pytest.importorskip("neo4j")
    prefix = f"promotion-sync-{uuid4().hex}"
    source_node_id = f"{prefix}-source"
    target_node_id = f"{prefix}-target"
    edge_id = f"{source_node_id}|{RelationshipType.SUPPLY_CHAIN.value}|{target_node_id}"
    node_ids = [source_node_id, target_node_id]
    deltas = [
        _contract_delta(
            "delta-1",
            source_node_id,
            target_node_id,
            {
                **_edge_properties(edge_id),
                "weight": 0.7,
            },
        ),
    ]
    endpoint_plan = _direct_node_sync_plan(source_node_id, target_node_id)
    writer = CapturingCanonicalWriter()

    with Neo4jClient(load_config_from_env()) as client:
        if not client.verify_connectivity():
            pytest.skip("Neo4j is not reachable with the configured environment.")

        manager = SchemaManager(client)
        manager.apply_schema()
        assert manager.verify_schema() is True
        status_manager = GraphStatusManager(InMemoryStatusStore(_ready_status()))

        try:
            sync_live_graph(endpoint_plan, client)
            for _ in range(2):
                promote_graph_deltas(
                    "cycle-1",
                    "selection-1",
                    candidate_reader=StaticCandidateReader(deltas),
                    entity_reader=StaticEntityReader(
                        {source_node_id},
                    ),
                    canonical_writer=writer,
                    client=client,
                    status_manager=status_manager,
                )

            counts = _live_graph_counts(client, node_ids, edge_id)
            serialized_payloads = _live_graph_serialized_payloads(
                client,
                source_node_id,
                edge_id,
            )
        finally:
            client.execute_write(
                "MATCH (n) WHERE n.node_id IN $node_ids DETACH DELETE n",
                {"node_ids": node_ids},
            )

    assert len(writer.plans) == 2
    assert counts == {
        "business_nodes": 2,
        "business_edges": 1,
    }
    assert serialized_payloads == {
        "node_properties_json": _json_payload(_node_properties(source_node_id)),
        "node_integration_prefix": source_node_id,
        "node_metadata": None,
        "node_mixed_aliases": None,
        "edge_properties_json": _json_payload(
            {
                **_edge_properties(edge_id),
                "contract_delta_id": "delta-1",
                "contract_delta_type": "upsert_relation",
                "evidence_refs": ["fact-1"],
                "subsystem_id": "subsystem-news",
                "weight": 0.7,
            },
        ),
        "edge_integration_prefix": edge_id,
        "edge_metadata": None,
        "edge_mixed_aliases": None,
        "edge_properties": None,
    }


def test_sync_removes_stale_top_level_properties_when_payload_becomes_json_only() -> None:
    pytest.importorskip("neo4j")
    prefix = f"promotion-sync-stale-props-{uuid4().hex}"
    source_node_id = f"{prefix}-source"
    target_node_id = f"{prefix}-target"
    edge_id = f"{prefix}-edge"
    node_ids = [source_node_id, target_node_id]

    scalar_plan = _direct_sync_plan(
        source_node_id,
        target_node_id,
        edge_id,
        node_properties={"score": 42},
        edge_properties={"score": 0.7},
    )
    nested_plan = _direct_sync_plan(
        source_node_id,
        target_node_id,
        edge_id,
        node_properties={"score": {"breakdown": {"a": 10, "b": 32}}},
        edge_properties={"score": {"breakdown": {"direct": 0.7}}},
    )

    with Neo4jClient(load_config_from_env()) as client:
        if not client.verify_connectivity():
            pytest.skip("Neo4j is not reachable with the configured environment.")

        manager = SchemaManager(client)
        manager.apply_schema()
        assert manager.verify_schema() is True

        try:
            sync_live_graph(scalar_plan, client)
            initial_payloads = _live_graph_score_payloads(client, source_node_id, edge_id)
            sync_live_graph(nested_plan, client)
            updated_payloads = _live_graph_score_payloads(client, source_node_id, edge_id)
        finally:
            client.execute_write(
                "MATCH (n) WHERE n.node_id IN $node_ids DETACH DELETE n",
                {"node_ids": node_ids},
            )

    assert initial_payloads == {
        "node_score": 42,
        "node_properties_json": _json_payload({"score": 42}),
        "edge_score": 0.7,
        "edge_properties_json": _json_payload({"score": 0.7}),
    }
    assert updated_payloads == {
        "node_score": None,
        "node_properties_json": _json_payload(
            {"score": {"breakdown": {"a": 10, "b": 32}}},
        ),
        "edge_score": None,
        "edge_properties_json": _json_payload(
            {"score": {"breakdown": {"direct": 0.7}}},
        ),
    }


def _live_graph_counts(
    client: Neo4jClient,
    node_ids: list[str],
    edge_id: str,
) -> dict[str, int]:
    rows = client.execute_read(
        """
MATCH (business_node)
WHERE business_node.node_id IN $business_node_ids
WITH count(business_node) AS business_nodes
MATCH ()-[business_edge:SUPPLY_CHAIN {edge_id: $edge_id}]->()
RETURN business_nodes, count(business_edge) AS business_edges
""",
        {
            "business_node_ids": node_ids,
            "edge_id": edge_id,
        },
    )
    row = rows[0]
    return {
        "business_nodes": row["business_nodes"],
        "business_edges": row["business_edges"],
    }


def _live_graph_serialized_payloads(
    client: Neo4jClient,
    source_node_id: str,
    edge_id: str,
) -> dict[str, object]:
    rows = client.execute_read(
        """
MATCH (node {node_id: $source_node_id})
MATCH ()-[edge:SUPPLY_CHAIN {edge_id: $edge_id}]->()
RETURN node.properties_json AS node_properties_json,
       node.integration_prefix AS node_integration_prefix,
       node.metadata AS node_metadata,
       node.mixed_aliases AS node_mixed_aliases,
       edge.properties_json AS edge_properties_json,
       edge.integration_prefix AS edge_integration_prefix,
       edge.metadata AS edge_metadata,
       edge.mixed_aliases AS edge_mixed_aliases,
       edge.properties AS edge_properties
""",
        {
            "source_node_id": source_node_id,
            "edge_id": edge_id,
        },
    )
    return rows[0]


def _ready_status() -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=3,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="integration-ready",
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _live_graph_score_payloads(
    client: Neo4jClient,
    source_node_id: str,
    edge_id: str,
) -> dict[str, object]:
    rows = client.execute_read(
        """
MATCH (node {node_id: $source_node_id})
MATCH ()-[edge:SUPPLY_CHAIN {edge_id: $edge_id}]->()
RETURN node.score AS node_score,
       node.properties_json AS node_properties_json,
       edge.score AS edge_score,
       edge.properties_json AS edge_properties_json
""",
        {
            "source_node_id": source_node_id,
            "edge_id": edge_id,
        },
    )
    return rows[0]


def _direct_node_sync_plan(
    source_node_id: str,
    target_node_id: str,
) -> PromotionPlan:
    return PromotionPlan(
        cycle_id="cycle-1",
        selection_ref="selection-1",
        delta_ids=["endpoint-nodes"],
        node_records=[
            GraphNodeRecord(
                node_id=source_node_id,
                canonical_entity_id=f"{source_node_id}-entity",
                label=NodeLabel.ENTITY.value,
                properties=_node_properties(source_node_id),
                created_at=NOW,
                updated_at=NOW,
            ),
            GraphNodeRecord(
                node_id=target_node_id,
                canonical_entity_id=f"{target_node_id}-entity",
                label=NodeLabel.ENTITY.value,
                properties={},
                created_at=NOW,
                updated_at=NOW,
            ),
        ],
        edge_records=[],
        assertion_records=[],
        created_at=NOW,
    )


def _direct_sync_plan(
    source_node_id: str,
    target_node_id: str,
    edge_id: str,
    *,
    node_properties: dict[str, object],
    edge_properties: dict[str, object],
) -> PromotionPlan:
    return PromotionPlan(
        cycle_id="cycle-1",
        selection_ref="selection-1",
        delta_ids=["delta-1"],
        node_records=[
            GraphNodeRecord(
                node_id=source_node_id,
                canonical_entity_id=f"{source_node_id}-entity",
                label=NodeLabel.ENTITY.value,
                properties=node_properties,
                created_at=NOW,
                updated_at=NOW,
            ),
            GraphNodeRecord(
                node_id=target_node_id,
                canonical_entity_id=f"{target_node_id}-entity",
                label=NodeLabel.ENTITY.value,
                properties={},
                created_at=NOW,
                updated_at=NOW,
            ),
        ],
        edge_records=[
            GraphEdgeRecord(
                edge_id=edge_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                relationship_type=RelationshipType.SUPPLY_CHAIN.value,
                properties=edge_properties,
                weight=0.7,
                created_at=NOW,
                updated_at=NOW,
            ),
        ],
        assertion_records=[],
        created_at=NOW,
    )


def _contract_delta(
    delta_id: str,
    source_node_id: str,
    target_node_id: str,
    properties: dict[str, object],
) -> CandidateGraphDelta:
    return CandidateGraphDelta(
        delta_id=delta_id,
        delta_type="upsert_relation",
        source_node=source_node_id,
        target_node=target_node_id,
        relation_type=RelationshipType.SUPPLY_CHAIN.value,
        properties=properties,
        evidence=["fact-1"],
        subsystem_id="subsystem-news",
    )


def _node_properties(node_id: str) -> dict[str, object]:
    return {
        "integration_prefix": node_id,
        "mixed_aliases": ["ULT", 123],
        "metadata": {"tier": "critical"},
    }


def _edge_properties(edge_id: str) -> dict[str, object]:
    return {
        "integration_prefix": edge_id,
        "mixed_aliases": ["SUPPLY_CHAIN", 1],
        "metadata": {"source_system": "fixture"},
    }


def _json_payload(payload: dict[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)
