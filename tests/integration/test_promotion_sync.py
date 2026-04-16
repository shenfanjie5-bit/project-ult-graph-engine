from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.config import load_config_from_env
from graph_engine.models import CandidateGraphDelta, PromotionPlan
from graph_engine.promotion import promote_graph_deltas
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.manager import SchemaManager

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
    assertion_id = f"{prefix}-assertion"
    edge_id = f"{prefix}-edge"
    node_ids = [source_node_id, target_node_id, assertion_id]
    deltas = [
        _node_delta("delta-1", source_node_id, f"{prefix}-entity-source"),
        _node_delta("delta-2", target_node_id, f"{prefix}-entity-target"),
        _edge_delta(
            "delta-3",
            edge_id,
            source_node_id,
            target_node_id,
            [f"{prefix}-entity-source", f"{prefix}-entity-target"],
        ),
        _assertion_delta(
            "delta-4",
            assertion_id,
            source_node_id,
            target_node_id,
            [f"{prefix}-entity-source", f"{prefix}-entity-target"],
        ),
    ]
    writer = CapturingCanonicalWriter()

    with Neo4jClient(load_config_from_env()) as client:
        if not client.verify_connectivity():
            pytest.skip("Neo4j is not reachable with the configured environment.")

        manager = SchemaManager(client)
        manager.apply_schema()
        assert manager.verify_schema() is True

        try:
            for _ in range(2):
                promote_graph_deltas(
                    "cycle-1",
                    "selection-1",
                    candidate_reader=StaticCandidateReader(deltas),
                    entity_reader=StaticEntityReader(
                        {f"{prefix}-entity-source", f"{prefix}-entity-target"},
                    ),
                    canonical_writer=writer,
                    client=client,
                )

            counts = _live_graph_counts(client, node_ids, edge_id, assertion_id)
        finally:
            client.execute_write(
                "MATCH (n) WHERE n.node_id IN $node_ids DETACH DELETE n",
                {"node_ids": node_ids},
            )

    assert len(writer.plans) == 2
    assert counts == {
        "business_nodes": 2,
        "business_edges": 1,
        "assertion_nodes": 1,
        "assertion_links": 2,
    }


def _live_graph_counts(
    client: Neo4jClient,
    node_ids: list[str],
    edge_id: str,
    assertion_id: str,
) -> dict[str, int]:
    rows = client.execute_read(
        """
MATCH (business_node)
WHERE business_node.node_id IN $business_node_ids
WITH count(business_node) AS business_nodes
MATCH ()-[business_edge:SUPPLY_CHAIN {edge_id: $edge_id}]->()
WITH business_nodes, count(business_edge) AS business_edges
MATCH (assertion:Assertion {node_id: $assertion_id})
WITH business_nodes, business_edges, count(assertion) AS assertion_nodes
MATCH ()-[assertion_link:ASSERTION_LINK {assertion_id: $assertion_id}]-()
RETURN business_nodes, business_edges, assertion_nodes, count(assertion_link) AS assertion_links
""",
        {
            "business_node_ids": node_ids[:2],
            "edge_id": edge_id,
            "assertion_id": assertion_id,
        },
    )
    row = rows[0]
    return {
        "business_nodes": row["business_nodes"],
        "business_edges": row["business_edges"],
        "assertion_nodes": row["assertion_nodes"],
        "assertion_links": row["assertion_links"],
    }


def _node_delta(
    delta_id: str,
    node_id: str,
    canonical_entity_id: str,
) -> CandidateGraphDelta:
    return CandidateGraphDelta(
        delta_id=delta_id,
        cycle_id="cycle-1",
        delta_type="node_add",
        source_entity_ids=[canonical_entity_id],
        payload={
            "node": {
                "node_id": node_id,
                "canonical_entity_id": canonical_entity_id,
                "label": NodeLabel.ENTITY.value,
                "properties": {"integration_prefix": node_id},
                "created_at": NOW,
                "updated_at": NOW,
            }
        },
        validation_status="frozen",
    )


def _edge_delta(
    delta_id: str,
    edge_id: str,
    source_node_id: str,
    target_node_id: str,
    source_entity_ids: list[str],
) -> CandidateGraphDelta:
    return CandidateGraphDelta(
        delta_id=delta_id,
        cycle_id="cycle-1",
        delta_type="edge_add",
        source_entity_ids=source_entity_ids,
        payload={
            "edge": {
                "edge_id": edge_id,
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "relationship_type": RelationshipType.SUPPLY_CHAIN.value,
                "properties": {"integration_prefix": edge_id},
                "weight": 0.7,
                "created_at": NOW,
                "updated_at": NOW,
            }
        },
        validation_status="frozen",
    )


def _assertion_delta(
    delta_id: str,
    assertion_id: str,
    source_node_id: str,
    target_node_id: str,
    source_entity_ids: list[str],
) -> CandidateGraphDelta:
    return CandidateGraphDelta(
        delta_id=delta_id,
        cycle_id="cycle-1",
        delta_type="assertion_add",
        source_entity_ids=source_entity_ids,
        payload={
            "assertion": {
                "assertion_id": assertion_id,
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "assertion_type": "integration",
                "evidence": {"source": "test"},
                "confidence": 0.9,
                "created_at": NOW,
            }
        },
        validation_status="frozen",
    )
