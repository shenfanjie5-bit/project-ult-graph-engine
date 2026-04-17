from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.config import load_config_from_env
from graph_engine.models import (
    ColdReloadPlan,
    GraphEdgeRecord,
    GraphNodeRecord,
    GraphSnapshot,
    Neo4jGraphStatus,
    PromotionPlan,
)
from graph_engine.reload import cold_reload
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.manager import DROP_ALL_CONFIRMATION_TOKEN, SchemaManager
from graph_engine.snapshots import build_graph_snapshot
from graph_engine.status import GraphStatusManager, check_live_graph_consistency
from graph_engine.sync import sync_live_graph

pytestmark = pytest.mark.skipif(
    os.getenv("NEO4J_PASSWORD") is None,
    reason="NEO4J_PASSWORD is not set; Neo4j reload integration tests require a database.",
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class InMemoryStatusStore:
    def __init__(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def read_current_status(self) -> Neo4jGraphStatus | None:
        return self.status

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        if self.status != expected_status:
            return False
        self.write_current_status(next_status)
        return True


class StaticCanonicalReader:
    def __init__(self, plan: ColdReloadPlan) -> None:
        self.plan = plan

    def read_cold_reload_plan(self, snapshot_ref: str) -> ColdReloadPlan:
        return self.plan


class StaticSnapshotReader:
    def __init__(self, snapshot: GraphSnapshot) -> None:
        self.snapshot = snapshot

    def read_graph_snapshot(self, snapshot_ref: str) -> GraphSnapshot:
        return self.snapshot


def test_cold_reload_rebuilds_live_graph_and_gds_projection() -> None:
    pytest.importorskip("neo4j")

    prefix = f"reload-{uuid4().hex}"
    source_node_id = f"{prefix}-source"
    target_node_id = f"{prefix}-target"
    pollution_node_id = f"{prefix}-pollution"
    edge_id = f"{prefix}-edge"
    projection_name = f"graph_engine_reload_{uuid4().hex}"
    promotion_plan = _promotion_plan(source_node_id, target_node_id, edge_id)

    with Neo4jClient(load_config_from_env()) as client:
        if not client.verify_connectivity():
            pytest.skip("Neo4j is not reachable with the configured environment.")

        schema_manager = SchemaManager(client)
        _drop_projection_if_present(client, projection_name)
        schema_manager.drop_all(
            confirmation_token=DROP_ALL_CONFIRMATION_TOKEN,
            test_mode=True,
        )
        schema_manager.apply_schema()
        assert schema_manager.verify_schema() is True

        try:
            sync_live_graph(promotion_plan, client)
            expected_snapshot = build_graph_snapshot("cycle-reload", 7, client)
            plan = ColdReloadPlan(
                snapshot_ref="snapshot-ref-reload",
                cycle_id="cycle-reload",
                expected_snapshot=expected_snapshot,
                node_records=promotion_plan.node_records,
                edge_records=promotion_plan.edge_records,
                assertion_records=promotion_plan.assertion_records,
                projection_name=projection_name,
                created_at=NOW,
            )
            client.execute_write(
                """
CREATE (:Entity {
    node_id: $node_id,
    canonical_entity_id: $canonical_entity_id,
    label: "Entity",
    properties_json: "{}"
})
""",
                {
                    "node_id": pollution_node_id,
                    "canonical_entity_id": f"{prefix}-pollution-entity",
                },
            )

            status_manager = GraphStatusManager(
                InMemoryStatusStore(_status(graph_generation_id=6)),
                clock=lambda: NOW,
            )
            try:
                status = cold_reload(
                    plan.snapshot_ref,
                    client=client,
                    canonical_reader=StaticCanonicalReader(plan),
                    status_manager=status_manager,
                    schema_manager=schema_manager,
                )
            except RuntimeError as exc:
                if str(exc) == "GDS plugin not available":
                    pytest.skip("GDS plugin is not available in the configured Neo4j instance.")
                raise

            assert status.graph_status == "ready"
            assert status.graph_generation_id == expected_snapshot.graph_generation_id
            assert status.node_count == expected_snapshot.node_count
            assert status.edge_count == expected_snapshot.edge_count
            assert status.checksum == expected_snapshot.checksum
            assert status.last_reload_at == NOW
            assert (
                check_live_graph_consistency(
                    plan.snapshot_ref,
                    client=client,
                    snapshot_reader=StaticSnapshotReader(expected_snapshot),
                    status_manager=status_manager,
                )
                is True
            )
            assert _projection_exists(client, projection_name) is True
            assert _node_count(client, pollution_node_id) == 0
        finally:
            _drop_projection_if_present(client, projection_name)
            schema_manager.drop_all(
                confirmation_token=DROP_ALL_CONFIRMATION_TOKEN,
                test_mode=True,
            )


def _promotion_plan(
    source_node_id: str,
    target_node_id: str,
    edge_id: str,
) -> PromotionPlan:
    return PromotionPlan(
        cycle_id="cycle-reload",
        selection_ref="selection-reload",
        delta_ids=[],
        node_records=[
            _node_record(source_node_id, f"{source_node_id}-entity"),
            _node_record(target_node_id, f"{target_node_id}-entity"),
        ],
        edge_records=[_edge_record(source_node_id, target_node_id, edge_id)],
        assertion_records=[],
        created_at=NOW,
    )


def _node_record(node_id: str, canonical_entity_id: str) -> GraphNodeRecord:
    return GraphNodeRecord(
        node_id=node_id,
        canonical_entity_id=canonical_entity_id,
        label=NodeLabel.ENTITY.value,
        properties={"integration_prefix": node_id},
        created_at=NOW,
        updated_at=NOW,
    )


def _edge_record(
    source_node_id: str,
    target_node_id: str,
    edge_id: str,
) -> GraphEdgeRecord:
    return GraphEdgeRecord(
        edge_id=edge_id,
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        relationship_type=RelationshipType.SUPPLY_CHAIN.value,
        properties={"integration_prefix": edge_id},
        weight=0.7,
        created_at=NOW,
        updated_at=NOW,
    )


def _status(*, graph_generation_id: int) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=graph_generation_id,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="old-checksum",
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _projection_exists(client: Neo4jClient, projection_name: str) -> bool:
    rows = client.execute_read(
        "CALL gds.graph.exists($projection_name) YIELD exists RETURN exists",
        {"projection_name": projection_name},
    )
    return bool(rows and rows[0].get("exists") is True)


def _drop_projection_if_present(client: Neo4jClient, projection_name: str) -> None:
    try:
        if _projection_exists(client, projection_name):
            client.execute_write(
                "CALL gds.graph.drop($projection_name) YIELD graphName RETURN graphName",
                {"projection_name": projection_name},
            )
    except Exception as exc:  # noqa: BLE001 - cleanup should not fail when GDS is absent.
        if "gds." not in str(exc).lower():
            raise


def _node_count(client: Neo4jClient, node_id: str) -> int:
    rows = client.execute_read(
        "MATCH (node {node_id: $node_id}) RETURN count(node) AS node_count",
        {"node_id": node_id},
    )
    return int(rows[0]["node_count"])
