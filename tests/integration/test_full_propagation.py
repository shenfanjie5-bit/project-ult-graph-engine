from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.config import load_config_from_env
from graph_engine.models import (
    GraphEdgeRecord,
    GraphImpactSnapshot,
    GraphNodeRecord,
    GraphSnapshot,
    Neo4jGraphStatus,
    PromotionPlan,
)
from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.manager import SchemaManager
from graph_engine.snapshots import build_graph_snapshot, compute_graph_snapshots
from graph_engine.status import GraphStatusManager
from graph_engine.sync import sync_live_graph
from tests.fakes import InMemoryStatusStore

pytestmark = pytest.mark.skipif(
    os.getenv("NEO4J_PASSWORD") is None,
    reason="NEO4J_PASSWORD is not set; Neo4j full propagation integration tests require a database.",
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class StaticRegimeReader:
    def read_regime_context(self, world_state_ref: str) -> dict[str, Any]:
        return {
            "world_state_ref": world_state_ref,
            "channel_multipliers": {
                "fundamental": 1.0,
                "event": 1.0,
                "reflexive": 1.0,
            },
            "regime_multipliers": {
                "fundamental": 1.0,
                "event": 1.0,
                "reflexive": 1.0,
            },
            "decay_policy": {"default": 1.0},
        }


class CapturingSnapshotWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[GraphSnapshot, GraphImpactSnapshot]] = []

    def write_snapshots(
        self,
        graph_snapshot: GraphSnapshot,
        impact_snapshot: GraphImpactSnapshot,
    ) -> None:
        self.calls.append((graph_snapshot, impact_snapshot))


def test_compute_graph_snapshots_generates_three_channel_impact_snapshot() -> None:
    pytest.importorskip("neo4j")

    prefix = f"000-full-propagation-{uuid4().hex}"
    source_node_id = f"{prefix}-source"
    fundamental_target_id = f"{prefix}-fundamental-target"
    event_target_id = f"{prefix}-event-target"
    reflexive_target_id = f"{prefix}-reflexive-target"
    supply_edge_id = f"{prefix}-supply-edge"
    event_edge_id = f"{prefix}-event-edge"
    reflexive_edge_id = f"{prefix}-reflexive-edge"
    node_ids = [
        source_node_id,
        fundamental_target_id,
        event_target_id,
        reflexive_target_id,
    ]
    writer = CapturingSnapshotWriter()

    with Neo4jClient(load_config_from_env()) as client:
        if not client.verify_connectivity():
            pytest.skip("Neo4j is not reachable with the configured environment.")

        manager = SchemaManager(client)
        manager.apply_schema()
        assert manager.verify_schema() is True

        try:
            sync_live_graph(
                _promotion_plan(
                    source_node_id=source_node_id,
                    fundamental_target_id=fundamental_target_id,
                    event_target_id=event_target_id,
                    reflexive_target_id=reflexive_target_id,
                    supply_edge_id=supply_edge_id,
                    event_edge_id=event_edge_id,
                    reflexive_edge_id=reflexive_edge_id,
                ),
                client,
            )
            live_snapshot = build_graph_snapshot(
                "cycle-1",
                1,
                client,
                status_manager=GraphStatusManager(
                    InMemoryStatusStore(_status_for_generation(1)),
                ),
            )
            graph_status = Neo4jGraphStatus(
                graph_status="ready",
                graph_generation_id=live_snapshot.graph_generation_id,
                node_count=live_snapshot.node_count,
                edge_count=live_snapshot.edge_count,
                key_label_counts=live_snapshot.key_label_counts,
                checksum=live_snapshot.checksum,
                last_verified_at=NOW,
                last_reload_at=None,
            )

            try:
                graph_snapshot, impact_snapshot = compute_graph_snapshots(
                    "cycle-1",
                    "world-state-1",
                    client=client,
                    graph_generation_id=1,
                    regime_reader=StaticRegimeReader(),
                    snapshot_writer=writer,
                    status_manager=GraphStatusManager(InMemoryStatusStore(graph_status)),
                    graph_name=f"{prefix}-projection",
                    result_limit=20,
                )
            except RuntimeError as exc:
                if str(exc) == "GDS plugin not available":
                    pytest.skip("GDS plugin is not available in the configured Neo4j instance.")
                raise
        finally:
            client.execute_write(
                "MATCH (node) WHERE node.node_id IN $node_ids DETACH DELETE node",
                {"node_ids": node_ids},
            )

    path_channels_by_edge = {
        (path["edge_id"], path["channel"])
        for path in impact_snapshot.activated_paths
    }
    channels_by_edge_id: dict[str, set[str]] = {}
    for path in impact_snapshot.activated_paths:
        channels_by_edge_id.setdefault(str(path["edge_id"]), set()).add(str(path["channel"]))

    assert (supply_edge_id, "fundamental") in path_channels_by_edge
    assert (event_edge_id, "event") in path_channels_by_edge
    assert (reflexive_edge_id, "reflexive") in path_channels_by_edge
    assert channels_by_edge_id[reflexive_edge_id] == {"reflexive"}
    duplicated_edges = {
        edge_id: channels
        for edge_id, channels in channels_by_edge_id.items()
        if len(channels) > 1
    }
    assert duplicated_edges == {}
    assert set(impact_snapshot.channel_breakdown) == {
        "fundamental",
        "event",
        "reflexive",
        "merged",
    }
    assert impact_snapshot.channel_breakdown["merged"]["enabled_channels"] == [
        "event",
        "fundamental",
        "reflexive",
    ]
    assert graph_snapshot.node_count >= 4
    assert writer.calls == [(graph_snapshot, impact_snapshot)]


def _promotion_plan(
    *,
    source_node_id: str,
    fundamental_target_id: str,
    event_target_id: str,
    reflexive_target_id: str,
    supply_edge_id: str,
    event_edge_id: str,
    reflexive_edge_id: str,
) -> PromotionPlan:
    return PromotionPlan(
        cycle_id="cycle-1",
        selection_ref="selection-1",
        delta_ids=["delta-1", "delta-2", "delta-3"],
        node_records=[
            _node_record(source_node_id),
            _node_record(fundamental_target_id),
            _node_record(event_target_id),
            _node_record(reflexive_target_id),
        ],
        edge_records=[
            _edge_record(
                source_node_id,
                fundamental_target_id,
                supply_edge_id,
                RelationshipType.SUPPLY_CHAIN.value,
                weight=2.0,
            ),
            _edge_record(
                source_node_id,
                event_target_id,
                event_edge_id,
                RelationshipType.EVENT_IMPACT.value,
                weight=3.0,
            ),
            _edge_record(
                source_node_id,
                reflexive_target_id,
                reflexive_edge_id,
                RelationshipType.OWNERSHIP.value,
                weight=4.0,
                extra_properties={"propagation_channel": "reflexive"},
            ),
        ],
        assertion_records=[],
        created_at=NOW,
    )


def _status_for_generation(graph_generation_id: int) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=graph_generation_id,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="pending",
        last_verified_at=NOW,
        last_reload_at=None,
    )


def _node_record(node_id: str) -> GraphNodeRecord:
    return GraphNodeRecord(
        node_id=node_id,
        canonical_entity_id=f"{node_id}-entity",
        label=NodeLabel.ENTITY.value,
        properties={"integration_prefix": node_id},
        created_at=NOW,
        updated_at=NOW,
    )


def _edge_record(
    source_node_id: str,
    target_node_id: str,
    edge_id: str,
    relationship_type: str,
    *,
    weight: float,
    extra_properties: dict[str, Any] | None = None,
) -> GraphEdgeRecord:
    properties = {
        "integration_prefix": edge_id,
        "evidence_confidence": 1.0,
        "recency_decay": 1.0,
    }
    properties.update(extra_properties or {})
    return GraphEdgeRecord(
        edge_id=edge_id,
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        relationship_type=relationship_type,
        properties=properties,
        weight=weight,
        created_at=NOW,
        updated_at=NOW,
    )
