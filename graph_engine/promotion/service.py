"""Promotion service entry point."""

from __future__ import annotations

from graph_engine.client import Neo4jClient
from graph_engine.models import PromotionPlan
from graph_engine.promotion.interfaces import (
    CandidateDeltaReader,
    CanonicalWriter,
    EntityAnchorReader,
)
from graph_engine.promotion.planner import build_promotion_plan, validate_entity_anchors
from graph_engine.status import GraphStatusManager
from graph_engine.sync import sync_live_graph


def promote_graph_deltas(
    cycle_id: str,
    selection_ref: str,
    *,
    candidate_reader: CandidateDeltaReader,
    entity_reader: EntityAnchorReader,
    canonical_writer: CanonicalWriter,
    client: Neo4jClient | None = None,
    status_manager: GraphStatusManager | None = None,
    sync_to_live_graph: bool = True,
) -> PromotionPlan:
    """Promote selected frozen graph deltas, then optionally mirror them to Neo4j."""

    if sync_to_live_graph and client is None:
        raise ValueError("client is required when sync_to_live_graph is True")
    if sync_to_live_graph and status_manager is None:
        raise ValueError("status_manager is required when sync_to_live_graph is True")

    deltas = candidate_reader.read_candidate_graph_deltas(cycle_id, selection_ref)
    validate_entity_anchors(deltas, entity_reader)
    plan = build_promotion_plan(cycle_id, selection_ref, deltas)

    if sync_to_live_graph and status_manager is not None:
        status_manager.require_ready()

    canonical_writer.write_canonical_records(plan)

    if sync_to_live_graph and client is not None:
        assert status_manager is not None
        status_manager.require_ready()
        sync_live_graph(plan, client)

    return plan
