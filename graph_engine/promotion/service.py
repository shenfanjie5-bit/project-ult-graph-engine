"""Promotion service entry point."""

from __future__ import annotations

from graph_engine.client import Neo4jClient
from graph_engine.models import Neo4jGraphStatus, PromotionPlan
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
        _sync_live_graph_with_status_barrier(plan, client, status_manager)

    return plan


def _sync_live_graph_with_status_barrier(
    plan: PromotionPlan,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
) -> None:
    """Mirror a promotion only while the ready status token still holds."""

    ready_status = status_manager.require_ready()
    if not status_manager.store.compare_and_write_current_status(
        expected_status=ready_status,
        next_status=ready_status,
    ):
        raise RuntimeError(
            "stale graph_status writer barrier rejected before promotion live sync",
        )

    sync_live_graph(plan, client)
    _require_status_token_unchanged(status_manager, ready_status)


def _require_status_token_unchanged(
    status_manager: GraphStatusManager,
    ready_status: Neo4jGraphStatus,
) -> None:
    current_status = status_manager.get_status()
    if current_status != ready_status:
        raise RuntimeError(
            "graph_status changed during promotion live sync; "
            "live graph mutation must be replayed",
        )
