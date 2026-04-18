"""Promotion service entry point."""

from __future__ import annotations

from contracts.schemas import CandidateGraphDelta

from graph_engine.client import Neo4jClient
from graph_engine.models import Neo4jGraphStatus, PromotionPlan
from graph_engine.promotion.interfaces import (
    CandidateDeltaReader,
    CanonicalWriter,
    EntityAnchorReader,
)
from graph_engine.promotion.planner import (
    build_promotion_plan,
    freeze_contract_deltas,
    validate_entity_anchors,
)
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
    """Promote selected contract graph deltas, then optionally mirror them to Neo4j."""

    if sync_to_live_graph and client is None:
        raise ValueError("client is required when sync_to_live_graph is True")
    if sync_to_live_graph and status_manager is None:
        raise ValueError("status_manager is required when sync_to_live_graph is True")

    contract_deltas = [
        _require_contract_delta(delta)
        for delta in candidate_reader.read_candidate_graph_deltas(cycle_id, selection_ref)
    ]
    deltas = freeze_contract_deltas(cycle_id, contract_deltas, entity_reader)
    validate_entity_anchors(deltas, entity_reader)
    plan = build_promotion_plan(cycle_id, selection_ref, deltas)

    if sync_to_live_graph and status_manager is not None:
        status_manager.require_ready()

    canonical_writer.write_canonical_records(plan)

    if sync_to_live_graph and client is not None:
        assert status_manager is not None
        _sync_live_graph_with_status_barrier(plan, client, status_manager)

    return plan


def _require_contract_delta(delta: object) -> CandidateGraphDelta:
    if not isinstance(delta, CandidateGraphDelta):
        raise TypeError(
            "CandidateDeltaReader must return contracts.schemas.CandidateGraphDelta "
            f"values, got {type(delta).__name__}",
        )
    return delta


def _sync_live_graph_with_status_barrier(
    plan: PromotionPlan,
    client: Neo4jClient,
    status_manager: GraphStatusManager,
) -> None:
    """Mirror a promotion only while the ready status token still holds."""

    ready_status, locked_status = status_manager.begin_sync()

    try:
        _require_status_token_unchanged(
            status_manager,
            locked_status,
            "before promotion live sync",
        )
        sync_live_graph(plan, client)
        _require_status_token_unchanged(
            status_manager,
            locked_status,
            "after promotion live sync",
        )
        status_manager.finish_sync(
            expected_status=locked_status,
            ready_status=ready_status,
        )
    except Exception:
        _mark_sync_failed_safely(status_manager, locked_status)
        raise


def _require_status_token_unchanged(
    status_manager: GraphStatusManager,
    expected_status: Neo4jGraphStatus,
    stage: str,
) -> None:
    current_status = status_manager.get_status()
    if current_status != expected_status:
        raise RuntimeError(
            f"graph_status changed {stage}; "
            "live graph mutation must be replayed",
        )


def _mark_sync_failed_safely(
    status_manager: GraphStatusManager,
    locked_status: Neo4jGraphStatus,
) -> None:
    try:
        status_manager.mark_sync_failed(expected_status=locked_status)
    except Exception:  # noqa: BLE001 - preserve the original promotion sync failure.
        return
