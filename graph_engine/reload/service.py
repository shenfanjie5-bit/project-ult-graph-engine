"""Cold reload orchestration for rebuilding the Neo4j live graph."""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from time import monotonic
from typing import TypeVar

from graph_engine.client import Neo4jClient
from graph_engine.models import ColdReloadPlan, GraphSnapshot, Neo4jGraphStatus, PromotionPlan
from graph_engine.reload.interfaces import CanonicalReader
from graph_engine.reload.projection import rebuild_gds_projection
from graph_engine.schema.manager import DROP_ALL_CONFIRMATION_TOKEN, SchemaManager
from graph_engine.status import GraphStatusManager, check_live_graph_consistency
from graph_engine.sync import sync_live_graph

_LOGGER = logging.getLogger(__name__)
_T = TypeVar("_T")


class ColdReloadTimeoutError(TimeoutError):
    """Raised when cold reload exceeds its hard runtime budget."""


def cold_reload(
    snapshot_ref: str,
    *,
    client: Neo4jClient,
    canonical_reader: CanonicalReader,
    status_manager: GraphStatusManager,
    schema_manager: SchemaManager | None = None,
    batch_size: int = 1000,
    timeout_seconds: float = 300.0,
) -> Neo4jGraphStatus:
    """Rebuild Neo4j from canonical truth and publish ready status only after verification."""

    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")

    manager = schema_manager or SchemaManager(client)
    deadline = monotonic() + timeout_seconds
    entered_rebuilding = False

    try:
        rebuilding_status = _run_stage_with_deadline(
            deadline,
            "mark_rebuilding",
            status_manager.mark_rebuilding,
        )
        entered_rebuilding = True

        plan = _run_stage_with_deadline(
            deadline,
            "read_cold_reload_plan",
            lambda: canonical_reader.read_cold_reload_plan(snapshot_ref),
        )

        _run_stage_with_deadline(
            deadline,
            "drop_all",
            lambda: manager.drop_all(
                confirmation_token=DROP_ALL_CONFIRMATION_TOKEN,
                graph_status=rebuilding_status,
            ),
        )

        _run_stage_with_deadline(
            deadline,
            "sync_live_graph",
            lambda: sync_live_graph(
                build_reload_promotion_plan(plan, snapshot_ref=snapshot_ref),
                client,
                batch_size=batch_size,
            ),
        )

        _run_stage_with_deadline(deadline, "apply_schema", manager.apply_schema)

        schema_verified = _run_stage_with_deadline(
            deadline,
            "verify_schema",
            manager.verify_schema,
        )
        if not schema_verified:
            raise RuntimeError("schema verification failed after cold reload")

        _run_stage_with_deadline(
            deadline,
            "rebuild_gds_projection",
            lambda: rebuild_gds_projection(client, plan.projection_name),
        )

        is_consistent = _run_stage_with_deadline(
            deadline,
            "check_live_graph_consistency",
            lambda: check_live_graph_consistency(
                snapshot_ref,
                client=client,
                snapshot_reader=_ExpectedSnapshotReader(plan.expected_snapshot),
                require_ready=False,
            ),
        )
        if not is_consistent:
            raise RuntimeError("live graph consistency check failed after cold reload")

        return _run_stage_with_deadline(
            deadline,
            "mark_ready",
            lambda: status_manager.mark_ready(
                node_count=plan.expected_snapshot.node_count,
                edge_count=plan.expected_snapshot.edge_count,
                key_label_counts=plan.expected_snapshot.key_label_counts,
                checksum=plan.expected_snapshot.checksum,
                graph_generation_id=plan.expected_snapshot.graph_generation_id,
                reload_completed=True,
            ),
        )
    except Exception:
        if entered_rebuilding:
            _mark_failed_safely(status_manager)
        raise


def build_reload_promotion_plan(
    plan: ColdReloadPlan,
    *,
    snapshot_ref: str,
) -> PromotionPlan:
    """Adapt a cold reload plan into the sync layer's canonical promotion batch."""

    return PromotionPlan(
        cycle_id=plan.cycle_id,
        selection_ref=f"cold-reload:{snapshot_ref}",
        delta_ids=[],
        node_records=plan.node_records,
        edge_records=plan.edge_records,
        assertion_records=plan.assertion_records,
        created_at=plan.created_at,
    )


class _ExpectedSnapshotReader:
    def __init__(self, snapshot: GraphSnapshot) -> None:
        self._snapshot = snapshot

    def read_graph_snapshot(self, snapshot_ref: str) -> GraphSnapshot:
        return self._snapshot


def _raise_if_deadline_exceeded(deadline: float, stage: str) -> None:
    if monotonic() > deadline:
        raise ColdReloadTimeoutError(f"cold reload timed out before {stage}")


def _run_stage_with_deadline(
    deadline: float,
    stage: str,
    operation: Callable[[], _T],
) -> _T:
    remaining_seconds = deadline - monotonic()
    if remaining_seconds <= 0:
        raise ColdReloadTimeoutError(f"cold reload timed out before {stage}")

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"cold-reload-{stage}")
    future = executor.submit(operation)
    timed_out = False
    try:
        return future.result(timeout=remaining_seconds)
    except FutureTimeoutError as exc:
        timed_out = True
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise ColdReloadTimeoutError(f"cold reload timed out during {stage}") from exc
    finally:
        if not timed_out:
            executor.shutdown(wait=True)


def _mark_failed_safely(status_manager: GraphStatusManager) -> None:
    try:
        status_manager.mark_failed()
    except Exception:  # noqa: BLE001 - preserve the original reload failure.
        _LOGGER.exception("failed to mark cold reload status as failed")
