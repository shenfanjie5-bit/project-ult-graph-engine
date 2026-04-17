"""Phase 0 consistency checks between canonical snapshots and the live graph."""

from __future__ import annotations

import logging
from typing import Protocol

from graph_engine.client import Neo4jClient
from graph_engine.live_metrics import read_live_graph_metrics
from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.status.manager import GraphStatusManager

_LOGGER = logging.getLogger(__name__)


class CanonicalSnapshotReader(Protocol):
    """Read canonical graph snapshots from the data-platform/Iceberg boundary."""

    def read_graph_snapshot(self, snapshot_ref: str) -> GraphSnapshot:
        """Return the canonical snapshot referenced by snapshot_ref."""


def check_live_graph_consistency(
    snapshot_ref: str,
    *,
    client: Neo4jClient,
    snapshot_reader: CanonicalSnapshotReader,
    status_manager: GraphStatusManager | None = None,
    require_ready: bool = True,
) -> bool:
    """Return whether live Neo4j metrics match the canonical graph snapshot."""

    if require_ready:
        if status_manager is None:
            raise ValueError("status_manager is required when require_ready=True")
        with status_manager.ready_read() as status:
            return _check_live_graph_consistency_after_status(
                snapshot_ref,
                client=client,
                snapshot_reader=snapshot_reader,
                status=status,
            )

    status = _resolve_status(status_manager, require_ready=False)
    return _check_live_graph_consistency_after_status(
        snapshot_ref,
        client=client,
        snapshot_reader=snapshot_reader,
        status=status,
    )


def _check_live_graph_consistency_after_status(
    snapshot_ref: str,
    *,
    client: Neo4jClient,
    snapshot_reader: CanonicalSnapshotReader,
    status: Neo4jGraphStatus | None,
) -> bool:
    """Run consistency reads after the caller has established status safety."""

    try:
        snapshot = snapshot_reader.read_graph_snapshot(snapshot_ref)
    except Exception:
        _LOGGER.exception(
            "live graph consistency snapshot read failed",
            extra={"snapshot_ref": snapshot_ref, "failure_stage": "snapshot_read"},
        )
        return False

    try:
        node_count, edge_count, key_label_counts, checksum = _read_live_graph_metrics(client)
    except Exception:
        _LOGGER.exception(
            "live graph consistency live graph read failed",
            extra={"snapshot_ref": snapshot_ref, "failure_stage": "live_graph_read"},
        )
        return False

    if status is not None and snapshot.graph_generation_id != status.graph_generation_id:
        _LOGGER.warning(
            "live graph consistency generation mismatch",
            extra={
                "snapshot_ref": snapshot_ref,
                "failure_stage": "result_validation",
                "snapshot_generation_id": snapshot.graph_generation_id,
                "status_generation_id": status.graph_generation_id,
            },
        )
        return False

    mismatches = [
        field_name
        for field_name, snapshot_value, live_value in (
            ("node_count", snapshot.node_count, node_count),
            ("edge_count", snapshot.edge_count, edge_count),
            ("key_label_counts", snapshot.key_label_counts, key_label_counts),
            ("checksum", snapshot.checksum, checksum),
        )
        if snapshot_value != live_value
    ]
    if mismatches:
        _LOGGER.warning(
            "live graph consistency metric mismatch",
            extra={
                "snapshot_ref": snapshot_ref,
                "failure_stage": "result_validation",
                "mismatches": mismatches,
            },
        )
        return False
    return True


def _resolve_status(
    status_manager: GraphStatusManager | None,
    *,
    require_ready: bool,
) -> Neo4jGraphStatus | None:
    if require_ready:
        if status_manager is None:
            raise ValueError("status_manager is required when require_ready=True")
        return status_manager.require_ready()

    if status_manager is None:
        return None
    try:
        return status_manager.get_status()
    except LookupError:
        return None


def _read_live_graph_metrics(client: Neo4jClient) -> tuple[int, int, dict[str, int], str]:
    return read_live_graph_metrics(client, strict=True)
