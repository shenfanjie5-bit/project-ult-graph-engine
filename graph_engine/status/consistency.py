"""Phase 0 consistency checks between canonical snapshots and the live graph."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol

from graph_engine.client import Neo4jClient
from graph_engine.live_metrics import read_live_graph_metrics
from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.status.manager import GraphStatusManager, ready_read

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

    with _status_read_context(status_manager, require_ready=require_ready) as status:
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


@contextmanager
def _status_read_context(
    status_manager: GraphStatusManager | None,
    *,
    require_ready: bool,
) -> Iterator[Neo4jGraphStatus | None]:
    if require_ready:
        with ready_read(status_manager, "live graph consistency") as status:
            yield status
        return

    if status_manager is None:
        yield None
        return
    try:
        yield status_manager.get_status()
    except LookupError:
        yield None


def _read_live_graph_metrics(client: Neo4jClient) -> tuple[int, int, dict[str, int], str]:
    return read_live_graph_metrics(client, strict=True)
