"""State machine for the current Neo4j live graph status."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.status.store import StatusStore


def require_ready_status(graph_status: Neo4jGraphStatus) -> Neo4jGraphStatus:
    """Return a ready status or block reads from a non-ready graph."""

    if graph_status.graph_status != "ready":
        raise PermissionError(
            "Neo4j live graph reads require graph_status='ready'; "
            f"received {graph_status.graph_status!r}",
        )
    return graph_status


class GraphStatusManager:
    """Manage allowed status transitions for the Neo4j live graph."""

    def __init__(
        self,
        store: StatusStore,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self._clock = clock or _utc_now

    def get_status(self) -> Neo4jGraphStatus:
        """Return the current status or raise when the status row is missing."""

        status = self.store.read_current_status()
        if status is None:
            raise LookupError("Neo4j graph status has not been initialized")
        return status

    def require_ready(self) -> Neo4jGraphStatus:
        """Return the current ready status or block non-ready graph reads."""

        return require_ready_status(self.get_status())

    def mark_rebuilding(self) -> Neo4jGraphStatus:
        """Move an empty, ready, or failed status into rebuilding."""

        current = self.store.read_current_status()
        current_state = current.graph_status if current is not None else None
        if current_state == "rebuilding":
            raise ValueError("cannot transition graph_status from 'rebuilding' to 'rebuilding'")

        if current_state not in {None, "ready", "failed"}:
            raise ValueError(f"cannot transition graph_status from {current_state!r} to 'rebuilding'")

        status = Neo4jGraphStatus(
            graph_status="rebuilding",
            graph_generation_id=current.graph_generation_id if current is not None else 0,
            node_count=0,
            edge_count=0,
            key_label_counts={},
            checksum="rebuilding",
            last_verified_at=None,
            last_reload_at=current.last_reload_at if current is not None else None,
        )
        self.store.write_current_status(status)
        return status

    def mark_ready(
        self,
        *,
        node_count: int,
        edge_count: int,
        key_label_counts: dict[str, int],
        checksum: str,
        graph_generation_id: int | None = None,
        reload_completed: bool = False,
    ) -> Neo4jGraphStatus:
        """Move from rebuilding to ready after live graph verification passes."""

        current = self.store.read_current_status()
        current_state = current.graph_status if current is not None else None
        if current_state != "rebuilding":
            raise ValueError(
                "cannot transition graph_status from "
                f"{current_state!r} to 'ready'",
            )
        assert current is not None

        next_generation_id = (
            current.graph_generation_id + 1
            if graph_generation_id is None
            else graph_generation_id
        )
        if next_generation_id < current.graph_generation_id:
            raise ValueError(
                "graph_generation_id cannot move backwards: "
                f"current={current.graph_generation_id}, requested={next_generation_id}",
            )

        now = self._clock()
        status = Neo4jGraphStatus(
            graph_status="ready",
            graph_generation_id=next_generation_id,
            node_count=node_count,
            edge_count=edge_count,
            key_label_counts=dict(key_label_counts),
            checksum=checksum,
            last_verified_at=now,
            last_reload_at=now if reload_completed else current.last_reload_at,
        )
        self.store.write_current_status(status)
        return status

    def mark_failed(
        self,
        *,
        node_count: int = 0,
        edge_count: int = 0,
        key_label_counts: dict[str, int] | None = None,
        checksum: str = "failed",
    ) -> Neo4jGraphStatus:
        """Expose the live graph as failed without silently restoring readiness."""

        current = self.store.read_current_status()
        current_state = current.graph_status if current is not None else None
        if current_state not in {"rebuilding", "ready"}:
            raise ValueError(
                "cannot transition graph_status from "
                f"{current_state!r} to 'failed'",
            )
        assert current is not None

        status = Neo4jGraphStatus(
            graph_status="failed",
            graph_generation_id=current.graph_generation_id,
            node_count=node_count,
            edge_count=edge_count,
            key_label_counts={} if key_label_counts is None else dict(key_label_counts),
            checksum=checksum,
            last_verified_at=None,
            last_reload_at=current.last_reload_at,
        )
        self.store.write_current_status(status)
        return status

    def mark_verified(self, snapshot: GraphSnapshot) -> Neo4jGraphStatus:
        """Refresh ready metrics from a canonical snapshot without bumping generation."""

        current = self.require_ready()
        if snapshot.graph_generation_id != current.graph_generation_id:
            raise ValueError(
                "snapshot graph_generation_id disagrees with current status: "
                f"snapshot={snapshot.graph_generation_id}, "
                f"current={current.graph_generation_id}",
            )

        status = Neo4jGraphStatus(
            graph_status="ready",
            graph_generation_id=current.graph_generation_id,
            node_count=snapshot.node_count,
            edge_count=snapshot.edge_count,
            key_label_counts=dict(snapshot.key_label_counts),
            checksum=snapshot.checksum,
            last_verified_at=self._clock(),
            last_reload_at=current.last_reload_at,
        )
        self.store.write_current_status(status)
        return status


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
