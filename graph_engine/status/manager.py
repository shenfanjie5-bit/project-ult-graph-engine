"""State machine for the current Neo4j live graph status."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Condition, RLock, get_ident
from typing import Literal

from graph_engine.models import GraphSnapshot, Neo4jGraphStatus
from graph_engine.status.store import StatusStore

_STORE_BARRIER_ATTR = "_graph_engine_status_read_barrier"
_BARRIER_REGISTRY_LOCK = RLock()
_BARRIER_REGISTRY: dict[int, "_ReadyReadBarrier"] = {}


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
        self._read_barrier = _barrier_for_store(store)

    def get_status(self) -> Neo4jGraphStatus:
        """Return the current status or raise when the status row is missing."""

        status = self.store.read_current_status()
        if status is None:
            raise LookupError("Neo4j graph status has not been initialized")
        return status

    def require_ready(self) -> Neo4jGraphStatus:
        """Return the current ready status or block non-ready graph reads."""

        return require_ready_status(self.get_status())

    @contextmanager
    def ready_read(self) -> Iterator[Neo4jGraphStatus]:
        """Hold a ready read lease across live Neo4j reads."""

        with self._read_barrier.ready_read(self.get_status) as status:
            yield status

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
        self._commit_transition(current, status)
        return status

    def begin_sync(self) -> tuple[Neo4jGraphStatus, Neo4jGraphStatus]:
        """Acquire an exclusive live-graph writer token for incremental sync."""

        current = self.require_ready()
        status = _copy_status_with_graph_status(current, "syncing")
        self._commit_transition(current, status)
        return current, status

    def finish_sync(
        self,
        *,
        expected_status: Neo4jGraphStatus,
        ready_status: Neo4jGraphStatus,
    ) -> Neo4jGraphStatus:
        """Release an incremental sync writer token back to its ready status."""

        if expected_status.graph_status != "syncing":
            raise ValueError("finish_sync requires expected graph_status='syncing'")
        if ready_status.graph_status != "ready":
            raise ValueError("finish_sync requires ready graph_status='ready'")

        self._commit_transition(expected_status, ready_status)
        return ready_status

    def mark_sync_failed(
        self,
        *,
        expected_status: Neo4jGraphStatus,
        node_count: int = 0,
        edge_count: int = 0,
        key_label_counts: dict[str, int] | None = None,
        checksum: str = "failed",
    ) -> Neo4jGraphStatus:
        """Release a failed incremental sync by exposing live graph failure."""

        if expected_status.graph_status != "syncing":
            raise ValueError("mark_sync_failed requires expected graph_status='syncing'")

        status = Neo4jGraphStatus(
            graph_status="failed",
            graph_generation_id=expected_status.graph_generation_id,
            node_count=node_count,
            edge_count=edge_count,
            key_label_counts={} if key_label_counts is None else dict(key_label_counts),
            checksum=checksum,
            last_verified_at=None,
            last_reload_at=expected_status.last_reload_at,
        )
        self._commit_transition(expected_status, status)
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
        self._commit_transition(current, status)
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
        if current_state not in {"rebuilding", "ready", "syncing"}:
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
        self._commit_transition(current, status)
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
        self._commit_transition(current, status)
        return status

    def _commit_transition(
        self,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> None:
        with self._read_barrier.transition():
            if not self.store.compare_and_write_current_status(
                expected_status=expected_status,
                next_status=next_status,
            ):
                raise RuntimeError(
                    "stale graph_status transition rejected because current status changed",
                )


class _ReadyReadBarrier:
    """Coordinate ready reads and status transitions for one status store."""

    def __init__(self) -> None:
        self._condition = Condition(RLock())
        self._active_read_leases = 0
        self._pending_transitions = 0
        self._read_depth_by_thread: dict[int, int] = {}

    @contextmanager
    def ready_read(
        self,
        read_status: Callable[[], Neo4jGraphStatus],
    ) -> Iterator[Neo4jGraphStatus]:
        thread_id = get_ident()
        acquired_outer_lease = False
        with self._condition:
            depth = self._read_depth_by_thread.get(thread_id, 0)
            if depth > 0:
                status = require_ready_status(read_status())
                self._read_depth_by_thread[thread_id] = depth + 1
            else:
                while self._pending_transitions > 0:
                    self._condition.wait()
                status = require_ready_status(read_status())
                self._active_read_leases += 1
                self._read_depth_by_thread[thread_id] = 1
                acquired_outer_lease = True
        try:
            yield status
        finally:
            with self._condition:
                depth = self._read_depth_by_thread[thread_id]
                if depth > 1:
                    self._read_depth_by_thread[thread_id] = depth - 1
                else:
                    del self._read_depth_by_thread[thread_id]
                    if acquired_outer_lease:
                        self._active_read_leases -= 1
                        if self._active_read_leases == 0:
                            self._condition.notify_all()

    @contextmanager
    def transition(self) -> Iterator[None]:
        with self._condition:
            self._pending_transitions += 1
            while self._active_read_leases > 0:
                self._condition.wait()
        try:
            yield
        finally:
            with self._condition:
                self._pending_transitions -= 1
                self._condition.notify_all()


def _barrier_for_store(store: StatusStore) -> _ReadyReadBarrier:
    with _BARRIER_REGISTRY_LOCK:
        barrier = getattr(store, _STORE_BARRIER_ATTR, None)
        if isinstance(barrier, _ReadyReadBarrier):
            return barrier

        barrier = _BARRIER_REGISTRY.get(id(store))
        if barrier is None:
            barrier = _ReadyReadBarrier()
            _BARRIER_REGISTRY[id(store)] = barrier

        try:
            setattr(store, _STORE_BARRIER_ATTR, barrier)
        except Exception:
            return barrier
        return barrier


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _copy_status_with_graph_status(
    status: Neo4jGraphStatus,
    graph_status: Literal["ready", "syncing", "rebuilding", "failed"],
) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status=graph_status,
        graph_generation_id=status.graph_generation_id,
        node_count=status.node_count,
        edge_count=status.edge_count,
        key_label_counts=dict(status.key_label_counts),
        checksum=status.checksum,
        last_verified_at=None if graph_status != "ready" else status.last_verified_at,
        last_reload_at=status.last_reload_at,
    )
