"""Shared test fakes."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from graph_engine.models import Neo4jGraphStatus


class InMemoryStatusStore:
    """In-memory StatusStore fake with write and CAS observability."""

    def __init__(self, status: Neo4jGraphStatus | None = None) -> None:
        self.status = status
        self.writes: list[Neo4jGraphStatus] = []
        self.compare_calls: list[tuple[Neo4jGraphStatus | None, Neo4jGraphStatus]] = []

    @property
    def graph_status(self) -> Neo4jGraphStatus | None:
        return self.status

    @graph_status.setter
    def graph_status(self, status: Neo4jGraphStatus | None) -> None:
        self.status = status

    def read_current_status(self) -> Neo4jGraphStatus | None:
        return self.status

    @contextmanager
    def ready_read_current_status(self) -> Iterator[Neo4jGraphStatus | None]:
        yield self.read_current_status()

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        self.status = status
        self.writes.append(status)

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        if isinstance(self.compare_calls, list):
            self.compare_calls.append((expected_status, next_status))
        if self.status != expected_status:
            return False
        self.write_current_status(next_status)
        return True
