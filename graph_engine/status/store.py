"""Status storage boundary for the current Neo4j live graph state."""

from __future__ import annotations

from typing import Protocol

from graph_engine.models import Neo4jGraphStatus


class StatusStore(Protocol):
    """Persistence boundary for the singleton Neo4j graph status row."""

    def read_current_status(self) -> Neo4jGraphStatus | None:
        """Return the current status row, or None before bootstrap."""

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        """Persist the current status row."""

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        """Atomically persist next_status only if the current row still matches."""
