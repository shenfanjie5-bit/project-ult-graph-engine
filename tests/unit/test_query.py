from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

import pytest

from graph_engine.models import Neo4jGraphStatus
from graph_engine.query import MAX_QUERY_DEPTH, query_subgraph, simulate_readonly_impact
from graph_engine.status import GraphStatusManager

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class InMemoryStatusStore:
    def __init__(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def read_current_status(self) -> Neo4jGraphStatus | None:
        return self.status

    def write_current_status(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def compare_and_write_current_status(
        self,
        *,
        expected_status: Neo4jGraphStatus | None,
        next_status: Neo4jGraphStatus,
    ) -> bool:
        if self.status != expected_status:
            return False
        self.write_current_status(next_status)
        return True


class FakeQueryClient:
    def __init__(self) -> None:
        self.read_calls: list[tuple[str, dict[str, Any]]] = []
        self.write_calls: list[tuple[str, dict[str, Any]]] = []

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.read_calls.append((query, parameters or {}))
        return [
            {
                "subgraph_nodes": [
                    {
                        "node_id": "node-a",
                        "canonical_entity_id": "entity-a",
                        "labels": ["Entity"],
                    }
                ],
                "subgraph_edges": [],
            }
        ]

    def execute_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.write_calls.append((query, parameters or {}))
        return []


@pytest.mark.parametrize("graph_status", ["syncing", "rebuilding", "failed"])
def test_query_subgraph_blocks_non_ready_status_before_reads(
    graph_status: Literal["syncing", "rebuilding", "failed"],
) -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="ready"):
        query_subgraph(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(graph_status=graph_status),
        )

    assert client.read_calls == []
    assert client.write_calls == []


def test_query_subgraph_returns_ready_result() -> None:
    client = FakeQueryClient()

    result = query_subgraph(
        ["entity-a"],
        2,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert result == {
        "graph_generation_id": 3,
        "subgraph_nodes": [
            {
                "node_id": "node-a",
                "canonical_entity_id": "entity-a",
                "labels": ["Entity"],
            }
        ],
        "subgraph_edges": [],
        "status": "ready",
    }
    assert client.read_calls[0][1] == {"seed_entities": ["entity-a"]}
    assert "[*0..2]" in client.read_calls[0][0]
    assert client.write_calls == []


def test_query_subgraph_rejects_depth_above_supported_bound_before_reads() -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match=f"<= {MAX_QUERY_DEPTH}"):
        query_subgraph(
            ["entity-a"],
            MAX_QUERY_DEPTH + 1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
        )

    assert client.read_calls == []
    assert client.write_calls == []


@pytest.mark.parametrize("graph_status", ["syncing", "rebuilding", "failed"])
def test_simulate_readonly_impact_blocks_non_ready_status_before_reads(
    graph_status: Literal["syncing", "rebuilding", "failed"],
) -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="ready"):
        simulate_readonly_impact(
            ["entity-a"],
            {"depth": 1},
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(graph_status=graph_status),
        )

    assert client.read_calls == []
    assert client.write_calls == []


def test_simulate_readonly_impact_uses_ready_gated_subgraph_read() -> None:
    client = FakeQueryClient()

    result = simulate_readonly_impact(
        ["entity-a"],
        {"depth": 2},
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert result["graph_generation_id"] == 3
    assert result["status"] == "ready"
    assert result["impacted_subgraph"]["subgraph_nodes"][0]["node_id"] == "node-a"
    assert "[*0..2]" in client.read_calls[0][0]
    assert client.write_calls == []


def test_simulate_readonly_impact_rejects_depth_above_supported_bound_before_reads() -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match=f"<= {MAX_QUERY_DEPTH}"):
        simulate_readonly_impact(
            ["entity-a"],
            {"depth": MAX_QUERY_DEPTH + 1},
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
        )

    assert client.read_calls == []
    assert client.write_calls == []


def _status_manager(
    *,
    graph_status: Literal["ready", "syncing", "rebuilding", "failed"] = "ready",
) -> GraphStatusManager:
    return GraphStatusManager(InMemoryStatusStore(_status(graph_status=graph_status)))


def _status(
    *,
    graph_status: Literal["ready", "syncing", "rebuilding", "failed"] = "ready",
) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status=graph_status,
        graph_generation_id=3,
        node_count=1,
        edge_count=0,
        key_label_counts={"Entity": 1},
        checksum="query-ready",
        last_verified_at=NOW if graph_status == "ready" else None,
        last_reload_at=None,
    )
