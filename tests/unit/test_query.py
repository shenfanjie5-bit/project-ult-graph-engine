from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

import pytest

from graph_engine.models import Neo4jGraphStatus, ReadonlySimulationRequest
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

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.read_calls.append((query, parameters or {}))
        return [
            {
                "nodes": [
                    {
                        "node_id": "node-a",
                        "canonical_entity_id": "entity-a",
                        "labels": ["Entity"],
                    },
                    {
                        "node_id": "node-b",
                        "canonical_entity_id": "entity-b",
                        "labels": ["Entity"],
                    },
                ],
                "relationships": [
                    {
                        "edge_id": "edge-1",
                        "relationship_type": "SUPPLY_CHAIN",
                        "source_node_id": "node-a",
                        "target_node_id": "node-b",
                    }
                ],
            }
        ]


@pytest.mark.parametrize("graph_status", ["rebuilding", "failed"])
def test_query_subgraph_blocks_non_ready_before_neo4j_reads(
    graph_status: Literal["rebuilding", "failed"],
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


def test_query_subgraph_requires_status_manager_before_neo4j_reads() -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match="status_manager"):
        query_subgraph(["entity-a"], 1, client=client)  # type: ignore[arg-type]

    assert client.read_calls == []


def test_query_subgraph_rejects_depth_above_supported_bound_before_reading() -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match="depth"):
        query_subgraph(
            ["entity-a"],
            MAX_QUERY_DEPTH + 1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
        )

    assert client.read_calls == []


def test_query_subgraph_reads_after_ready_gate() -> None:
    client = FakeQueryClient()

    result = query_subgraph(
        ["entity-a"],
        2,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert result["seed_entities"] == ["entity-a"]
    assert result["depth"] == 2
    assert [node["node_id"] for node in result["nodes"]] == ["node-a", "node-b"]
    assert len(result["relationships"]) == 1
    assert len(client.read_calls) == 1
    assert "*0..2" in client.read_calls[0][0]
    assert client.read_calls[0][1] == {"seed_entities": ["entity-a"]}


@pytest.mark.parametrize("graph_status", ["rebuilding", "failed"])
def test_simulate_readonly_impact_blocks_non_ready_before_neo4j_reads(
    graph_status: Literal["rebuilding", "failed"],
) -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="ready"):
        simulate_readonly_impact(
            ["entity-a"],
            _simulation_request(),
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(graph_status=graph_status),
        )

    assert client.read_calls == []


def test_simulate_readonly_impact_reads_after_ready_gate() -> None:
    client = FakeQueryClient()

    result = simulate_readonly_impact(
        ["entity-a"],
        _simulation_request(depth=1, result_limit=1),
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert result["graph_generation_id"] == 1
    assert result["subgraph"]["depth"] == 1
    assert result["impacted_entities"] == [
        {
            "node_id": "node-a",
            "canonical_entity_id": "entity-a",
            "labels": ["Entity"],
            "score": 1.0,
            "is_seed": True,
        }
    ]
    assert len(client.read_calls) == 1


def _status_manager(
    *,
    graph_status: Literal["ready", "rebuilding", "failed"] = "ready",
    graph_generation_id: int = 1,
) -> GraphStatusManager:
    return GraphStatusManager(
        InMemoryStatusStore(
            Neo4jGraphStatus(
                graph_status=graph_status,
                graph_generation_id=graph_generation_id,
                node_count=2,
                edge_count=1,
                key_label_counts={"Entity": 2},
                checksum="abc123" if graph_status == "ready" else graph_status,
                last_verified_at=NOW if graph_status == "ready" else None,
                last_reload_at=None,
            )
        )
    )


def _simulation_request(
    *,
    depth: int = 1,
    result_limit: int = 100,
) -> ReadonlySimulationRequest:
    return ReadonlySimulationRequest(
        cycle_id="cycle-1",
        world_state_ref="world-state-1",
        graph_generation_id=1,
        depth=depth,
        result_limit=result_limit,
    )
