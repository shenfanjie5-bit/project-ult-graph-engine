from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Literal

import pytest
from pydantic import ValidationError

from graph_engine.models import GraphQueryResult, Neo4jGraphStatus
from graph_engine.query import (
    MAX_QUERY_DEPTH,
    query_propagation_paths,
    query_subgraph,
)
from graph_engine.status import GraphStatusManager
from tests.fakes import InMemoryStatusStore

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)
_MUTATION_PATTERN = re.compile(r"\b(MERGE|SET|DELETE|CREATE)\b", re.IGNORECASE)


class FakeQueryClient:
    def __init__(self) -> None:
        self.read_calls: list[tuple[str, dict[str, Any]]] = []
        self.write_calls: list[tuple[str, dict[str, Any]]] = []

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        call_parameters = parameters or {}
        self.read_calls.append((query, call_parameters))
        assert _MUTATION_PATTERN.search(query) is None
        if "RETURN nodes, relationships" in query:
            return [{"nodes": _node_rows(), "relationships": _edge_rows()}]
        if " AS paths" in query:
            if "RETURN [] AS paths" in query:
                return [{"paths": []}]
            return [{"paths": _path_rows(call_parameters.get("channels"))}]
        return []

    def execute_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.write_calls.append((query, parameters or {}))
        raise AssertionError("query APIs must not call execute_write")


class GateAwareClient(FakeQueryClient):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        assert self.events == ["enter"]
        return super().execute_read(query, parameters)


class RecordingStatusManager:
    def __init__(self, status: Neo4jGraphStatus, events: list[str]) -> None:
        self.status = status
        self.events = events

    @contextmanager
    def ready_read(self) -> Iterator[Neo4jGraphStatus]:
        self.events.append("enter")
        try:
            yield self.status
        finally:
            self.events.append("exit")


def test_graph_query_result_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        GraphQueryResult(
            graph_generation_id=1,
            subgraph_nodes=[],
            subgraph_edges=[],
            status="ready",
            extra_field=True,
        )


@pytest.mark.parametrize(
    ("seed_entities", "match"),
    [
        ([], "seed_entities"),
        (["entity-a", " "], "non-empty"),
        (["entity-a", 42], "strings"),
    ],
)
def test_query_subgraph_validates_seed_entities_before_reading(
    seed_entities: list[Any],
    match: str,
) -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match=match):
        query_subgraph(
            seed_entities,  # type: ignore[arg-type]
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
        )

    assert client.read_calls == []
    assert client.write_calls == []


@pytest.mark.parametrize(
    ("depth", "max_depth", "match"),
    [
        (-1, MAX_QUERY_DEPTH, "greater than or equal"),
        (MAX_QUERY_DEPTH + 1, MAX_QUERY_DEPTH, "less than or equal"),
        (True, MAX_QUERY_DEPTH, "integer"),
        (1, -1, "max_depth"),
    ],
)
def test_query_subgraph_validates_depth_before_reading(
    depth: Any,
    max_depth: Any,
    match: str,
) -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match=match):
        query_subgraph(
            ["entity-a"],
            depth,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
            max_depth=max_depth,  # type: ignore[arg-type]
        )

    assert client.read_calls == []


def test_query_subgraph_validates_result_limit_before_reading() -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match="result_limit"):
        query_subgraph(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
            result_limit=0,
        )

    assert client.read_calls == []


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


def test_query_subgraph_blocks_active_writer_lock_before_neo4j_reads() -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="writer lock"):
        query_subgraph(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(writer_lock_token="incremental-sync"),
        )

    assert client.read_calls == []


def test_query_subgraph_reads_inside_ready_gate_and_normalizes_result() -> None:
    events: list[str] = []
    client = GateAwareClient(events)
    status = _ready_status(graph_generation_id=42)

    result = query_subgraph(
        [" entity-b ", "entity-b", "node-a"],
        2,
        client=client,  # type: ignore[arg-type]
        status_manager=RecordingStatusManager(status, events),  # type: ignore[arg-type]
        result_limit=10,
    )

    assert events == ["enter", "exit"]
    assert result.graph_generation_id == 42
    assert result.status == "ready"
    assert [node["node_id"] for node in result.subgraph_nodes] == ["node-a", "node-b"]
    assert result.subgraph_nodes[1]["properties"] == {"name": "Beta", "rank": 2}
    assert [edge["edge_id"] for edge in result.subgraph_edges] == ["edge-1", "edge-2"]
    assert result.subgraph_edges[0]["weight"] == 0.7
    assert len(client.read_calls) == 1
    query, parameters = client.read_calls[0]
    assert "*0..2" in query
    assert parameters == {"result_limit": 10, "seed_entities": ["entity-b", "node-a"]}


def test_query_subgraph_uses_generation_from_status_not_neo4j_rows() -> None:
    client = FakeQueryClient()

    result = query_subgraph(
        ["entity-a"],
        1,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(graph_generation_id=77),
    )

    assert result.graph_generation_id == 77


@pytest.mark.parametrize("graph_status", ["rebuilding", "failed"])
def test_query_propagation_paths_blocks_non_ready_before_neo4j_reads(
    graph_status: Literal["rebuilding", "failed"],
) -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="ready"):
        query_propagation_paths(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(graph_status=graph_status),
        )

    assert client.read_calls == []


def test_query_propagation_paths_blocks_active_writer_lock_before_neo4j_reads() -> None:
    client = FakeQueryClient()

    with pytest.raises(PermissionError, match="writer lock"):
        query_propagation_paths(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(writer_lock_token="incremental-sync"),
        )

    assert client.read_calls == []


@pytest.mark.parametrize(
    ("channels", "match"),
    [
        ([], "channels"),
        ([""], "non-empty"),
        (["unknown"], "unknown"),
        ([1], "strings"),
    ],
)
def test_query_propagation_paths_validates_channels_before_reading(
    channels: list[Any],
    match: str,
) -> None:
    client = FakeQueryClient()

    with pytest.raises(ValueError, match=match):
        query_propagation_paths(
            ["entity-a"],
            1,
            client=client,  # type: ignore[arg-type]
            status_manager=_status_manager(),
            channels=channels,  # type: ignore[arg-type]
        )

    assert client.read_calls == []


def test_query_propagation_paths_filters_channels_and_normalizes_paths() -> None:
    client = FakeQueryClient()

    paths = query_propagation_paths(
        ["entity-a"],
        2,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
        channels=["event", "event"],
        result_limit=5,
    )

    assert paths == [
        {
            "channel": "event",
            "edge_id": "edge-2",
            "source_node_id": "node-b",
            "target_node_id": "node-c",
            "relationship_type": "EVENT_IMPACT",
            "score": 0.2,
            "path_length": 2,
            "properties": {"kind": "news"},
        }
    ]
    query, parameters = client.read_calls[0]
    assert "*1..2" in query
    assert "propagation_channel" in query
    assert parameters == {
        "channels": ["event"],
        "result_limit": 5,
        "seed_entities": ["entity-a"],
    }


def test_query_propagation_paths_depth_zero_returns_empty_path_summary() -> None:
    client = FakeQueryClient()

    paths = query_propagation_paths(
        ["entity-a"],
        0,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert paths == []
    assert "*1.." not in client.read_calls[0][0]


def test_query_apis_do_not_call_execute_write() -> None:
    client = FakeQueryClient()

    query_subgraph(
        ["entity-a"],
        1,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )
    query_propagation_paths(
        ["entity-a"],
        1,
        client=client,  # type: ignore[arg-type]
        status_manager=_status_manager(),
    )

    assert client.write_calls == []


def _node_rows() -> list[dict[str, Any]]:
    return [
        {
            "node_id": "node-b",
            "canonical_entity_id": "entity-b",
            "labels": ["Entity"],
            "properties": {
                "canonical_entity_id": "entity-b",
                "node_id": "node-b",
                "properties_json": '{"name": "Beta"}',
                "rank": 2,
            },
        },
        {
            "node_id": "node-a",
            "canonical_entity_id": "entity-a",
            "labels": ["Entity"],
            "properties": {"name": "Alpha"},
        },
        {
            "node_id": "node-b",
            "canonical_entity_id": "entity-b",
            "labels": ["Entity"],
            "properties": {
                "canonical_entity_id": "entity-b",
                "node_id": "node-b",
                "properties_json": '{"name": "Beta"}',
                "rank": 2,
            },
        },
    ]


def _edge_rows() -> list[dict[str, Any]]:
    return [
        {
            "edge_id": "edge-2",
            "source_node_id": "node-b",
            "target_node_id": "node-c",
            "relationship_type": "EVENT_IMPACT",
            "properties": {"properties_json": '{"kind": "news"}', "weight": 0.2},
        },
        {
            "edge_id": "edge-1",
            "source_node_id": "node-a",
            "target_node_id": "node-b",
            "relationship_type": "SUPPLY_CHAIN",
            "properties": {"properties_json": '{"kind": "supply"}'},
            "weight": 0.7,
        },
        {
            "edge_id": "edge-1",
            "source_node_id": "node-a",
            "target_node_id": "node-b",
            "relationship_type": "SUPPLY_CHAIN",
            "properties": {"properties_json": '{"kind": "supply"}'},
            "weight": 0.7,
        },
    ]


def _path_rows(channels: list[str] | None) -> list[dict[str, Any]]:
    paths = [
        {
            "channel": "event",
            "edge_id": "edge-2",
            "source_node_id": "node-b",
            "target_node_id": "node-c",
            "relationship_type": "EVENT_IMPACT",
            "score": 0.2,
            "path_length": 2,
            "properties": {"properties_json": '{"kind": "news"}'},
        },
        {
            "channel": "fundamental",
            "edge_id": "edge-1",
            "source_node_id": "node-a",
            "target_node_id": "node-b",
            "relationship_type": "SUPPLY_CHAIN",
            "score": 0.7,
            "path_length": 1,
            "properties": {"properties_json": '{"kind": "supply"}'},
        },
        {
            "channel": "fundamental",
            "edge_id": "edge-1",
            "source_node_id": "node-a",
            "target_node_id": "node-b",
            "relationship_type": "SUPPLY_CHAIN",
            "score": 0.7,
            "path_length": 1,
            "properties": {"properties_json": '{"kind": "supply"}'},
        },
    ]
    if channels is None:
        return paths
    return [path for path in paths if path["channel"] in channels]


def _status_manager(
    *,
    graph_status: Literal["ready", "rebuilding", "failed"] = "ready",
    graph_generation_id: int = 1,
    writer_lock_token: str | None = None,
) -> GraphStatusManager:
    return GraphStatusManager(
        InMemoryStatusStore(
            _ready_status(
                graph_status=graph_status,
                graph_generation_id=graph_generation_id,
                writer_lock_token=writer_lock_token,
            ),
        ),
    )


def _ready_status(
    *,
    graph_status: Literal["ready", "rebuilding", "failed"] = "ready",
    graph_generation_id: int = 1,
    writer_lock_token: str | None = None,
) -> Neo4jGraphStatus:
    return Neo4jGraphStatus(
        graph_status=graph_status,
        graph_generation_id=graph_generation_id,
        node_count=3,
        edge_count=2,
        key_label_counts={"Entity": 3},
        checksum="abc123" if graph_status == "ready" else graph_status,
        last_verified_at=NOW if graph_status == "ready" else None,
        last_reload_at=None,
        writer_lock_token=writer_lock_token,
    )
