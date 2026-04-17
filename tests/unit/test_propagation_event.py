from __future__ import annotations

from typing import Any

import pytest

from graph_engine.models import PropagationContext
from graph_engine.propagation import EVENT_RELATIONSHIP_TYPES, run_event_propagation


class FakeEventGDSClient:
    def __init__(
        self,
        *,
        exists_results: list[bool] | None = None,
        fail_on_gds_exists: bool = False,
        fail_on_pagerank: bool = False,
        path_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.exists_results = exists_results or [False, True]
        self.fail_on_gds_exists = fail_on_gds_exists
        self.fail_on_pagerank = fail_on_pagerank
        self.path_rows = path_rows or []
        self.read_calls: list[tuple[str, dict[str, Any]]] = []
        self.write_calls: list[tuple[str, dict[str, Any]]] = []

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params = parameters or {}
        self.read_calls.append((query, params))
        if "gds.graph.exists" in query:
            if self.fail_on_gds_exists:
                raise RuntimeError(
                    "There is no procedure with the name `gds.graph.exists` registered"
                )
            exists = self.exists_results.pop(0) if self.exists_results else False
            return [{"exists": exists}]
        if "gds.pageRank.stream" in query:
            if self.fail_on_pagerank:
                raise RuntimeError("pagerank failed")
            return [
                {
                    "node_id": "node-c",
                    "canonical_entity_id": "entity-c",
                    "labels": ["Entity"],
                    "stable_node_id": "node-c",
                    "score": 0.7,
                },
                {
                    "node_id": "node-b",
                    "canonical_entity_id": "entity-b",
                    "labels": ["Entity"],
                    "stable_node_id": "node-b",
                    "score": 0.3,
                },
            ]
        if "MATCH (source)-[relationship]->(target)" in query:
            rows = list(self.path_rows)
            rows.sort(
                key=lambda row: (
                    -_path_score(row, params),
                    str(row["source_node_id"]),
                    str(row["relationship_type"]),
                    str(row["target_node_id"]),
                    str(row["edge_id"]),
                ),
            )
            return rows[: int(params.get("result_limit", len(rows)))]
        return []

    def execute_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.write_calls.append((query, parameters or {}))
        return []


def test_run_event_propagation_requires_event_channel_before_neo4j_reads() -> None:
    client = FakeEventGDSClient()

    with pytest.raises(PermissionError, match="event channel"):
        run_event_propagation(
            _context(enabled_channels=["fundamental"]),
            client,  # type: ignore[arg-type]
            graph_name="unit-event",
        )

    assert client.read_calls == []
    assert client.write_calls == []


def test_run_event_propagation_uses_projection_and_explains_paths() -> None:
    client = FakeEventGDSClient(
        exists_results=[True, True],
        path_rows=[
            _path_row(
                edge_id="edge-zero",
                target_node_id="node-zero",
                target_entity_id="entity-zero",
                relation_weight=100.0,
                evidence_confidence=0.0,
                recency_decay=1.0,
            ),
            _path_row(
                edge_id="edge-mid",
                target_node_id="node-b",
                target_entity_id="entity-b",
                relation_weight=1.0,
                evidence_confidence=0.5,
                recency_decay=1.0,
            ),
            _path_row(
                edge_id="edge-top",
                target_node_id="node-c",
                target_entity_id="entity-c",
                relation_weight=2.0,
                evidence_confidence=1.0,
                recency_decay=1.0,
            ),
        ],
    )

    result = run_event_propagation(
        _context(),
        client,  # type: ignore[arg-type]
        graph_name="unit-event",
        max_iterations=7,
        result_limit=2,
    )

    write_queries = [query for query, _ in client.write_calls]
    assert "gds.graph.drop" in write_queries[0]
    assert "gds.graph.project" in write_queries[1]
    assert "gds.graph.drop" in write_queries[2]
    assert all(" SET " not in query and " DELETE " not in query for query in write_queries)

    projection_params = client.write_calls[1][1]
    assert projection_params["graph_name"] == "unit-event"
    assert set(projection_params["relationship_projection"]) == set(EVENT_RELATIONSHIP_TYPES)

    pagerank_call = next(
        (query, params)
        for query, params in client.read_calls
        if "gds.pageRank.stream" in query
    )
    assert pagerank_call[1]["max_iterations"] == 7
    assert pagerank_call[1]["result_limit"] == 2

    path_call = next(
        (query, params)
        for query, params in client.read_calls
        if "MATCH (source)-[relationship]->(target)" in query
    )
    assert "ORDER BY path_score DESC" in path_call[0]
    assert path_call[1]["relationship_types"] == list(EVENT_RELATIONSHIP_TYPES)

    assert [path["edge_id"] for path in result.activated_paths] == ["edge-top", "edge-mid"]
    assert result.activated_paths[0]["channel"] == "event"
    assert result.activated_paths[0]["explanation"] == {
        "relation_weight": 2.0,
        "evidence_confidence": 1.0,
        "channel_multiplier": 2.0,
        "regime_multiplier": 0.5,
        "recency_decay": 1.0,
        "score": 2.0,
    }
    assert [entity["node_id"] for entity in result.impacted_entities] == ["node-c", "node-b"]
    assert result.impacted_entities[0]["pagerank_score"] == pytest.approx(0.7)
    assert result.impacted_entities[0]["path_count"] == 1
    assert result.channel_breakdown["event"] == {
        "relationship_types": list(EVENT_RELATIONSHIP_TYPES),
        "path_count": 2,
        "impacted_entity_count": 2,
        "total_path_score": 2.5,
        "channel_multiplier": 2.0,
        "regime_multiplier": 0.5,
    }


def test_run_event_propagation_cleans_up_projection_on_exception() -> None:
    client = FakeEventGDSClient(exists_results=[False, True], fail_on_pagerank=True)

    with pytest.raises(RuntimeError, match="pagerank failed"):
        run_event_propagation(
            _context(),
            client,  # type: ignore[arg-type]
            graph_name="unit-event",
        )

    write_queries = [query for query, _ in client.write_calls]
    assert any("gds.graph.project" in query for query in write_queries)
    assert any("gds.graph.drop" in query for query in write_queries)


def test_run_event_propagation_reports_missing_gds_consistently() -> None:
    client = FakeEventGDSClient(fail_on_gds_exists=True)

    with pytest.raises(RuntimeError, match="^GDS plugin not available$"):
        run_event_propagation(
            _context(),
            client,  # type: ignore[arg-type]
            graph_name="unit-event",
        )


def _context(*, enabled_channels: list[str] | None = None) -> PropagationContext:
    return PropagationContext(
        cycle_id="cycle-1",
        world_state_ref="world-state-1",
        graph_generation_id=1,
        enabled_channels=enabled_channels or ["event"],  # type: ignore[arg-type]
        channel_multipliers={"event": 2.0},
        regime_multipliers={"event": 0.5},
        decay_policy={},
        regime_context={},
    )


def _path_row(
    *,
    edge_id: str,
    target_node_id: str,
    target_entity_id: str,
    relation_weight: float,
    evidence_confidence: float,
    recency_decay: float,
) -> dict[str, Any]:
    return {
        "source_node_id": "node-a",
        "source_entity_id": "entity-a",
        "source_labels": ["Entity"],
        "target_node_id": target_node_id,
        "target_entity_id": target_entity_id,
        "target_labels": ["Entity"],
        "edge_id": edge_id,
        "relationship_type": EVENT_RELATIONSHIP_TYPES[0],
        "relation_weight": relation_weight,
        "evidence_confidence": evidence_confidence,
        "recency_decay": recency_decay,
    }


def _path_score(row: dict[str, Any], params: dict[str, Any]) -> float:
    return (
        _float_or_default(row.get("relation_weight"))
        * _float_or_default(row.get("evidence_confidence"))
        * float(params.get("channel_multiplier", 1.0))
        * float(params.get("regime_multiplier", 1.0))
        * _float_or_default(row.get("recency_decay"))
    )


def _float_or_default(value: Any, default: float = 1.0) -> float:
    if value is None:
        return default
    return float(value)
