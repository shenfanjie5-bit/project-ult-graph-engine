from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

import pytest

from graph_engine.models import Neo4jGraphStatus, PropagationContext
from graph_engine.propagation import (
    FUNDAMENTAL_RELATIONSHIP_TYPES,
    build_propagation_context,
    compute_path_score,
    run_fundamental_propagation,
)

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


class StaticRegimeReader:
    def __init__(self, regime_context: dict[str, Any]) -> None:
        self.regime_context = regime_context
        self.calls: list[str] = []

    def read_regime_context(self, world_state_ref: str) -> dict[str, Any]:
        self.calls.append(world_state_ref)
        return self.regime_context


class FakeGDSClient:
    def __init__(
        self,
        *,
        exists_results: list[bool] | None = None,
        fail_on_pagerank: bool = False,
        path_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.exists_results = exists_results or [False, True]
        self.fail_on_pagerank = fail_on_pagerank
        self.path_rows = path_rows
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
            exists = self.exists_results.pop(0) if self.exists_results else False
            return [{"exists": exists}]
        if "gds.pageRank.stream" in query:
            if self.fail_on_pagerank:
                raise RuntimeError("pagerank failed")
            return [
                {
                    "node_id": "node-b",
                    "canonical_entity_id": "entity-b",
                    "labels": ["Entity"],
                    "stable_node_id": "node-b",
                    "score": 0.2,
                },
                {
                    "node_id": "node-a",
                    "canonical_entity_id": "entity-a",
                    "labels": ["Entity"],
                    "stable_node_id": "node-a",
                    "score": 0.9,
                },
            ]
        if "MATCH (source)-[relationship]->(target)" in query:
            if self.path_rows is not None:
                return self.path_rows
            return [
                {
                    "source_node_id": "node-a",
                    "source_entity_id": "entity-a",
                    "source_labels": ["Entity"],
                    "target_node_id": "node-b",
                    "target_entity_id": "entity-b",
                    "target_labels": ["Entity"],
                    "edge_id": "edge-1",
                    "relationship_type": "SUPPLY_CHAIN",
                    "relation_weight": 0.5,
                    "evidence_confidence": None,
                    "recency_decay": None,
                },
                {
                    "source_node_id": "node-b",
                    "source_entity_id": "entity-b",
                    "source_labels": ["Entity"],
                    "target_node_id": "node-c",
                    "target_entity_id": "entity-c",
                    "target_labels": ["Entity"],
                    "edge_id": "edge-2",
                    "relationship_type": "OWNERSHIP",
                    "relation_weight": 0.8,
                    "evidence_confidence": 0.5,
                    "recency_decay": 0.5,
                },
            ]
        return []

    def execute_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.write_calls.append((query, parameters or {}))
        return []


def test_compute_path_score_uses_documented_multiplication() -> None:
    assert compute_path_score(
        relation_weight=0.8,
        evidence_confidence=0.5,
        channel_multiplier=1.5,
        regime_multiplier=0.75,
        recency_decay=0.9,
    ) == pytest.approx(0.405)


def test_build_propagation_context_reads_regime_context_only() -> None:
    reader = StaticRegimeReader(
        {
            "channel_multipliers": {"fundamental": 1.25},
            "regime_multipliers": {"fundamental": 0.8},
            "decay_policy": {"half_life_days": 30},
        }
    )

    context = build_propagation_context(
        "cycle-1",
        "world-state-1",
        7,
        regime_reader=reader,
    )

    assert reader.calls == ["world-state-1"]
    assert context.channel_multipliers == {"fundamental": 1.25}
    assert context.regime_multipliers == {"fundamental": 0.8}
    assert context.decay_policy == {"half_life_days": 30}
    assert "main_core" not in sys.modules


def test_build_propagation_context_rejects_non_ready_status_before_reading() -> None:
    reader = StaticRegimeReader({})
    graph_status = Neo4jGraphStatus(
        graph_status="rebuilding",
        graph_generation_id=1,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="pending",
        last_verified_at=NOW,
        last_reload_at=None,
    )

    with pytest.raises(PermissionError, match="ready"):
        build_propagation_context(
            "cycle-1",
            "world-state-1",
            1,
            regime_reader=reader,
            graph_status=graph_status,
        )

    assert reader.calls == []


def test_run_fundamental_propagation_uses_gds_projection_and_explains_paths() -> None:
    client = FakeGDSClient(exists_results=[True, True])
    context = _context()

    result = run_fundamental_propagation(
        context,
        client,  # type: ignore[arg-type]
        graph_name="unit-fundamental",
        max_iterations=5,
        result_limit=10,
    )

    write_queries = [query for query, _ in client.write_calls]
    assert "gds.graph.drop" in write_queries[0]
    assert "gds.graph.project" in write_queries[1]
    assert "gds.graph.drop" in write_queries[2]
    assert all(" SET " not in query and " DELETE " not in query for query in write_queries)

    projection_params = client.write_calls[1][1]
    assert projection_params["graph_name"] == "unit-fundamental"
    assert set(projection_params["relationship_projection"]) == set(
        FUNDAMENTAL_RELATIONSHIP_TYPES
    )

    pagerank_call = next(
        (query, params)
        for query, params in client.read_calls
        if "gds.pageRank.stream" in query
    )
    assert pagerank_call[1]["max_iterations"] == 5
    assert pagerank_call[1]["result_limit"] == 10

    assert [entity["node_id"] for entity in result.impacted_entities] == [
        "node-b",
        "node-c",
    ]
    assert [entity["score"] for entity in result.impacted_entities] == [
        pytest.approx(0.5),
        pytest.approx(0.2),
    ]
    assert result.impacted_entities[0]["pagerank_score"] == pytest.approx(0.2)
    assert result.activated_paths[0]["edge_id"] == "edge-1"
    assert result.activated_paths[0]["score"] == pytest.approx(0.5)
    assert result.activated_paths[0]["explanation"] == {
        "relation_weight": 0.5,
        "evidence_confidence": 1.0,
        "channel_multiplier": 2.0,
        "regime_multiplier": 0.5,
        "recency_decay": 1.0,
        "score": 0.5,
    }
    assert result.channel_breakdown["fundamental"]["path_count"] == 2


def test_run_fundamental_propagation_generates_unique_default_projection_names() -> None:
    first_client = FakeGDSClient()
    second_client = FakeGDSClient()

    run_fundamental_propagation(_context(), first_client)  # type: ignore[arg-type]
    run_fundamental_propagation(_context(), second_client)  # type: ignore[arg-type]

    first_project = next(
        params["graph_name"]
        for query, params in first_client.write_calls
        if "gds.graph.project" in query
    )
    second_project = next(
        params["graph_name"]
        for query, params in second_client.write_calls
        if "gds.graph.project" in query
    )

    assert first_project.startswith("graph_engine_fundamental_cycle_1_")
    assert second_project.startswith("graph_engine_fundamental_cycle_1_")
    assert first_project != second_project


def test_impacted_entities_use_five_factor_path_scores() -> None:
    client = FakeGDSClient(
        path_rows=[
            {
                "source_node_id": "node-a",
                "source_entity_id": "entity-a",
                "source_labels": ["Entity"],
                "target_node_id": "node-zero",
                "target_entity_id": "entity-zero",
                "target_labels": ["Entity"],
                "edge_id": "edge-zero",
                "relationship_type": "SUPPLY_CHAIN",
                "relation_weight": 100.0,
                "evidence_confidence": 0.0,
                "recency_decay": 1.0,
            },
            {
                "source_node_id": "node-a",
                "source_entity_id": "entity-a",
                "source_labels": ["Entity"],
                "target_node_id": "node-small",
                "target_entity_id": "entity-small",
                "target_labels": ["Entity"],
                "edge_id": "edge-small",
                "relationship_type": "SUPPLY_CHAIN",
                "relation_weight": 0.1,
                "evidence_confidence": 1.0,
                "recency_decay": 1.0,
            },
        ],
    )

    result = run_fundamental_propagation(
        _context(),
        client,  # type: ignore[arg-type]
        graph_name="unit-fundamental",
    )

    assert [entity["node_id"] for entity in result.impacted_entities] == [
        "node-small",
        "node-zero",
    ]
    assert result.impacted_entities[0]["score"] == pytest.approx(0.1)
    assert result.impacted_entities[1]["score"] == pytest.approx(0.0)


def test_run_fundamental_propagation_cleans_up_projection_on_exception() -> None:
    client = FakeGDSClient(exists_results=[False, True], fail_on_pagerank=True)

    with pytest.raises(RuntimeError, match="pagerank failed"):
        run_fundamental_propagation(
            _context(),
            client,  # type: ignore[arg-type]
            graph_name="unit-fundamental",
        )

    write_queries = [query for query, _ in client.write_calls]
    assert any("gds.graph.project" in query for query in write_queries)
    assert any("gds.graph.drop" in query for query in write_queries)


def _context() -> PropagationContext:
    return PropagationContext(
        cycle_id="cycle-1",
        world_state_ref="world-state-1",
        graph_generation_id=1,
        enabled_channels=["fundamental"],
        channel_multipliers={"fundamental": 2.0},
        regime_multipliers={"fundamental": 0.5},
        decay_policy={},
        regime_context={},
    )
