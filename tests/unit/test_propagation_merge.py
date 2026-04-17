from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

import graph_engine.propagation.pipeline as propagation_pipeline
from graph_engine.models import Neo4jGraphStatus, PropagationContext, PropagationResult
from graph_engine.propagation import merge_propagation_results, run_full_propagation
from graph_engine.status import GraphStatusManager
from tests.fakes import InMemoryStatusStore

NOW = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)


def test_merge_propagation_results_rejects_empty_results() -> None:
    with pytest.raises(ValueError, match="empty"):
        merge_propagation_results([])


@pytest.mark.parametrize(
    ("left_kwargs", "right_kwargs", "message"),
    [
        ({"cycle_id": "cycle-1"}, {"cycle_id": "cycle-2"}, "cycle_id"),
        ({"graph_generation_id": 1}, {"graph_generation_id": 2}, "graph_generation_id"),
    ],
)
def test_merge_propagation_results_rejects_incompatible_results(
    left_kwargs: dict[str, Any],
    right_kwargs: dict[str, Any],
    message: str,
) -> None:
    left = _result("fundamental", **left_kwargs)
    right = _result("event", **right_kwargs)

    with pytest.raises(ValueError, match=message):
        merge_propagation_results([left, right])


def test_merge_propagation_results_validates_channel_payloads_and_explanations() -> None:
    missing_path_channel = _result("fundamental")
    missing_path_channel.activated_paths[0].pop("channel")

    with pytest.raises(ValueError, match="activated path payload is missing channel"):
        merge_propagation_results([missing_path_channel])

    missing_entity_channel = _result("fundamental")
    missing_entity_channel.impacted_entities[0].pop("channel")

    with pytest.raises(ValueError, match="impacted entity payload is missing channel"):
        merge_propagation_results([missing_entity_channel])

    missing_explanation_key = _result("fundamental")
    missing_explanation_key.activated_paths[0]["explanation"].pop("regime_multiplier")

    with pytest.raises(ValueError, match="regime_multiplier"):
        merge_propagation_results([missing_explanation_key])


def test_merge_propagation_results_sorts_paths_after_merge_and_applies_limit() -> None:
    fundamental = _result(
        "fundamental",
        paths=[
            _path("fundamental", edge_id="edge-fund", score=1.0, source_node_id="node-a"),
            _path("fundamental", edge_id="edge-fund-low", score=0.1),
        ],
        entities=[_entity("fundamental", node_id="node-fund", score=1.1)],
    )
    event = _result(
        "event",
        paths=[
            _path("event", edge_id="edge-event", score=1.0, source_node_id="node-z"),
            _path("event", edge_id="edge-event-top", score=2.0),
        ],
        entities=[_entity("event", node_id="node-event", score=3.0)],
    )
    reflexive = _result(
        "reflexive",
        paths=[_path("reflexive", edge_id="edge-reflexive", score=1.0)],
        entities=[_entity("reflexive", node_id="node-reflexive", score=1.0)],
    )

    merged = merge_propagation_results(
        [fundamental, event, reflexive],
        result_limit=4,
    )

    assert [path["edge_id"] for path in merged.activated_paths] == [
        "edge-event-top",
        "edge-event",
        "edge-fund",
        "edge-reflexive",
    ]
    assert merged.channel_breakdown["merged"]["path_count"] == 4
    assert merged.channel_breakdown["merged"]["total_path_score"] == pytest.approx(5.0)
    assert merged.channel_breakdown["merged"]["result_limit"] == 4


def test_merge_propagation_results_aggregates_entities_by_canonical_id() -> None:
    fundamental = _result(
        "fundamental",
        paths=[
            _path(
                "fundamental",
                edge_id="edge-fund",
                score=0.4,
                target_node_id="node-a",
                target_entity_id="entity-shared",
            )
        ],
        entities=[
            _entity(
                "fundamental",
                node_id="node-a",
                canonical_entity_id="entity-shared",
                labels=["Supplier"],
                score=0.4,
                path_count=1,
            )
        ],
    )
    event = _result(
        "event",
        paths=[
            _path(
                "event",
                edge_id="edge-event-1",
                score=0.2,
                target_node_id="node-b",
                target_entity_id="entity-shared",
            ),
            _path(
                "event",
                edge_id="edge-event-2",
                score=0.6,
                target_node_id="node-b",
                target_entity_id="entity-shared",
            ),
        ],
        entities=[
            _entity(
                "event",
                node_id="node-b",
                canonical_entity_id="entity-shared",
                labels=["Issuer"],
                score=0.8,
                path_count=2,
            )
        ],
    )

    merged = merge_propagation_results([fundamental, event])

    assert len(merged.impacted_entities) == 1
    entity = merged.impacted_entities[0]
    assert entity["canonical_entity_id"] == "entity-shared"
    assert entity["node_id"] == "node-a"
    assert entity["labels"] == ["Issuer", "Supplier"]
    assert entity["score"] == pytest.approx(1.2)
    assert entity["channel_scores"] == {"event": 0.8, "fundamental": 0.4}
    assert entity["channels"] == ["event", "fundamental"]
    assert entity["path_count"] == 3
    assert merged.channel_breakdown["fundamental"]["path_count"] == 1
    assert merged.channel_breakdown["event"]["path_count"] == 2


def test_merge_propagation_results_aggregates_entities_by_node_id_without_canonical_id() -> None:
    fundamental = _result(
        "fundamental",
        entities=[
            _entity(
                "fundamental",
                node_id="node-shared",
                canonical_entity_id=None,
                score=0.5,
            )
        ],
    )
    reflexive = _result(
        "reflexive",
        entities=[
            _entity(
                "reflexive",
                node_id="node-shared",
                canonical_entity_id=None,
                score=0.25,
            )
        ],
    )

    merged = merge_propagation_results([fundamental, reflexive])

    assert len(merged.impacted_entities) == 1
    assert merged.impacted_entities[0]["canonical_entity_id"] is None
    assert merged.impacted_entities[0]["node_id"] == "node-shared"
    assert merged.impacted_entities[0]["score"] == pytest.approx(0.75)
    assert merged.impacted_entities[0]["channel_scores"] == {
        "fundamental": 0.5,
        "reflexive": 0.25,
    }


def test_run_full_propagation_uses_distinct_projection_names_per_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None, int, int]] = []

    monkeypatch.setattr(
        propagation_pipeline,
        "run_fundamental_propagation",
        _runner("fundamental", calls),
    )
    monkeypatch.setattr(
        propagation_pipeline,
        "run_event_propagation",
        _runner("event", calls),
    )
    monkeypatch.setattr(
        propagation_pipeline,
        "run_reflexive_propagation",
        _runner("reflexive", calls),
    )

    result = run_full_propagation(
        _context(enabled_channels=["fundamental", "event", "reflexive"]),
        object(),  # type: ignore[arg-type]
        status_manager=_status_manager(),
        graph_name="unit-projection",
        max_iterations=7,
        result_limit=10,
    )

    assert calls == [
        ("fundamental", "unit-projection-fundamental", 7, 10),
        ("event", "unit-projection-event", 7, 10),
        ("reflexive", "unit-projection-reflexive", 7, 10),
    ]
    assert result.channel_breakdown["merged"]["enabled_channels"] == [
        "event",
        "fundamental",
        "reflexive",
    ]


def test_run_full_propagation_reuses_projection_name_for_single_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None, int, int]] = []
    monkeypatch.setattr(
        propagation_pipeline,
        "run_event_propagation",
        _runner("event", calls),
    )

    run_full_propagation(
        _context(enabled_channels=["event"]),
        object(),  # type: ignore[arg-type]
        status_manager=_status_manager(),
        graph_name="unit-projection",
    )

    assert calls == [("event", "unit-projection", 20, 100)]


def _runner(channel: str, calls: list[tuple[str, str | None, int, int]]):
    def run_channel(
        context: PropagationContext,
        client: Any,
        *,
        status_manager: GraphStatusManager,
        graph_name: str | None = None,
        max_iterations: int = 20,
        result_limit: int = 100,
    ) -> PropagationResult:
        calls.append((channel, graph_name, max_iterations, result_limit))
        return _result(channel, graph_generation_id=context.graph_generation_id)

    return run_channel


def _result(
    channel: str,
    *,
    cycle_id: str = "cycle-1",
    graph_generation_id: int = 1,
    paths: list[dict[str, Any]] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> PropagationResult:
    activated_paths = paths or [_path(channel, edge_id=f"edge-{channel}", score=1.0)]
    impacted_entities = entities or [_entity(channel, node_id=f"node-{channel}", score=1.0)]
    return PropagationResult(
        cycle_id=cycle_id,
        graph_generation_id=graph_generation_id,
        activated_paths=activated_paths,
        impacted_entities=impacted_entities,
        channel_breakdown={
            channel: {
                "path_count": len(activated_paths),
                "impacted_entity_count": len(impacted_entities),
                "total_path_score": sum(float(path["score"]) for path in activated_paths),
            },
        },
    )


def _path(
    channel: str,
    *,
    edge_id: str,
    score: float,
    source_node_id: str = "node-source",
    relationship_type: str = "SUPPLY_CHAIN",
    target_node_id: str = "node-target",
    target_entity_id: str | None = "entity-target",
) -> dict[str, Any]:
    return {
        "channel": channel,
        "source_node_id": source_node_id,
        "source_entity_id": "entity-source",
        "source_labels": ["Entity"],
        "target_node_id": target_node_id,
        "target_entity_id": target_entity_id,
        "target_labels": ["Entity"],
        "edge_id": edge_id,
        "relationship_type": relationship_type,
        "score": score,
        "explanation": {
            "relation_weight": score,
            "evidence_confidence": 1.0,
            "channel_multiplier": 1.0,
            "regime_multiplier": 1.0,
            "recency_decay": 1.0,
            "score": score,
        },
    }


def _entity(
    channel: str,
    *,
    node_id: str,
    score: float,
    canonical_entity_id: str | None = "entity-target",
    labels: list[str] | None = None,
    path_count: int = 1,
) -> dict[str, Any]:
    return {
        "channel": channel,
        "node_id": node_id,
        "canonical_entity_id": canonical_entity_id,
        "labels": ["Entity"] if labels is None else labels,
        "score": score,
        "path_count": path_count,
    }


def _context(*, enabled_channels: list[str]) -> PropagationContext:
    return PropagationContext(
        cycle_id="cycle-1",
        world_state_ref="world-state-1",
        graph_generation_id=1,
        enabled_channels=enabled_channels,  # type: ignore[arg-type]
        channel_multipliers={channel: 1.0 for channel in enabled_channels},
        regime_multipliers={channel: 1.0 for channel in enabled_channels},
        decay_policy={},
        regime_context={},
    )


def _status_manager() -> GraphStatusManager:
    return GraphStatusManager(
        InMemoryStatusStore(
            Neo4jGraphStatus(
                graph_status="ready",
                graph_generation_id=1,
                node_count=0,
                edge_count=0,
                key_label_counts={},
                checksum="abc123",
                last_verified_at=NOW,
                last_reload_at=None,
            ),
        ),
    )
