"""Full propagation orchestration across enabled channels."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from graph_engine.client import Neo4jClient
from graph_engine.models import Neo4jGraphStatus, PropagationContext, PropagationResult
from graph_engine.propagation.event import run_event_propagation
from graph_engine.propagation.fundamental import run_fundamental_propagation
from graph_engine.propagation.merge import merge_propagation_results
from graph_engine.propagation.reflexive import run_reflexive_propagation
from graph_engine.status import GraphStatusManager


class _PropagationRunner(Protocol):
    def __call__(
        self,
        context: PropagationContext,
        client: Neo4jClient,
        *,
        status_manager: GraphStatusManager,
        graph_name: str | None = None,
        max_iterations: int = 20,
        result_limit: int = 100,
    ) -> PropagationResult:
        ...


def run_full_propagation(
    context: PropagationContext,
    client: Neo4jClient,
    *,
    graph_name: str | None = None,
    max_iterations: int = 20,
    result_limit: int = 100,
    status_manager: GraphStatusManager | None = None,
) -> PropagationResult:
    """Run all enabled propagation channels and merge the explainable results."""

    if max_iterations < 1:
        raise ValueError("max_iterations must be greater than zero")
    if result_limit < 1:
        raise ValueError("result_limit must be greater than zero")

    resolved_status_manager = status_manager or _context_status_manager(context)
    enabled_channels = list(context.enabled_channels)
    results: list[PropagationResult] = []
    for channel in enabled_channels:
        runner = _runner_for_channel(channel)
        results.append(
            runner(
                context,
                client,
                status_manager=resolved_status_manager,
                graph_name=_projection_name(
                    graph_name,
                    channel=channel,
                    enabled_channel_count=len(enabled_channels),
                ),
                max_iterations=max_iterations,
                result_limit=result_limit,
            )
        )
    return merge_propagation_results(results, result_limit=result_limit)


def _runner_for_channel(channel: str) -> _PropagationRunner:
    if channel == "fundamental":
        return run_fundamental_propagation
    if channel == "event":
        return run_event_propagation
    if channel == "reflexive":
        return run_reflexive_propagation
    raise ValueError(f"unknown propagation channel: {channel!r}")


def _projection_name(
    graph_name: str | None,
    *,
    channel: str,
    enabled_channel_count: int,
) -> str | None:
    if graph_name is None:
        return None
    if enabled_channel_count == 1:
        return graph_name
    return f"{graph_name}-{channel}"


def _context_status_manager(context: PropagationContext) -> GraphStatusManager:
    status = Neo4jGraphStatus(
        graph_status="ready",
        graph_generation_id=context.graph_generation_id,
        node_count=0,
        edge_count=0,
        key_label_counts={},
        checksum="context-ready",
        last_verified_at=datetime.now(timezone.utc),
        last_reload_at=None,
    )
    return GraphStatusManager(_StaticStatusStore(status))


class _StaticStatusStore:
    def __init__(self, status: Neo4jGraphStatus) -> None:
        self.status = status

    def read_current_status(self) -> Neo4jGraphStatus:
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
        self.status = next_status
        return True
