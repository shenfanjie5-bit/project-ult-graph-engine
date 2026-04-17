"""Graph and impact snapshot generation."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.live_metrics import (
    checksum_payload as _checksum_payload,
    read_live_graph_metrics,
)
from graph_engine.models import (
    GraphImpactSnapshot,
    GraphSnapshot,
    Neo4jGraphStatus,
    PropagationChannel,
    PropagationResult,
)
from graph_engine.propagation.context import RegimeContextReader, build_propagation_context
from graph_engine.propagation.pipeline import run_full_propagation
from graph_engine.snapshots.writer import SnapshotWriter
from graph_engine.status import GraphStatusManager, hold_ready_read, require_ready_status

_DEFAULT_SNAPSHOT_CHANNELS: tuple[PropagationChannel, ...] = (
    "fundamental",
    "event",
    "reflexive",
)


def build_graph_snapshot(
    cycle_id: str,
    graph_generation_id: int,
    client: Neo4jClient,
    *,
    status_manager: GraphStatusManager,
) -> GraphSnapshot:
    """Read live graph metrics and return a deterministic structural snapshot."""

    with hold_ready_read(status_manager, "graph snapshot generation") as ready_status:
        _validate_generation_input(graph_generation_id, ready_status)
        node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
        return _graph_snapshot_from_metrics(
            cycle_id,
            graph_generation_id,
            node_count=node_count,
            edge_count=edge_count,
            key_label_counts=key_label_counts,
            checksum=checksum,
        )


def build_graph_impact_snapshot(
    cycle_id: str,
    world_state_ref: str,
    propagation_result: PropagationResult,
) -> GraphImpactSnapshot:
    """Build the downstream impact snapshot from a propagation result."""

    channel_breakdown = _complete_channel_breakdown(propagation_result.channel_breakdown)
    payload = {
        "cycle_id": cycle_id,
        "graph_generation_id": propagation_result.graph_generation_id,
        "world_state_ref": world_state_ref,
        "activated_paths": propagation_result.activated_paths,
        "impacted_entities": propagation_result.impacted_entities,
        "channel_breakdown": channel_breakdown,
    }
    impact_hash = _checksum_payload(payload)
    return GraphImpactSnapshot(
        cycle_id=cycle_id,
        impact_snapshot_id=f"graph-impact-{cycle_id}-{impact_hash[:12]}",
        regime_context_ref=world_state_ref,
        activated_paths=propagation_result.activated_paths,
        impacted_entities=propagation_result.impacted_entities,
        channel_breakdown=channel_breakdown,
    )


def compute_graph_snapshots(
    cycle_id: str,
    world_state_ref: str,
    *,
    client: Neo4jClient,
    graph_generation_id: int | None = None,
    regime_reader: RegimeContextReader,
    snapshot_writer: SnapshotWriter,
    status_manager: GraphStatusManager,
    graph_status: Neo4jGraphStatus | None = None,
    graph_name: str | None = None,
    enabled_channels: Sequence[PropagationChannel] | None = None,
    max_iterations: int = 20,
    result_limit: int = 100,
) -> tuple[GraphSnapshot, GraphImpactSnapshot]:
    """Run full propagation and write graph plus impact snapshots."""

    with hold_ready_read(status_manager, "snapshot generation") as ready_status:
        if graph_status is not None:
            require_ready_status(graph_status)
            _validate_status_matches_ready_status(ready_status, graph_status)
        _validate_generation_input(graph_generation_id, ready_status)
        _validate_pre_propagation_status_and_metrics(ready_status, client)
        requested_channels = list(
            _DEFAULT_SNAPSHOT_CHANNELS if enabled_channels is None else enabled_channels
        )
        context = build_propagation_context(
            cycle_id,
            world_state_ref,
            ready_status.graph_generation_id,
            regime_reader=regime_reader,
            graph_status=ready_status,
            enabled_channels=requested_channels,
        )
        propagation_result = run_full_propagation(
            context,
            client,
            status_manager=status_manager,
            graph_name=graph_name,
            max_iterations=max_iterations,
            result_limit=result_limit,
        )
        graph_snapshot = build_graph_snapshot(
            cycle_id,
            ready_status.graph_generation_id,
            client,
            status_manager=status_manager,
        )
        _validate_status_metrics(
            ready_status,
            node_count=graph_snapshot.node_count,
            edge_count=graph_snapshot.edge_count,
            key_label_counts=graph_snapshot.key_label_counts,
            checksum=graph_snapshot.checksum,
        )
        impact_snapshot = build_graph_impact_snapshot(
            cycle_id,
            world_state_ref,
            propagation_result,
        )
        _validate_publication_status_and_metrics(
            ready_status,
            status_manager,
            client,
        )
        snapshot_writer.write_snapshots(graph_snapshot, impact_snapshot)
        return graph_snapshot, impact_snapshot


def _validate_generation_input(
    graph_generation_id: int | None,
    graph_status: Neo4jGraphStatus,
) -> None:
    if graph_generation_id is None:
        return
    if graph_generation_id != graph_status.graph_generation_id:
        raise ValueError(
            "graph_generation_id disagrees with Neo4jGraphStatus: "
            f"received {graph_generation_id}, "
            f"status has {graph_status.graph_generation_id}",
        )


def _complete_channel_breakdown(channel_breakdown: dict[str, Any]) -> dict[str, Any]:
    completed: dict[str, Any] = {
        channel: channel_breakdown.get(channel, {})
        for channel in _DEFAULT_SNAPSHOT_CHANNELS
    }
    for key, value in channel_breakdown.items():
        if key not in completed and key != "merged":
            completed[key] = value
    completed["merged"] = channel_breakdown.get("merged", {})
    return completed


def _validate_pre_propagation_status_and_metrics(
    ready_status: Neo4jGraphStatus,
    client: Neo4jClient,
) -> None:
    node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
    _validate_status_metrics(
        ready_status,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )


def _validate_status_metrics(
    graph_status: Neo4jGraphStatus,
    *,
    node_count: int,
    edge_count: int,
    key_label_counts: dict[str, int],
    checksum: str,
) -> None:
    mismatches: list[str] = []
    if graph_status.node_count != node_count:
        mismatches.append(
            f"node_count status={graph_status.node_count} live={node_count}",
        )
    if graph_status.edge_count != edge_count:
        mismatches.append(
            f"edge_count status={graph_status.edge_count} live={edge_count}",
        )
    if graph_status.key_label_counts != key_label_counts:
        mismatches.append(
            "key_label_counts "
            f"status={graph_status.key_label_counts!r} live={key_label_counts!r}",
        )
    if graph_status.checksum != checksum:
        mismatches.append(
            f"checksum status={graph_status.checksum!r} live={checksum!r}",
        )
    if mismatches:
        raise ValueError(
            "live graph metrics disagree with Neo4jGraphStatus: "
            + "; ".join(mismatches),
        )


def _validate_publication_status_and_metrics(
    ready_status: Neo4jGraphStatus,
    status_manager: GraphStatusManager,
    client: Neo4jClient,
) -> None:
    try:
        current_status = require_ready_status(status_manager.get_status())
    except PermissionError as exc:
        raise RuntimeError("ready graph status changed before snapshot publication") from exc
    _validate_status_matches_ready_status(current_status, ready_status)

    node_count, edge_count, key_label_counts, checksum = _read_graph_metrics(client)
    _validate_status_metrics(
        current_status,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
    )


def _validate_status_matches_ready_status(
    current_status: Neo4jGraphStatus,
    ready_status: Neo4jGraphStatus,
) -> None:
    checks = (
        (
            "graph_generation_id",
            current_status.graph_generation_id,
            ready_status.graph_generation_id,
        ),
        ("node_count", current_status.node_count, ready_status.node_count),
        ("edge_count", current_status.edge_count, ready_status.edge_count),
        (
            "key_label_counts",
            current_status.key_label_counts,
            ready_status.key_label_counts,
        ),
        ("checksum", current_status.checksum, ready_status.checksum),
    )
    mismatches = [
        f"{field} current={current_value!r} ready={ready_value!r}"
        for field, current_value, ready_value in checks
        if current_value != ready_value
    ]
    if mismatches:
        raise RuntimeError(
            "ready graph status changed before snapshot publication: "
            + "; ".join(mismatches),
        )


def _graph_snapshot_from_metrics(
    cycle_id: str,
    graph_generation_id: int,
    *,
    node_count: int,
    edge_count: int,
    key_label_counts: dict[str, int],
    checksum: str,
) -> GraphSnapshot:
    return GraphSnapshot(
        cycle_id=cycle_id,
        snapshot_id=f"graph-snapshot-{cycle_id}-{graph_generation_id}-{checksum[:12]}",
        graph_generation_id=graph_generation_id,
        node_count=node_count,
        edge_count=edge_count,
        key_label_counts=key_label_counts,
        checksum=checksum,
        created_at=datetime.now(timezone.utc),
    )


def _read_graph_metrics(client: Neo4jClient) -> tuple[int, int, dict[str, int], str]:
    return read_live_graph_metrics(client, strict=True)
