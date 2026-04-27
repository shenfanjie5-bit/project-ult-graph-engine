"""Dagster Phase 1 asset factory for graph promotion and snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Protocol

from graph_engine.client import Neo4jClient
from graph_engine.models import GraphImpactSnapshot, GraphSnapshot, Neo4jGraphStatus, PromotionPlan
from graph_engine.promotion import promote_graph_deltas
from graph_engine.promotion.interfaces import (
    CandidateDeltaReader,
    CanonicalWriter,
    EntityAnchorReader,
)
from graph_engine.propagation.context import RegimeContextReader
from graph_engine.reload import ArtifactCanonicalReader, CanonicalReader
from graph_engine.snapshots import SnapshotWriter, compute_graph_snapshots
from graph_engine.status import GraphStatusManager, require_ready_status

PHASE0_READINESS_ASSET_KEY = "phase0_readiness_ping"
PHASE0_CANDIDATE_FREEZE_ASSET_KEY = "candidate_freeze"
PHASE0_GRAPH_STATUS_ASSET_KEY = "graph_status"
PHASE1_GROUP_NAME = "phase1"
PHASE1_GRAPH_PROMOTION_ASSET_KEY = "graph_promotion"
PHASE1_GRAPH_SNAPSHOT_ASSET_KEY = "graph_snapshot"
GRAPH_PHASE1_RESOURCE_KEY = "graph_phase1_runtime"


@dataclass(frozen=True, slots=True)
class GraphPromotionAssetRequest:
    """Runtime input resolved from Phase 0 asset outputs."""

    cycle_id: str
    selection_ref: str
    phase0_readiness: object
    candidate_freeze: object
    graph_status: object


@dataclass(frozen=True, slots=True)
class GraphPromotionAssetResult:
    """Materialized graph promotion result passed to ``graph_snapshot``."""

    cycle_id: str
    selection_ref: str
    graph_generation_id: int
    delta_ids: tuple[str, ...]
    plan: PromotionPlan
    graph_status: Neo4jGraphStatus


@dataclass(frozen=True, slots=True)
class GraphSnapshotAssetRequest:
    """Runtime input for the graph snapshot asset."""

    cycle_id: str
    world_state_ref: str
    graph_generation_id: int
    promotion: GraphPromotionAssetResult


@dataclass(frozen=True, slots=True)
class ColdReloadArtifactProof:
    """Proof that the persisted graph snapshot can seed cold reload."""

    snapshot_ref: str
    cycle_id: str
    snapshot_id: str
    graph_generation_id: int
    node_count: int
    edge_count: int
    key_label_counts: dict[str, int]
    checksum: str


@dataclass(frozen=True, slots=True)
class GraphSnapshotAssetResult:
    """Materialized graph snapshot artifact and cold-reload proof."""

    graph_snapshot_id: str
    impact_snapshot_id: str
    artifact_ref: str
    graph_snapshot: GraphSnapshot
    impact_snapshot: GraphImpactSnapshot
    cold_reload_proof: ColdReloadArtifactProof


class GraphPhase1Runtime(Protocol):
    """Runtime adapter invoked by the Dagster assets."""

    def promote_graph(
        self,
        request: GraphPromotionAssetRequest,
    ) -> GraphPromotionAssetResult:
        """Promote Phase 0 frozen candidate deltas into Layer A and Neo4j."""

    def compute_graph_snapshot(
        self,
        request: GraphSnapshotAssetRequest,
    ) -> GraphSnapshotAssetResult:
        """Compute, persist, and prove a formal-readable graph snapshot."""


class GraphPhase1Service:
    """Default runtime backed by graph-engine services.

    This service expects data-platform-facing adapters for candidate reads and
    Layer A writes. Graph-engine does not synthesize Phase 0 data: it only
    consumes the frozen candidate selection reference emitted by Phase 0.
    """

    def __init__(
        self,
        *,
        candidate_reader: CandidateDeltaReader,
        entity_reader: EntityAnchorReader,
        canonical_writer: CanonicalWriter,
        client: Neo4jClient,
        status_manager: GraphStatusManager,
        regime_reader: RegimeContextReader,
        snapshot_writer: SnapshotWriter,
        artifact_reader: CanonicalReader | None = None,
        world_state_ref: str = "world-state:latest",
        graph_name: str | None = None,
        enabled_channels: Sequence[str] | None = None,
        max_iterations: int = 20,
        result_limit: int = 100,
    ) -> None:
        self.candidate_reader = candidate_reader
        self.entity_reader = entity_reader
        self.canonical_writer = canonical_writer
        self.client = client
        self.status_manager = status_manager
        self.regime_reader = regime_reader
        self.snapshot_writer = snapshot_writer
        self.artifact_reader = artifact_reader or ArtifactCanonicalReader()
        self.world_state_ref = world_state_ref
        self.graph_name = graph_name
        self.enabled_channels = enabled_channels
        self.max_iterations = max_iterations
        self.result_limit = result_limit

    def promote_graph(
        self,
        request: GraphPromotionAssetRequest,
    ) -> GraphPromotionAssetResult:
        _ensure_phase0_readiness(request.phase0_readiness)
        phase0_status = _ensure_phase0_graph_status_ready(request.graph_status)
        current_status = self.status_manager.require_ready()
        if phase0_status is not None:
            _validate_phase0_status_matches_runtime(current_status, phase0_status)

        plan = promote_graph_deltas(
            request.cycle_id,
            request.selection_ref,
            candidate_reader=self.candidate_reader,
            entity_reader=self.entity_reader,
            canonical_writer=self.canonical_writer,
            client=self.client,
            status_manager=self.status_manager,
        )
        ready_status = self.status_manager.require_ready()
        return GraphPromotionAssetResult(
            cycle_id=request.cycle_id,
            selection_ref=request.selection_ref,
            graph_generation_id=ready_status.graph_generation_id,
            delta_ids=tuple(plan.delta_ids),
            plan=plan,
            graph_status=ready_status,
        )

    def compute_graph_snapshot(
        self,
        request: GraphSnapshotAssetRequest,
    ) -> GraphSnapshotAssetResult:
        graph_snapshot, impact_snapshot = compute_graph_snapshots(
            request.cycle_id,
            request.world_state_ref,
            client=self.client,
            graph_generation_id=request.graph_generation_id,
            regime_reader=self.regime_reader,
            snapshot_writer=self.snapshot_writer,
            status_manager=self.status_manager,
            graph_name=self.graph_name,
            enabled_channels=self.enabled_channels,  # type: ignore[arg-type]
            max_iterations=self.max_iterations,
            result_limit=self.result_limit,
        )
        artifact_ref = _artifact_ref_from_snapshot_writer(
            self.snapshot_writer,
            graph_snapshot,
        )
        proof = prove_cold_reload_artifact(
            artifact_ref,
            artifact_reader=self.artifact_reader,
            expected_status=self.status_manager.require_ready(),
        )
        return GraphSnapshotAssetResult(
            graph_snapshot_id=graph_snapshot.graph_snapshot_id,
            impact_snapshot_id=impact_snapshot.impact_snapshot_id,
            artifact_ref=artifact_ref,
            graph_snapshot=graph_snapshot,
            impact_snapshot=impact_snapshot,
            cold_reload_proof=proof,
        )


class GraphPhase1AssetFactoryProvider:
    """Provider object consumed structurally by orchestrator."""

    def __init__(
        self,
        runtime: GraphPhase1Runtime | None = None,
        *,
        resource_key: str = GRAPH_PHASE1_RESOURCE_KEY,
        world_state_ref: str = "world-state:latest",
    ) -> None:
        self.runtime = runtime
        self.resource_key = resource_key
        self.world_state_ref = world_state_ref

    def get_assets(self) -> tuple[object, ...]:
        dagster = _require_dagster()
        resource_key = self.resource_key
        world_state_ref = self.world_state_ref

        @dagster.asset(
            name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
            required_resource_keys={resource_key},
        )
        def graph_promotion(
            context,
            phase0_readiness_ping: object,
            candidate_freeze: object,
            graph_status: object,
        ) -> GraphPromotionAssetResult:
            cycle_id, selection_ref = _cycle_binding_from_phase0(
                candidate_freeze,
                context=context,
            )
            runtime = _runtime_from_context(context, resource_key)
            return runtime.promote_graph(
                GraphPromotionAssetRequest(
                    cycle_id=cycle_id,
                    selection_ref=selection_ref,
                    phase0_readiness=phase0_readiness_ping,
                    candidate_freeze=candidate_freeze,
                    graph_status=graph_status,
                ),
            )

        @dagster.asset(
            name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
            required_resource_keys={resource_key},
        )
        def graph_snapshot(
            context,
            graph_promotion: GraphPromotionAssetResult,
        ) -> GraphSnapshotAssetResult:
            runtime = _runtime_from_context(context, resource_key)
            return runtime.compute_graph_snapshot(
                GraphSnapshotAssetRequest(
                    cycle_id=graph_promotion.cycle_id,
                    world_state_ref=_world_state_ref_from_promotion(
                        graph_promotion,
                        default=world_state_ref,
                    ),
                    graph_generation_id=graph_promotion.graph_generation_id,
                    promotion=graph_promotion,
                ),
            )

        return (graph_promotion, graph_snapshot)

    def get_checks(self) -> tuple[object, ...]:
        return ()

    def get_resources(self) -> Mapping[str, object]:
        dagster = _require_dagster()
        runtime = self.runtime or _FailClosedGraphPhase1Runtime()
        return {
            self.resource_key: dagster.ResourceDefinition.hardcoded_resource(runtime),
        }


def build_graph_phase1_provider(
    runtime: GraphPhase1Runtime | None = None,
    *,
    resource_key: str = GRAPH_PHASE1_RESOURCE_KEY,
    world_state_ref: str = "world-state:latest",
) -> GraphPhase1AssetFactoryProvider:
    """Return an orchestrator-compatible graph Phase 1 provider."""

    return GraphPhase1AssetFactoryProvider(
        runtime,
        resource_key=resource_key,
        world_state_ref=world_state_ref,
    )


def prove_cold_reload_artifact(
    snapshot_ref: str,
    *,
    artifact_reader: CanonicalReader,
    expected_status: Neo4jGraphStatus,
) -> ColdReloadArtifactProof:
    """Fail closed unless a persisted artifact can reproduce live metrics."""

    ready_status = require_ready_status(expected_status)
    plan = artifact_reader.read_cold_reload_plan(snapshot_ref)
    snapshot = plan.expected_snapshot
    mismatches: list[str] = []
    for field_name, snapshot_value, status_value in (
        ("graph_generation_id", snapshot.graph_generation_id, ready_status.graph_generation_id),
        ("node_count", snapshot.node_count, ready_status.node_count),
        ("edge_count", snapshot.edge_count, ready_status.edge_count),
        ("key_label_counts", snapshot.key_label_counts, ready_status.key_label_counts),
        ("checksum", snapshot.checksum, ready_status.checksum),
    ):
        if snapshot_value != status_value:
            mismatches.append(
                f"{field_name} artifact={snapshot_value!r} status={status_value!r}",
            )
    if mismatches:
        raise ValueError(
            "cold reload artifact metrics disagree with Neo4jGraphStatus: "
            + "; ".join(mismatches),
        )

    return ColdReloadArtifactProof(
        snapshot_ref=snapshot_ref,
        cycle_id=plan.cycle_id,
        snapshot_id=snapshot.snapshot_id,
        graph_generation_id=snapshot.graph_generation_id,
        node_count=snapshot.node_count,
        edge_count=snapshot.edge_count,
        key_label_counts=dict(snapshot.key_label_counts),
        checksum=snapshot.checksum,
    )


class _FailClosedGraphPhase1Runtime:
    def promote_graph(
        self,
        request: GraphPromotionAssetRequest,
    ) -> GraphPromotionAssetResult:
        raise RuntimeError(_FAIL_CLOSED_RUNTIME_MESSAGE)

    def compute_graph_snapshot(
        self,
        request: GraphSnapshotAssetRequest,
    ) -> GraphSnapshotAssetResult:
        raise RuntimeError(_FAIL_CLOSED_RUNTIME_MESSAGE)


_FAIL_CLOSED_RUNTIME_MESSAGE = (
    "Graph Phase 1 runtime dependencies are not configured; provide real "
    "candidate_reader, canonical_writer, Neo4j client/status, regime_reader, "
    "and formal artifact snapshot writer resources."
)


def _ensure_phase0_readiness(value: object) -> None:
    ready = _value_by_name(value, ("ready", "passed", "ok"))
    if ready is not None and not bool(ready):
        raise PermissionError("Phase 1 graph promotion requires Phase 0 readiness")
    status = _value_by_name(value, ("status", "graph_status"))
    if isinstance(status, str) and status.lower() in {"failed", "blocked", "rebuilding"}:
        raise PermissionError(
            f"Phase 1 graph promotion requires ready Phase 0 status; got {status!r}",
        )
    if isinstance(value, str) and value.strip().lower() in {"failed", "blocked"}:
        raise PermissionError(
            f"Phase 1 graph promotion requires ready Phase 0 status; got {value!r}",
        )


def _ensure_phase0_graph_status_ready(value: object) -> Neo4jGraphStatus | None:
    if isinstance(value, Neo4jGraphStatus):
        return require_ready_status(value)

    if isinstance(value, Mapping) or _has_any_attr(value, ("graph_status", "status")):
        status_value = _value_by_name(value, ("graph_status", "status"))
        if status_value != "ready":
            raise PermissionError(
                "Phase 1 graph promotion requires graph_status='ready'; "
                f"got {status_value!r}",
            )
        writer_lock_token = _value_by_name(value, ("writer_lock_token",))
        if writer_lock_token is not None:
            raise PermissionError("Phase 1 graph promotion requires no graph writer lock")
        try:
            return Neo4jGraphStatus.model_validate(value)
        except Exception:
            return None

    if isinstance(value, str):
        normalized = value.lower()
        if "rebuilding" in normalized or "failed" in normalized or "blocked" in normalized:
            raise PermissionError(
                "Phase 1 graph promotion requires graph_status='ready'; "
                f"got {value!r}",
            )
        if "ready" in normalized:
            return None

    raise ValueError(
        "graph_status Phase 0 dependency must expose a ready Neo4j graph status",
    )


def _validate_phase0_status_matches_runtime(
    current_status: Neo4jGraphStatus,
    phase0_status: Neo4jGraphStatus,
) -> None:
    mismatches = [
        field_name
        for field_name in (
            "graph_generation_id",
            "node_count",
            "edge_count",
            "key_label_counts",
            "checksum",
        )
        if getattr(current_status, field_name) != getattr(phase0_status, field_name)
    ]
    if mismatches:
        raise ValueError(
            "Phase 0 graph_status disagrees with runtime Neo4jGraphStatus: "
            + ", ".join(mismatches),
        )


def _cycle_binding_from_phase0(
    candidate_freeze: object,
    *,
    context: object,
) -> tuple[str, str]:
    cycle_id = _value_by_name(candidate_freeze, ("cycle_id",))
    if cycle_id is None and isinstance(candidate_freeze, str) and candidate_freeze.startswith("CYCLE_"):
        cycle_id = candidate_freeze
    if cycle_id is None:
        cycle_id = _context_tag(context, "cycle_id")
    if cycle_id is None or not str(cycle_id):
        raise ValueError(
            "candidate_freeze Phase 0 dependency must expose cycle_id "
            "or run tag 'cycle_id'",
        )

    selection_ref = _value_by_name(
        candidate_freeze,
        ("selection_ref", "candidate_selection_ref", "snapshot_ref"),
    )
    if selection_ref is None:
        selection_ref = f"cycle_candidate_selection:{cycle_id}"
    return str(cycle_id), str(selection_ref)


def _world_state_ref_from_promotion(
    graph_promotion: GraphPromotionAssetResult,
    *,
    default: str,
) -> str:
    value = _value_by_name(graph_promotion, ("world_state_ref",))
    if value is None or not str(value):
        return default
    return str(value)


def _artifact_ref_from_snapshot_writer(
    snapshot_writer: SnapshotWriter,
    graph_snapshot: GraphSnapshot,
) -> str:
    ref_for_snapshot = getattr(snapshot_writer, "artifact_ref_for_graph_snapshot", None)
    if callable(ref_for_snapshot):
        ref = ref_for_snapshot(graph_snapshot)
        if ref:
            return str(ref)

    last_ref = getattr(snapshot_writer, "last_artifact_ref", None)
    if last_ref:
        return str(last_ref)

    raise RuntimeError(
        "snapshot_writer must expose persisted graph_snapshot artifact_ref "
        "for cold reload proof",
    )


def _runtime_from_context(context: object, resource_key: str) -> GraphPhase1Runtime:
    resources = getattr(context, "resources")
    runtime = getattr(resources, resource_key)
    return runtime


def _context_tag(context: object, key: str) -> object | None:
    run_tags = getattr(context, "run_tags", None)
    if isinstance(run_tags, Mapping) and key in run_tags:
        return run_tags[key]
    run = getattr(context, "run", None)
    tags = getattr(run, "tags", None)
    if isinstance(tags, Mapping):
        return tags.get(key)
    return None


def _value_by_name(value: object, names: Sequence[str]) -> object | None:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _has_any_attr(value: object, names: Sequence[str]) -> bool:
    return any(hasattr(value, name) for name in names)


def _require_dagster() -> Any:
    try:
        return import_module("dagster")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dagster is required to build graph-engine Phase 1 provider assets",
        ) from exc


__all__ = [
    "GRAPH_PHASE1_RESOURCE_KEY",
    "PHASE0_CANDIDATE_FREEZE_ASSET_KEY",
    "PHASE0_GRAPH_STATUS_ASSET_KEY",
    "PHASE0_READINESS_ASSET_KEY",
    "PHASE1_GRAPH_PROMOTION_ASSET_KEY",
    "PHASE1_GRAPH_SNAPSHOT_ASSET_KEY",
    "PHASE1_GROUP_NAME",
    "ColdReloadArtifactProof",
    "GraphPhase1AssetFactoryProvider",
    "GraphPhase1Runtime",
    "GraphPhase1Service",
    "GraphPromotionAssetRequest",
    "GraphPromotionAssetResult",
    "GraphSnapshotAssetRequest",
    "GraphSnapshotAssetResult",
    "build_graph_phase1_provider",
    "prove_cold_reload_artifact",
]
