"""Guarded utilities for the holdings live graph proof.

The module is intentionally a library surface for the assembly cross-repo
runner. It does not load environment variables, open network connections, or
run the production propagation pipeline by itself.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from contracts.schemas import CandidateGraphDelta

from graph_engine.models import PropagationContext, PropagationResult, PromotionPlan
from graph_engine.promotion.interfaces import EntityAnchorReader
from graph_engine.promotion.service import promote_graph_deltas
from graph_engine.propagation import (
    HoldingsAlgorithmConfig,
    run_co_holding_crowding,
    run_northbound_anomaly,
)
from graph_engine.status import GraphStatusManager

_CONFIRM_ENV = "GRAPH_ENGINE_LIVE_PROOF_CONFIRM"
_NAMESPACE_ENV = "GRAPH_ENGINE_LIVE_PROOF_NAMESPACE"
_NEO4J_DATABASE_ENV = "NEO4J_DATABASE"
_CONFIRM_VALUE = "1"
_DEFAULT_NEO4J_DATABASE = "neo4j"
_SAFE_NAMESPACE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{2,95}$")
_SAFE_FILE_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
_PROOF_PATH_MARKER_PATTERN = re.compile(r"(^|[.-])(proof|smoke|test)([.-]|$)")
_ALLOWED_HOLDINGS_RELATIONSHIPS = frozenset({"CO_HOLDING", "NORTHBOUND_HOLD"})


class ReadOnlyGraphClient(Protocol):
    """Graph client surface needed by proof readback and algorithms."""

    def execute_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Run a read query and return rows."""


@dataclass(frozen=True)
class HoldingsLiveGraphProofConfig:
    """Validated live graph proof gates."""

    namespace: str
    neo4j_database: str
    artifact_root: Path


@dataclass(frozen=True)
class LayerAArtifactSummary:
    """Summary of the Layer A canonical artifact written for a proof."""

    manifest_path: Path
    records_path: Path
    cycle_id: str
    selection_ref: str
    delta_count: int
    node_count: int
    edge_count: int
    assertion_count: int
    relation_counts: dict[str, int]


@dataclass(frozen=True)
class Neo4jEdgeVerificationSummary:
    """Read-only verification result for synced holdings edges."""

    expected_edge_count: int
    edge_count: int
    relation_counts: dict[str, int]
    missing_edge_ids: list[str]
    disallowed_relation_types: list[str]


@dataclass(frozen=True)
class HoldingsAlgorithmProofSummary:
    """Summary of explicit #55 holdings algorithm output."""

    cycle_id: str
    graph_generation_id: int
    co_holding_path_count: int
    northbound_path_count: int
    total_path_count: int
    impacted_entity_count: int
    co_holding_diagnostics: dict[str, int]
    northbound_diagnostics: dict[str, int]


@dataclass(frozen=True)
class HoldingsLiveGraphProofSummary:
    """End-to-end holdings proof summary for assembly evidence."""

    namespace: str
    neo4j_database: str
    cycle_id: str
    selection_ref: str
    layer_a_artifact: LayerAArtifactSummary
    edge_verification: Neo4jEdgeVerificationSummary
    algorithm_proof: HoldingsAlgorithmProofSummary


@dataclass(frozen=True)
class LoadedCandidateDeltaReader:
    """CandidateDeltaReader for already-loaded frozen holdings candidates."""

    deltas: Sequence[CandidateGraphDelta]

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list[CandidateGraphDelta]:
        """Return validated in-memory candidate deltas."""

        del cycle_id, selection_ref
        validate_holdings_candidate_deltas(self.deltas)
        return list(self.deltas)


class LayerAArtifactCanonicalWriter:
    """CanonicalWriter implementation that persists a PromotionPlan as artifacts."""

    def __init__(self, artifact_root: str | Path, *, namespace: str) -> None:
        config = _validate_artifact_destination(artifact_root, namespace=namespace)
        self.artifact_root = config.artifact_root
        self.namespace = config.namespace
        self.last_summary: LayerAArtifactSummary | None = None

    def write_canonical_records(self, plan: PromotionPlan) -> None:
        """Write a promotion plan in a curated Layer A artifact shape."""

        self.last_summary = write_layer_a_artifact(
            plan,
            artifact_root=self.artifact_root,
            namespace=self.namespace,
        )


def validate_holdings_live_graph_proof_env(
    env: Mapping[str, str | None],
    *,
    artifact_root: str | Path,
) -> HoldingsLiveGraphProofConfig:
    """Validate gates before any live proof can write artifacts or Neo4j."""

    if env.get(_CONFIRM_ENV) != _CONFIRM_VALUE:
        raise PermissionError(f"{_CONFIRM_ENV}=1 is required for holdings live graph proof")

    namespace = _require_namespace(env.get(_NAMESPACE_ENV))
    neo4j_database = str(env.get(_NEO4J_DATABASE_ENV) or "").strip()
    if not neo4j_database:
        raise PermissionError(f"{_NEO4J_DATABASE_ENV} is required for holdings live graph proof")
    if neo4j_database.lower() == _DEFAULT_NEO4J_DATABASE:
        raise PermissionError("default Neo4j database 'neo4j' is not allowed for this proof")

    artifact_config = _validate_artifact_destination(artifact_root, namespace=namespace)
    return HoldingsLiveGraphProofConfig(
        namespace=namespace,
        neo4j_database=neo4j_database,
        artifact_root=artifact_config.artifact_root,
    )


def validate_holdings_candidate_deltas(
    deltas: Sequence[CandidateGraphDelta],
) -> None:
    """Fail closed unless every candidate delta is a supported holdings edge."""

    relation_types = sorted({delta.relation_type for delta in deltas})
    disallowed = [
        relation_type
        for relation_type in relation_types
        if relation_type not in _ALLOWED_HOLDINGS_RELATIONSHIPS
    ]
    if disallowed:
        raise ValueError(f"unsupported holdings proof relation types: {disallowed}")
    if not deltas:
        raise ValueError("holdings proof requires at least one candidate delta")


def validate_holdings_promotion_plan(plan: PromotionPlan) -> None:
    """Fail closed unless the promotion plan only contains holdings edges."""

    relation_types = sorted({edge.relationship_type for edge in plan.edge_records})
    disallowed = [
        relation_type
        for relation_type in relation_types
        if relation_type not in _ALLOWED_HOLDINGS_RELATIONSHIPS
    ]
    if disallowed:
        raise ValueError(f"unsupported holdings proof relation types: {disallowed}")
    if not plan.edge_records:
        raise ValueError("holdings proof requires at least one promoted edge")


def write_layer_a_artifact(
    plan: PromotionPlan,
    *,
    artifact_root: str | Path,
    namespace: str,
) -> LayerAArtifactSummary:
    """Write canonical records as sanitized JSON + JSONL Layer A artifacts."""

    artifact_config = _validate_artifact_destination(artifact_root, namespace=namespace)
    validate_holdings_promotion_plan(plan)

    artifact_dir = (
        artifact_config.artifact_root
        / artifact_config.namespace
        / "layer_a"
        / _safe_file_component(plan.cycle_id)
        / _safe_file_component(plan.selection_ref)
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    records_path = artifact_dir / "canonical_records.jsonl"
    manifest_path = artifact_dir / "manifest.json"

    records = list(_canonical_record_lines(plan))
    _atomic_write_text(records_path, "\n".join(records) + "\n")

    relation_counts = dict(
        sorted(Counter(edge.relationship_type for edge in plan.edge_records).items())
    )
    manifest = {
        "artifact_type": "holdings_live_graph_layer_a",
        "layer": "Layer A",
        "namespace": artifact_config.namespace,
        "cycle_id": plan.cycle_id,
        "selection_ref": plan.selection_ref,
        "records_ref": "canonical_records.jsonl",
        "delta_count": len(plan.delta_ids),
        "node_count": len(plan.node_records),
        "edge_count": len(plan.edge_records),
        "assertion_count": len(plan.assertion_records),
        "relation_counts": relation_counts,
        "relation_types": sorted(relation_counts),
        "delta_ids": sorted(plan.delta_ids),
        "created_at": plan.created_at.isoformat(),
    }
    _atomic_write_json(manifest_path, manifest)

    return LayerAArtifactSummary(
        manifest_path=manifest_path,
        records_path=records_path,
        cycle_id=plan.cycle_id,
        selection_ref=plan.selection_ref,
        delta_count=len(plan.delta_ids),
        node_count=len(plan.node_records),
        edge_count=len(plan.edge_records),
        assertion_count=len(plan.assertion_records),
        relation_counts=relation_counts,
    )


def verify_holdings_edges(
    client: ReadOnlyGraphClient,
    *,
    edge_ids: Sequence[str],
    strict: bool = True,
) -> Neo4jEdgeVerificationSummary:
    """Verify synced holdings edges with a read-only query scoped by edge id."""

    expected_edge_ids = sorted({str(edge_id) for edge_id in edge_ids if str(edge_id).strip()})
    if not expected_edge_ids:
        raise ValueError("edge_ids must not be empty")

    rows = client.execute_read(
        """
MATCH ()-[relationship]->()
WHERE relationship.edge_id IN $edge_ids
RETURN relationship.edge_id AS edge_id,
       type(relationship) AS relationship_type
ORDER BY edge_id ASC
""",
        {"edge_ids": expected_edge_ids},
    )
    seen_edge_ids = sorted(
        str(row.get("edge_id"))
        for row in rows
        if row.get("edge_id") is not None
    )
    relation_counts = dict(
        sorted(Counter(str(row.get("relationship_type")) for row in rows).items())
    )
    missing_edge_ids = sorted(set(expected_edge_ids) - set(seen_edge_ids))
    disallowed_relation_types = sorted(
        relation_type
        for relation_type in relation_counts
        if relation_type not in _ALLOWED_HOLDINGS_RELATIONSHIPS
    )

    summary = Neo4jEdgeVerificationSummary(
        expected_edge_count=len(expected_edge_ids),
        edge_count=len(seen_edge_ids),
        relation_counts=relation_counts,
        missing_edge_ids=missing_edge_ids,
        disallowed_relation_types=disallowed_relation_types,
    )
    if strict:
        if missing_edge_ids:
            raise ValueError(f"missing synced holdings edges: {missing_edge_ids}")
        if disallowed_relation_types:
            raise ValueError(
                "unexpected holdings proof relation types: "
                f"{disallowed_relation_types}"
            )
    return summary


def build_holdings_proof_context(
    *,
    cycle_id: str,
    graph_generation_id: int,
    namespace: str,
    world_state_ref: str | None = None,
) -> PropagationContext:
    """Create the explicit #55 read-only algorithm context for a proof run."""

    safe_namespace = _require_namespace(namespace)
    return PropagationContext(
        cycle_id=cycle_id,
        world_state_ref=world_state_ref or f"holdings-live-graph-proof:{safe_namespace}",
        graph_generation_id=graph_generation_id,
        enabled_channels=["event", "reflexive"],
        channel_multipliers={"event": 1.0, "reflexive": 1.0},
        regime_multipliers={"event": 1.0, "reflexive": 1.0},
        decay_policy={},
        regime_context={"proof_namespace": safe_namespace},
    )


def run_holdings_algorithm_proof(
    context: PropagationContext,
    client: ReadOnlyGraphClient,
    *,
    status_manager: GraphStatusManager,
    config: HoldingsAlgorithmConfig | None = None,
    result_limit: int = 100,
) -> HoldingsAlgorithmProofSummary:
    """Run explicit holdings-only #55 algorithms and summarize their output."""

    if "event" not in context.enabled_channels or "reflexive" not in context.enabled_channels:
        raise PermissionError("holdings live graph proof requires event and reflexive channels")

    co_holding = run_co_holding_crowding(
        context,
        client,  # type: ignore[arg-type]
        status_manager=status_manager,
        config=config,
        result_limit=result_limit,
    )
    northbound = run_northbound_anomaly(
        context,
        client,  # type: ignore[arg-type]
        status_manager=status_manager,
        config=config,
        result_limit=result_limit,
    )
    return HoldingsAlgorithmProofSummary(
        cycle_id=context.cycle_id,
        graph_generation_id=context.graph_generation_id,
        co_holding_path_count=len(co_holding.activated_paths),
        northbound_path_count=len(northbound.activated_paths),
        total_path_count=len(co_holding.activated_paths) + len(northbound.activated_paths),
        impacted_entity_count=_merged_impacted_entity_count(co_holding, northbound),
        co_holding_diagnostics=_co_holding_diagnostics(co_holding),
        northbound_diagnostics=_northbound_diagnostics(northbound),
    )


def run_holdings_live_graph_proof(
    *,
    cycle_id: str,
    selection_ref: str,
    candidate_deltas: Sequence[CandidateGraphDelta],
    entity_reader: EntityAnchorReader,
    client: Any,
    status_manager: GraphStatusManager,
    env: Mapping[str, str | None],
    artifact_root: str | Path,
    config: HoldingsAlgorithmConfig | None = None,
    result_limit: int = 100,
) -> HoldingsLiveGraphProofSummary:
    """Run the guarded holdings proof with injected services.

    The only Neo4j write path is the existing promotion live-sync barrier.
    This function must be called by a runner that already provisioned a
    disposable Neo4j database and one-time upstream queue/freeze state.
    """

    proof_config = validate_holdings_live_graph_proof_env(env, artifact_root=artifact_root)
    validate_holdings_candidate_deltas(candidate_deltas)

    canonical_writer = LayerAArtifactCanonicalWriter(
        proof_config.artifact_root,
        namespace=proof_config.namespace,
    )
    plan = promote_graph_deltas(
        cycle_id,
        selection_ref,
        candidate_reader=LoadedCandidateDeltaReader(candidate_deltas),
        entity_reader=entity_reader,
        canonical_writer=canonical_writer,
        client=client,
        status_manager=status_manager,
        sync_to_live_graph=True,
    )
    validate_holdings_promotion_plan(plan)
    if canonical_writer.last_summary is None:
        raise RuntimeError("Layer A artifact writer did not record a summary")

    edge_verification = verify_holdings_edges(
        client,
        edge_ids=[edge.edge_id for edge in plan.edge_records],
    )
    ready_status = status_manager.require_ready()
    proof_context = build_holdings_proof_context(
        cycle_id=cycle_id,
        graph_generation_id=ready_status.graph_generation_id,
        namespace=proof_config.namespace,
    )
    algorithm_proof = run_holdings_algorithm_proof(
        proof_context,
        client,
        status_manager=status_manager,
        config=config,
        result_limit=result_limit,
    )

    return HoldingsLiveGraphProofSummary(
        namespace=proof_config.namespace,
        neo4j_database=proof_config.neo4j_database,
        cycle_id=cycle_id,
        selection_ref=selection_ref,
        layer_a_artifact=canonical_writer.last_summary,
        edge_verification=edge_verification,
        algorithm_proof=algorithm_proof,
    )


def _validate_artifact_destination(
    artifact_root: str | Path,
    *,
    namespace: str,
) -> HoldingsLiveGraphProofConfig:
    safe_namespace = _require_namespace(namespace)
    resolved_root = Path(artifact_root).expanduser().resolve(strict=False)
    if resolved_root.exists() and not resolved_root.is_dir():
        raise ValueError("artifact_root must be a directory")
    if not _path_has_proof_marker(resolved_root):
        raise ValueError("artifact_root must be under a proof/smoke/test workspace")
    return HoldingsLiveGraphProofConfig(
        namespace=safe_namespace,
        neo4j_database="<not-validated>",
        artifact_root=resolved_root,
    )


def _require_namespace(namespace: str | None) -> str:
    value = str(namespace or "").strip()
    if not value:
        raise PermissionError(f"{_NAMESPACE_ENV} is required for holdings live graph proof")
    if not _SAFE_NAMESPACE_PATTERN.fullmatch(value):
        raise ValueError(
            f"{_NAMESPACE_ENV} must match {_SAFE_NAMESPACE_PATTERN.pattern}",
        )
    if value.lower() in {"default", "live", "prod", "production", "neo4j"}:
        raise ValueError(f"{_NAMESPACE_ENV} must be unique, not {value!r}")
    return value


def _path_has_proof_marker(path: Path) -> bool:
    return any(_PROOF_PATH_MARKER_PATTERN.search(part.lower()) for part in path.parts)


def _canonical_record_lines(plan: PromotionPlan) -> list[str]:
    lines: list[str] = []
    for node_record in plan.node_records:
        lines.append(
            _json_dumps(
                {
                    "record_type": "node",
                    "record": node_record.model_dump(mode="json"),
                }
            )
        )
    for edge_record in plan.edge_records:
        lines.append(
            _json_dumps(
                {
                    "record_type": "edge",
                    "record": edge_record.model_dump(mode="json"),
                }
            )
        )
    for assertion_record in plan.assertion_records:
        lines.append(
            _json_dumps(
                {
                    "record_type": "assertion",
                    "record": assertion_record.model_dump(mode="json"),
                }
            )
        )
    return lines


def _safe_file_component(value: str) -> str:
    sanitized = _SAFE_FILE_COMPONENT_PATTERN.sub("-", value.strip()).strip(".-")
    if not sanitized:
        raise ValueError("file component must not be empty after sanitization")
    return sanitized[:120]


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, _json_dumps(payload) + "\n")


def _atomic_write_text(path: Path, text: str) -> None:
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def _json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _co_holding_diagnostics(result: PropagationResult) -> dict[str, int]:
    breakdown = result.channel_breakdown.get("reflexive", {}).get("co_holding_crowding", {})
    return _int_diagnostics(breakdown.get("diagnostics"))


def _northbound_diagnostics(result: PropagationResult) -> dict[str, int]:
    breakdown = result.channel_breakdown.get("event", {}).get("northbound_anomaly", {})
    return _int_diagnostics(breakdown.get("diagnostics"))


def _int_diagnostics(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(key): int(count)
        for key, count in value.items()
        if isinstance(count, int | float)
    }


def _merged_impacted_entity_count(*results: PropagationResult) -> int:
    entity_ids: set[str] = set()
    for result in results:
        for entity in result.impacted_entities:
            node_id = entity.get("node_id")
            if node_id is not None:
                entity_ids.add(str(node_id))
    return len(entity_ids)
