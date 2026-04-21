"""Assembly-facing public entrypoints for graph-engine.

This module is the single boundary that ``assembly`` (registry + compat
checks + bootstrap) imports to introspect this package. The five
``module-level singleton instances`` below match the assembly Protocols
in ``assembly/src/assembly/contracts/entrypoints.py`` and the signature
shape enforced by ``assembly/src/assembly/compat/checks/public_api_boundary.py``:

- ``health_probe.check(*, timeout_sec: float)``
- ``smoke_hook.run(*, profile_id: str)``
- ``init_hook.initialize(*, resolved_env: dict[str, str])``
- ``version_declaration.declare()``
- ``cli.invoke(argv: list[str])``

CLAUDE.md guardrails this file enforces by construction (graph-engine
§10 red lines):

- **Truth Before Mirror** (#1) — the version declaration carries
  ``canonical_truth_layer="iceberg"`` as a structural marker; assembly
  cross-checks via boundary tier deny scan that no Neo4j-as-truth
  shortcut got introduced.
- **Promotion Before Propagation** (#2) — smoke_hook drives only the
  promotion path, never the live-graph-first shortcut.
- **No Raw Text** (#6) — public.py never imports ``lightrag``,
  ``langchain``, or any text/parser stack; only consumes contracted
  ``CandidateGraphDelta`` from contracts.
- **No business module imports** (CLAUDE.md §10 #5 + #3) — public.py
  never imports ``main_core`` (regime is read-only data input, not
  code dependency), nor any of ``data_platform`` /
  ``subsystem_announcement`` / ``subsystem_news`` / ``audit_eval`` /
  ``orchestrator`` / ``assembly``.
- **Status Guard** (#7) — health_probe verifies ``Neo4jGraphStatus``
  enum is intact (initializing / ready / reloading / inconsistent /
  blocked); smoke_hook does NOT execute live queries (no real Neo4j
  needed).

graph-engine is a CONSUMER of canonical Ex-3 (re-exports
``CandidateGraphDelta`` / ``GraphSnapshot`` / ``GraphImpactSnapshot``
from contracts), NOT a producer that goes through subsystem-sdk. So
unlike announcement/news public.py, this file does NOT exercise the
``subsystem-sdk.SubmitClient`` path or canonical-wire-mapper.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Final

from graph_engine.version import __version__ as _GRAPH_ENGINE_VERSION


_HEALTHY: Final[str] = "healthy"
_DEGRADED: Final[str] = "degraded"
_DOWN: Final[str] = "down"

# Ex types graph-engine CONSUMES (from contracts). graph-engine does
# NOT produce Ex-1/2/3 wire payloads (it consumes Ex-3 / writes Layer
# A snapshots / mirrors to Neo4j); listed here so assembly's compat
# matrix can verify cross-module lineage.
_CONSUMED_EX_TYPES: Final[tuple[str, ...]] = ("Ex-3",)


def _probe_contracts_re_exports() -> dict[str, Any]:
    """Confirm contracts schemas re-exported by graph_engine.models are
    the IDENTITY (same Python object) of what contracts itself exports.
    A drift here means graph-engine forked the schema (forbidden — only
    contracts owns Ex-3 canonical shape per CLAUDE.md domain rules).
    """

    try:
        from contracts.schemas import (
            CandidateGraphDelta as ContractsCandidateGraphDelta,
        )
        from contracts.schemas import GraphImpactSnapshot as ContractsImpact
        from contracts.schemas import GraphSnapshot as ContractsSnapshot

        from graph_engine.models import (
            CandidateGraphDelta,
            GraphImpactSnapshot,
            GraphSnapshot,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "available": False,
            "reason": f"contracts schema re-export check failed: {exc!r}",
        }

    drift: list[str] = []
    if CandidateGraphDelta is not ContractsCandidateGraphDelta:
        drift.append("CandidateGraphDelta")
    if GraphSnapshot is not ContractsSnapshot:
        drift.append("GraphSnapshot")
    if GraphImpactSnapshot is not ContractsImpact:
        drift.append("GraphImpactSnapshot")
    if drift:
        return {
            "available": False,
            "reason": (
                "graph_engine.models forked contracts schema for: "
                f"{drift}; CLAUDE.md violation (only contracts owns "
                "canonical Ex-3 / GraphSnapshot / GraphImpactSnapshot)"
            ),
        }
    return {"available": True, "re_exports_verified": ["CandidateGraphDelta", "GraphSnapshot", "GraphImpactSnapshot"]}


def _probe_neo4j_status_literal() -> dict[str, Any]:
    """Confirm ``Neo4jGraphStatus.graph_status`` Literal is intact (3
    canonical states per CLAUDE.md §10 #7 status guard). Drift here
    means a state was silently added/removed and downstream readers
    may guard on a stale set.

    Note: ``Neo4jGraphStatus`` is a ``BaseModel`` (not an Enum); the
    canonical state set is encoded as a ``Literal`` on the
    ``graph_status`` field. We pull it via Pydantic's
    ``model_fields[...].annotation``.
    """

    try:
        from typing import get_args

        from graph_engine.models import Neo4jGraphStatus
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "available": False,
            "reason": f"Neo4jGraphStatus import failed: {exc!r}",
        }

    annotation = Neo4jGraphStatus.model_fields["graph_status"].annotation
    actual = set(get_args(annotation))
    expected = {"ready", "rebuilding", "failed"}
    if actual != expected:
        return {
            "available": False,
            "reason": (
                f"Neo4jGraphStatus.graph_status drifted: expected "
                f"{sorted(expected)}, got {sorted(actual)}"
            ),
        }
    return {"available": True, "status_values": sorted(actual)}


class _HealthProbe:
    """Probe graph-engine-internal invariants without doing any network
    IO or opening a real Neo4j / Postgres connection.

    `check(*, timeout_sec)` returns a structured dict with status one of
    ``healthy`` / ``degraded`` / ``down``. ``timeout_sec`` is accepted
    for assembly Protocol compliance but unused — none of these checks
    do IO.
    """

    def check(self, *, timeout_sec: float) -> dict[str, Any]:
        details: dict[str, Any] = {
            "consumed_ex_types": list(_CONSUMED_EX_TYPES),
            "canonical_truth_layer": "iceberg",
        }

        # Invariant 1: contracts re-exports are identity (no fork —
        # CLAUDE.md domain rule).
        re_exports = _probe_contracts_re_exports()
        details["contracts_re_exports"] = re_exports
        # Treat missing contracts as ``degraded`` (offline-first dev
        # venv without [contracts-schemas] extra is allowed); fork
        # detection is fatal (``down``).
        if not re_exports["available"]:
            if "could not import" in re_exports.get("reason", "") or (
                "import failed" in re_exports.get("reason", "")
            ):
                status_after_re_exports = _DEGRADED
            else:
                return {
                    "status": _DOWN,
                    "details": details,
                    "timeout_sec": timeout_sec,
                }
        else:
            status_after_re_exports = _HEALTHY

        # Invariant 2: Neo4jGraphStatus.graph_status Literal stable
        # (CLAUDE.md §10 #7 status guard depends on this exact 3-state
        # set: ready / rebuilding / failed).
        status_enum = _probe_neo4j_status_literal()
        details["neo4j_status_literal"] = status_enum
        if not status_enum["available"]:
            return {
                "status": _DOWN,
                "details": details,
                "timeout_sec": timeout_sec,
            }

        return {
            "status": status_after_re_exports,
            "details": details,
            "timeout_sec": timeout_sec,
        }


class _SmokeHook:
    """Run a one-shot end-to-end smoke that exercises the canonical
    delta -> promotion plan path WITHOUT opening Neo4j / Postgres.

    1. Build a minimal valid ``CandidateGraphDelta`` (canonical Ex-3
       wire shape that announcement / news produce).
    2. Drive it through the REAL ``promote_graph_deltas`` consumer
       service with stub reader / entity reader / canonical writer
       — covers ``freeze_contract_deltas`` + ``validate_entity_anchors``
       + ``build_promotion_plan`` end-to-end (NOT just schema
       re-validation).
    3. Asserts the resulting ``PromotionPlan`` carries the canonical
       delta forward (cycle / selection / delta_id / edge record
       source/target preserved).

    Codex stage 2.10 review #1 P2 fix: the previous version stopped
    at ``CandidateGraphDelta.model_validate(...)`` which is the
    contracts re-export — calling it didn't exercise any graph-engine
    code and would stay green if ``promote_graph_deltas`` was broken.
    This rewrite drives the real consumer service with
    ``sync_to_live_graph=False`` so no Neo4j / Postgres connection
    is needed (preserves the offline-first smoke contract).

    Profile-aware only insofar as it rejects unknown profile_ids.
    Heavy deps (Neo4j driver / Postgres / GDS) are NEVER imported here.
    """

    _SUPPORTED_PROFILES: Final[frozenset[str]] = frozenset(
        {"lite-local", "full-dev"}
    )

    def run(self, *, profile_id: str) -> dict[str, Any]:
        if profile_id not in self._SUPPORTED_PROFILES:
            return {
                "passed": False,
                "failure_reason": (
                    f"unknown profile_id={profile_id!r}; supported: "
                    f"{sorted(self._SUPPORTED_PROFILES)}"
                ),
                "profile_id": profile_id,
            }

        try:
            from typing import get_args

            from graph_engine import promote_graph_deltas
            from graph_engine.models import (
                CandidateGraphDelta,
                Neo4jGraphStatus,
                PromotionPlan,
            )
        except Exception as exc:
            return {
                "passed": False,
                "failure_reason": (
                    f"graph_engine import failed: {exc!r}"
                ),
                "profile_id": profile_id,
            }

        # Confirm Neo4jGraphStatus.graph_status has the canonical 3
        # states (status guard contract). Smoke does NOT open a real
        # Neo4j connection (would violate the offline-first smoke
        # contract).
        graph_status_values = set(
            get_args(
                Neo4jGraphStatus.model_fields["graph_status"].annotation
            )
        )
        if graph_status_values != {"ready", "rebuilding", "failed"}:
            return {
                "passed": False,
                "failure_reason": (
                    "Neo4jGraphStatus.graph_status drifted from "
                    "canonical 3-state set"
                ),
                "profile_id": profile_id,
            }

        # Build a synthetic but minimally valid CandidateGraphDelta.
        # ``delta_type="upsert_edge"`` and ``relation_type="SUPPLY_CHAIN"``
        # are required to pass graph-engine's promotion-side validators
        # (see promotion/planner.py::_CONTRACT_DELTA_TYPE_TO_INTERNAL +
        # schema/definitions.py::RelationshipType).
        try:
            delta = CandidateGraphDelta(
                subsystem_id="subsystem-news",
                delta_id="smoke-graph-delta-001",
                delta_type="upsert_edge",
                source_node="ENT_GRAPH_SMOKE_SRC",
                target_node="ENT_GRAPH_SMOKE_DST",
                relation_type="SUPPLY_CHAIN",
                properties={"smoke": "minimal"},
                evidence=[
                    "smoke-evidence-ref-001",
                    "smoke-evidence-ref-002",
                ],
            )
        except Exception as exc:
            return {
                "passed": False,
                "failure_reason": (
                    f"CandidateGraphDelta construction failed: {exc!r}"
                ),
                "profile_id": profile_id,
            }

        # Drive REAL ``promote_graph_deltas`` (CONSUMER service) with
        # in-memory stub reader / entity reader / canonical writer so
        # no IO is required. ``sync_to_live_graph=False`` skips the
        # Neo4j sync path (which would need a real driver).
        cycle_id = "smoke-graph-cycle-001"
        selection_ref = "smoke-graph-selection-001"
        canonical_writes: list[Any] = []

        class _StubReader:
            def read_candidate_graph_deltas(
                self, _cid: str, _sref: str
            ) -> list[CandidateGraphDelta]:
                return [delta]

        class _StubEntityReader:
            def canonical_entity_ids_for_node_ids(
                self, node_ids: set[str]
            ) -> dict[str, str]:
                # Resolve every node id to itself (sentinel: smoke
                # entities are already canonical).
                return {nid: nid for nid in node_ids}

            def existing_entity_ids(
                self, entity_ids: set[str]
            ) -> set[str]:
                return set(entity_ids)

        class _StubCanonicalWriter:
            def write_canonical_records(self, plan: PromotionPlan) -> None:
                canonical_writes.append(plan)

        try:
            plan = promote_graph_deltas(
                cycle_id=cycle_id,
                selection_ref=selection_ref,
                candidate_reader=_StubReader(),
                entity_reader=_StubEntityReader(),
                canonical_writer=_StubCanonicalWriter(),
                sync_to_live_graph=False,
            )
        except Exception as exc:
            return {
                "passed": False,
                "failure_reason": (
                    f"promote_graph_deltas raised: {exc!r}"
                ),
                "profile_id": profile_id,
            }

        # Verify CLAUDE.md §10 #1 (truth-before-mirror): canonical
        # writer was invoked with the returned plan exactly once.
        if canonical_writes != [plan]:
            return {
                "passed": False,
                "failure_reason": (
                    f"canonical_writer.write_canonical_records called "
                    f"{len(canonical_writes)} times with "
                    f"{[id(p) for p in canonical_writes]} (expected "
                    f"exactly [plan id={id(plan)}])"
                ),
                "profile_id": profile_id,
            }

        # Verify the plan carries the canonical delta forward.
        if delta.delta_id not in plan.delta_ids:
            return {
                "passed": False,
                "failure_reason": (
                    f"PromotionPlan.delta_ids missing canonical "
                    f"delta_id {delta.delta_id!r}; got {plan.delta_ids}"
                ),
                "profile_id": profile_id,
            }
        if not plan.edge_records:
            return {
                "passed": False,
                "failure_reason": (
                    "PromotionPlan.edge_records empty; "
                    "upsert_edge candidate must produce at least one "
                    "GraphEdgeRecord"
                ),
                "profile_id": profile_id,
            }

        return {
            "passed": True,
            "profile_id": profile_id,
            "details": {
                "delta_id": delta.delta_id,
                "delta_type": delta.delta_type,
                "relation_type": delta.relation_type,
                "evidence_count": len(delta.evidence),
                "promotion_plan_cycle_id": plan.cycle_id,
                "promotion_plan_delta_count": len(plan.delta_ids),
                "promotion_plan_edge_record_count": len(plan.edge_records),
                "consumed_ex_types": list(_CONSUMED_EX_TYPES),
                "canonical_truth_layer": "iceberg",
            },
        }


class _InitHook:
    """No-op initialization. graph-engine has no global mutable state to
    set up at bootstrap (Neo4j driver / Postgres status store are
    constructed per-call inside the runtime services, not eagerly at
    import time). Returns ``None`` per assembly Protocol;
    ``resolved_env`` is accepted for compliance.
    """

    def initialize(self, *, resolved_env: dict[str, str]) -> None:
        _ = resolved_env
        return None


class _VersionDeclaration:
    """Declare the graph-engine + contracts schema versions assembly
    should reconcile in the registry. Returns a stable dict shape:

        {
            "module_id": "graph-engine",
            "module_version": "<package version>",
            "consumed_ex_types": ["Ex-3"],
            "contract_version": "<contracts schema version or 'unknown'>",
            "neo4j_status_enum_values": [...],
            "canonical_truth_layer": "iceberg",
        }

    Note: ``consumed_ex_types`` (NOT ``supported_ex_types``) — graph-
    engine is a CONSUMER of Ex-3 candidates, not a producer. The
    semantic distinction matters for assembly's cross-module lineage
    matrix (announcement / news / etc. PRODUCE Ex-1/2/3; graph-engine
    CONSUMES Ex-3 + WRITES GraphSnapshot / GraphImpactSnapshot back to
    Layer A).
    """

    def declare(self) -> dict[str, Any]:
        return {
            "module_id": "graph-engine",
            "module_version": _GRAPH_ENGINE_VERSION,
            "consumed_ex_types": list(_CONSUMED_EX_TYPES),
            "contract_version": self._safe_contract_version(),
            "neo4j_status_enum_values": self._safe_status_enum_values(),
            # CLAUDE.md §10 #1 truth-before-mirror invariant: Iceberg is
            # the canonical truth, Neo4j is the hot mirror only. Marker
            # is structural so assembly compat checks can verify it.
            "canonical_truth_layer": "iceberg",
        }

    @staticmethod
    def _safe_contract_version() -> str:
        # Returns the contracts package version with the canonical ``v``
        # prefix required by assembly's ``ContractVersion`` regex
        # (``^v\d+\.\d+\.\d+$``); see
        # ``assembly/src/assembly/contracts/primitives.py``. The bare
        # ``contracts.__version__`` (e.g. ``"0.1.3"``) would fail that
        # regex, blocking any registry contract_version upgrade.
        # Exception path keeps ``"unknown"`` as the explicit degraded-state
        # marker — callers (Stage 4 §4.1 registry upgrade) detect it and
        # leave the registry's ``contract_version`` at ``v0.0.0``.
        try:
            from contracts import __version__ as contracts_version

            return f"v{contracts_version}"
        except Exception:
            return "unknown"

    @staticmethod
    def _safe_status_enum_values() -> list[str]:
        try:
            from typing import get_args

            from graph_engine.models import Neo4jGraphStatus

            return sorted(
                get_args(
                    Neo4jGraphStatus.model_fields["graph_status"].annotation
                )
            )
        except Exception:
            return []


class _Cli:
    """Tiny graph-engine CLI for assembly's smoke probes; intentionally
    minimal to keep iron rule #2 boundary (no business logic in CLI).
    Supported argv:

    - ``["version"]`` — print version_declaration JSON to stdout, exit 0
    - ``["health", "--timeout-sec", "<float>"]`` — print health JSON,
      exit 0 on healthy/degraded, 1 on down
    - ``["smoke", "--profile-id", "<id>"]`` — print smoke JSON, exit 0
      on passed, 1 on failed
    """

    def invoke(self, argv: list[str]) -> int:
        if not argv:
            sys.stderr.write(
                "usage: graph-engine-cli "
                "{version|health|smoke} [args]\n"
            )
            return 2

        command = argv[0]
        rest = argv[1:]

        if command == "version":
            sys.stdout.write(
                json.dumps(version_declaration.declare()) + "\n"
            )
            return 0

        if command == "health":
            timeout_sec = self._parse_kw_float(
                rest, "--timeout-sec", default=1.0
            )
            if timeout_sec is None:
                return 2
            result = health_probe.check(timeout_sec=timeout_sec)
            sys.stdout.write(json.dumps(result) + "\n")
            return 0 if result["status"] in {_HEALTHY, _DEGRADED} else 1

        if command == "smoke":
            profile_id = self._parse_kw_str(
                rest, "--profile-id", default=None
            )
            if profile_id is None:
                sys.stderr.write("smoke requires --profile-id <id>\n")
                return 2
            result = smoke_hook.run(profile_id=profile_id)
            sys.stdout.write(json.dumps(result) + "\n")
            return 0 if result.get("passed") else 1

        sys.stderr.write(f"unknown command: {command!r}\n")
        return 2

    @staticmethod
    def _parse_kw_float(
        rest: list[str], flag: str, *, default: float
    ) -> float | None:
        if flag not in rest:
            return default
        idx = rest.index(flag)
        if idx + 1 >= len(rest):
            sys.stderr.write(f"{flag} requires a value\n")
            return None
        try:
            return float(rest[idx + 1])
        except ValueError:
            sys.stderr.write(
                f"{flag} must be a float; got {rest[idx + 1]!r}\n"
            )
            return None

    @staticmethod
    def _parse_kw_str(
        rest: list[str], flag: str, *, default: str | None
    ) -> str | None:
        if flag not in rest:
            return default
        idx = rest.index(flag)
        if idx + 1 >= len(rest):
            sys.stderr.write(f"{flag} requires a value\n")
            return None
        return rest[idx + 1]


# Module-level singleton instances — assembly registry references these
# by their lowercase attribute names (not the underscore-prefixed classes).
health_probe = _HealthProbe()
smoke_hook = _SmokeHook()
init_hook = _InitHook()
version_declaration = _VersionDeclaration()
cli = _Cli()


__all__ = [
    "cli",
    "health_probe",
    "init_hook",
    "smoke_hook",
    "version_declaration",
]
