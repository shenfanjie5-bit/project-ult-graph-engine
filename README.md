# graph-engine

Graph promotion + propagation + Cold Reload + readonly subgraph
queries. Sole owner of:

- Layer A canonical graph truth → Neo4j live-graph promotion
- fundamental / event / reflexive propagation channels
- `graph_snapshot` + `graph_impact_snapshot` writeback
- `neo4j_graph_status` + Cold Reload closed loop
- readonly subgraph queries + readonly local impact simulation

Source of truth:

- `docs/graph-engine.project-doc.md`
- `CLAUDE.md` (project-specific guardrails — domain rules, status
  guard, blocker triggers)

## Position in the system

graph-engine is a **CONSUMER** of canonical Ex-3
(`CandidateGraphDelta`) re-exported from `contracts.schemas`. It does
NOT produce Ex-1/2/3 wire payloads (those come from announcement /
news / other subsystem producers). It does NOT exercise
`subsystem-sdk.SubmitClient` — that's the producer side.

M4.4 live bridge evidence was produced on 2026-05-03 from a real
`data-platform.candidate_queue` Ex-3 row through `PostgresCandidateDeltaReader`
into `promote_graph_deltas(..., sync_to_live_graph=False)`, yielding a
`PromotionPlan` with edge output. Evidence is recorded under assembly's
`reports/stabilization/m4-bridge-live-proof-20260503.md`.

## Holdings graph support plan

The next domain extension is holdings graph support, but it must stay within
the existing graph-engine boundary: consume canonical Ex-3 only, promote to
Layer A first, then sync Neo4j.

Implemented interface/type decisions:

- Do **not** add holdings-specific Pydantic models in `contracts`; producers
  continue to submit `Ex3CandidateGraphDelta`.
- Add only two graph relationship enum values for holdings:
  `RelationshipType.CO_HOLDING` and `RelationshipType.NORTHBOUND_HOLD`.
- `CO_HOLDING` means a fund/portfolio entity holds a listed-security entity;
  holdings producers may include `producer_context.graph_node_upserts` when
  a source or target endpoint node is not already present. `graph_node_upserts`
  are holdings-only (`CO_HOLDING` / `NORTHBOUND_HOLD`), endpoint-only, must
  not conflict with an existing endpoint mapping, and every upserted
  `canonical_entity_id` must already exist as a public entity-registry anchor
  confirmed by `EntityAnchorReader.existing_entity_ids`; graph-engine and
  producers do not create canonical entities in issue #56.
- `NORTHBOUND_HOLD` means northbound aggregate capital holds or changes a
  position in a listed-security entity.
- Represent top shareholder relationships with existing `OWNERSHIP`.
- Represent pledge status as `OWNERSHIP` properties such as `pledge_ratio`;
  do not add a separate `PLEDGE_STATUS` relationship.
- Issue #56 covers enum, planner, query, and propagation-channel support only.
  Issue #55 remains the later place for co-holding cluster or
  northbound-anomaly algorithms.

CLAUDE.md §10 domain invariants this module enforces by construction:

- **Truth Before Mirror** (#1): Iceberg is canonical truth; Neo4j is
  the hot mirror only. Cold Reload always rebuilds from Iceberg.
- **Promotion Before Propagation** (#2): Layer A write succeeds first,
  then Neo4j sync. Never write Neo4j without Layer A first.
- **Regime Is Read-only** (#3): `world_state_snapshot` is data input
  only; this module never imports `main_core.*`.
- **Readonly Simulation** (#4): event-driven local simulation reads
  the live graph, never writes it; never produces formal snapshots.
- **No Raw Text** (#6): no `lightrag` / `langchain` / `docling`; only
  contracted `CandidateGraphDelta`.
- **Status Guard** (#7): `Neo4jGraphStatus.graph_status` Literal is
  pinned to the canonical 3-state set
  `{"ready", "rebuilding", "failed"}`. Live-graph reads must check
  status before proceeding.

## Public API surface

`graph_engine.public` exposes 5 module-level singleton instances that
match the assembly Protocols
(`assembly.contracts.entrypoints`):

| singleton | method | purpose |
|---|---|---|
| `health_probe` | `check(*, timeout_sec: float)` | probe domain invariants without IO; returns `HealthResult`-conformant dict |
| `smoke_hook` | `run(*, profile_id: str)` | drive `promote_graph_deltas` with stub IO; one-shot end-to-end |
| `init_hook` | `initialize(*, resolved_env: dict[str, str])` | no-op (no global mutable state) |
| `version_declaration` | `declare()` | `{module_id, module_version, contract_version, compatible_contract_range, consumed_ex_types, neo4j_status_enum_values, canonical_truth_layer}` |
| `cli` | `invoke(argv: list[str])` | `version` / `health` / `smoke` subcommands |

## Health probe boundary (4-way `kind` taxonomy)

`_probe_contracts_re_exports()` distinguishes four structurally
distinct failure modes — the probe boundary went through codex review
#11 / #12 / #13 to land at this shape. The caller branches on `kind`
directly without parsing reason strings.

| `kind` | trigger | health status |
|---|---|---|
| `contracts_missing` | top-level `contracts` package absent (exact `No module named 'contracts'`) | `degraded` (offline-first dev only) |
| `contracts_broken` | `contracts.schemas` submodule (or any other `contracts.*` submodule, or transitive dep) missing while `contracts` itself is installed | `blocked` (structural break in installed package) |
| `graph_engine_models_broken` | any failure importing `graph_engine.models` (renamed/deleted internal namespace, broken in-repo dep) | `blocked` (in-repo regression) |
| `drift` | `graph_engine.models.{CandidateGraphDelta, GraphSnapshot, GraphImpactSnapshot}` is not the SAME Python object as `contracts.schemas.*` | `blocked` (forked schema — CLAUDE.md domain violation) |

The exact-match check on the FULL dotted missing-module name is
deliberate: a missing submodule of an installed package is a
structural regression, not a benign offline-first miss. Submodule
misses must NOT be silently downgraded.

The boundary is regression-protected by 5 tests in
`tests/contract/test_runtime_contract.py::TestContractsReExportProbeBoundary`
including `test_probe_tags_contracts_schemas_submodule_miss_as_contracts_broken`
which guards against re-introducing the codex #13 P2 conflation.

## Test baseline

```bash
.venv/bin/python -m pytest tests/contract/                    # public API + kind-tag boundary
.venv/bin/python -m pytest tests/smoke/                       # 5 singletons end-to-end
.venv/bin/python -m pytest tests/boundary/                    # CLAUDE.md red-line guards
.venv/bin/python -m pytest tests/regression/                  # shared-fixture cases
.venv/bin/python -m pytest tests/integration/                 # subgraph query / propagation
```

Focused #56 holdings evidence:

```bash
.venv/bin/python -m pytest tests/contract/test_contracts_alignment.py \
  tests/unit/test_schema.py tests/unit/test_promotion.py \
  tests/unit/test_query.py tests/unit/test_propagation_channels.py \
  tests/boundary/test_red_lines.py -q
```

Refresh global pass/skip counts only after running the full suite in the
current venv; this README intentionally avoids a stale full-suite count.

## Execution rules

1. Read `docs/graph-engine.project-doc.md` and the §10 red-line list
   in `CLAUDE.md` first.
2. Keep work inside this module; never reverse-import `main-core` /
   `data-platform` / `subsystem-*` / `audit-eval` / `orchestrator` /
   `assembly`. Cross-module data flows through `contracts.schemas`
   types.
3. Single Issue covers one capability — promotion / propagation /
   reload-status / query-simulation. Do not bundle.
4. Health probe + smoke hook MUST stay offline-first (no real Neo4j /
   Postgres connection at module load or probe time).

## Position in module set

| upstream | this module | downstream |
|---|---|---|
| contracts (Ex-3 schema), entity-registry (canonical IDs) | graph-engine (promotion + propagation) | main-core (snapshot reads), audit-eval (history), orchestrator (Phase 0/1) |
