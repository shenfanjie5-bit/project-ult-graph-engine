"""Unit tests for ``build_graph_phase1_runtime_from_env`` (M2.3a-2).

The factory composes 7 dependencies; full env-driven construction requires
data-platform / main-core / contracts to be importable plus NEO4J_* /
DATABASE_URL / GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT env vars. These tests
exercise the override pattern (which is the test seam used in production
runtime composition tests) and the EnvironmentError fail-paths.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.providers import (
    GraphPhase1Service,
    build_graph_phase1_runtime_from_env,
)
from graph_engine.snapshots import FormalArtifactSnapshotWriter
from graph_engine.status import GraphStatusManager


NOW = datetime(2026, 4, 29, 12, 30, 45, tzinfo=timezone.utc)


def _live_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "changeme123")
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://postgres:changeme@localhost:5432/proj",
    )
    monkeypatch.setenv(
        "GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT", str(tmp_path / "phase1_artifacts")
    )


class _StaticCandidateReader:
    def read_candidate_graph_deltas(self, cycle_id: str, selection_ref: str) -> list[Any]:
        return []


class _StaticEntityReader:
    def canonical_entity_ids_for_node_ids(self, node_ids: set[str]) -> dict[str, str]:
        return {}

    def existing_entity_ids(self, entity_ids: set[str]) -> set[str]:
        return set()


class _CapturingCanonicalWriter:
    def __init__(self) -> None:
        self.plans: list[Any] = []

    def write_canonical_records(self, plan: Any) -> None:
        self.plans.append(plan)


class _StaticRegimeReader:
    def read_regime_context(self, world_state_ref: str) -> dict[str, Any]:
        return {
            "world_state_ref": world_state_ref,
            "channel_multipliers": {"fundamental": 1.0},
            "regime_multipliers": {"fundamental": 1.0},
            "decay_policy": {"default": 1.0},
        }


def test_build_runtime_from_env_returns_real_service_with_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Full override path: env supplies graph-engine internals (Neo4j +
    PostgreSQL + snapshot artifact root); cross-module adapters are passed
    explicitly. This is the test recipe production-runtime composition
    helpers are expected to follow when they want isolated unit coverage."""

    _live_env(monkeypatch, tmp_path)

    runtime = build_graph_phase1_runtime_from_env(
        candidate_reader=_StaticCandidateReader(),
        entity_reader=_StaticEntityReader(),
        canonical_writer=_CapturingCanonicalWriter(),
        regime_reader=_StaticRegimeReader(),
    )

    assert isinstance(runtime, GraphPhase1Service)
    assert isinstance(runtime.client, Neo4jClient)
    assert isinstance(runtime.status_manager, GraphStatusManager)
    assert isinstance(runtime.snapshot_writer, FormalArtifactSnapshotWriter)


def test_build_runtime_raises_when_neo4j_password_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _live_env(monkeypatch, tmp_path)
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)

    with pytest.raises(EnvironmentError, match="NEO4J_URI/USER/PASSWORD"):
        build_graph_phase1_runtime_from_env(
            candidate_reader=_StaticCandidateReader(),
            entity_reader=_StaticEntityReader(),
            canonical_writer=_CapturingCanonicalWriter(),
            regime_reader=_StaticRegimeReader(),
        )


def test_build_runtime_raises_when_database_url_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _live_env(monkeypatch, tmp_path)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(EnvironmentError, match="DATABASE_URL"):
        build_graph_phase1_runtime_from_env(
            candidate_reader=_StaticCandidateReader(),
            entity_reader=_StaticEntityReader(),
            canonical_writer=_CapturingCanonicalWriter(),
            regime_reader=_StaticRegimeReader(),
        )


def test_build_runtime_raises_when_snapshot_artifact_root_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _live_env(monkeypatch, tmp_path)
    monkeypatch.delenv("GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT", raising=False)

    with pytest.raises(EnvironmentError, match="SNAPSHOT_ARTIFACT_ROOT"):
        build_graph_phase1_runtime_from_env(
            candidate_reader=_StaticCandidateReader(),
            entity_reader=_StaticEntityReader(),
            canonical_writer=_CapturingCanonicalWriter(),
            regime_reader=_StaticRegimeReader(),
        )


def test_build_runtime_accepts_explicit_snapshot_writer_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``snapshot_writer=`` override bypasses GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT
    so tests can supply an in-process writer."""

    _live_env(monkeypatch, tmp_path)
    monkeypatch.delenv("GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT", raising=False)

    snapshot_writer = FormalArtifactSnapshotWriter(tmp_path / "explicit")
    runtime = build_graph_phase1_runtime_from_env(
        candidate_reader=_StaticCandidateReader(),
        entity_reader=_StaticEntityReader(),
        canonical_writer=_CapturingCanonicalWriter(),
        regime_reader=_StaticRegimeReader(),
        snapshot_writer=snapshot_writer,
    )

    assert runtime.snapshot_writer is snapshot_writer


def test_phase1_provider_get_resources_raises_when_runtime_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase 0 pattern: get_resources must raise rather than silently
    substitute a fail-closed runtime (M2.3a-2 architectural drift fix)."""

    from graph_engine.providers.phase1 import GraphPhase1AssetFactoryProvider

    provider = GraphPhase1AssetFactoryProvider(runtime=None)
    with pytest.raises(RuntimeError, match="not configured"):
        provider.get_resources()


# ---------------------------------------------------------------------------
# Cross-module lazy-import path tests (M2.3a-2 review fold-in)
# ---------------------------------------------------------------------------


def _install_fake_data_platform(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Inject a minimal fake ``data_platform.cycle.graph_phase1_adapters``
    module into sys.modules so the lazy import path inside
    ``build_graph_phase1_runtime_from_env`` succeeds without requiring
    data-platform to be pip-installed in this venv. Returns the recorded
    factory call sites for assertions."""

    import sys
    import types

    calls: dict[str, Any] = {
        "PostgresCandidateDeltaReader.from_env": False,
        "IcebergEntityAnchorReader.from_env": False,
        "IcebergCanonicalGraphWriter.from_env": False,
    }

    class _FakeCandidateReader:
        def read_candidate_graph_deltas(self, cycle_id: str, selection_ref: str):
            return []

        @classmethod
        def from_env(cls):
            calls["PostgresCandidateDeltaReader.from_env"] = True
            return cls()

    class _FakeEntityReader:
        def canonical_entity_ids_for_node_ids(self, node_ids):
            return {}

        def existing_entity_ids(self, entity_ids):
            return set()

        @classmethod
        def from_env(cls):
            calls["IcebergEntityAnchorReader.from_env"] = True
            return cls()

    class _FakeCanonicalWriter:
        def write_canonical_records(self, plan):
            pass

        @classmethod
        def from_env(cls):
            calls["IcebergCanonicalGraphWriter.from_env"] = True
            return cls()

    fake_module = types.ModuleType(
        "data_platform.cycle.graph_phase1_adapters"
    )
    fake_module.PostgresCandidateDeltaReader = _FakeCandidateReader  # type: ignore[attr-defined]
    fake_module.IcebergEntityAnchorReader = _FakeEntityReader  # type: ignore[attr-defined]
    # M2.6 follow-up #1: real writer name (StubCanonicalGraphWriter alias
    # also available for backwards compat but we exercise the canonical name).
    fake_module.IcebergCanonicalGraphWriter = _FakeCanonicalWriter  # type: ignore[attr-defined]

    fake_data_platform = types.ModuleType("data_platform")
    fake_data_platform_cycle = types.ModuleType("data_platform.cycle")
    monkeypatch.setitem(sys.modules, "data_platform", fake_data_platform)
    monkeypatch.setitem(sys.modules, "data_platform.cycle", fake_data_platform_cycle)
    monkeypatch.setitem(
        sys.modules,
        "data_platform.cycle.graph_phase1_adapters",
        fake_module,
    )
    return calls


def _install_fake_main_core(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Inject a minimal fake ``main_core.adapters.graph_engine`` module."""

    import sys
    import types

    calls: dict[str, Any] = {"build_regime_context_reader_from_env": False}

    class _FakeRegimeReader:
        def read_regime_context(self, world_state_ref: str):
            return {
                "world_state_ref": world_state_ref,
                "channel_multipliers": {"fundamental": 1.0},
                "regime_multipliers": {"fundamental": 1.0},
                "decay_policy": {"default": 1.0},
            }

    def _fake_factory():
        calls["build_regime_context_reader_from_env"] = True
        return _FakeRegimeReader()

    fake_module = types.ModuleType("main_core.adapters.graph_engine")
    fake_module.build_regime_context_reader_from_env = _fake_factory  # type: ignore[attr-defined]

    fake_main_core = types.ModuleType("main_core")
    fake_main_core_adapters = types.ModuleType("main_core.adapters")
    monkeypatch.setitem(sys.modules, "main_core", fake_main_core)
    monkeypatch.setitem(sys.modules, "main_core.adapters", fake_main_core_adapters)
    monkeypatch.setitem(
        sys.modules,
        "main_core.adapters.graph_engine",
        fake_module,
    )
    return calls


def test_build_runtime_from_env_lazy_imports_cross_module_adapters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end: with NO overrides + fake cross-module modules in sys.modules
    + live env, the factory executes the full lazy-import path and composes a
    real ``GraphPhase1Service``. This is the closest unit-test approximation
    of the M2.6 deployment-env wiring."""

    _live_env(monkeypatch, tmp_path)
    dp_calls = _install_fake_data_platform(monkeypatch)
    mc_calls = _install_fake_main_core(monkeypatch)

    runtime = build_graph_phase1_runtime_from_env()

    assert isinstance(runtime, GraphPhase1Service)
    # Verify each lazy-import path actually fired.
    assert dp_calls["PostgresCandidateDeltaReader.from_env"] is True
    assert dp_calls["IcebergEntityAnchorReader.from_env"] is True
    assert dp_calls["IcebergCanonicalGraphWriter.from_env"] is True
    assert mc_calls["build_regime_context_reader_from_env"] is True


def test_build_runtime_raises_when_data_platform_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ImportError on the data-platform lazy import surfaces as
    EnvironmentError so the orchestrator's single ``except EnvironmentError``
    catches it correctly."""

    import sys

    _live_env(monkeypatch, tmp_path)
    _install_fake_main_core(monkeypatch)
    # Force ImportError on data_platform by removing any cached module.
    for module_name in list(sys.modules):
        if module_name == "data_platform" or module_name.startswith("data_platform."):
            monkeypatch.delitem(sys.modules, module_name)
    # Block future imports by setting sys.modules[name] = None (Python's
    # documented "permanently fail import" sentinel).
    monkeypatch.setitem(sys.modules, "data_platform", None)

    with pytest.raises(EnvironmentError, match="data-platform"):
        build_graph_phase1_runtime_from_env()


def test_build_runtime_raises_when_main_core_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ImportError on the main-core lazy import surfaces as EnvironmentError."""

    import sys

    _live_env(monkeypatch, tmp_path)
    _install_fake_data_platform(monkeypatch)
    for module_name in list(sys.modules):
        if module_name == "main_core" or module_name.startswith("main_core."):
            monkeypatch.delitem(sys.modules, module_name)
    monkeypatch.setitem(sys.modules, "main_core", None)

    with pytest.raises(EnvironmentError, match="main-core"):
        build_graph_phase1_runtime_from_env()


def test_build_fail_closed_graph_phase1_provider_returns_typed_provider() -> None:
    """Public factory replaces the M2.3a-2 private import workaround in
    orchestrator's _build_fail_closed_graph_phase1_provider helper."""

    from graph_engine.providers import build_fail_closed_graph_phase1_provider

    provider = build_fail_closed_graph_phase1_provider()

    # The factory returns a provider whose runtime is the fail-closed stub;
    # asset-evaluation will raise rather than silently no-op.
    assert provider.__class__.__name__ == "GraphPhase1AssetFactoryProvider"
    assert provider.runtime.__class__.__name__ == "_FailClosedGraphPhase1Runtime"
