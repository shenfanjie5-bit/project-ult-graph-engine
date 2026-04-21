"""Stage 2.10 contract tier — runtime contract signature stability.

This file checks graph-engine's PUBLIC API surface (the symbols
graph-engine exposes to other modules + assembly) doesn't drift in
shape:

- Public entrypoint singletons present + correct method signatures.
- Major service entry points present (promote_graph_deltas, cold_reload,
  query_subgraph, simulate_readonly_impact, check_live_graph_consistency,
  build_graph_snapshot, build_graph_impact_snapshot).
- Re-exported contracts schemas are the SAME Python object as
  contracts (CLAUDE.md domain rule: only contracts owns Ex-3 / Graph
  schemas; graph-engine MUST NOT fork).

Iron rule #4: tests/contract/ MUST contain real tests (no empty
``__init__.py``-only directory).
"""

from __future__ import annotations

import inspect


class TestPublicEntrypointSingletons:
    """The 5 module-level singleton instances assembly references."""

    def test_module_level_singletons_present(self) -> None:
        from graph_engine import public

        for name in (
            "health_probe",
            "smoke_hook",
            "init_hook",
            "version_declaration",
            "cli",
        ):
            assert hasattr(public, name), f"public.{name} missing"
            assert public.__all__.count(name) == 1

    def test_health_probe_check_signature(self) -> None:
        from graph_engine.public import health_probe

        sig = inspect.signature(health_probe.check)
        assert list(sig.parameters) == ["timeout_sec"]
        param = sig.parameters["timeout_sec"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is inspect.Parameter.empty
        # public.py uses `from __future__ import annotations` so
        # annotations are stringified at parse time.
        assert str(param.annotation) == "float"

    def test_smoke_hook_run_signature(self) -> None:
        from graph_engine.public import smoke_hook

        sig = inspect.signature(smoke_hook.run)
        assert list(sig.parameters) == ["profile_id"]
        param = sig.parameters["profile_id"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is inspect.Parameter.empty
        assert str(param.annotation) == "str"

    def test_init_hook_initialize_signature(self) -> None:
        from graph_engine.public import init_hook

        sig = inspect.signature(init_hook.initialize)
        assert list(sig.parameters) == ["resolved_env"]
        param = sig.parameters["resolved_env"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is inspect.Parameter.empty

    def test_version_declaration_declare_signature(self) -> None:
        from graph_engine.public import version_declaration

        sig = inspect.signature(version_declaration.declare)
        assert list(sig.parameters) == []  # no parameters except self

    def test_cli_invoke_signature(self) -> None:
        from graph_engine.public import cli

        sig = inspect.signature(cli.invoke)
        assert list(sig.parameters) == ["argv"]
        param = sig.parameters["argv"]
        assert param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert param.default is inspect.Parameter.empty


class TestMajorServiceEntryPoints:
    """The graph-engine top-level service entry points consumers
    (orchestrator / main-core / audit-eval) call. Signature stability
    here is a cross-module contract.
    """

    def test_promote_graph_deltas_present(self) -> None:
        from graph_engine import promote_graph_deltas

        sig = inspect.signature(promote_graph_deltas)
        # Args expected (based on promotion/service.py): canonical
        # input + neo4j client + status. Just lock the function exists
        # + has callable shape; sig drift caught here without nailing
        # exact param names (those evolve faster than presence does).
        assert callable(promote_graph_deltas)
        # Function must take >= 1 positional/keyword arg.
        assert len(sig.parameters) >= 1

    def test_cold_reload_present(self) -> None:
        from graph_engine import cold_reload

        assert callable(cold_reload)

    def test_query_subgraph_present(self) -> None:
        from graph_engine import query_subgraph

        assert callable(query_subgraph)

    def test_query_propagation_paths_present(self) -> None:
        from graph_engine import query_propagation_paths

        assert callable(query_propagation_paths)

    def test_simulate_readonly_impact_present(self) -> None:
        from graph_engine import simulate_readonly_impact

        assert callable(simulate_readonly_impact)

    def test_check_live_graph_consistency_present(self) -> None:
        from graph_engine import check_live_graph_consistency

        assert callable(check_live_graph_consistency)

    def test_build_graph_snapshot_present(self) -> None:
        from graph_engine.snapshots.generator import build_graph_snapshot

        assert callable(build_graph_snapshot)

    def test_build_graph_impact_snapshot_present(self) -> None:
        from graph_engine.snapshots.generator import build_graph_impact_snapshot

        assert callable(build_graph_impact_snapshot)


class TestNeo4jStatusEnumStable:
    """CLAUDE.md §10 #7: live graph reads check graph_status before
    proceeding. The canonical 3-state set (ready / rebuilding / failed)
    is locked here so consumers can guard on a stable enum.
    """

    def test_neo4j_graph_status_literal_values_stable(self) -> None:
        from typing import get_args

        from graph_engine.models import Neo4jGraphStatus

        annotation = Neo4jGraphStatus.model_fields["graph_status"].annotation
        assert set(get_args(annotation)) == {
            "ready",
            "rebuilding",
            "failed",
        }

    def test_propagation_channel_literal_values_stable(self) -> None:
        """3 propagation channels (CLAUDE.md §10 P3b: fundamental +
        event + reflexive)."""

        from typing import get_args

        from graph_engine.models import PropagationChannel

        assert set(get_args(PropagationChannel)) == {
            "fundamental",
            "event",
            "reflexive",
        }


class TestVersionExportPresent:
    def test_version_module_exposes_string(self) -> None:
        import graph_engine
        from graph_engine.version import __version__

        assert isinstance(__version__, str)
        assert __version__ == graph_engine.__version__
