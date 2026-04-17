"""Infrastructure primitives for the graph-engine package."""

from graph_engine.client import Neo4jClient
from graph_engine.config import Neo4jConfig, load_config_from_env
from graph_engine.models import (
    CandidateGraphDelta,
    ColdReloadPlan,
    GraphAssertionRecord,
    GraphEdgeRecord,
    GraphImpactSnapshot,
    GraphNodeRecord,
    GraphSnapshot,
    Neo4jGraphStatus,
    PropagationContext,
    PropagationResult,
    PromotionPlan,
    ReadonlySimulationRequest,
)
from graph_engine.query import query_subgraph, simulate_readonly_impact
from graph_engine.reload import (
    CanonicalReader,
    ColdReloadTimeoutError,
    cold_reload,
    rebuild_gds_projection,
)
from graph_engine.schema import (
    NodeLabel,
    RelationshipType,
    SchemaManager,
    get_constraint_statements,
    get_index_statements,
)
from graph_engine.status import (
    CanonicalSnapshotReader,
    GraphStatusManager,
    PostgreSQLStatusStore,
    PostgresStatusStore,
    StatusStore,
    check_live_graph_consistency,
    require_ready_read,
    require_ready_status,
)

__version__ = "0.1.0"

__all__ = [
    "CandidateGraphDelta",
    "CanonicalReader",
    "CanonicalSnapshotReader",
    "ColdReloadPlan",
    "ColdReloadTimeoutError",
    "GraphAssertionRecord",
    "GraphEdgeRecord",
    "GraphImpactSnapshot",
    "GraphNodeRecord",
    "GraphSnapshot",
    "GraphStatusManager",
    "Neo4jClient",
    "Neo4jConfig",
    "Neo4jGraphStatus",
    "NodeLabel",
    "PostgreSQLStatusStore",
    "PostgresStatusStore",
    "PromotionPlan",
    "PropagationContext",
    "PropagationResult",
    "ReadonlySimulationRequest",
    "RelationshipType",
    "SchemaManager",
    "StatusStore",
    "__version__",
    "check_live_graph_consistency",
    "cold_reload",
    "get_constraint_statements",
    "get_index_statements",
    "load_config_from_env",
    "query_subgraph",
    "rebuild_gds_projection",
    "require_ready_read",
    "require_ready_status",
    "simulate_readonly_impact",
]
