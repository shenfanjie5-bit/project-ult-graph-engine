"""Infrastructure primitives for the graph-engine package."""

from graph_engine.client import Neo4jClient
from graph_engine.config import Neo4jConfig, load_config_from_env
from graph_engine.models import (
    CandidateGraphDelta,
    GraphAssertionRecord,
    GraphEdgeRecord,
    GraphImpactSnapshot,
    GraphNodeRecord,
    GraphSnapshot,
    Neo4jGraphStatus,
    PropagationContext,
    PropagationResult,
    PromotionPlan,
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
    StatusStore,
    check_live_graph_consistency,
    require_ready_status,
)

__version__ = "0.1.0"

__all__ = [
    "CandidateGraphDelta",
    "CanonicalSnapshotReader",
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
    "PromotionPlan",
    "PropagationContext",
    "PropagationResult",
    "RelationshipType",
    "SchemaManager",
    "StatusStore",
    "__version__",
    "check_live_graph_consistency",
    "get_constraint_statements",
    "get_index_statements",
    "load_config_from_env",
    "require_ready_status",
]
