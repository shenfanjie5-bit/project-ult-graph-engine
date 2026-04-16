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
)
from graph_engine.schema import (
    NodeLabel,
    RelationshipType,
    SchemaManager,
    get_constraint_statements,
    get_index_statements,
)

__version__ = "0.1.0"

__all__ = [
    "CandidateGraphDelta",
    "GraphAssertionRecord",
    "GraphEdgeRecord",
    "GraphImpactSnapshot",
    "GraphNodeRecord",
    "GraphSnapshot",
    "Neo4jClient",
    "Neo4jConfig",
    "Neo4jGraphStatus",
    "NodeLabel",
    "RelationshipType",
    "SchemaManager",
    "__version__",
    "get_constraint_statements",
    "get_index_statements",
    "load_config_from_env",
]
