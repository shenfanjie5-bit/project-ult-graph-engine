"""Persistent graph-domain records shared by graph-engine workflows."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Self, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PropagationChannel = Literal["fundamental", "event", "reflexive"]
_ALLOWED_PROPAGATION_CHANNELS: frozenset[str] = frozenset(get_args(PropagationChannel))


class GraphNodeRecord(BaseModel):
    """Canonical graph node persisted in Layer A."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    node_id: str = Field(min_length=1)
    canonical_entity_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class GraphEdgeRecord(BaseModel):
    """Canonical graph relationship persisted in Layer A."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    edge_id: str = Field(min_length=1)
    source_node_id: str = Field(min_length=1)
    target_node_id: str = Field(min_length=1)
    relationship_type: str = Field(min_length=1)
    properties: dict[str, Any]
    weight: float = Field(default=1.0, ge=0.0)
    created_at: datetime
    updated_at: datetime


class GraphAssertionRecord(BaseModel):
    """Canonical assertion and evidence record."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    assertion_id: str = Field(min_length=1)
    source_node_id: str = Field(min_length=1)
    target_node_id: str | None
    assertion_type: str = Field(min_length=1)
    evidence: dict[str, Any]
    confidence: float = Field(ge=0.0, le=1.0)
    created_at: datetime


class CandidateGraphDelta(BaseModel):
    """Validated or frozen candidate change waiting for graph promotion."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    delta_id: str = Field(min_length=1)
    cycle_id: str = Field(min_length=1)
    delta_type: Literal["node_add", "edge_add", "edge_update", "assertion_add"]
    source_entity_ids: list[str] = Field(min_length=1)
    payload: dict[str, Any]
    validation_status: Literal["validated", "rejected", "frozen"]


class PromotionPlan(BaseModel):
    """Runtime plan for promoting frozen graph deltas into the canonical graph."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    selection_ref: str = Field(min_length=1)
    delta_ids: list[str]
    node_records: list[GraphNodeRecord]
    edge_records: list[GraphEdgeRecord]
    assertion_records: list[GraphAssertionRecord]
    created_at: datetime


class PropagationContext(BaseModel):
    """Runtime context for one read-only propagation run."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    world_state_ref: str = Field(min_length=1)
    graph_generation_id: int = Field(ge=0)
    enabled_channels: list[PropagationChannel] = Field(min_length=1)
    channel_multipliers: dict[str, float]
    regime_multipliers: dict[str, float]
    decay_policy: dict[str, Any]
    regime_context: dict[str, Any]

    @field_validator("enabled_channels")
    @classmethod
    def _validate_enabled_channels(
        cls,
        enabled_channels: list[PropagationChannel],
    ) -> list[PropagationChannel]:
        if not enabled_channels:
            raise ValueError("enabled_channels must not be empty")
        unknown_channels = [
            channel for channel in enabled_channels if channel not in _ALLOWED_PROPAGATION_CHANNELS
        ]
        if unknown_channels:
            raise ValueError(f"unknown propagation channels: {unknown_channels}")
        if len(set(enabled_channels)) != len(enabled_channels):
            raise ValueError("enabled_channels must not contain duplicates")
        return enabled_channels


class PropagationResult(BaseModel):
    """Serializable propagation output used to build impact snapshots."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    graph_generation_id: int = Field(ge=0)
    activated_paths: list[dict[str, Any]]
    impacted_entities: list[dict[str, Any]]
    channel_breakdown: dict[str, Any]


class ReadonlySimulationRequest(BaseModel):
    """Runtime request for one read-only local impact simulation."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    world_state_ref: str = Field(min_length=1)
    graph_generation_id: int = Field(ge=0)
    depth: int = Field(default=1, ge=0)
    result_limit: int = Field(default=100, ge=1)


class GraphQueryResult(BaseModel):
    """Status-gated read-only live graph query result."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    graph_generation_id: int = Field(ge=0)
    subgraph_nodes: list[dict[str, Any]]
    subgraph_edges: list[dict[str, Any]]
    status: Literal["ready"]


class GraphSnapshot(BaseModel):
    """Structural snapshot of the promoted graph for a cycle."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    graph_generation_id: int = Field(ge=0)
    node_count: int = Field(ge=0)
    edge_count: int = Field(ge=0)
    key_label_counts: dict[str, int]
    checksum: str = Field(min_length=1)
    created_at: datetime


class ColdReloadPlan(BaseModel):
    """Runtime plan for rebuilding Neo4j from canonical graph truth."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    snapshot_ref: str = Field(min_length=1)
    cycle_id: str = Field(min_length=1)
    expected_snapshot: GraphSnapshot
    node_records: list[GraphNodeRecord]
    edge_records: list[GraphEdgeRecord]
    assertion_records: list[GraphAssertionRecord]
    projection_name: str = Field(min_length=1)
    created_at: datetime


class GraphImpactSnapshot(BaseModel):
    """Propagation impact snapshot emitted for downstream read-only consumers."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cycle_id: str = Field(min_length=1)
    impact_snapshot_id: str = Field(min_length=1)
    regime_context_ref: str = Field(min_length=1)
    activated_paths: list[dict[str, Any]]
    impacted_entities: list[dict[str, Any]]
    channel_breakdown: dict[str, Any]


class Neo4jGraphStatus(BaseModel):
    """Current live graph status stored outside Neo4j."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    graph_status: Literal["ready", "rebuilding", "failed"]
    graph_generation_id: int = Field(ge=0)
    node_count: int = Field(ge=0)
    edge_count: int = Field(ge=0)
    key_label_counts: dict[str, int]
    checksum: str = Field(min_length=1)
    last_verified_at: datetime | None
    last_reload_at: datetime | None
    writer_lock_token: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _writer_lock_requires_ready_status(self) -> Self:
        if self.writer_lock_token is not None and self.graph_status != "ready":
            raise ValueError("writer_lock_token requires graph_status='ready'")
        return self
