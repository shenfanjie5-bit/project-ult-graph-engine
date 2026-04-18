"""Protocol boundaries for graph promotion dependencies."""

from __future__ import annotations

from typing import Protocol

from contracts.schemas import CandidateGraphDelta

from graph_engine.models import PromotionPlan


class CandidateDeltaReader(Protocol):
    """Read contract candidate graph deltas from the upstream input surface."""

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list[CandidateGraphDelta]:
        """Return candidate graph deltas selected for promotion."""


class EntityAnchorReader(Protocol):
    """Read canonical entity anchors owned by entity-registry."""

    def existing_entity_ids(self, entity_ids: set[str]) -> set[str]:
        """Return the subset of entity ids that already exist as canonical anchors."""


class CanonicalWriter(Protocol):
    """Persist promoted records into Layer A canonical storage."""

    def write_canonical_records(self, plan: PromotionPlan) -> None:
        """Write canonical graph records for a promotion plan."""
