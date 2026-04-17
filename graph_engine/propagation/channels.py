"""Shared propagation channel classification helpers."""

from __future__ import annotations

from graph_engine.schema.definitions import RelationshipType

DEFAULT_CHANNEL_BY_RELATIONSHIP_TYPE: dict[str, str] = {
    RelationshipType.SUPPLY_CHAIN.value: "fundamental",
    RelationshipType.OWNERSHIP.value: "fundamental",
    RelationshipType.INDUSTRY_CHAIN.value: "fundamental",
    RelationshipType.SECTOR_MEMBERSHIP.value: "fundamental",
    RelationshipType.EVENT_IMPACT.value: "event",
}


def effective_channel_expression(relationship_variable: str = "relationship") -> str:
    """Return the Cypher expression used to classify propagation relationships."""

    return (
        f"coalesce({relationship_variable}.propagation_channel, "
        f"{relationship_variable}.channel, "
        f"{relationship_variable}.impact_channel, "
        f"{_default_channel_case_expression(relationship_variable)})"
    )


def effective_channel_selector(
    channel: str,
    relationship_variable: str = "relationship",
) -> str:
    """Return the Cypher predicate for one propagation channel."""

    return f'{effective_channel_expression(relationship_variable)} = "{channel}"'


def _default_channel_case_expression(relationship_variable: str) -> str:
    case_parts = [
        f'WHEN "{relationship_type}" THEN "{channel}"'
        for relationship_type, channel in DEFAULT_CHANNEL_BY_RELATIONSHIP_TYPE.items()
    ]
    return f"CASE type({relationship_variable}) {' '.join(case_parts)} ELSE null END"
