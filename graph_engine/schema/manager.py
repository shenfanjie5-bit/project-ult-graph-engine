"""Schema lifecycle operations for the Neo4j live graph."""

from __future__ import annotations

import re
from typing import Any

from graph_engine.client import Neo4jClient
from graph_engine.schema.indexes import get_constraint_statements, get_index_statements

_SCHEMA_NAME_PATTERN = re.compile(r"^CREATE\s+(?:INDEX|CONSTRAINT)\s+(\S+)\s+", re.IGNORECASE)


class SchemaManager:
    """Apply, verify, and clear the Neo4j schema used by graph-engine."""

    def __init__(self, client: Neo4jClient) -> None:
        self.client = client

    def apply_schema(self) -> None:
        """Create all required constraints and indexes using idempotent DDL."""

        for statement in [*get_constraint_statements(), *get_index_statements()]:
            self.client.execute_write(statement)

    def verify_schema(self) -> bool:
        """Check that every required index and constraint name is present."""

        try:
            constraint_rows = self.client.execute_read(
                "SHOW CONSTRAINTS YIELD name RETURN collect(name) AS names"
            )
            index_rows = self.client.execute_read("SHOW INDEXES YIELD name RETURN collect(name) AS names")
        except Exception:  # noqa: BLE001 - verification should fail closed.
            return False

        actual_constraints = _extract_schema_names(constraint_rows)
        actual_indexes = _extract_schema_names(index_rows)
        required_constraints = {_extract_statement_name(statement) for statement in get_constraint_statements()}
        required_indexes = {_extract_statement_name(statement) for statement in get_index_statements()}

        return required_constraints <= actual_constraints and required_indexes <= actual_indexes

    def drop_all(self) -> None:
        """Delete all graph data and drop user-created schema objects."""

        self.client.execute_write("MATCH (n) DETACH DELETE n")

        constraint_rows = self.client.execute_read("SHOW CONSTRAINTS YIELD name RETURN name")
        for constraint_name in _extract_schema_names(constraint_rows):
            self.client.execute_write(
                f"DROP CONSTRAINT {_quote_identifier(constraint_name)} IF EXISTS"
            )

        index_rows = self.client.execute_read(
            "SHOW INDEXES YIELD name, type RETURN name, type"
        )
        for row in index_rows:
            index_name = row.get("name")
            index_type = row.get("type")
            if isinstance(index_name, str) and index_type != "LOOKUP":
                self.client.execute_write(f"DROP INDEX {_quote_identifier(index_name)} IF EXISTS")


def _extract_schema_names(rows: list[dict[str, Any]]) -> set[str]:
    names: set[str] = set()
    for row in rows:
        name = row.get("name")
        if isinstance(name, str):
            names.add(name)

        collected_names = row.get("names")
        if isinstance(collected_names, list):
            names.update(item for item in collected_names if isinstance(item, str))
    return names


def _extract_statement_name(statement: str) -> str:
    match = _SCHEMA_NAME_PATTERN.match(statement)
    if match is None:
        raise ValueError(f"schema statement does not include a name: {statement}")
    return match.group(1)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"
