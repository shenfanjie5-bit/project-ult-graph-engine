from __future__ import annotations

from unittest.mock import MagicMock

from graph_engine.schema.definitions import NodeLabel, RelationshipType
from graph_engine.schema.indexes import get_constraint_statements, get_index_statements
from graph_engine.schema.manager import SchemaManager


def test_node_label_enum_covers_core_labels() -> None:
    assert {label.name for label in NodeLabel} == {
        "ENTITY",
        "SECTOR",
        "INDUSTRY",
        "REGION",
        "EVENT",
        "ASSERTION",
    }


def test_relationship_type_enum_covers_core_relationships() -> None:
    assert {relationship.name for relationship in RelationshipType} == {
        "SUPPLY_CHAIN",
        "OWNERSHIP",
        "INDUSTRY_CHAIN",
        "SECTOR_MEMBERSHIP",
        "EVENT_IMPACT",
        "ASSERTION_LINK",
    }


def test_get_index_statements_returns_idempotent_cypher() -> None:
    statements = get_index_statements()

    assert len(statements) >= 4
    assert all(statement.startswith("CREATE INDEX ") for statement in statements)
    assert all(" IF NOT EXISTS " in statement for statement in statements)
    assert all(" ON " in statement for statement in statements)


def test_get_constraint_statements_returns_idempotent_cypher() -> None:
    statements = get_constraint_statements()

    assert len(statements) >= 2
    assert all(statement.startswith("CREATE CONSTRAINT ") for statement in statements)
    assert all(" IF NOT EXISTS " in statement for statement in statements)
    assert any("node_id IS UNIQUE" in statement for statement in statements)


def test_schema_manager_apply_schema_executes_constraints_then_indexes(
    mock_neo4j_client: MagicMock,
) -> None:
    manager = SchemaManager(mock_neo4j_client)

    manager.apply_schema()

    executed = [call.args[0] for call in mock_neo4j_client.execute_write.call_args_list]
    assert executed == [*get_constraint_statements(), *get_index_statements()]


def test_schema_manager_verify_schema_returns_true_when_required_names_exist(
    mock_neo4j_client: MagicMock,
) -> None:
    constraint_names = [
        statement.split()[2]
        for statement in get_constraint_statements()
    ]
    index_names = [statement.split()[2] for statement in get_index_statements()]
    mock_neo4j_client.execute_read.side_effect = [
        [{"names": constraint_names}],
        [{"names": index_names}],
    ]

    assert SchemaManager(mock_neo4j_client).verify_schema() is True


def test_schema_manager_verify_schema_returns_false_when_required_name_is_missing(
    mock_neo4j_client: MagicMock,
) -> None:
    mock_neo4j_client.execute_read.side_effect = [
        [{"names": []}],
        [{"names": []}],
    ]

    assert SchemaManager(mock_neo4j_client).verify_schema() is False


def test_schema_manager_verify_schema_returns_false_on_read_error(
    mock_neo4j_client: MagicMock,
) -> None:
    mock_neo4j_client.execute_read.side_effect = RuntimeError("unavailable")

    assert SchemaManager(mock_neo4j_client).verify_schema() is False


def test_schema_manager_drop_all_deletes_data_and_schema(
    mock_neo4j_client: MagicMock,
) -> None:
    mock_neo4j_client.execute_read.side_effect = [
        [{"name": "constraint_one"}],
        [{"name": "index_one", "type": "RANGE"}, {"name": "lookup", "type": "LOOKUP"}],
    ]

    SchemaManager(mock_neo4j_client).drop_all()

    executed = [call.args[0] for call in mock_neo4j_client.execute_write.call_args_list]
    assert executed == [
        "MATCH (n) DETACH DELETE n",
        "DROP CONSTRAINT `constraint_one` IF EXISTS",
        "DROP INDEX `index_one` IF EXISTS",
    ]
