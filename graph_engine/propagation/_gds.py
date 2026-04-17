"""Shared GDS projection helpers for propagation runners."""

from __future__ import annotations

from typing import Any

from graph_engine.client import Neo4jClient


def drop_projection_if_exists(client: Neo4jClient, graph_name: str) -> None:
    rows = execute_gds_read(
        client,
        "CALL gds.graph.exists($graph_name) YIELD exists RETURN exists",
        {"graph_name": graph_name},
    )
    if rows and rows[0].get("exists") is True:
        execute_gds_write(
            client,
            "CALL gds.graph.drop($graph_name) YIELD graphName RETURN graphName",
            {"graph_name": graph_name},
        )


def execute_gds_read(
    client: Neo4jClient,
    query: str,
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        return client.execute_read(query, parameters)
    except Exception as exc:
        if _is_missing_gds_error(exc):
            raise RuntimeError("GDS plugin not available") from exc
        raise


def execute_gds_write(
    client: Neo4jClient,
    query: str,
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        return client.execute_write(query, parameters)
    except Exception as exc:
        if _is_missing_gds_error(exc):
            raise RuntimeError("GDS plugin not available") from exc
        raise


def _is_missing_gds_error(exc: Exception) -> bool:
    message = str(exc).lower()
    missing_markers = (
        "no procedure",
        "not registered",
        "unknown procedure",
        "procedure not found",
    )
    return "gds." in message and any(marker in message for marker in missing_markers)
