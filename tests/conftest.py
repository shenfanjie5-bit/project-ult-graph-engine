from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from graph_engine.client import Neo4jClient
from graph_engine.config import Neo4jConfig


@pytest.fixture
def neo4j_config() -> Neo4jConfig:
    return Neo4jConfig(password="test-password")


@pytest.fixture
def mock_neo4j_client() -> MagicMock:
    client = MagicMock(spec=Neo4jClient)
    client.execute_read.return_value = []
    client.execute_write.return_value = []
    return client
