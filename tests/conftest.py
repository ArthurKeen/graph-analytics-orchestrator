"""Pytest configuration and fixtures."""

import os
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_env_amp():
    """Mock environment variables for AMP deployment."""
    env_vars = {
        "ARANGO_ENDPOINT": "https://test.arangodb.cloud:8529",
        "ARANGO_USER": "testuser",
        "ARANGO_PASSWORD": "testpass",
        "ARANGO_DATABASE": "testdb",
        "ARANGO_VERIFY_SSL": "true",
        "GAE_DEPLOYMENT_MODE": "amp",
        "ARANGO_GRAPH_API_KEY_ID": "test-key-id",
        "ARANGO_GRAPH_API_KEY_SECRET": "test-key-secret",
        "ARANGO_GAE_PORT": "8829",
    }
    with patch.dict(os.environ, env_vars, clear=False):
        yield env_vars


@pytest.fixture
def mock_env_self_managed():
    """Mock environment variables for self-managed deployment."""
    env_vars = {
        "ARANGO_ENDPOINT": "https://test.local:8529",
        "ARANGO_USER": "root",
        "ARANGO_PASSWORD": "testpass",
        "ARANGO_DATABASE": "testdb",
        "ARANGO_VERIFY_SSL": "false",
        "GAE_DEPLOYMENT_MODE": "self_managed",
    }
    with patch.dict(os.environ, env_vars, clear=False):
        yield env_vars


@pytest.fixture
def mock_arango_client():
    """Mock ArangoDB client."""
    mock_client = MagicMock()
    mock_sys_db = MagicMock()
    mock_sys_db.version.return_value = {"version": "3.10.0"}
    mock_sys_db.databases.return_value = ["_system", "testdb"]
    mock_client.db.return_value = mock_sys_db
    return mock_client
