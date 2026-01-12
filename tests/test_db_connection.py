"""Tests for database connection module."""

import pytest
from unittest.mock import patch, MagicMock, Mock

from graph_analytics_orchestrator.db_connection import (
    get_db_connection,
    get_connection_info,
)
from graph_analytics_orchestrator.config import ArangoConfig


class TestGetDBConnection:
    """Tests for get_db_connection function."""

    @patch("graph_analytics_orchestrator.db_connection.ArangoClient")
    @patch("graph_analytics_orchestrator.db_connection.get_arango_config")
    def test_successful_connection(
        self, mock_get_config, mock_client_class, mock_env_amp
    ):
        """Test successful database connection."""
        # Setup mocks
        mock_config = {
            "endpoint": "https://test:8529",
            "user": "testuser",
            "password": "testpass",
            "database": "testdb",
            "verify_ssl": "true",
        }
        mock_get_config.return_value = mock_config

        mock_client = MagicMock()
        mock_sys_db = MagicMock()
        mock_sys_db.version.return_value = {"version": "3.10.0"}
        mock_sys_db.databases.return_value = ["_system", "testdb"]
        mock_client.db.return_value = mock_sys_db
        mock_client_class.return_value = mock_client

        # Test
        db = get_db_connection()

        # Verify
        assert db is not None
        mock_client_class.assert_called_once_with(hosts="https://test:8529")
        assert mock_client.db.call_count == 2  # _system and testdb

    @patch("graph_analytics_orchestrator.db_connection.ArangoClient")
    @patch("graph_analytics_orchestrator.db_connection.get_arango_config")
    def test_connection_failure(self, mock_get_config, mock_client_class, mock_env_amp):
        """Test connection failure handling."""
        mock_config = {
            "endpoint": "https://test:8529",
            "user": "testuser",
            "password": "testpass",
            "database": "testdb",
            "verify_ssl": "true",
        }
        mock_get_config.return_value = mock_config

        mock_client = MagicMock()
        mock_sys_db = MagicMock()
        mock_sys_db.version.side_effect = Exception("Connection failed")
        mock_client.db.return_value = mock_sys_db
        mock_client_class.return_value = mock_client

        # Test
        with pytest.raises(ConnectionError, match="Failed to connect"):
            get_db_connection()

    @patch("graph_analytics_orchestrator.db_connection.ArangoClient")
    @patch("graph_analytics_orchestrator.db_connection.get_arango_config")
    def test_database_not_exists(
        self, mock_get_config, mock_client_class, mock_env_amp
    ):
        """Test when database doesn't exist."""
        mock_config = {
            "endpoint": "https://test:8529",
            "user": "testuser",
            "password": "testpass",
            "database": "nonexistent",
            "verify_ssl": "true",
        }
        mock_get_config.return_value = mock_config

        mock_client = MagicMock()
        mock_sys_db = MagicMock()
        mock_sys_db.version.return_value = {"version": "3.10.0"}
        mock_sys_db.databases.return_value = ["_system", "testdb"]
        mock_client.db.return_value = mock_sys_db
        mock_client_class.return_value = mock_client

        # Test
        with pytest.raises(ValueError, match="does not exist"):
            get_db_connection()

    @patch("graph_analytics_orchestrator.db_connection.get_arango_config")
    def test_password_masked_in_error(self, mock_get_config, mock_env_amp):
        """Test that password is masked in error messages."""
        mock_config = {
            "endpoint": "https://test:8529",
            "user": "testuser",
            "password": "secretpassword",
            "database": "testdb",
            "verify_ssl": "true",
        }
        mock_get_config.return_value = mock_config

        with patch(
            "graph_analytics_orchestrator.db_connection.ArangoClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_sys_db = MagicMock()
            mock_sys_db.version.side_effect = Exception(
                "Password secretpassword failed"
            )
            mock_client.db.return_value = mock_sys_db
            mock_client_class.return_value = mock_client

            # Test
            with pytest.raises(ConnectionError) as exc_info:
                get_db_connection()

            # Verify password is masked
            assert "secretpassword" not in str(exc_info.value)
            assert "***MASKED***" in str(exc_info.value)


class TestGetConnectionInfo:
    """Tests for get_connection_info function."""

    @patch("graph_analytics_orchestrator.db_connection.get_arango_config")
    def test_get_connection_info(self, mock_get_config, mock_env_amp):
        """Test getting connection info."""
        mock_config = {
            "endpoint": "https://test:8529",
            "user": "testuser",
            "database": "testdb",
            "verify_ssl": "true",
        }
        mock_get_config.return_value = mock_config

        info = get_connection_info()

        assert info["endpoint"] == "https://test:8529"
        assert info["database"] == "testdb"
        assert info["user"] == "testuser"
        assert info["verify_ssl"] == "true"
