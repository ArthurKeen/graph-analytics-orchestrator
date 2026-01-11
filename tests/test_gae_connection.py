"""Tests for GAE connection module."""

import pytest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, timedelta
import time

from graph_analytics_orchestrator.gae_connection import (
    GAEManager,
    GenAIGAEConnection,
    get_gae_connection,
    GAEConnectionBase
)
from graph_analytics_orchestrator.config import DeploymentMode


class TestGAEManager:
    """Tests for GAEManager class."""
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    @patch('graph_analytics_orchestrator.gae_connection.subprocess.run')
    def test_init_amp_mode(self, mock_subprocess, mock_get_config, mock_env_amp):
        """Test initialization in AMP mode."""
        mock_config = {
            'deployment_mode': 'amp',
            'api_key_id': 'test-key-id',
            'api_key_secret': 'test-key-secret',
            'deployment_url': 'https://test.arangodb.cloud',
            'gae_port': '8829',
            'access_token': ''
        }
        mock_get_config.return_value = mock_config
        
        # Mock subprocess for token generation
        mock_result = MagicMock()
        mock_result.stdout = 'test-token-123'
        mock_subprocess.return_value = mock_result
        
        manager = GAEManager()
        
        assert manager.api_key_id == 'test-key-id'
        assert manager.deployment_url == 'https://test.arangodb.cloud'
        assert manager.gae_port == '8829'
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    def test_init_wrong_mode(self, mock_get_config):
        """Test initialization with wrong deployment mode."""
        mock_config = {
            'deployment_mode': 'self_managed',
        }
        mock_get_config.return_value = mock_config
        
        with pytest.raises(ValueError, match="requires AMP deployment mode"):
            GAEManager()
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    @patch('graph_analytics_orchestrator.gae_connection.subprocess.run')
    def test_refresh_token_success(self, mock_subprocess, mock_get_config, mock_env_amp):
        """Test successful token refresh."""
        mock_config = {
            'deployment_mode': 'amp',
            'api_key_id': 'test-key-id',
            'api_key_secret': 'test-key-secret',
            'deployment_url': 'https://test.arangodb.cloud',
            'gae_port': '8829',
            'access_token': ''
        }
        mock_get_config.return_value = mock_config
        
        mock_result = MagicMock()
        mock_result.stdout = 'new-token-456'
        mock_subprocess.return_value = mock_result
        
        manager = GAEManager()
        manager._refresh_token()
        
        assert manager.access_token == 'new-token-456'
        assert manager.token_created_at is not None
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    @patch('graph_analytics_orchestrator.gae_connection.subprocess.run')
    def test_refresh_token_invalid_chars(self, mock_subprocess, mock_get_config):
        """Test token refresh with invalid characters in API keys."""
        mock_config = {
            'deployment_mode': 'amp',
            'api_key_id': 'test;key',
            'api_key_secret': 'test-secret',
            'deployment_url': 'https://test.arangodb.cloud',
            'gae_port': '8829',
            'access_token': ''
        }
        mock_get_config.return_value = mock_config
        
        with pytest.raises(ValueError, match="invalid characters"):
            GAEManager()
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    def test_is_token_expired(self, mock_get_config, mock_env_amp):
        """Test token expiration check."""
        mock_config = {
            'deployment_mode': 'amp',
            'api_key_id': 'test-key-id',
            'api_key_secret': 'test-key-secret',
            'deployment_url': 'https://test.arangodb.cloud',
            'gae_port': '8829',
            'access_token': 'test-token'
        }
        mock_get_config.return_value = mock_config
        
        manager = GAEManager()
        
        # Token not expired (just created)
        assert manager._is_token_expired() is False
        
        # Make token old
        manager.token_created_at = datetime.now() - timedelta(hours=25)
        assert manager._is_token_expired() is True


class TestGenAIGAEConnection:
    """Tests for GenAIGAEConnection class."""
    
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_init_self_managed(self, mock_get_config, mock_env_self_managed):
        """Test initialization in self-managed mode."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        connection = GenAIGAEConnection()
        
        assert connection.db_endpoint == 'https://test.local:8529'
        assert connection.db_name == 'testdb'
        assert connection.db_user == 'root'
    
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_init_missing_credentials(self, mock_get_config):
        """Test initialization with missing credentials."""
        mock_config = {
            'endpoint': '',
            'password': '',
            'database': 'testdb',
            'user': 'root'
        }
        mock_get_config.return_value = mock_config
        
        with pytest.raises(ValueError, match="Database credentials are required"):
            GenAIGAEConnection()
    
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    @patch('graph_analytics_orchestrator.gae_connection.requests.post')
    def test_get_jwt_token_success(self, mock_post, mock_get_config, mock_env_self_managed):
        """Test successful JWT token retrieval."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        mock_response = MagicMock()
        mock_response.json.return_value = {'jwt': 'test-jwt-token'}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        connection = GenAIGAEConnection()
        token = connection._get_jwt_token()
        
        assert token == 'test-jwt-token'
        assert connection.jwt_token == 'test-jwt-token'
    
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_ensure_service_reuses_existing(self, mock_get_config, mock_env_self_managed):
        """Test ensure_service reuses an existing DEPLOYED service."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        connection = GenAIGAEConnection()
        
        # Mock list_services to return a deployed service
        connection.list_services = MagicMock(return_value=[
            {'serviceId': 's1', 'status': 'FAILED', 'type': 'gral'},
            {'serviceId': 's2', 'status': 'DEPLOYED', 'type': 'gral'}
        ])
        
        # Mock health checks
        connection._get_engine_url = MagicMock(return_value="https://test.local:8529/gral/s2")
        connection.get_engine_version = MagicMock(return_value={'version': '1.0'})
        connection.start_engine = MagicMock()  # Should not be called
        
        service_id = connection.ensure_service(reuse_existing=True)
        
        assert service_id == 's2'
        assert connection.engine_id == 's2'
        connection.start_engine.assert_not_called()
        connection.get_engine_version.assert_called_once()
        
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_ensure_service_starts_new_if_none_exist(self, mock_get_config, mock_env_self_managed):
        """Test ensure_service starts a new service if no suitable existing one is found."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        connection = GenAIGAEConnection()
        
        # Mock list_services to return no deployed services
        connection.list_services = MagicMock(return_value=[
            {'serviceId': 's1', 'status': 'FAILED', 'type': 'gral'}
        ])
        
        # Mock start_engine and health checks
        connection.start_engine = MagicMock(return_value='new-service-id')
        connection._get_engine_url = MagicMock(return_value="https://test.local:8529/gral/new-service-id")
        connection.get_engine_version = MagicMock(return_value={'version': '1.0'})
        
        service_id = connection.ensure_service(reuse_existing=True)
        
        assert service_id == 'new-service-id'
        connection.start_engine.assert_called_once()
        
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_ensure_service_starts_new_if_reuse_disabled(self, mock_get_config, mock_env_self_managed):
        """Test ensure_service starts a new service if reuse_existing=False."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        connection = GenAIGAEConnection()
        
        # Mock list_services (should not be called if reuse is False, or result ignored)
        connection.list_services = MagicMock(return_value=[
            {'serviceId': 's2', 'status': 'DEPLOYED', 'type': 'gral'}
        ])
        
        connection.start_engine = MagicMock(return_value='new-service-id')
        connection._get_engine_url = MagicMock(return_value="https://test.local:8529/gral/new-service-id")
        connection.get_engine_version = MagicMock(return_value={'version': '1.0'})
        
        service_id = connection.ensure_service(reuse_existing=False)
        
        assert service_id == 'new-service-id'
        connection.start_engine.assert_called_once()
        
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_ensure_service_waits_for_ready(self, mock_get_config, mock_env_self_managed):
        """Test ensure_service retries health check until ready."""
        mock_config = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        mock_get_config.return_value = mock_config
        
        connection = GenAIGAEConnection()
        connection.list_services = MagicMock(return_value=[
             {'serviceId': 's1', 'status': 'DEPLOYED', 'type': 'gral'}
        ])
        connection._get_engine_url = MagicMock(return_value="https://test.local:8529/gral/s1")
        
        # First call fails, second succeeds
        connection.get_engine_version = MagicMock(side_effect=[Exception("Not ready"), {'version': '1.0'}])
        
        with patch('time.sleep', return_value=None):  # Skip sleep delay
            service_id = connection.ensure_service(reuse_existing=True, max_retries=5)
            
        assert service_id == 's1'
        assert connection.get_engine_version.call_count == 2


class TestGetGAEConnection:
    """Tests for get_gae_connection factory function."""
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    def test_get_connection_amp(self, mock_get_config, mock_env_amp):
        """Test getting connection for AMP mode."""
        mock_config = {
            'deployment_mode': 'amp',
            'api_key_id': 'test-key-id',
            'api_key_secret': 'test-key-secret',
            'deployment_url': 'https://test.arangodb.cloud',
            'gae_port': '8829',
            'access_token': 'test-token'
        }
        mock_get_config.return_value = mock_config
        
        with patch('graph_analytics_orchestrator.gae_connection.GAEManager') as mock_manager:
            connection = get_gae_connection()
            mock_manager.assert_called_once()
    
    @patch('graph_analytics_orchestrator.gae_connection.get_gae_config')
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_get_connection_self_managed(self, mock_arango_config, mock_gae_config, mock_env_self_managed):
        """Test getting connection for self-managed mode."""
        mock_gae_config.return_value = {'deployment_mode': 'self_managed'}
        mock_arango_config.return_value = {
            'endpoint': 'https://test.local:8529',
            'user': 'root',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        with patch('graph_analytics_orchestrator.gae_connection.GenAIGAEConnection') as mock_connection:
            connection = get_gae_connection()
            mock_connection.assert_called_once()


class TestGenAIGAEConnectionNewMethods:
    """Tests for additional methods in GenAIGAEConnection."""
    
    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_list_services(self, mock_get_config, mock_env_self_managed):
        """Test list_services() method."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        gae.jwt_token = 'test-token'
        
        with patch('graph_analytics_orchestrator.gae_connection.requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                'services': [
                    {'serviceId': 'arangodb-gral-abc123', 'status': 'running'},
                    {'serviceId': 'arangodb-gral-def456', 'status': 'running'}
                ]
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response
            
            services = gae.list_services()
            
            assert len(services) == 2
            assert services[0]['serviceId'] == 'arangodb-gral-abc123'
            mock_post.assert_called_once()

    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_list_graphs(self, mock_get_config, mock_env_self_managed):
        """Test list_graphs() method."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        gae.engine_id = 'arangodb-gral-abc123'
        gae.jwt_token = 'test-token'
        
        with patch.object(gae, '_request') as mock_request:
            mock_request.return_value = [
                {'graph_id': '1', 'vertex_count': 100},
                {'graph_id': '2', 'vertex_count': 200}
            ]
            
            graphs = gae.list_graphs()
            
            assert len(graphs) == 2
            assert graphs[0]['graph_id'] == '1'
            mock_request.assert_called_once_with(
                method='GET',
                endpoint='v1/graphs',
                error_message='Failed to list graphs'
            )

    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_delete_graph(self, mock_get_config, mock_env_self_managed):
        """Test delete_graph() method."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        gae.engine_id = 'arangodb-gral-abc123'
        gae.jwt_token = 'test-token'
        
        with patch.object(gae, '_request') as mock_request:
            mock_request.return_value = {'status': 'deleted'}
            
            result = gae.delete_graph('1')
            
            assert result['status'] == 'deleted'
            mock_request.assert_called_once_with(
                method='DELETE',
                endpoint='v1/graphs/1',
                success_message='Graph 1 deleted successfully',
                error_message='Failed to delete graph 1'
            )

    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_wait_for_job_completed(self, mock_get_config, mock_env_self_managed):
        """Test wait_for_job() with completed job."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        gae.engine_id = 'arangodb-gral-abc123'
        
        with patch.object(gae, 'get_job') as mock_get_job:
            mock_get_job.return_value = {
                'job_id': 'job1',
                'state': 'completed',
                'status': {'state': 'completed'}
            }
            
            result = gae.wait_for_job('job1', poll_interval=0.1, max_wait=5)
            
            assert result['state'] == 'completed'
            assert mock_get_job.called

    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_test_connection_success(self, mock_get_config, mock_env_self_managed):
        """Test test_connection() with successful connection."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        
        with patch.object(gae, '_get_jwt_token'), \
             patch.object(gae, 'list_services', return_value=[]):
            
            result = gae.test_connection()
            
            assert result is True

    @patch('graph_analytics_orchestrator.gae_connection.get_arango_config')
    def test_load_graph_with_graph_name(self, mock_get_config, mock_env_self_managed):
        """Test load_graph() with graph_name parameter."""
        mock_config = {
            'endpoint': 'https://test.com:8529',
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        mock_get_config.return_value = mock_config
        
        gae = GenAIGAEConnection()
        gae.engine_id = 'arangodb-gral-abc123'
        gae.db_name = 'testdb'
        gae.jwt_token = 'test-token'
        
        with patch.object(gae, '_request') as mock_request:
            mock_request.return_value = {
                'job_id': 'job1',
                'graph_id': 'graph1'
            }
            
            result = gae.load_graph(graph_name='test_graph')
            
            assert result['job_id'] == 'job1'
            assert result['graph_id'] == 'graph1'
            assert result['id'] == 'job1'
            
            call_args = mock_request.call_args
            assert call_args[1]['payload']['graph_name'] == 'test_graph'
            assert call_args[1]['payload']['database'] == 'testdb'
