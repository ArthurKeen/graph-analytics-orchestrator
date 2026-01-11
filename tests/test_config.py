"""Tests for configuration module."""

import os
import pytest
from unittest.mock import patch

from graph_analytics_orchestrator.config import (
    ArangoConfig,
    GAEConfig,
    DeploymentMode,
    get_arango_config,
    get_gae_config,
    parse_ssl_verify,
    _extract_deployment_url,
    get_required_env,
    validate_required_env_vars
)


class TestArangoConfig:
    """Tests for ArangoConfig class."""
    
    def test_init_with_all_vars(self, mock_env_amp):
        """Test initialization with all environment variables."""
        config = ArangoConfig()
        
        assert config.endpoint == 'https://test.arangodb.cloud:8529'
        assert config.user == 'testuser'
        assert config.password == 'testpass'
        assert config.database == 'testdb'
        assert config.verify_ssl is True
        assert config.timeout == 300
    
    def test_init_with_defaults(self):
        """Test initialization with default values."""
        env_vars = {
            'ARANGO_ENDPOINT': 'https://test:8529',
            'ARANGO_PASSWORD': 'pass',
            'ARANGO_DATABASE': 'db',
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = ArangoConfig()
            assert config.user == 'root'  # Default
            assert config.verify_ssl is True  # Default
            assert config.timeout == 300  # Default
    
    def test_init_missing_required(self):
        """Test initialization with missing required variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="ARANGO_ENDPOINT"):
                ArangoConfig()
    
    def test_to_dict_masks_password(self, mock_env_amp):
        """Test that to_dict masks password by default."""
        config = ArangoConfig()
        result = config.to_dict()
        
        assert result['password'] == '***MASKED***'
        assert result['endpoint'] == 'https://test.arangodb.cloud:8529'
        assert result['user'] == 'testuser'
    
    def test_to_dict_shows_password_when_requested(self, mock_env_amp):
        """Test that to_dict can show password when mask_secrets=False."""
        config = ArangoConfig()
        result = config.to_dict(mask_secrets=False)
        
        assert result['password'] == 'testpass'


class TestGAEConfig:
    """Tests for GAEConfig class."""
    
    def test_init_amp_mode(self, mock_env_amp):
        """Test initialization in AMP mode."""
        config = GAEConfig()
        
        assert config.deployment_mode == DeploymentMode.AMP
        assert config.api_key_id == 'test-key-id'
        assert config.api_key_secret == 'test-key-secret'
        assert config.deployment_url == 'https://test.arangodb.cloud'
        assert config.gae_port == 8829
    
    def test_init_self_managed_mode(self, mock_env_self_managed):
        """Test initialization in self-managed mode."""
        config = GAEConfig()
        
        assert config.deployment_mode == DeploymentMode.SELF_MANAGED
    
    def test_init_invalid_mode(self):
        """Test initialization with invalid deployment mode."""
        env_vars = {
            'ARANGO_ENDPOINT': 'https://test:8529',
            'ARANGO_PASSWORD': 'pass',
            'ARANGO_DATABASE': 'db',
            'GAE_DEPLOYMENT_MODE': 'invalid',
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(ValueError, match="Invalid GAE_DEPLOYMENT_MODE"):
                GAEConfig()
    
    def test_init_amp_missing_keys(self):
        """Test initialization in AMP mode with missing API keys."""
        env_vars = {
            'ARANGO_ENDPOINT': 'https://test:8529',
            'ARANGO_PASSWORD': 'pass',
            'ARANGO_DATABASE': 'db',
            'GAE_DEPLOYMENT_MODE': 'amp',
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(ValueError, match="ARANGO_GRAPH_API_KEY_ID"):
                GAEConfig()
    
    def test_to_dict_masks_secrets(self, mock_env_amp):
        """Test that to_dict masks secrets by default."""
        config = GAEConfig()
        result = config.to_dict()
        
        assert result['api_key_id'] == '***MASKED***'
        assert result['api_key_secret'] == '***MASKED***'
        assert result['deployment_url'] == 'https://test.arangodb.cloud'
    
    def test_to_dict_shows_secrets_when_requested(self, mock_env_amp):
        """Test that to_dict can show secrets when mask_secrets=False."""
        config = GAEConfig()
        result = config.to_dict(mask_secrets=False)
        
        assert result['api_key_id'] == 'test-key-id'
        assert result['api_key_secret'] == 'test-key-secret'


class TestHelperFunctions:
    """Tests for helper functions."""
    
    def test_extract_deployment_url_with_port(self):
        """Test extracting deployment URL from endpoint with port."""
        url = _extract_deployment_url('https://test.arangodb.cloud:8529')
        assert url == 'https://test.arangodb.cloud'
    
    def test_extract_deployment_url_without_port(self):
        """Test extracting deployment URL from endpoint without port."""
        url = _extract_deployment_url('https://test.arangodb.cloud')
        assert url == 'https://test.arangodb.cloud'
    
    def test_extract_deployment_url_with_path(self):
        """Test extracting deployment URL from endpoint with path."""
        url = _extract_deployment_url('https://test.arangodb.cloud:8529/path')
        assert url == 'https://test.arangodb.cloud'
    
    def test_parse_ssl_verify_true(self):
        """Test parsing SSL verify as True."""
        assert parse_ssl_verify('true') is True
        assert parse_ssl_verify('True') is True
        assert parse_ssl_verify('1') is True
        assert parse_ssl_verify('yes') is True
        assert parse_ssl_verify('on') is True
    
    def test_parse_ssl_verify_false(self):
        """Test parsing SSL verify as False."""
        assert parse_ssl_verify('false') is False
        assert parse_ssl_verify('False') is False
        assert parse_ssl_verify('0') is False
        assert parse_ssl_verify('no') is False
        assert parse_ssl_verify('off') is False
    
    def test_parse_ssl_verify_with_boolean(self):
        """Test parse_ssl_verify() handles boolean values."""
        # Test boolean True
        assert parse_ssl_verify(True) is True
        
        # Test boolean False
        assert parse_ssl_verify(False) is False
        
        # Verify string values still work (regression test)
        assert parse_ssl_verify('true') is True
        assert parse_ssl_verify('false') is False
    
    def test_get_required_env_exists(self):
        """Test getting required environment variable that exists."""
        with patch.dict(os.environ, {'TEST_VAR': 'test_value'}, clear=False):
            value = get_required_env('TEST_VAR')
            assert value == 'test_value'
    
    def test_get_required_env_missing(self):
        """Test getting required environment variable that doesn't exist."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="TEST_VAR"):
                get_required_env('TEST_VAR')
    
    def test_get_required_env_custom_error(self):
        """Test getting required environment variable with custom error message."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Custom error"):
                get_required_env('TEST_VAR', error_msg="Custom error message")
    
    def test_validate_required_env_vars_all_present(self):
        """Test validating required environment variables when all present."""
        with patch.dict(os.environ, {'VAR1': 'val1', 'VAR2': 'val2'}, clear=False):
            validate_required_env_vars(['VAR1', 'VAR2'])  # Should not raise
    
    def test_validate_required_env_vars_missing(self):
        """Test validating required environment variables when some missing."""
        with patch.dict(os.environ, {'VAR1': 'val1'}, clear=False):
            with pytest.raises(ValueError, match="Missing required"):
                validate_required_env_vars(['VAR1', 'VAR2', 'VAR3'])
    
    def test_get_arango_config(self, mock_env_amp):
        """Test get_arango_config function."""
        # Test default (unmasked for internal use)
        config = get_arango_config()
        
        assert config['endpoint'] == 'https://test.arangodb.cloud:8529'
        assert config['user'] == 'testuser'
        assert config['password'] == 'testpass'
        assert config['database'] == 'testdb'

        # Test masked (for logging/display)
        config_masked = get_arango_config(mask_secrets=True)
        assert config_masked['password'] == '***MASKED***'
    
    def test_get_gae_config(self, mock_env_amp):
        """Test get_gae_config function returns unmasked values for internal use."""
        config = get_gae_config()
        
        assert config['deployment_mode'] == 'amp'
        # Should return actual values, not masked (for internal library use)
        assert config['api_key_id'] != '***MASKED***'
        assert config['api_key_secret'] != '***MASKED***'
        assert len(config['api_key_id']) > 0
        assert len(config['api_key_secret']) > 0
    
    @pytest.mark.skip(reason="Permission error in sandbox environment")
    def test_load_env_vars_prioritizes_cwd(self, tmp_path, monkeypatch):
        """Test that .env in current working directory is loaded first."""
        import os
        from graph_analytics_orchestrator.config import load_env_vars
        
        # Create .env in temporary directory (simulating project root)
        test_env = tmp_path / '.env'
        test_env.write_text('TEST_VAR=from_cwd\n')
        
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Load env vars
        load_env_vars()
        
        # Verify it loaded from CWD
        assert os.getenv('TEST_VAR') == 'from_cwd'

