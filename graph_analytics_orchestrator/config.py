"""
Configuration management for Graph Analytics AI library.

Supports both Arango Managed Platform (AMP) and self-managed deployments.
"""

import os
import warnings
from pathlib import Path
from typing import Dict, Optional, List, Union
from enum import Enum
from dotenv import load_dotenv

# Import constants (avoid circular import)
try:
    from . import constants
    DEFAULT_ARANGO_PORT = constants.DEFAULT_ARANGO_PORT
    DEFAULT_GAE_PORT = constants.DEFAULT_GAE_PORT
    DEFAULT_TIMEOUT_SECONDS = constants.DEFAULT_TIMEOUT
except ImportError:
    # Fallback if constants not available (shouldn't happen)
    DEFAULT_ARANGO_PORT = 8529
    DEFAULT_GAE_PORT = 8829
    DEFAULT_TIMEOUT_SECONDS = 30


class DeploymentMode(Enum):
    """Deployment mode for GAE."""
    AMP = "amp"  # Arango Managed Platform
    SELF_MANAGED = "self_managed"  # Self-managed via GenAI Suite


# Constants (kept for backward compatibility)
DEFAULT_SSL_VERIFY = True
DEFAULT_TIMEOUT = DEFAULT_TIMEOUT_SECONDS


def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Returns:
        Path: Absolute path to project root
    """
    # Try to find project root by looking for .env or setup.py
    current = Path(__file__).parent
    while current != current.parent:
        if (current / '.env').exists() or (current / 'setup.py').exists():
            return current
        current = current.parent
    # Fallback to current directory
    return Path.cwd()


def get_env_path() -> Path:
    """
    Get the path to the .env file.
    
    Returns:
        Path: Absolute path to .env file
    """
    return get_project_root() / '.env'


def load_env_vars() -> None:
    """
    Load environment variables from .env file.

    If required variables are already present in the environment, no file load
    is attempted. File load errors (e.g., permission issues) are ignored with a
    warning to keep runtime and tests resilient.
    """

    def _required_vars_present() -> bool:
        required = {"ARANGO_ENDPOINT", "ARANGO_PASSWORD", "ARANGO_DATABASE"}
        return all(os.getenv(v) for v in required)

    def _safe_load(path: Optional[Path] = None) -> None:
        try:
            if path is None:
                load_dotenv()
            else:
                load_dotenv(path)
        except PermissionError:
            warnings.warn(
                f"Skipping .env load due to permission error at {path}", UserWarning
            )
        except OSError:
            warnings.warn(
                f"Skipping .env load due to access error at {path}", UserWarning
            )

    # Skip file reads when env is already populated (common in tests/CI)
    if _required_vars_present():
        return

    # First, try loading from current working directory (most common case)
    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        _safe_load(cwd_env)
        if _required_vars_present():
            return

    # Fallback to library's project root
    env_path = get_env_path()
    if env_path.exists():
        _safe_load(env_path)
        if _required_vars_present():
            return

    # Last resort: try loading from current directory (dotenv default)
    _safe_load()


def get_required_env(var_name: str, error_msg: Optional[str] = None) -> str:
    """
    Get a required environment variable.
    
    Args:
        var_name: Name of the environment variable
        error_msg: Optional custom error message
    
    Returns:
        str: Value of the environment variable
        
    Raises:
        ValueError: If environment variable is not set
    """
    value = os.getenv(var_name)
    if not value:
        if error_msg:
            raise ValueError(error_msg)
        raise ValueError(f"Required environment variable '{var_name}' is not set")
    return value


def validate_required_env_vars(var_names: List[str]) -> None:
    """
    Validate that required environment variables are set.
    
    Args:
        var_names: List of required variable names
        
    Raises:
        ValueError: If any required variables are missing
    """
    missing_vars = [var for var in var_names if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


class ArangoConfig:
    """ArangoDB connection configuration."""
    
    def __init__(self):
        """Initialize from environment variables."""
        load_env_vars()
        
        self.endpoint = get_required_env('ARANGO_ENDPOINT')
        self.user = os.getenv('ARANGO_USER', 'root')
        self.password = get_required_env('ARANGO_PASSWORD')
        self.database = get_required_env('ARANGO_DATABASE')
        verify_ssl_str = os.getenv('ARANGO_VERIFY_SSL', str(DEFAULT_SSL_VERIFY))
        self.verify_ssl = parse_ssl_verify(verify_ssl_str)
        
        # Warn if SSL verification is disabled
        if not self.verify_ssl:
            warnings.warn(
                "SSL verification is disabled. This is insecure and may allow "
                "man-in-the-middle attacks. Only disable for development/testing "
                "with self-signed certificates.",
                UserWarning
            )
        
        self.timeout = int(os.getenv('ARANGO_TIMEOUT', str(DEFAULT_TIMEOUT)))
    
    def to_dict(self, mask_secrets: bool = True) -> Dict[str, str]:
        """
        Convert to dictionary.
        
        Args:
            mask_secrets: If True, mask password in output (default: True)
        """
        password = '***MASKED***' if mask_secrets else self.password
        return {
            'endpoint': self.endpoint,
            'user': self.user,
            'password': password,
            'database': self.database,
            'verify_ssl': self.verify_ssl,
            'timeout': str(self.timeout)
        }


def _extract_deployment_url(endpoint: str) -> str:
    """
    Extract deployment URL from endpoint by removing port.
    
    Args:
        endpoint: Full endpoint URL (e.g., https://example.arangodb.cloud:8529)
        
    Returns:
        Deployment URL without port (e.g., https://example.arangodb.cloud)
    """
    # Remove port if present (e.g., https://example.arangodb.cloud:8529 -> https://example.arangodb.cloud)
    if '://' in endpoint:
        parts = endpoint.split('://', 1)
        host_part = parts[1].split('/', 1)[0]
        if ':' in host_part:
            host = host_part.split(':', 1)[0]
            return f"{parts[0]}://{host}"
        else:
            return f"{parts[0]}://{host_part}"
    else:
        return endpoint.rsplit(':', 1)[0] if ':' in endpoint else endpoint


class GAEConfig:
    """Graph Analytics Engine configuration."""
    
    def __init__(self):
        """Initialize from environment variables."""
        load_env_vars()
        
        # Determine deployment mode
        mode_str = os.getenv('GAE_DEPLOYMENT_MODE', 'amp').lower()
        if mode_str in ('amp', 'managed', 'arangograph'):
            self.deployment_mode = DeploymentMode.AMP
        elif mode_str in ('self_managed', 'self-managed', 'genai', 'gen-ai'):
            self.deployment_mode = DeploymentMode.SELF_MANAGED
        else:
            raise ValueError(
                f"Invalid GAE_DEPLOYMENT_MODE: {mode_str}. "
                f"Must be 'amp' or 'self_managed'"
            )
        
        if self.deployment_mode == DeploymentMode.AMP:
            # AMP requires API keys and deployment URL
            self.api_key_id = get_required_env('ARANGO_GRAPH_API_KEY_ID')
            self.api_key_secret = get_required_env('ARANGO_GRAPH_API_KEY_SECRET')
            self.access_token = os.getenv('ARANGO_GRAPH_TOKEN', '')
            
            # Extract deployment URL from endpoint
            endpoint = get_required_env('ARANGO_ENDPOINT')
            self.deployment_url = _extract_deployment_url(endpoint)
            
            self.gae_port = int(os.getenv('ARANGO_GAE_PORT', str(DEFAULT_GAE_PORT)))
            
        else:  # SELF_MANAGED
            # Self-managed uses same credentials as ArangoDB
            # No additional config needed - uses JWT tokens from ArangoDB
            pass
    
    def to_dict(self, mask_secrets: bool = True) -> Dict[str, str]:
        """
        Convert to dictionary.
        
        Args:
            mask_secrets: If True, mask secrets in output (default: True)
        """
        result = {
            'deployment_mode': self.deployment_mode.value
        }
        
        if self.deployment_mode == DeploymentMode.AMP:
            api_key_id = '***MASKED***' if mask_secrets else self.api_key_id
            api_key_secret = '***MASKED***' if mask_secrets else self.api_key_secret
            access_token = '***MASKED***' if mask_secrets and self.access_token else self.access_token
            
            result.update({
                'api_key_id': api_key_id,
                'api_key_secret': api_key_secret,
                'access_token': access_token,
                'deployment_url': self.deployment_url,
                'gae_port': str(self.gae_port)
            })
        
        return result


def get_arango_config(mask_secrets: bool = False) -> Dict[str, str]:
    """
    Get ArangoDB connection configuration from environment.
    
    Args:
        mask_secrets: If True, mask password in output (default: False for internal use)
    
    Returns:
        dict: Configuration dictionary
        
    Raises:
        ValueError: If required variables are missing
    """
    config = ArangoConfig()
    return config.to_dict(mask_secrets=mask_secrets)


def get_gae_config() -> Dict[str, str]:
    """
    Get Graph Analytics Engine configuration from environment.
    
    Returns:
        dict: Configuration dictionary (unmasked for internal use)
        
    Raises:
        ValueError: If required variables are missing
    """
    config = GAEConfig()
    return config.to_dict(mask_secrets=False)  # Don't mask for internal library use


def parse_ssl_verify(value: Union[str, bool]) -> bool:
    """
    Parse SSL verification string to boolean.
    
    Args:
        value: String value ('true', 'false', '1', '0', etc.) or bool
        
    Returns:
        bool: Parsed boolean value
    """
    # Handle boolean input
    if isinstance(value, bool):
        return value
    
    # Handle string input
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    
    # Default to True for other types
    return True

