"""
Utility functions for credential validation and error checking.

Based on lessons learned from real-world customer deployment issues.
"""

import os
from typing import Dict, List, Tuple, Optional


def validate_endpoint_format(endpoint: str) -> Tuple[bool, Optional[str]]:
    """
    Validate endpoint format and check for common issues.
    
    Args:
        endpoint: ArangoDB endpoint URL
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not endpoint:
        return False, "Endpoint is empty"
    
    # Check for protocol
    if not endpoint.startswith(('http://', 'https://')):
        return False, "Endpoint must start with http:// or https://"
    
    # Extract hostname part
    try:
        parts = endpoint.split('://', 1)
        if len(parts) != 2:
            return False, "Invalid endpoint format"
        
        host_part = parts[1].split('/', 1)[0]
        
        # Check for port
        if ':' not in host_part:
            return False, (
                f"Endpoint is missing port number.\n"
                f"  Current: {endpoint}\n"
                f"  Should be: {endpoint}:8529\n"
                f"  This is the #1 cause of 401 errors!"
            )
        
        # Check port is 8529 (ArangoDB default)
        host, port = host_part.rsplit(':', 1)
        if port != '8529':
            return False, (
                f"Endpoint has non-standard port: {port}\n"
                f"  ArangoDB typically uses port 8529\n"
                f"  Current: {endpoint}\n"
                f"  Expected: {parts[0]}://{host}:8529"
            )
        
        return True, None
        
    except Exception as e:
        return False, f"Error parsing endpoint: {e}"


def check_password_format(password: str) -> Tuple[bool, List[str]]:
    """
    Check password for common formatting issues.
    
    Args:
        password: Password string to check
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    if not password:
        issues.append("Password is empty")
        return False, issues
    
    # Check for leading spaces
    if password.startswith(' '):
        issues.append("Password has leading space(s)")
    
    # Check for trailing spaces
    if password.endswith(' '):
        issues.append("Password has trailing space(s)")
    
    # Check for quotes (sometimes copied from documentation)
    if password.startswith('"') and password.endswith('"'):
        issues.append("Password appears to be wrapped in quotes")
    
    if password.startswith("'") and password.endswith("'"):
        issues.append("Password appears to be wrapped in single quotes")
    
    return len(issues) == 0, issues


def validate_credentials(
    endpoint: Optional[str] = None,
    password: Optional[str] = None,
    username: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """
    Validate credentials and return any issues found.
    
    Args:
        endpoint: ArangoDB endpoint
        password: Database password
        username: Database username
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    # Validate endpoint
    if endpoint:
        is_valid, error = validate_endpoint_format(endpoint)
        if not is_valid and error:
            issues.append(f"Endpoint issue: {error}")
    
    # Validate password
    if password:
        is_valid, password_issues = check_password_format(password)
        if not is_valid:
            issues.extend([f"Password issue: {issue}" for issue in password_issues])
    
    # Check username
    if username:
        if not username.strip():
            issues.append("Username is empty or whitespace only")
        if username.startswith(' ') or username.endswith(' '):
            issues.append("Username has leading/trailing spaces")
    
    return len(issues) == 0, issues


def get_credential_validation_report() -> str:
    """
    Get a validation report for current credentials from environment.
    
    Returns:
        Formatted validation report string
    """
    from .config import get_arango_config
    
    try:
        config = get_arango_config()
        endpoint = config.get('endpoint', '')
        password = config.get('password', '')
        username = config.get('user', '')
        
        is_valid, issues = validate_credentials(
            endpoint=endpoint,
            password=password,
            username=username
        )
        
        report_lines = ["Credential Validation Report", "=" * 50]
        
        if is_valid:
            report_lines.append("All credentials appear valid")
        else:
            report_lines.append("Issues found:")
            for issue in issues:
                report_lines.append(f"  - {issue}")
        
        report_lines.append("")
        report_lines.append("Current Configuration:")
        report_lines.append(f"  Endpoint: {endpoint}")
        report_lines.append(f"  Username: {username}")
        report_lines.append(f"  Password: {'***MASKED***' if password else 'NOT SET'}")
        
        return "\n".join(report_lines)
        
    except Exception as e:
        return f"Error generating validation report: {e}"

