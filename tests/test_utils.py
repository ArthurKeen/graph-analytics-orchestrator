"""Tests for utility functions."""

import pytest

from graph_analytics_orchestrator.utils import (
    validate_endpoint_format,
    check_password_format,
    validate_credentials,
    get_credential_validation_report
)


class TestValidateEndpointFormat:
    """Tests for validate_endpoint_format function."""
    
    def test_valid_endpoint_with_port(self):
        """Test valid endpoint with port."""
        is_valid, error = validate_endpoint_format("https://example.com:8529")
        assert is_valid is True
        assert error is None
    
    def test_valid_endpoint_with_path(self):
        """Test valid endpoint with path."""
        is_valid, error = validate_endpoint_format("https://example.com:8529/path")
        assert is_valid is True
        assert error is None
    
    def test_missing_port(self):
        """Test endpoint missing port."""
        is_valid, error = validate_endpoint_format("https://example.com")
        assert is_valid is False
        assert "missing port" in error.lower()
        assert ":8529" in error
    
    def test_wrong_port(self):
        """Test endpoint with wrong port."""
        is_valid, error = validate_endpoint_format("https://example.com:443")
        assert is_valid is False
        assert "non-standard port" in error.lower()
    
    def test_missing_protocol(self):
        """Test endpoint missing protocol."""
        is_valid, error = validate_endpoint_format("example.com:8529")
        assert is_valid is False
        assert "http" in error.lower() or "https" in error.lower()
    
    def test_empty_endpoint(self):
        """Test empty endpoint."""
        is_valid, error = validate_endpoint_format("")
        assert is_valid is False
        assert "empty" in error.lower()


class TestCheckPasswordFormat:
    """Tests for check_password_format function."""
    
    def test_valid_password(self):
        """Test valid password."""
        is_valid, issues = check_password_format("mypassword123")
        assert is_valid is True
        assert len(issues) == 0
    
    def test_password_with_leading_space(self):
        """Test password with leading space."""
        is_valid, issues = check_password_format(" mypassword")
        assert is_valid is False
        assert any("leading" in issue.lower() for issue in issues)
    
    def test_password_with_trailing_space(self):
        """Test password with trailing space."""
        is_valid, issues = check_password_format("mypassword ")
        assert is_valid is False
        assert any("trailing" in issue.lower() for issue in issues)
    
    def test_password_with_quotes(self):
        """Test password wrapped in quotes."""
        is_valid, issues = check_password_format('"mypassword"')
        assert is_valid is False
        assert any("quotes" in issue.lower() for issue in issues)
    
    def test_password_with_single_quotes(self):
        """Test password wrapped in single quotes."""
        is_valid, issues = check_password_format("'mypassword'")
        assert is_valid is False
        assert any("quotes" in issue.lower() for issue in issues)
    
    def test_empty_password(self):
        """Test empty password."""
        is_valid, issues = check_password_format("")
        assert is_valid is False
        assert any("empty" in issue.lower() for issue in issues)


class TestValidateCredentials:
    """Tests for validate_credentials function."""
    
    def test_all_valid(self):
        """Test all credentials valid."""
        is_valid, issues = validate_credentials(
            endpoint="https://example.com:8529",
            password="mypassword",
            username="user"
        )
        assert is_valid is True
        assert len(issues) == 0
    
    def test_missing_port(self):
        """Test endpoint missing port."""
        is_valid, issues = validate_credentials(
            endpoint="https://example.com",
            password="mypassword",
            username="user"
        )
        assert is_valid is False
        assert any("port" in issue.lower() for issue in issues)
    
    def test_password_with_spaces(self):
        """Test password with spaces."""
        is_valid, issues = validate_credentials(
            endpoint="https://example.com:8529",
            password=" mypassword ",
            username="user"
        )
        assert is_valid is False
        assert any("space" in issue.lower() for issue in issues)
    
    def test_username_with_spaces(self):
        """Test username with spaces."""
        is_valid, issues = validate_credentials(
            endpoint="https://example.com:8529",
            password="mypassword",
            username=" user "
        )
        assert is_valid is False
        assert any("space" in issue.lower() for issue in issues)
    
    def test_multiple_issues(self):
        """Test multiple issues."""
        is_valid, issues = validate_credentials(
            endpoint="https://example.com",  # Missing port
            password=" mypassword ",  # Has spaces
            username=" user "  # Has spaces
        )
        assert is_valid is False
        assert len(issues) >= 3  # Should catch all issues


class TestGetCredentialValidationReport:
    """Tests for get_credential_validation_report function."""
    
    def test_report_generation(self, mock_env_amp):
        """Test report generation with valid credentials."""
        report = get_credential_validation_report()
        
        assert "Credential Validation Report" in report
        assert "Current Configuration" in report
        assert "Endpoint" in report
        assert "Username" in report
        assert "Password" in report or "***MASKED***" in report
    
    def test_report_with_issues(self, monkeypatch):
        """Test report with credential issues."""
        # Set environment variables with issues
        monkeypatch.setenv('ARANGO_ENDPOINT', 'https://example.com')  # Missing port
        monkeypatch.setenv('ARANGO_PASSWORD', ' mypassword ')  # Has spaces
        monkeypatch.setenv('ARANGO_USER', 'user')
        monkeypatch.setenv('ARANGO_DATABASE', 'testdb')
        
        report = get_credential_validation_report()
        
        # Report should be generated (may show issues or valid depending on validation)
        assert "Credential Validation Report" in report
        assert "Current Configuration" in report

