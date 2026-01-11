# Enhanced Error Messages

## Overview

The library includes comprehensive error messages based on real customer experience, particularly from common issues encountered during credential authentication.

## Key Improvements

### 1. Missing Port Detection

**Issue:** Endpoint URL missing `:8529` port (most common cause of 401 errors)

**Detection:**
- Automatically checks endpoint format during initialization
- Warns if port is missing
- Provides clear fix instructions

**Example:**
```python
# Warning during initialization:
  ARANGO_ENDPOINT appears to be missing the port number.
  Current: https://example.arangodb.cloud
  Expected: https://example.arangodb.cloud:8529
  If you get 401 errors, add :8529 to your endpoint URL.
```

### 2. Enhanced 401 Error Messages

**Issue:** Authentication failures with unclear error messages

**Enhancement:**
- Detailed explanation of what 401 means
- Lists common causes
- Provides step-by-step troubleshooting
- Checks for specific issues (missing port, password spaces)

**Example:**
```
 Authentication failed (401 Unauthorized)
   URL: https://example.com/_open/auth
   This usually means:
   1. Wrong username or password
   2. Endpoint URL is incorrect (missing port :8529?)
   3. Network/VPN access issue
   4. Password may have extra spaces (check .env file)

     WARNING: Your endpoint 'https://example.com' is missing port :8529
   It should be: https://example.com:8529
   This is the #1 cause of 401 errors!

   Troubleshooting steps:
   1. Verify endpoint includes :8529 port
   2. Check credentials match exactly (no extra spaces)
   3. Verify credentials work in ArangoDB web UI
   4. Check network/VPN connectivity
```

### 3. Password Formatting Detection

**Issue:** Passwords with extra spaces or quotes cause authentication failures

**Detection:**
- Checks for leading/trailing spaces
- Detects quoted passwords
- Warns about formatting issues

**Example:**
```python
from graph_analytics_orchestrator import check_password_format

is_valid, issues = check_password_format(" mypassword ")
# Returns: (False, ["Password has leading space(s)", "Password has trailing space(s)"])
```

### 4. Limited User Support

**Issue:** Limited-permission users can't access `_system` database

**Enhancement:**
- Gracefully handles authorization errors for `_system` access
- Falls back to direct database connection
- Provides clear warnings about permission limitations

**Example:**
```
  Warning: Cannot list databases (user may have limited permissions)
   Attempting direct connection to 'restore' database...
 Connected to database: restore
```

### 5. Authorization Error Details

**Issue:** Generic error messages don't help users fix problems

**Enhancement:**
- Specific error detection (401, ERR 11, "not authorized")
- Detailed explanation of what the error means
- Common causes listed
- Step-by-step troubleshooting guide

**Example:**
```
 Authorization Error Detected

This error means the server rejected your credentials or permissions.

Common causes:
  1. User doesn't have access to _system database (limited users)
  2. Wrong username or password
  3. Password has extra spaces (check .env file)
  4. Endpoint missing port :8529

Troubleshooting:
  1. Verify credentials in .env file (no spaces, no quotes)
  2. Check endpoint includes port: ARANGO_ENDPOINT=https://hostname:8529
  3. Verify credentials work in web UI
  4. For limited users, connect directly to target database (skip _system)
```

## Usage

### Validate Credentials Before Connecting

```python
from graph_analytics_orchestrator import get_credential_validation_report

# Get validation report
report = get_credential_validation_report()
print(report)
```

### Check Specific Issues

```python
from graph_analytics_orchestrator import validate_endpoint_format, check_password_format

# Check endpoint
is_valid, error = validate_endpoint_format("https://example.com")
if not is_valid:
    print(f"Issue: {error}")

# Check password
is_valid, issues = check_password_format(" mypassword ")
if not is_valid:
    for issue in issues:
        print(f"Issue: {issue}")
```

## Common Issues and Fixes

### Issue 1: Missing Port

**Error:** `401 Unauthorized for url: https://example.com/_open/auth`

**Fix:**
```bash
# Wrong
ARANGO_ENDPOINT=https://example.com

# Correct
ARANGO_ENDPOINT=https://example.com:8529
```

### Issue 2: Password with Spaces

**Error:** `401 Unauthorized` even with correct password

**Fix:**
```bash
# Wrong (has spaces)
ARANGO_PASSWORD= mypassword 

# Correct (no spaces)
ARANGO_PASSWORD=mypassword
```

### Issue 3: Limited User Permissions

**Error:** `[HTTP 401][ERR 11] not authorized to execute this request`

**Fix:**
- Library automatically handles this
- Connects directly to target database
- Skips `_system` database operations

### Issue 4: Quoted Password

**Error:** Authentication fails

**Fix:**
```bash
# Wrong (quoted)
ARANGO_PASSWORD="mypassword"

# Correct (no quotes)
ARANGO_PASSWORD=mypassword
```

## Source of Improvements

These enhancements are based on issues encountered during prior real-world customer deployments:

1. **Missing Port Issue:** Most common cause of 401 errors
2. **Password Formatting:** Extra spaces from PDF copy-paste
3. **Limited User Access:** Users without `_system` database permissions
4. **Unclear Error Messages:** Generic errors that don't help users

All improvements have been tested and validated with actual customer scenarios.

## Testing

The enhanced error messages are tested in:
- `tests/test_db_connection.py` - Database connection error handling
- `tests/test_gae_connection.py` - GAE connection error handling
- `tests/test_config.py` - Configuration validation

## Future Enhancements

- Add interactive credential checker CLI tool
- Add automatic credential fix suggestions
- Add connection test utility
- Add credential validation in configuration loading

