"""
ArangoDB Connection Helper

Provides a unified interface to connect to ArangoDB clusters.
"""

from arango import ArangoClient
from typing import Optional

from .config import get_arango_config, parse_ssl_verify


def get_db_connection():
    """
    Establish connection to ArangoDB cluster.

    Returns:
        StandardDatabase: ArangoDB database connection

    Raises:
        ValueError: If required credentials are missing
        ConnectionError: If connection fails
    """
    # Get configuration from environment
    config = get_arango_config()

    endpoint = config["endpoint"]
    username = config["user"]
    password = config["password"]
    database = config["database"]
    verify_ssl = parse_ssl_verify(config["verify_ssl"])

    # Initialize ArangoDB client
    client = ArangoClient(hosts=endpoint)

    # Connect to system database to verify credentials
    sys_db = client.db(
        "_system", username=username, password=password, verify=verify_ssl
    )

    # Check connection
    try:
        sys_db.version()
        print(f"OK: Successfully connected to ArangoDB at {endpoint}")
    except Exception as e:
        # Don't expose password in error messages
        error_msg = str(e).replace(password, "***MASKED***")

        # Enhanced error messages for common issues
        error_str = str(e).lower()
        if "401" in error_str or "not authorized" in error_str or "err 11" in error_str:
            # This is an authorization error
            enhanced_msg = (
                f"Failed to connect to ArangoDB: {error_msg}\n\n"
                f"Authorization Error Detected\n\n"
                f"This error means the server rejected your credentials or permissions.\n\n"
                f"Common causes:\n"
                f"  1. User doesn't have access to _system database (limited users)\n"
                f"  2. Wrong username or password\n"
                f"  3. Password has extra spaces (check .env file)\n"
                f"  4. Endpoint missing port :8529\n\n"
                f"Troubleshooting:\n"
                f"  1. Verify credentials in .env file (no spaces, no quotes)\n"
                f"  2. Check endpoint includes port: ARANGO_ENDPOINT=https://hostname:8529\n"
                f"  3. Verify credentials work in web UI\n"
                f"  4. For limited users, connect directly to target database (skip _system)\n"
            )
            raise ConnectionError(enhanced_msg)
        else:
            raise ConnectionError(f"Failed to connect to ArangoDB: {error_msg}")

    # Connect to target database
    # Note: For limited users, we may not be able to list databases
    # So we'll try to connect directly and handle errors gracefully
    listing_succeeded = False
    db_names = []

    try:
        available_dbs = sys_db.databases()
        listing_succeeded = True
        # Handle both dict format and string format
        if available_dbs and isinstance(available_dbs[0], dict):
            db_names = [db["name"] for db in available_dbs]
        else:
            db_names = available_dbs
    except Exception as e:
        error_str = str(e).lower()
        # If it's an authorization error, user might be limited - that's okay
        if "401" in error_str or "not authorized" in error_str or "err 11" in error_str:
            print(f"Warning: Cannot list databases (user may have limited permissions)")
            print(f"   Attempting direct connection to '{database}' database...")
            # Continue - will try direct connection
        else:
            print(f"Warning: Could not verify database existence: {e}")
            # Continue anyway - let the connection attempt fail if database doesn't exist

    if listing_succeeded:
        if database not in db_names:
            raise ValueError(
                f"Database '{database}' does not exist on this cluster. "
                f"Available: {db_names}"
            )

    db = client.db(database, username=username, password=password, verify=verify_ssl)
    print(f"OK: Connected to database: {database}")

    return db


def get_connection_info():
    """
    Get connection information without establishing a connection.

    Returns:
        dict: Connection configuration details
    """
    config = get_arango_config()

    return {
        "endpoint": config["endpoint"],
        "database": config["database"],
        "user": config["user"],
        "verify_ssl": config["verify_ssl"],
    }
