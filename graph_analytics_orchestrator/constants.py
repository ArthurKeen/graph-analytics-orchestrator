"""
Constants for Graph Analytics AI library.

Centralizes magic numbers and hardcoded values for better maintainability.
"""

# Port Numbers
DEFAULT_ARANGO_PORT = 8529
DEFAULT_GAE_PORT = 8829

# Timeouts (in seconds)
DEFAULT_TIMEOUT = 300  # 5 minutes for general operations
DEFAULT_ENGINE_API_TIMEOUT = 30  # 30 seconds for engine API readiness
DEFAULT_JOB_TIMEOUT = 3600  # 1 hour for job completion

# Polling Intervals (in seconds)
DEFAULT_POLL_INTERVAL = 2  # 2 seconds between status checks
DEFAULT_RETRY_DELAY = 2  # 2 seconds between retries

# Token Management (in hours)
TOKEN_LIFETIME_HOURS = 24  # ArangoGraph tokens expire after 24 hours
TOKEN_REFRESH_THRESHOLD_HOURS = 1  # Refresh token 1 hour before expiry

# Algorithm Defaults
DEFAULT_DAMPING_FACTOR = 0.85  # PageRank damping factor
DEFAULT_MAX_SUPERSTEPS = 100  # Maximum iterations for iterative algorithms
DEFAULT_START_LABEL_ATTRIBUTE = "_key"  # Label Propagation initial attribute

# Store Results Defaults
DEFAULT_PARALLELISM = 8  # Degree of parallelism for storing results
DEFAULT_BATCH_SIZE = 10000  # Batch size for writing results

# Status Strings
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FINISHED = "finished"
STATUS_ERROR = "error"

# HTTP Headers
AUTHORIZATION_BEARER_PREFIX = "bearer"

# API Endpoints
API_VERSION_PREFIX = "v1/"

# Status Icons (for logging)
ICON_SUCCESS = "[SUCCESS]"
ICON_ERROR = "[ERROR]"
ICON_WARNING = "[WARNING]"

# Job Status Values (for normalization)
COMPLETED_STATES = ["done", "finished", "completed", "succeeded"]
FAILED_STATES = ["failed", "error"]
