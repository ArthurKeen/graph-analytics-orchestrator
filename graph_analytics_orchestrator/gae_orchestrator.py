"""
Graph Analytics Engine (GAE) Orchestrator

Automates complete GAE analysis workflows including:
- Engine lifecycle management (deploy, use, delete)
- Graph loading and algorithm execution
- Result storage and verification
- Cost tracking and monitoring
- Error handling and retries
- Multi-analysis orchestration

Works with both Arango Managed Platform (AMP) and self-managed deployments.
"""

import time
import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Any
from datetime import datetime
from enum import Enum

from .gae_connection import get_gae_connection, GAEConnectionBase
from .db_connection import get_db_connection
from .config import get_arango_config
from .constants import DEFAULT_JOB_TIMEOUT, DEFAULT_POLL_INTERVAL


class AnalysisStatus(Enum):
    """Status of an analysis workflow."""
    PENDING = "pending"
    ENGINE_DEPLOYING = "engine_deploying"
    GRAPH_LOADING = "graph_loading"
    ALGORITHM_RUNNING = "algorithm_running"
    STORING_RESULTS = "storing_results"
    COMPLETED = "completed"
    FAILED = "failed"
    CLEANING_UP = "cleaning_up"


@dataclass
class AnalysisConfig:
    """Configuration for a GAE analysis."""
    
    # Analysis identification
    name: str
    description: str = ""
    
    # Graph configuration
    vertex_collections: List[str] = field(default_factory=list)
    edge_collections: List[str] = field(default_factory=list)
    vertex_attributes: Optional[List[str]] = None  # Attributes to load (e.g., ['_key'])
    database: Optional[str] = None  # None = use default from config
    
    # Algorithm configuration
    algorithm: str = "pagerank"  # pagerank, label_propagation, scc, wcc, betweenness
    algorithm_params: Dict[str, Any] = field(default_factory=dict)
    result_field: Optional[str] = None  # e.g., "pagerank_product_demand"
    
    # Engine configuration
    engine_size: str = "e16"  # e4, e8, e16, e32, e64, e128 (AMP only, ignored for self-managed)
    engine_type: str = "gral"
    
    # Storage configuration
    target_collection: str = "graph_analysis_results"  # Where to store results
    
    # Workflow options
    auto_cleanup: bool = True  # Automatically delete engine after completion
    retry_on_failure: bool = True
    max_retries: int = 3
    timeout_seconds: int = DEFAULT_JOB_TIMEOUT  # 1 hour max
    
    # Cost tracking
    estimated_cost_usd: Optional[float] = None
    
    def __post_init__(self):
        """Validate and set defaults."""
        if not self.database:
            config = get_arango_config()
            self.database = config['database']
        
        if not self.result_field:
            self.result_field = f"{self.algorithm}_{self.name}"
        
        # Set default algorithm parameters
        if not self.algorithm_params:
            self.algorithm_params = self._get_default_params()
    
    def _get_default_params(self) -> Dict[str, Any]:
        """Get default parameters for the algorithm."""
        defaults = {
            "pagerank": {
                "damping_factor": 0.85,
                "maximum_supersteps": 100
            },
            "label_propagation": {
                "start_label_attribute": "_key",
                "maximum_supersteps": 100
            },
            "scc": {},
            "wcc": {},
            "betweenness": {
                "maximum_supersteps": 100
            }
        }
        return defaults.get(self.algorithm, {})


@dataclass
class AnalysisResult:
    """Results from a completed analysis."""
    
    # Analysis info
    config: AnalysisConfig
    status: AnalysisStatus
    
    # Timing
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Engine info
    engine_id: Optional[str] = None
    engine_size: Optional[str] = None
    
    # Graph info
    graph_id: Optional[str] = None
    vertex_count: Optional[int] = None
    edge_count: Optional[int] = None
    
    # Job info
    job_id: Optional[str] = None
    algorithm: Optional[str] = None
    
    # Results
    results_stored: bool = False
    documents_updated: Optional[int] = None
    
    # Cost tracking
    estimated_cost_usd: Optional[float] = None
    engine_runtime_minutes: Optional[float] = None
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        result = asdict(self)
        result['status'] = self.status.value
        result['start_time'] = self.start_time.isoformat()
        if self.end_time:
            result['end_time'] = self.end_time.isoformat()
        # Convert config to dict
        result['config'] = asdict(self.config)
        return result


class GAEOrchestrator:
    """
    Orchestrates complete GAE analysis workflows.
    
    Handles engine lifecycle, algorithm execution, result storage,
    cost tracking, and error recovery.
    
    Works with both AMP and self-managed deployments.
    """
    
    # Engine hourly costs (approximate, in USD) - AMP only
    ENGINE_COSTS = {
        'e4': 0.20,
        'e8': 0.30,
        'e16': 0.40,
        'e32': 0.80,
        'e64': 1.60,
        'e128': 3.20
    }
    
    # Non-retryable error patterns (configuration/setup issues)
    NON_RETRYABLE_ERRORS = [
        'ARANGO_GRAPH_TOKEN not set',
        'ARANGO_ENDPOINT not set',
        'ARANGO_DATABASE not set',
        'Missing required environment',
        'Configuration error',
        'Invalid configuration',
        'maximum recursion depth'
    ]
    
    def __init__(self, verbose: bool = True, gae_connection: Optional[GAEConnectionBase] = None):
        """
        Initialize orchestrator.
        
        Args:
            verbose: Print progress messages
            gae_connection: Optional GAE connection (will be created if not provided)
        """
        self.verbose = verbose
        self.gae: Optional[GAEConnectionBase] = gae_connection
        self.db = None
        
        # Analysis tracking
        self.current_analysis: Optional[AnalysisResult] = None
        self.analysis_history: List[AnalysisResult] = []
    
    def _log(self, message: str, level: str = "INFO"):
        """Log message if verbose."""
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {level}: {message}")
    
    def _initialize_connections(self):
        """Initialize GAE and database connections."""
        if not self.gae:
            self._log("Initializing GAE connection...")
            self.gae = get_gae_connection()
        
        if not self.db:
            self._log("Initializing database connection...")
            self.db = get_db_connection()
    
    def _is_retryable_error(self, error_message: str) -> bool:
        """
        Check if an error is retryable.
        
        Configuration errors, missing tokens, and recursion errors
        should NOT be retried.
        """
        error_lower = error_message.lower()
        for pattern in self.NON_RETRYABLE_ERRORS:
            if pattern.lower() in error_lower:
                return False
        return True
    
    def _check_existing_engines(self):
        """
        Check for existing running engines and warn/fail.
        
        Only works for AMP deployments.
        """
        try:
            # Only GAEManager has list_engines method
            if hasattr(self.gae, 'list_engines'):
                existing = self.gae.list_engines()
                if existing:
                    engine_info = [f"{e['id']} ({e.get('size_id', 'unknown')})" for e in existing]
                    raise RuntimeError(
                        f"Engines already running: {', '.join(engine_info)}. "
                        f"Delete them first or risk multiple billing charges."
                    )
        except AttributeError:
            # Self-managed doesn't have list_engines, skip check
            pass
        except Exception as e:
            if "already running" in str(e):
                raise
            # If we can't check, log warning but continue
            self._log(f"Warning: Could not check for existing engines: {e}", "WARN")
    
    def run_analysis(self, config: AnalysisConfig) -> AnalysisResult:
        """
        Run a complete analysis workflow.
        
        This is the main entry point. It handles the entire workflow:
        1. Check for existing engines (safety check, AMP only)
        2. Deploy engine
        3. Load graph
        4. Run algorithm
        5. Store results
        6. Cleanup (always, even on failure)
        
        Args:
            config: Analysis configuration
            
        Returns:
            AnalysisResult with complete information
        """
        self._log(f"=== Starting Analysis: {config.name} ===")
        
        # Create result tracker
        result = AnalysisResult(
            config=config,
            status=AnalysisStatus.PENDING,
            start_time=datetime.now(),
            engine_size=config.engine_size,
            algorithm=config.algorithm
        )
        self.current_analysis = result
        
        # Initialize connections and do safety checks ONCE (before retry loop)
        self._initialize_connections()
        self._check_existing_engines()
        
        # Retry loop (not recursion!)
        attempt = 0
        max_attempts = config.max_retries + 1 if config.retry_on_failure else 1
        
        while attempt < max_attempts:
            if attempt > 0:
                result.retry_count = attempt
                self._log(f"Retry attempt {attempt}/{config.max_retries}")
            
            try:
                
                # Execute workflow steps
                self._deploy_engine(result)
                self._load_graph(result)
                self._run_algorithm(result)
                self._store_results(result)
                
                # Mark as completed
                result.status = AnalysisStatus.COMPLETED
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                
                # Calculate costs (AMP only)
                result.engine_runtime_minutes = result.duration_seconds / 60
                hourly_cost = self.ENGINE_COSTS.get(config.engine_size, 0)
                if hourly_cost > 0:  # Only calculate if we have cost data
                    result.estimated_cost_usd = (result.engine_runtime_minutes / 60) * hourly_cost
                
                self._log(f"OK: Analysis completed successfully!")
                self._log(f"  Duration: {result.duration_seconds:.1f}s ({result.engine_runtime_minutes:.1f} min)")
                if result.estimated_cost_usd:
                    self._log(f"  Estimated cost: ${result.estimated_cost_usd:.4f}")
                
                # Success - break out of retry loop
                break
                
            except Exception as e:
                result.status = AnalysisStatus.FAILED
                result.error_message = str(e)
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                
                self._log(f"ERROR: Analysis failed: {e}", "ERROR")
                
                # Check if error is retryable
                is_retryable = self._is_retryable_error(str(e))
                
                if not is_retryable:
                    self._log(f"Error is not retryable (configuration/setup issue)", "ERROR")
                    self._log(f"Fix the issue and try again", "ERROR")
                    break  # Don't retry configuration errors
                
                # Check if we should retry
                if config.retry_on_failure and attempt < config.max_retries:
                    attempt += 1
                    self._log(f"Error appears transient, will retry...")
                    # Clean up failed engine before retry
                    if result.engine_id:
                        try:
                            self._cleanup_engine(result)
                            result.engine_id = None  # Reset for retry
                        except Exception as cleanup_err:
                            self._log(f"Cleanup before retry failed: {cleanup_err}", "WARN")
                    continue  # Retry
                else:
                    break  # No more retries
        
        # ALWAYS cleanup engine, even on failure
        # This is critical to prevent orphaned engines
        if result.engine_id:
            try:
                if config.auto_cleanup:
                    self._cleanup_engine(result)
                else:
                    self._log(f"Engine {result.engine_id} left running (auto_cleanup=False)", "WARN")
            except Exception as e:
                self._log(f"CRITICAL: Engine cleanup failed: {e}", "ERROR")
                self._log(f"You MUST manually delete engine: {result.engine_id}", "ERROR")
        
        # Add to history
        self.analysis_history.append(result)
        self.current_analysis = None
        
        return result
    
    def _deploy_engine(self, result: AnalysisResult):
        """Deploy GAE engine."""
        result.status = AnalysisStatus.ENGINE_DEPLOYING
        self._log(f"Deploying {result.config.engine_size} engine...")
        
        try:
            engine_info = self.gae.deploy_engine(
                size_id=result.config.engine_size,
                type_id=result.config.engine_type
            )
            
            result.engine_id = engine_info['id']
            self._log(f"OK: Engine deployed: {result.engine_id}")
        except Exception as e:
            # If deployment fails, try to capture engine_id for cleanup
            if hasattr(self.gae, 'current_engine_id') and self.gae.current_engine_id and not result.engine_id:
                result.engine_id = self.gae.current_engine_id
                self._log(f"Deployment failed but captured engine_id: {result.engine_id}")
            raise
    
    def _load_graph(self, result: AnalysisResult):
        """Load graph data into engine."""
        result.status = AnalysisStatus.GRAPH_LOADING
        self._log(f"Loading graph from {result.config.database}...")
        self._log(f"  Vertices: {result.config.vertex_collections}")
        self._log(f"  Edges: {result.config.edge_collections}")
        if result.config.vertex_attributes:
            self._log(f"  Attributes: {result.config.vertex_attributes}")
        
        graph_info = self.gae.load_graph(
            database=result.config.database,
            vertex_collections=result.config.vertex_collections,
            edge_collections=result.config.edge_collections,
            vertex_attributes=result.config.vertex_attributes
        )
        
        result.graph_id = graph_info.get('graph_id') or graph_info.get('id')
        
        # Wait for load to complete
        job_id = graph_info.get('job_id') or graph_info.get('id')
        if job_id:
            self._wait_for_job(job_id, "Graph loading")
        
        # Get graph details
        try:
            graph_details = self.gae.get_graph(result.graph_id)
            result.vertex_count = graph_details.get('vertex_count', 0)
            result.edge_count = graph_details.get('edge_count', 0)
        except:
            # Graph details may not be available immediately
            pass
        
        self._log(f"OK: Graph loaded: {result.graph_id}")
        if result.vertex_count:
            self._log(f"  Vertices: {result.vertex_count:,}")
        if result.edge_count:
            self._log(f"  Edges: {result.edge_count:,}")
    
    def _run_algorithm(self, result: AnalysisResult):
        """Run the configured algorithm."""
        result.status = AnalysisStatus.ALGORITHM_RUNNING
        self._log(f"Running {result.config.algorithm}...")
        
        # Build algorithm parameters (graph_id + any custom params)
        params = {
            'graph_id': result.graph_id,
            **result.config.algorithm_params
        }
        
        # Call the appropriate algorithm
        if result.config.algorithm == "pagerank":
            job_info = self.gae.run_pagerank(**params)
        elif result.config.algorithm == "label_propagation":
            job_info = self.gae.run_label_propagation(**params)
        elif result.config.algorithm == "scc":
            job_info = self.gae.run_scc(**params)
        elif result.config.algorithm == "wcc":
            job_info = self.gae.run_wcc(**params)
        elif result.config.algorithm == "betweenness":
            # Betweenness not yet implemented in base class, skip for now
            raise ValueError("Betweenness centrality not yet supported")
        else:
            raise ValueError(f"Unsupported algorithm: {result.config.algorithm}")
        
        result.job_id = job_info.get('job_id') or job_info.get('id')
        
        # Wait for completion
        job_result = self._wait_for_job(result.job_id, f"{result.config.algorithm} computation")
        
        execution_time = job_result.get('statistics', {}).get('execution_time_ms', 0) / 1000
        if execution_time > 0:
            self._log(f"OK: Algorithm completed in {execution_time:.3f}s")
    
    def _store_results(self, result: AnalysisResult):
        """Store algorithm results back to database."""
        result.status = AnalysisStatus.STORING_RESULTS
        self._log(f"Storing results to {result.config.target_collection}...")
        
        store_info = self.gae.store_results(
            target_collection=result.config.target_collection,
            job_ids=[result.job_id],
            attribute_names=[result.config.result_field],
            database=result.config.database
        )
        
        # Wait for storage to complete
        store_job_id = store_info.get('job_id') or store_info.get('id')
        if store_job_id:
            self._wait_for_job(store_job_id, "Results storage")
        
        result.results_stored = True
        
        # Get count of stored documents
        try:
            collection = self.db.collection(result.config.target_collection)
            result.documents_updated = collection.count()
            self._log(f"OK: Results stored: {result.documents_updated:,} documents")
        except Exception as e:
            self._log(f"Warning: Could not count stored documents: {e}", "WARN")
    
    def _cleanup_engine(self, result: AnalysisResult):
        """Delete the engine to stop billing."""
        # Preserve original status if it's already terminal
        original_status = result.status
        result.status = AnalysisStatus.CLEANING_UP
        self._log(f"Cleaning up engine {result.engine_id}...")
        
        try:
            self.gae.delete_engine(result.engine_id)
            self._log(f"OK: Engine deleted (billing stopped)")
        finally:
            # Restore terminal status
            if original_status in [AnalysisStatus.COMPLETED, AnalysisStatus.FAILED]:
                result.status = original_status
    
    def _wait_for_job(self, job_id: str, description: str, 
                      poll_interval: int = DEFAULT_POLL_INTERVAL) -> Dict[str, Any]:
        """
        Wait for a job to complete.
        
        Args:
            job_id: Job ID to monitor
            description: Human-readable description for logging
            poll_interval: Seconds between status checks
            
        Returns:
            Final job details
            
        Raises:
            RuntimeError: If job fails
        """
        self._log(f"  Waiting for {description}... (job: {job_id})")
        
        start_time = time.time()
        last_status = None
        
        while True:
            job = self.gae.get_job(job_id)
            
            # GAE API uses different response formats:
            # 1. status-based: {'status': 'succeeded'|'failed'|'running'}
            # 2. progress-based: {'progress': X, 'total': Y, 'error': bool}
            # 3. state-based: {'state': 'done'|'failed'|'running'}
            
            # Check for progress-based format first
            if 'progress' in job and 'total' in job:
                progress = job.get('progress', 0)
                total = job.get('total', 1)
                has_error = job.get('error', False)
                
                if has_error:
                    error_msg = job.get('error_message', 'Unknown error')
                    raise RuntimeError(f"{description} failed: {error_msg}")
                
                if progress >= total and total > 0:
                    elapsed = time.time() - start_time
                    self._log(f"  OK: {description} completed ({elapsed:.1f}s)")
                    return job
                
                current_status = f"{progress}/{total}"
                if current_status != last_status:
                    self._log(f"    Progress: {current_status}")
                    last_status = current_status
            
            # Check for status-based format
            elif 'status' in job:
                status = job['status']
                
                if status != last_status:
                    self._log(f"    Status: {status}")
                    last_status = status
                
                if status == 'succeeded':
                    elapsed = time.time() - start_time
                    self._log(f"  OK: {description} completed ({elapsed:.1f}s)")
                    return job
                
                if status == 'failed':
                    error = job.get('error', 'Unknown error')
                    raise RuntimeError(f"{description} failed: {error}")
            
            # Check for state-based format (GenAI Platform)
            elif 'state' in job:
                state = job['state']
                
                if state != last_status:
                    self._log(f"    State: {state}")
                    last_status = state
                
                if state in ['done', 'finished', 'completed']:
                    elapsed = time.time() - start_time
                    self._log(f"  OK: {description} completed ({elapsed:.1f}s)")
                    return job
                
                if state in ['failed', 'error']:
                    error = job.get('error', 'Unknown error')
                    raise RuntimeError(f"{description} failed: {error}")
            
            else:
                # Unknown format - log and continue
                if last_status != 'unknown':
                    self._log(f"    Status: unknown format")
                    last_status = 'unknown'
            
            # Check timeout
            if self.current_analysis:
                elapsed = time.time() - start_time
                if elapsed > self.current_analysis.config.timeout_seconds:
                    raise TimeoutError(f"{description} timed out after {elapsed:.0f}s")
            
            time.sleep(poll_interval)
    
    def run_batch(self, configs: List[AnalysisConfig]) -> List[AnalysisResult]:
        """
        Run multiple analyses in sequence.
        
        Each analysis gets its own engine that is cleaned up after completion.
        """
        self._log(f"=== Starting Batch Analysis: {len(configs)} analyses ===")
        
        results = []
        for i, config in enumerate(configs, 1):
            self._log(f"\n--- Analysis {i}/{len(configs)}: {config.name} ---")
            result = self.run_analysis(config)
            results.append(result)
            
            if result.status == AnalysisStatus.COMPLETED:
                self._log(f"OK: Completed {i}/{len(configs)}")
            else:
                self._log(f"ERROR: Failed {i}/{len(configs)}: {result.error_message}")
        
        # Final summary
        self._log(f"\n=== Batch Complete ===")
        completed = sum(1 for r in results if r.status == AnalysisStatus.COMPLETED)
        failed = len(results) - completed
        total_cost = sum(r.estimated_cost_usd or 0 for r in results)
        total_time = sum(r.duration_seconds or 0 for r in results)
        
        self._log(f"Completed: {completed}/{len(results)}")
        self._log(f"Failed: {failed}/{len(results)}")
        self._log(f"Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        if total_cost > 0:
            self._log(f"Total cost: ${total_cost:.4f}")
        
        return results
    
    def get_summary(self, result: AnalysisResult) -> str:
        """Get a human-readable summary of an analysis."""
        lines = [
            f"Analysis: {result.config.name}",
            f"Status: {result.status.value}",
            f"Algorithm: {result.algorithm}",
            f"Duration: {result.duration_seconds:.1f}s",
        ]
        
        if result.vertex_count:
            lines.append(f"Graph: {result.vertex_count:,} vertices, {result.edge_count:,} edges")
        
        if result.documents_updated:
            lines.append(f"Results: {result.documents_updated:,} documents updated")
        
        if result.estimated_cost_usd:
            lines.append(f"Cost: ${result.estimated_cost_usd:.4f}")
        
        if result.error_message:
            lines.append(f"Error: {result.error_message}")
        
        return "\n".join(lines)
    
    def save_history(self, filepath: str = "analysis_history.json"):
        """Save analysis history to JSON file."""
        history_data = [r.to_dict() for r in self.analysis_history]
        
        with open(filepath, 'w') as f:
            json.dump(history_data, f, indent=2)
        
        self._log(f"History saved to {filepath}")
    
    def estimate_cost(self, config: AnalysisConfig, 
                      estimated_runtime_minutes: float = 15) -> float:
        """
        Estimate cost for an analysis (AMP only).
        
        Args:
            config: Analysis configuration
            estimated_runtime_minutes: Expected runtime in minutes
            
        Returns:
            Estimated cost in USD (0 for self-managed)
        """
        hourly_cost = self.ENGINE_COSTS.get(config.engine_size, 0)
        return (estimated_runtime_minutes / 60) * hourly_cost

