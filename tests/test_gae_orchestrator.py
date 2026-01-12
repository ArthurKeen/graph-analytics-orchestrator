"""Tests for GAE orchestrator module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from graph_analytics_orchestrator.gae_orchestrator import (
    GAEOrchestrator,
    AnalysisConfig,
    AnalysisResult,
    AnalysisStatus,
)


class TestAnalysisConfig:
    """Tests for AnalysisConfig dataclass."""

    def test_init_minimal(self, mock_env_amp):
        """Test initialization with minimal parameters."""
        config = AnalysisConfig(
            name="test_analysis", vertex_collections=["v1"], edge_collections=["e1"]
        )

        assert config.name == "test_analysis"
        assert config.algorithm == "pagerank"  # Default
        assert config.engine_size == "e16"  # Default
        assert config.database is not None  # Set in __post_init__

    def test_init_with_params(self, mock_env_amp):
        """Test initialization with all parameters."""
        config = AnalysisConfig(
            name="test_analysis",
            description="Test description",
            vertex_collections=["v1", "v2"],
            edge_collections=["e1"],
            algorithm="wcc",
            engine_size="e32",
            algorithm_params={"custom": "param"},
            target_collection="results",
        )

        assert config.name == "test_analysis"
        assert config.description == "Test description"
        assert config.algorithm == "wcc"
        assert config.engine_size == "e32"
        assert config.algorithm_params == {"custom": "param"}

    def test_default_algorithm_params(self, mock_env_amp):
        """Test that default algorithm parameters are set."""
        config = AnalysisConfig(
            name="test",
            vertex_collections=["v1"],
            edge_collections=["e1"],
            algorithm="pagerank",
        )

        assert "damping_factor" in config.algorithm_params
        assert config.algorithm_params["damping_factor"] == 0.85


class TestGAEOrchestrator:
    """Tests for GAEOrchestrator class."""

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_init(self, mock_db, mock_gae, mock_env_amp):
        """Test orchestrator initialization."""
        mock_gae_conn = MagicMock()
        mock_gae.return_value = mock_gae_conn
        mock_db_conn = MagicMock()
        mock_db.return_value = mock_db_conn

        orchestrator = GAEOrchestrator()

        # Connections should be lazy loaded
        assert orchestrator.gae is None
        assert orchestrator.db is None
        assert orchestrator.verbose is True

        # Initialize
        orchestrator._initialize_connections()
        assert orchestrator.gae is not None
        assert orchestrator.db is not None

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_is_retryable_error(self, mock_db, mock_gae, mock_env_amp):
        """Test retryable error detection."""
        orchestrator = GAEOrchestrator()

        # Non-retryable errors
        assert orchestrator._is_retryable_error("ARANGO_GRAPH_TOKEN not set") is False
        assert orchestrator._is_retryable_error("Configuration error") is False

        # Retryable errors
        assert orchestrator._is_retryable_error("Connection timeout") is True
        assert orchestrator._is_retryable_error("Network error") is True

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_estimate_cost(self, mock_db, mock_gae, mock_env_amp):
        """Test cost estimation."""
        orchestrator = GAEOrchestrator()

        config = AnalysisConfig(
            name="test",
            vertex_collections=["v1"],
            edge_collections=["e1"],
            engine_size="e16",
        )

        cost = orchestrator.estimate_cost(config, estimated_runtime_minutes=30)

        # e16 costs --.40/hour, 30 minutes = 0.5 hours = --.20
        assert cost == 0.20

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_get_summary(self, mock_db, mock_gae, mock_env_amp):
        """Test getting analysis summary."""
        orchestrator = GAEOrchestrator()

        config = AnalysisConfig(
            name="test_analysis", vertex_collections=["v1"], edge_collections=["e1"]
        )

        result = AnalysisResult(
            config=config,
            status=AnalysisStatus.COMPLETED,
            start_time=datetime.now(),
            vertex_count=1000,
            edge_count=5000,
            documents_updated=1000,
            estimated_cost_usd=0.10,
            duration_seconds=60.0,
            algorithm="pagerank",
        )

        summary = orchestrator.get_summary(result)

        assert "test_analysis" in summary
        assert "1,000" in summary
        assert "5,000" in summary
        assert "0.10" in summary

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_run_analysis_success(self, mock_get_db, mock_get_gae, mock_env_amp):
        """Test a full successful analysis run."""
        # Setup mocks
        mock_gae = MagicMock()
        mock_get_gae.return_value = mock_gae
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        # Mock GAE method responses
        mock_gae.list_engines.return_value = []
        mock_gae.deploy_engine.return_value = {"id": "engine123"}
        mock_gae.load_graph.return_value = {"job_id": "load123", "graph_id": "graph123"}
        mock_gae.get_graph.return_value = {"vertex_count": 100, "edge_count": 200}
        mock_gae.run_pagerank.return_value = {"job_id": "algo123"}
        mock_gae.store_results.return_value = {"job_id": "store123"}

        # Mock job status responses
        mock_gae.get_job.side_effect = [
            {"status": "succeeded"},  # load job
            {
                "statistics": {"execution_time_ms": 1000},
                "status": "succeeded",
            },  # algo job
            {"status": "succeeded"},  # store job
        ]

        # Mock DB count
        mock_db.collection.return_value.count.return_value = 100

        # Create orchestrator and run
        orchestrator = GAEOrchestrator()
        config = AnalysisConfig(
            name="test_run",
            vertex_collections=["v1"],
            edge_collections=["e1"],
            algorithm="pagerank",
        )

        result = orchestrator.run_analysis(config)

        # Verify results
        assert result.status == AnalysisStatus.COMPLETED
        assert result.engine_id == "engine123"
        assert result.graph_id == "graph123"
        assert result.job_id == "algo123"
        assert result.vertex_count == 100
        assert result.edge_count == 200
        assert result.documents_updated == 100

        # Verify GAE method calls
        mock_gae.deploy_engine.assert_called_once()
        mock_gae.load_graph.assert_called_once()
        mock_gae.run_pagerank.assert_called_once()
        mock_gae.store_results.assert_called_once()
        mock_gae.delete_engine.assert_called_once_with("engine123")

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_run_analysis_retry_success(self, mock_get_db, mock_get_gae, mock_env_amp):
        """Test analysis run that succeeds after a retry."""
        mock_gae = MagicMock()
        mock_get_gae.return_value = mock_gae
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        # Mock GAE method responses with explicit values to avoid MagicMock formatting errors
        mock_gae.list_engines.return_value = []
        mock_gae.current_engine_id = "old-engine"
        mock_gae.deploy_engine.side_effect = [
            Exception("Transient connection error"),
            {"id": "engine123"},
        ]

        mock_gae.load_graph.return_value = {"job_id": "load123", "graph_id": "graph123"}
        mock_gae.get_graph.return_value = {"vertex_count": 100, "edge_count": 200}
        mock_gae.run_pagerank.return_value = {"job_id": "algo123"}
        mock_gae.store_results.return_value = {"job_id": "store123"}
        mock_gae.get_job.return_value = {"status": "succeeded"}

        orchestrator = GAEOrchestrator()
        config = AnalysisConfig(
            name="test_retry",
            vertex_collections=["v1"],
            edge_collections=["e1"],
            retry_on_failure=True,
            max_retries=1,
        )

        result = orchestrator.run_analysis(config)

        assert result.status == AnalysisStatus.COMPLETED
        assert result.retry_count == 1
        assert mock_gae.deploy_engine.call_count == 2

    @patch("graph_analytics_orchestrator.gae_orchestrator.get_gae_connection")
    @patch("graph_analytics_orchestrator.gae_orchestrator.get_db_connection")
    def test_run_analysis_non_retryable_failure(
        self, mock_get_db, mock_get_gae, mock_env_amp
    ):
        """Test analysis run fails immediately on non-retryable error."""
        mock_gae = MagicMock()
        mock_get_gae.return_value = mock_gae
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        # Configuration error is non-retryable
        mock_gae.list_engines.return_value = []
        mock_gae.current_engine_id = "failed-engine"
        mock_gae.deploy_engine.side_effect = Exception(
            "Invalid configuration: missing API key"
        )

        orchestrator = GAEOrchestrator()
        config = AnalysisConfig(
            name="test_fail",
            vertex_collections=["v1"],
            edge_collections=["e1"],
            retry_on_failure=True,
            max_retries=3,
        )

        result = orchestrator.run_analysis(config)

        assert result.status == AnalysisStatus.FAILED
        assert result.retry_count == 0
        assert mock_gae.deploy_engine.call_count == 1
