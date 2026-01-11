# Graph Analytics Orchestrator

A unified Python library for orchestrating ArangoDB Graph Analytics Engine (GAE) operations across both Arango Managed Platform (AMP) and self-managed deployments. Scope is limited to GAE orchestration (deploy → load → analyze → store → cleanup); LLM/agentic workflows and PRD/use-case generation are out of scope.

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Features

- **Unified Interface** - Single API for both AMP and self-managed deployments
- **Complete Automation** - Full workflow orchestration (deploy → load → analyze → store → cleanup)
- **Multiple Algorithms** - Support for PageRank, WCC, SCC, Label Propagation, and more
- **Result Management** - Index management, validation, comparison, and batch operations
- **Query Helpers** - Cross-reference results, find top influential vertices, join with vertex details
- **Export Utilities** - Export results to CSV and JSON formats
- **Cost Tracking** - Automatic cost calculation for AMP deployments
- **Error Handling** - Robust retry logic and guaranteed cleanup
- **Easy Configuration** - Simple `.env` file-based configuration
- **Production Ready** - Extracted from three production projects

## Quick Start

### Installation

```bash
# Install from source (development)
git clone https://github.com/ArthurKeen/graph-analytics-orchestrator.git
cd graph-analytics-orchestrator
pip install -e .

# Or install from PyPI (when published)
pip install graph-analytics-orchestrator
```

### Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:

**For AMP (Arango Managed Platform):**
```bash
# ArangoDB Connection
ARANGO_ENDPOINT=https://your-cluster.arangodb.cloud:8529
ARANGO_USER=root
ARANGO_PASSWORD=your-password
ARANGO_DATABASE=your-database

# GAE Configuration
GAE_DEPLOYMENT_MODE=amp
ARANGO_GRAPH_API_KEY_ID=your-api-key-id
ARANGO_GRAPH_API_KEY_SECRET=your-api-key-secret
ARANGO_GAE_PORT=8829
```

**For Self-Managed:**
```bash
# ArangoDB Connection
ARANGO_ENDPOINT=https://your-endpoint:8529
ARANGO_USER=root
ARANGO_PASSWORD=your-password
ARANGO_DATABASE=your-database
ARANGO_VERIFY_SSL=false

# GAE Configuration
GAE_DEPLOYMENT_MODE=self_managed
# No additional credentials needed
```

### Basic Usage

```python
from graph_analytics_orchestrator import GAEOrchestrator, AnalysisConfig

# Define your analysis
config = AnalysisConfig(
    name="product_demand",
    description="PageRank analysis of product demand",
    vertex_collections=["users", "products"],
    edge_collections=["clicks"],
    algorithm="pagerank",
    engine_size="e16",  # AMP only, ignored for self-managed
    target_collection="graph_analysis_results"
)

# Run the analysis (fully automated)
orchestrator = GAEOrchestrator()
result = orchestrator.run_analysis(config)

# Check results
print(f"Status: {result.status}")
print(f"Documents updated: {result.documents_updated}")
print(f"Cost: ${result.estimated_cost_usd}")  # AMP only
```

## Documentation

- **[PRD.md](PRD.md)** - Product Requirements Document
- **[Result Management API](docs/RESULT_MANAGEMENT_API.md)** - API documentation for result management, queries, and export
- **[Result Management Examples](docs/RESULT_MANAGEMENT_EXAMPLES.md)** - Usage examples for result operations
- **[Examples](#examples)** - Code examples below

## Supported Algorithms

| Algorithm | Use Case | Parameters |
|-----------|----------|------------|
| **PageRank** | Influence analysis, centrality | `damping_factor`, `maximum_supersteps` |
| **WCC** | Community detection, data quality | None |
| **SCC** | Cyclic relationships, temporal analysis | None |
| **Label Propagation** | Community detection, clustering | `start_label_attribute`, `synchronous`, `random_tiebreak`, `maximum_supersteps` |

## Examples

### PageRank Analysis

```python
from graph_analytics_orchestrator import GAEOrchestrator, AnalysisConfig

config = AnalysisConfig(
    name="user_influence",
    vertex_collections=["users"],
    edge_collections=["follows"],
    algorithm="pagerank",
    algorithm_params={
        "damping_factor": 0.85,
        "maximum_supersteps": 100
    },
    target_collection="users",
    result_field="pagerank_score"
)

orchestrator = GAEOrchestrator()
result = orchestrator.run_analysis(config)
```

### Community Detection (Label Propagation)

```python
config = AnalysisConfig(
    name="product_communities",
    vertex_collections=["products"],
    edge_collections=["co_purchased"],
    algorithm="label_propagation",
    algorithm_params={
        "start_label_attribute": "_key",
        "synchronous": False,
        "maximum_supersteps": 200
    },
    target_collection="products",
    result_field="community_id"
)

orchestrator = GAEOrchestrator()
result = orchestrator.run_analysis(config)
```

### Weakly Connected Components

```python
config = AnalysisConfig(
    name="entity_resolution",
    vertex_collections=["entities"],
    edge_collections=["similarity_edges"],
    algorithm="wcc",
    target_collection="entities",
    result_field="component_id"
)

orchestrator = GAEOrchestrator()
result = orchestrator.run_analysis(config)
```

### Batch Analysis

```python
configs = [
    AnalysisConfig(name="analysis1", vertex_collections=["v1"], edge_collections=["e1"], algorithm="pagerank"),
    AnalysisConfig(name="analysis2", vertex_collections=["v2"], edge_collections=["e2"], algorithm="wcc"),
]

orchestrator = GAEOrchestrator()
results = orchestrator.run_batch(configs)

for result in results:
    print(f"{result.config.name}: {result.status}")
```

## Architecture

### Component Structure

```
graph_analytics_orchestrator/
├── __init__.py          # Public API
├── config.py            # Configuration management
├── db_connection.py     # ArangoDB connection
├── gae_connection.py    # GAE connection (AMP & self-managed)
├── gae_orchestrator.py  # Workflow orchestration
├── results.py           # Result collection management & batch operations
├── queries.py           # Result query helpers
├── export.py            # Export utilities (CSV, JSON)
└── utils.py             # Utility functions
```

### Deployment Models

**Arango Managed Platform (AMP):**
- Uses API keys and `oasisctl` for authentication
- Configurable engine sizes (e4, e8, e16, e32, e64, e128)
- Cost tracking based on engine size and runtime

**Self-Managed (GenAI Platform):**
- Uses JWT tokens from ArangoDB
- Engine size managed by platform
- No cost tracking (on-premises)

## Workflow

The orchestrator automates the complete workflow:

1. **Engine Deployment** - Deploy or start GAE engine
2. **Graph Loading** - Load graph data from ArangoDB collections
3. **Algorithm Execution** - Run the configured algorithm
4. **Result Storage** - Write results back to ArangoDB
5. **Cleanup** - Delete/stop engine to prevent orphaned resources

All steps include error handling, retry logic, and guaranteed cleanup.

## Cost Management (AMP Only)

The library automatically tracks costs for AMP deployments:

```python
result = orchestrator.run_analysis(config)
print(f"Cost: ${result.estimated_cost_usd}")
print(f"Runtime: {result.engine_runtime_minutes} minutes")
```

Engine costs (approximate, per hour):
- e4: $0.20
- e8: $0.30
- e16: $0.40
- e32: $0.80
- e64: $1.60
- e128: $3.20

## Error Handling

The library includes comprehensive error handling:

- **Automatic Retry** - Transient errors are automatically retried
- **Non-Retryable Errors** - Configuration errors are not retried
- **Guaranteed Cleanup** - Engines are always cleaned up, even on failure
- **Safety Checks** - Warns about existing engines before deployment

## Migration from Existing Projects

If you're migrating from prior projects (AMP or self-managed), the library provides a unified interface that simplifies the orchestration logic. See the examples below for common patterns.

## Requirements

- Python 3.8+
- ArangoDB cluster (AMP or self-managed)
- For AMP: `oasisctl` CLI tool (for token generation)

## Installation

### Dependencies

```bash
pip install python-arango requests python-dotenv
```

### Optional: Development Dependencies

```bash
pip install pytest pytest-cov black flake8 mypy
```

## Configuration Reference

### Environment Variables

**Required for All Deployments:**
- `ARANGO_ENDPOINT` - ArangoDB endpoint URL
- `ARANGO_USER` - Database username
- `ARANGO_PASSWORD` - Database password
- `ARANGO_DATABASE` - Database name

**For AMP Deployments:**
- `GAE_DEPLOYMENT_MODE=amp`
- `ARANGO_GRAPH_API_KEY_ID` - API key ID
- `ARANGO_GRAPH_API_KEY_SECRET` - API key secret
- `ARANGO_GRAPH_TOKEN` - (Optional) Pre-generated token
- `ARANGO_GAE_PORT` - (Optional) GAE port (default: 8829)

**For Self-Managed Deployments:**
- `GAE_DEPLOYMENT_MODE=self_managed`
- No additional GAE credentials needed

## API Reference

### GAEOrchestrator

Main orchestrator class for running analyses.

```python
orchestrator = GAEOrchestrator(verbose=True)
result = orchestrator.run_analysis(config)
results = orchestrator.run_batch(configs)
```

### AnalysisConfig

Configuration for a GAE analysis.

```python
config = AnalysisConfig(
    name="analysis_name",
    vertex_collections=["collection1", "collection2"],
    edge_collections=["edge_collection"],
    algorithm="pagerank",
    engine_size="e16",
    target_collection="results",
    algorithm_params={...}
)
```

### AnalysisResult

Result object from a completed analysis.

```python
result.status  # AnalysisStatus enum
result.vertex_count  # Number of vertices
result.edge_count  # Number of edges
result.documents_updated  # Documents updated
result.estimated_cost_usd  # Cost (AMP only)
result.duration_seconds  # Runtime
```

## Contributing

Contributions are welcome! Please see the contributing guidelines (to be added).

## License

MIT License - see LICENSE file for details.

## Support

For issues, questions, or contributions:

1. Review the [PRD](PRD.md) for detailed documentation
2. Open an issue on GitHub

## Changelog

### Version 1.0.0 (December 2025)

- Initial release
- Support for AMP and self-managed deployments
- Complete workflow orchestration
- Cost tracking for AMP
- Comprehensive error handling

## Acknowledgments

- ArangoDB team for the Graph Analytics Engine

