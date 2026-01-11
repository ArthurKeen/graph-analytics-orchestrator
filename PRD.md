# Product Requirements Document (PRD): Graph Analytics AI Library

**Document Status:** Living Document  
**Last Updated:** December 2025  
**Version:** 1.1.0  
**Purpose:** Common library for orchestrating ArangoDB Graph Analytics Engine (GAE) operations

---

## 1. Introduction and Goals

### 1.1 Project Overview

This library provides a unified interface for orchestrating ArangoDB Graph Analytics Engine (GAE) operations across multiple deployment models. It was extracted from three production projects with overlapping functionality:

1. **Project Alpha** - AMP deployment
2. **Project Beta** - AMP deployment
3. **Project Gamma** - Self-managed deployment

### 1.2 Key Objectives

1. **Unified Interface:** Provide a single, consistent API for GAE operations regardless of deployment model
2. **Deployment Flexibility:** Support both Arango Managed Platform (AMP) and self-managed deployments
3. **Complete Workflow Automation:** Automate the full lifecycle of graph analytics operations
4. **Cost Management:** Track and manage costs for cloud-based deployments
5. **Error Handling:** Robust error handling and retry logic for production use
6. **Code Reusability:** Eliminate duplicate code across projects

### 1.3 Core Requirements

- Support for multiple graph algorithms (PageRank, WCC, SCC, Label Propagation, Betweenness)
- Automatic engine lifecycle management (deploy, use, cleanup)
- Result storage and verification
- Cost tracking for AMP deployments
- Comprehensive error handling and retry logic
- Support for batch analysis workflows
- Configuration via environment variables (.env file)
- **Scope:** Orchestration of GAE jobs (deploy → load → analyze → store → cleanup) with robust error handling and cost tracking

---

## 2. Deployment Models

### 2.1 Arango Managed Platform (AMP)

**Authentication:**
- Uses API keys (`ARANGO_GRAPH_API_KEY_ID`, `ARANGO_GRAPH_API_KEY_SECRET`)
- Generates access tokens via `oasisctl` CLI tool
- Tokens expire after 24 hours (automatic refresh supported)

**Engine Management:**
- Deploy engines via Management API
- Configurable engine sizes (e4, e8, e16, e32, e64, e128)
- Cost tracking based on engine size and runtime

### 2.2 Self-Managed (GenAI Platform)

**Authentication:**
- Uses JWT tokens from ArangoDB (`/_open/auth` endpoint)
- Same credentials as ArangoDB connection
- Tokens obtained via standard ArangoDB authentication

**Engine Management:**
- Start engines via GenAI service API (`/gen-ai/v1/graphanalytics`)
- Engine size not configurable (managed by platform)
- No cost tracking (on-premises deployment)

---

## 3. Core Functionality

### 3.1 Graph Analytics Algorithms

The library supports the following algorithms:

| Algorithm | Use Case | Parameters |
|-----------|----------|------------|
| **PageRank** | Influence analysis, centrality | damping_factor, maximum_supersteps |
| **Weakly Connected Components (WCC)** | Community detection, data quality | None |
| **Strongly Connected Components (SCC)** | Cyclic relationships, temporal analysis | None |
| **Label Propagation** | Community detection, clustering | start_label_attribute, synchronous, random_tiebreak, maximum_supersteps |
| **Betweenness Centrality** | Bridge detection, critical paths | maximum_supersteps |

### 3.2 Workflow Orchestration

The orchestrator automates the complete workflow:

1. **Engine Deployment:** Deploy or start GAE engine
2. **Graph Loading:** Load graph data from ArangoDB collections
3. **Algorithm Execution:** Run the configured algorithm
4. **Result Storage:** Write results back to ArangoDB
5. **Cleanup:** Delete/stop engine to prevent orphaned resources

### 3.3 Error Handling

- **Retry Logic:** Automatic retry for transient errors
- **Non-Retryable Errors:** Configuration errors are not retried
- **Guaranteed Cleanup:** Engines are always cleaned up, even on failure
- **Safety Checks:** Warns about existing engines before deployment

### 3.4 Cost Management (AMP Only)

- Tracks engine runtime
- Calculates estimated costs based on engine size
- Provides cost estimates before analysis
- Logs cost information in analysis results

---

## 4. Configuration

### 4.1 Environment Variables

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

### 4.2 Configuration File

Configuration is managed via `.env` file (see `.env.example` for template).

---

## 5. Usage Examples

### 5.1 Basic Analysis

```python
from graph_analytics_orchestrator import GAEOrchestrator, AnalysisConfig

# Define analysis
config = AnalysisConfig(
    name="product_demand",
    vertex_collections=["users", "products"],
    edge_collections=["clicks"],
    algorithm="pagerank",
    engine_size="e16"
)

# Run analysis
orchestrator = GAEOrchestrator()
result = orchestrator.run_analysis(config)

# Check results
print(f"Status: {result.status}")
print(f"Documents updated: {result.documents_updated}")
print(f"Cost: ${result.estimated_cost_usd}")
```

### 5.2 Batch Analysis

```python
configs = [
    AnalysisConfig(name="analysis1", ...),
    AnalysisConfig(name="analysis2", ...),
]

results = orchestrator.run_batch(configs)
```

### 5.3 Custom Algorithm Parameters

```python
config = AnalysisConfig(
    name="community_detection",
    vertex_collections=["nodes"],
    edge_collections=["edges"],
    algorithm="label_propagation",
    algorithm_params={
        "start_label_attribute": "_key",
        "synchronous": False,
        "maximum_supersteps": 200
    }
)
```

---

## 6. Architecture

### 6.1 Component Structure

```
graph_analytics_orchestrator/
├── __init__.py          # Public API
├── config.py            # Configuration management
├── constants.py         # Shared constants
├── db_connection.py     # ArangoDB connection
├── gae_connection.py    # GAE connection (AMP & self-managed)
├── gae_orchestrator.py  # Workflow orchestration
├── results.py           # Result management helpers
├── queries.py           # Query helpers
├── export.py            # Export utilities
└── utils.py             # Shared utilities
```

### 6.2 Connection Abstraction

The library uses a base class (`GAEConnectionBase`) with two implementations:
- `GAEManager` - For AMP deployments
- `GenAIGAEConnection` - For self-managed deployments

A factory function (`get_gae_connection()`) automatically selects the appropriate implementation based on configuration.

---

## 7. Success Criteria

### 7.1 Functional Requirements

-  Support both AMP and self-managed deployments
-  Automate complete workflow (deploy → load → analyze → store → cleanup)
-  Support all major graph algorithms
-  Handle errors gracefully with retry logic
-  Track costs for AMP deployments
-  Provide clear error messages and logging

### 7.2 Non-Functional Requirements

-  Configuration via environment variables
-  Comprehensive documentation
-  Code reusability (single library for all projects)
-  Backward compatibility where possible

---

## 8. Scope Focus

- Scope: GAE orchestration for AMP and self-managed deployments (deploy → load → analyze → store → cleanup).
- Out of scope: LLM/agentic workflows, PRD/use-case/report generation.

## 9. Future Enhancements (Orchestration)

- Reliability and DX: hardened retries/timeouts, better validation, clearer errors.
- Performance: batch execution optimizations, engine sizing guidance, resource cleanup.
- Observability: structured logging, status/progress reporting, optional metrics/hooks.
- Operations: runbooks, diagnostics/support bundles, improved CLI ergonomics.
- Exports/Integrations: richer export formats and optional scheduler/Airflow-friendly hooks.

## 10. Implementation Roadmap (Orchestration)

- **v1.1.0 — Reliability and DX:** retries/timeouts, config validation, CLI polish, more examples.
- **v1.2.0 — Scaling and Performance:** workload benchmarks, engine sizing guidance, batch/path optimizations, parallelism controls.
- **v1.3.0 — Observability and Ops:** metrics/hooks, status/progress reporting, better failure diagnostics, operational runbooks.

## 11. References

### 11.1 Source Projects

- **Project Alpha:** Entity resolution use case
- **Project Beta:** Consumer behavior analytics platform
- **Project Gamma:** Investigator network analysis

### 11.2 Documentation

- ArangoDB Graph Analytics Engine: https://docs.arangodb.com/stable/arangograph/graph-analytics/
- ArangoDB Python Driver: https://python-arango.readthedocs.io/
- Oasisctl CLI: https://github.com/arangodb-managed/oasisctl

---

## 12. Version History

### Version 1.1.0 (Planned)
- Reliability and DX improvements (retries/timeouts, validation, CLI polish)
- Additional examples and config guidance

### Version 1.0.0 (December 2025)

- Initial release
- Support for AMP and self-managed deployments
- Complete workflow orchestration
- Cost tracking for AMP
- Comprehensive error handling

