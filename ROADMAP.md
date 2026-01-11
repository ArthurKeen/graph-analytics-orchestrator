# Graph Analytics Orchestrator - Roadmap

## Overview

Focused plan for the Graph Analytics Orchestrator: core GAE automation and robustness improvements. Scope excludes LLM/agentic workflows and PRD/use-case generation; the roadmap targets deploy → load → analyze → store → cleanup.

## Current Status (v1.2.0)

- Core GAE orchestration (AMP and self-managed)
- Full workflow automation (deploy → load → analyze → store → cleanup)
- Cost tracking for AMP deployments
- Robust error handling
- Test suite (42 unit tests)
- Security improvements (password masking, command injection prevention)
- Migration guides for source projects

## Upcoming Milestones

### v1.1.0 — Reliability and DX
- Harden retries and timeouts for long-running jobs
- Improve logging/structured events
- Add better validation for configs and inputs
- Enhance CLI ergonomics (flags, examples)
- Add examples for common algorithms and batch runs

### v1.2.0 — Scaling and Performance
- Benchmark common workloads (AMP and self-managed)
- Add guidance for engine sizing and cost/perf tradeoffs
- Optimize batch execution paths
- Improve parallelism controls and resource cleanup

### v1.3.0 — Observability and Ops
- Add optional metrics/export hooks (Prometheus-friendly)
- Add richer status reporting and progress tracking
- Improve failure diagnostics and support bundles
- Document operational runbooks

## Long-Term Opportunities

- Multi-tenant orchestration
- Pluggable storage backends for results
- Enhanced export formats (Parquet, Arrow)
- UI/dashboard for orchestration status
- Result versioning and lineage tracking
- SLA monitoring and alerting

