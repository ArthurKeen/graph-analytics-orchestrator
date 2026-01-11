"""
Basic usage example for Graph Analytics AI library.

This example demonstrates how to run a simple PageRank analysis.
"""

from graph_analytics_orchestrator import GAEOrchestrator, AnalysisConfig


def main():
    """Run a basic PageRank analysis."""
    
    # Define the analysis configuration
    config = AnalysisConfig(
        name="example_pagerank",
        description="Example PageRank analysis",
        vertex_collections=["users", "products"],
        edge_collections=["interactions"],
        algorithm="pagerank",
        algorithm_params={
            "damping_factor": 0.85,
            "maximum_supersteps": 100
        },
        engine_size="e16",  # AMP only, ignored for self-managed
        target_collection="graph_analysis_results",
        result_field="pagerank_score"
    )
    
    # Create orchestrator
    orchestrator = GAEOrchestrator(verbose=True)
    
    # Run the analysis
    print("Starting analysis...")
    result = orchestrator.run_analysis(config)
    
    # Print results
    print("\n" + "=" * 60)
    print("Analysis Results")
    print("=" * 60)
    print(f"Status: {result.status.value}")
    print(f"Algorithm: {result.algorithm}")
    print(f"Duration: {result.duration_seconds:.1f} seconds")
    
    if result.vertex_count:
        print(f"Graph: {result.vertex_count:,} vertices, {result.edge_count:,} edges")
    
    if result.documents_updated:
        print(f"Documents updated: {result.documents_updated:,}")
    
    if result.estimated_cost_usd:
        print(f"Estimated cost: ${result.estimated_cost_usd:.4f}")
    
    if result.error_message:
        print(f"Error: {result.error_message}")
    
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    main()

