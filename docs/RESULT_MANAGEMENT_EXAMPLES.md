# Result Management - Usage Examples

This document provides practical examples for using the result management, query, and export modules.

---

## Table of Contents

1. [Basic Setup](#basic-setup)
2. [Result Collection Management](#result-collection-management)
3. [Querying Results](#querying-results)
4. [Exporting Results](#exporting-results)
5. [Complete Workflow Example](#complete-workflow-example)

---

## Basic Setup

### Using Connection Class (Backward Compatible)

```python
from graph_analytics_orchestrator import GenAIGAEConnection

# Initialize connection
gae = GenAIGAEConnection()

# All methods available directly on connection object
gae.ensure_result_collection_indexes(['pagerank_results'])
results = gae.cross_reference_results('pagerank_results', 'wcc_results')
gae.export_results_to_csv('pagerank_results', 'output.csv')
```

### Using Module Functions (New Pattern)

```python
from graph_analytics_orchestrator import get_db_connection
from graph_analytics_orchestrator import results, queries, export

# Get database connection
db = get_db_connection()

# Use module functions directly
results.ensure_result_collection_indexes(db, ['pagerank_results'])
results_list = queries.cross_reference_results(db, 'pagerank_results', 'wcc_results')
export.export_results_to_csv(db, 'pagerank_results', 'output.csv')
```

---

## Result Collection Management

### Example 1: Ensure Indexes on Result Collections

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Ensure indexes on common result collections
index_result = results.ensure_result_collection_indexes(
    db,
    ['pagerank_results', 'wcc_results', 'label_propagation_results'],
    verbose=True
)

print(f"Created: {index_result['created']} indexes")
print(f"Existing: {index_result['existing']} indexes")
print(f"Missing: {index_result['missing']} collections")
```

### Example 2: Verify Result Collection Structure

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Verify a result collection
verification = results.verify_result_collection(
    db,
    'pagerank_results',
    check_id_field=True,
    check_index=True
)

if verification['valid']:
    print(f"Collection is valid with {verification['count']} documents")
    print(f"Has 'id' field: {verification['has_id_field']}")
    print(f"Has index: {verification['has_index']}")
else:
    print("Collection validation failed")
    if not verification['exists']:
        print("Collection does not exist")
```

### Example 3: Validate Result Schema

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Validate PageRank results schema
validation = results.validate_result_schema(
    db,
    'pagerank_results',
    expected_fields=['id', 'pagerank_influence'],
    expected_field_types={
        'pagerank_influence': float,
        'id': str
    },
    sample_size=100
)

if validation['valid']:
    print("Schema validation passed")
else:
    print(f"Schema validation failed: {validation['issues']}")
```

### Example 4: Compare Two Result Collections

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Compare two PageRank runs
comparison = results.compare_result_collections(
    db,
    'pagerank_results_v1',
    'pagerank_results_v2',
    compare_fields=['pagerank_influence']
)

print(f"Collection 1: {comparison['collection1_count']} documents")
print(f"Collection 2: {comparison['collection2_count']} documents")
print(f"Overlap: {comparison['overlap_count']} ({comparison['overlap_percentage']:.1f}%)")
print(f"Field differences: {comparison['field_differences']}")
```

### Example 5: Bulk Update Metadata

```python
from graph_analytics_orchestrator import get_db_connection, results
from datetime import datetime

db = get_db_connection()

# Add metadata to all results
metadata = {
    'analysis_date': datetime.now().strftime('%Y-%m-%d'),
    'version': '1.0',
    'algorithm': 'pagerank'
}

count = results.bulk_update_result_metadata(
    db,
    'pagerank_results',
    metadata,
    batch_size=1000
)

print(f"Updated {count} documents with metadata")
```

### Example 6: Copy Results with Filter

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Copy top results to a new collection
count = results.copy_results(
    db,
    'pagerank_results',
    'top_pagerank_results',
    filter_query='r.pagerank_influence >= 0.000002',
    transform="MERGE(r, {tier: 'high_influence'})",
    batch_size=1000
)

print(f"Copied {count} documents to top_pagerank_results")
```

### Example 7: Delete Low-Value Results

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

# Delete low-influence results
count = results.delete_results_by_filter(
    db,
    'pagerank_results',
    'r.pagerank_influence < 0.000001',
    batch_size=1000
)

print(f"Deleted {count} low-influence documents")
```

---

## Querying Results

### Example 1: Cross-Reference PageRank and WCC Results

```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()

# Find influential vertices in specific connected component
results = queries.cross_reference_results(
    db,
    'pagerank_results',
    'wcc_results',
    filter1='r.pagerank_influence >= 0.000002',
    filter2="w.component_id == 'nodes/00Hj-5Cag'",
    limit=100
)

print(f"Found {len(results)} influential vertices in component")
for result in results:
    pr_data = result['result1']
    wcc_data = result['result2']
    print(f"ID: {result['id']}, Influence: {pr_data['pagerank_influence']}, Component: {wcc_data['component_id']}")
```

### Example 2: Get Top Influential Connected Vertices

```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()

# Get top 50 influential vertices in largest connected component
results = queries.get_top_influential_connected(
    db,
    pagerank_collection='pagerank_results',
    wcc_collection='wcc_results',
    min_influence=0.000002,
    limit=50,
    include_vertex_details=True,
    vertex_fields=['full_name', 'category']
)

print(f"Top {len(results)} influential connected vertices:")
for result in results:
    name = result.get('full_name', 'Unknown')
    influence = result['pagerank_influence']
    print(f"{name}: {influence:.6f}")
```

### Example 3: Get Results with Vertex Details

```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()

# Get PageRank results with person details
results = queries.get_results_with_details(
    db,
    'pagerank_results',
    vertex_collection='nodes',
    result_filter='r.pagerank_influence >= 0.000002',
    fields=['full_name', 'category', 'email'],
    limit=100
)

print(f"Found {len(results)} results with details")
for result in results:
    name = result.get('full_name', 'Unknown')
    influence = result['result_data']['pagerank_influence']
    specialties = result.get('category', [])
    print(f"{name}: {influence:.6f}, Specialties: {specialties}")
```

---

## Exporting Results

### Example 1: Export to CSV

```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()

# Export top PageRank results to CSV
count = export.export_results_to_csv(
    db,
    'pagerank_results',
    'top_influencers.csv',
    query="""
    FOR r IN pagerank_results
      FILTER r.pagerank_influence >= 0.000002
      SORT r.pagerank_influence DESC
      LIMIT 1000
      RETURN r
    """,
    include_headers=True
)

print(f"Exported {count} rows to top_influencers.csv")
```

### Example 2: Export to CSV with Vertex Join

```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()

# Export with vertex details
count = export.export_results_to_csv(
    db,
    'pagerank_results',
    'influencers_with_details.csv',
    join_vertex=True,
    vertex_fields=['full_name', 'category', 'email'],
    include_headers=True
)

print(f"Exported {count} rows with vertex details")
```

### Example 3: Export to JSON

```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()

# Export WCC results to JSON
count = export.export_results_to_json(
    db,
    'wcc_results',
    'components.json',
    query="""
    FOR r IN wcc_results
      FILTER r.component_id != null
      RETURN r
    """,
    pretty=True
)

print(f"Exported {count} documents to components.json")
```

### Example 4: Export to JSON with Vertex Join

```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()

# Export with vertex details
count = export.export_results_to_json(
    db,
    'pagerank_results',
    'influencers.json',
    join_vertex=True,
    vertex_fields=['full_name', 'category'],
    pretty=True
)

print(f"Exported {count} documents with vertex details")
```

---

## Complete Workflow Example

```python
"""
Complete workflow: Run analysis, manage results, query, and export.
"""

from graph_analytics_orchestrator import (
    GenAIGAEConnection,
    AnalysisConfig,
    GAEOrchestrator,
    get_db_connection
)
from graph_analytics_orchestrator import results, queries, export

# Step 1: Run analysis
orchestrator = GAEOrchestrator()
config = AnalysisConfig(
    name="network_analysis",
    vertex_collections=["nodes"],
    edge_collections=["edges"],
    algorithm="pagerank",
    target_collection="pagerank_results"
)
result = orchestrator.run_analysis(config)

# Step 2: Ensure indexes for efficient querying
db = get_db_connection()
results.ensure_result_collection_indexes(
    db,
    ['pagerank_results', 'wcc_results'],
    verbose=True
)

# Step 3: Verify result collection
verification = results.verify_result_collection(db, 'pagerank_results')
if not verification['valid']:
    print("Warning: Result collection validation failed")

# Step 4: Query top influential vertices
top_results = queries.get_top_influential_connected(
    db,
    min_influence=0.000002,
    limit=100,
    include_vertex_details=True,
    vertex_fields=['full_name']
)

# Step 5: Export results
export.export_results_to_csv(
    db,
    'pagerank_results',
    'network_analysis_results.csv',
    join_vertex=True,
    vertex_fields=['full_name', 'category']
)

# Step 6: Add metadata
results.bulk_update_result_metadata(
    db,
    'pagerank_results',
    {
        'analysis_date': '2025-01-01',
        'version': '1.0',
        'algorithm': 'pagerank'
    }
)

print("Workflow complete!")
```

---

## Tips and Best Practices

1. **Always ensure indexes** before running queries on large result collections
2. **Verify collections** after running analyses to ensure data quality
3. **Use filters** in queries to reduce data transfer and improve performance
4. **Batch operations** are automatically handled for large collections
5. **Export with vertex joins** when you need human-readable output
6. **Validate schemas** when working with multiple analysis runs
7. **Compare collections** to track changes between analysis versions

---

## Error Handling

```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()

try:
    # Attempt to verify collection
    verification = results.verify_result_collection(db, 'pagerank_results')
    if not verification['valid']:
        print(f"Validation issues: {verification}")
except Exception as e:
    print(f"Error verifying collection: {e}")
```

---

For more information, see [RESULT_MANAGEMENT_API.md](RESULT_MANAGEMENT_API.md).

