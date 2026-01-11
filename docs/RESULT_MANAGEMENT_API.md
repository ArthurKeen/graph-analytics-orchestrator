# Result Management API Documentation

This document provides comprehensive API documentation for the result management, query, and export modules.

---

## Table of Contents

1. [Result Collection Management](#result-collection-management)
2. [Result Query Helpers](#result-query-helpers)
3. [Export Utilities](#export-utilities)
4. [Usage Examples](#usage-examples)

---

## Result Collection Management

### `ensure_result_collection_indexes()`

Ensure indexes exist on 'id' field for result collections.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def ensure_result_collection_indexes(
    db: StandardDatabase,
    collection_names: Optional[List[str]] = None,
    verbose: bool = False
) -> Dict[str, int]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `collection_names` (Optional[List[str]]): List of collection names to index. Defaults to `['pagerank_results', 'wcc_results', 'label_propagation_results']`
- `verbose` (bool): Whether to log progress messages. Default: `False`

**Returns:**
- `Dict[str, int]`: Dictionary with counts:
  - `'created'`: Number of indexes created
  - `'existing'`: Number of indexes that already existed
  - `'missing'`: Number of collections that don't exist

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
result = results.ensure_result_collection_indexes(
    db,
    ['pagerank_results', 'wcc_results'],
    verbose=True
)
print(f"Created: {result['created']}, Existing: {result['existing']}")
```

---

### `verify_result_collection()`

Verify that a result collection has the expected structure.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def verify_result_collection(
    db: StandardDatabase,
    collection_name: str,
    check_id_field: bool = True,
    check_index: bool = True
) -> Dict[str, Any]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `collection_name` (str): Name of result collection to verify
- `check_id_field` (bool): Whether to verify 'id' field exists. Default: `True`
- `check_index` (bool): Whether to verify index on 'id' field exists. Default: `True`

**Returns:**
- `Dict[str, Any]`: Verification results:
  - `'exists'`: Whether collection exists
  - `'count'`: Number of documents in collection
  - `'has_id_field'`: Whether 'id' field exists in documents
  - `'has_index'`: Whether index on 'id' field exists
  - `'valid'`: Overall validity (all checks pass)

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
verification = results.verify_result_collection(db, 'pagerank_results')
if verification['valid']:
    print(f"Collection is valid with {verification['count']} documents")
```

---

### `validate_result_schema()`

Validate that result collection matches expected schema.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def validate_result_schema(
    db: StandardDatabase,
    result_collection: str,
    expected_fields: Optional[List[str]] = None,
    expected_field_types: Optional[Dict[str, type]] = None,
    sample_size: int = 100
) -> Dict[str, Any]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name
- `expected_fields` (Optional[List[str]]): List of required field names. Defaults to `['id']`
- `expected_field_types` (Optional[Dict[str, type]]): Dict mapping field names to expected types
- `sample_size` (int): Number of documents to sample for validation. Default: `100`

**Returns:**
- `Dict[str, Any]`: Validation results:
  - `'valid'`: Overall validity
  - `'has_required_fields'`: Whether all required fields exist
  - `'field_types_match'`: Whether field types match expectations
  - `'sample_count'`: Number of documents sampled
  - `'issues'`: List of validation issues found

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
validation = results.validate_result_schema(
    db,
    'pagerank_results',
    expected_fields=['id', 'pagerank_influence'],
    expected_field_types={'pagerank_influence': float}
)
if not validation['valid']:
    print(f"Issues: {validation['issues']}")
```

---

### `compare_result_collections()`

Compare two result collections (e.g., different runs of same algorithm).

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def compare_result_collections(
    db: StandardDatabase,
    collection1: str,
    collection2: str,
    compare_fields: Optional[List[str]] = None
) -> Dict[str, Any]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `collection1` (str): First result collection name
- `collection2` (str): Second result collection name
- `compare_fields` (Optional[List[str]]): Optional list of fields to compare. If None, compares 'id' overlap only

**Returns:**
- `Dict[str, Any]`: Comparison results:
  - `'collection1_count'`: Document count in first collection
  - `'collection2_count'`: Document count in second collection
  - `'overlap_count'`: Number of overlapping IDs
  - `'overlap_percentage'`: Percentage overlap
  - `'collection1_only'`: Documents only in first collection
  - `'collection2_only'`: Documents only in second collection
  - `'field_differences'`: Dict of field differences (if compare_fields specified)

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
comparison = results.compare_result_collections(
    db,
    'pagerank_results_v1',
    'pagerank_results_v2',
    compare_fields=['pagerank_influence']
)
print(f"Overlap: {comparison['overlap_percentage']:.1f}%")
```

---

### `bulk_update_result_metadata()`

Add metadata fields to all results in a collection.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def bulk_update_result_metadata(
    db: StandardDatabase,
    result_collection: str,
    metadata: Dict[str, Any],
    filter_query: Optional[str] = None,
    batch_size: int = 1000
) -> int
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name
- `metadata` (Dict[str, Any]): Dictionary of metadata fields to add
- `filter_query` (Optional[str]): Optional AQL filter (e.g., `"r.pagerank_influence >= 0.000002"`)
- `batch_size` (int): Batch size for updates. Default: `1000`

**Returns:**
- `int`: Number of documents updated

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
count = results.bulk_update_result_metadata(
    db,
    'pagerank_results',
    {'analysis_date': '2025-01-01', 'version': '1.0'},
    batch_size=1000
)
print(f"Updated {count} documents")
```

---

### `copy_results()`

Copy results from one collection to another with optional filtering/transformation.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def copy_results(
    db: StandardDatabase,
    source_collection: str,
    target_collection: str,
    filter_query: Optional[str] = None,
    transform: Optional[str] = None,
    batch_size: int = 1000
) -> int
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `source_collection` (str): Source result collection name
- `target_collection` (str): Target collection name (will be created if doesn't exist)
- `filter_query` (Optional[str]): Optional AQL filter for source documents
- `transform` (Optional[str]): Optional AQL transform expression (e.g., `"MERGE(r, {new_field: r.pagerank_influence * 1000})"`)
- `batch_size` (int): Batch size for copying. Default: `1000`

**Returns:**
- `int`: Number of documents copied

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
count = results.copy_results(
    db,
    'pagerank_results',
    'top_pagerank_results',
    filter_query='r.pagerank_influence >= 0.000002',
    transform="MERGE(r, {tier: 'high_influence'})"
)
print(f"Copied {count} documents")
```

---

### `delete_results_by_filter()`

Delete results matching a filter query.

**Module:** `graph_analytics_orchestrator.results`

**Signature:**
```python
def delete_results_by_filter(
    db: StandardDatabase,
    result_collection: str,
    filter_query: str,
    batch_size: int = 1000
) -> int
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name
- `filter_query` (str): AQL filter expression (e.g., `"r.pagerank_influence < 0.000001"`)
- `batch_size` (int): Batch size for deletions. Default: `1000`

**Returns:**
- `int`: Number of documents deleted

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, results

db = get_db_connection()
count = results.delete_results_by_filter(
    db,
    'pagerank_results',
    'r.pagerank_influence < 0.000001'
)
print(f"Deleted {count} documents")
```

---

## Result Query Helpers

### `cross_reference_results()`

Cross-reference two result collections by 'id' field.

**Module:** `graph_analytics_orchestrator.queries`

**Signature:**
```python
def cross_reference_results(
    db: StandardDatabase,
    collection1: str,
    collection2: str,
    filter1: Optional[str] = None,
    filter2: Optional[str] = None,
    join_fields: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `collection1` (str): First result collection name
- `collection2` (str): Second result collection name
- `filter1` (Optional[str]): Optional AQL filter for collection1 (e.g., `"r.pagerank_influence >= 0.000002"`)
- `filter2` (Optional[str]): Optional AQL filter for collection2 (e.g., `"w.component_id == 'nodes/xxx'"`)
- `join_fields` (Optional[Dict[str, str]]): Optional dict mapping collection1 fields to collection2 fields. Defaults to `{'id': 'id'}`
- `limit` (Optional[int]): Optional limit on number of results

**Returns:**
- `List[Dict[str, Any]]`: List of joined result documents with structure:
  - `'id'`: The join key value
  - `'result1'`: Document from first collection
  - `'result2'`: Document from second collection

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()
results = queries.cross_reference_results(
    db,
    'pagerank_results',
    'wcc_results',
    filter1='r.pagerank_influence >= 0.000002',
    filter2="w.component_id == 'nodes/00Hj-5Cag'",
    limit=100
)
for result in results:
    print(f"ID: {result['id']}, Influence: {result['result1']['pagerank_influence']}")
```

---

### `get_top_influential_connected()`

Get top influential vertices who are in the connected component.

**Module:** `graph_analytics_orchestrator.queries`

**Signature:**
```python
def get_top_influential_connected(
    db: StandardDatabase,
    pagerank_collection: str = 'pagerank_results',
    wcc_collection: str = 'wcc_results',
    component_id: Optional[str] = None,
    min_influence: Optional[float] = None,
    limit: int = 100,
    include_vertex_details: bool = False,
    vertex_collection: str = 'nodes',
    vertex_fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `pagerank_collection` (str): PageRank results collection name. Default: `'pagerank_results'`
- `wcc_collection` (str): WCC results collection name. Default: `'wcc_results'`
- `component_id` (Optional[str]): Component ID to filter by. If None, uses largest component
- `min_influence` (Optional[float]): Minimum PageRank influence score
- `limit` (int): Number of top results to return. Default: `100`
- `include_vertex_details` (bool): Whether to join with vertex collection for details. Default: `False`
- `vertex_collection` (str): Vertex collection name if including details. Default: `'nodes'`
- `vertex_fields` (Optional[List[str]]): Optional list of vertex fields to include

**Returns:**
- `List[Dict[str, Any]]`: List of influential connected vertices with structure:
  - `'vertex_id'`: Vertex document ID
  - `'pagerank_influence'`: PageRank influence score
  - `'component_id'`: Component ID
  - `'in_connected_network'`: Always `True`
  - Additional vertex fields if `include_vertex_details=True`

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()
results = queries.get_top_influential_connected(
    db,
    min_influence=0.000002,
    limit=50,
    include_vertex_details=True,
    vertex_fields=['full_name']
)
for result in results:
    print(f"{result.get('full_name', 'Unknown')}: {result['pagerank_influence']}")
```

---

### `get_results_with_details()`

Get result collection data joined with vertex details.

**Module:** `graph_analytics_orchestrator.queries`

**Signature:**
```python
def get_results_with_details(
    db: StandardDatabase,
    result_collection: str,
    vertex_collection: str = 'nodes',
    result_filter: Optional[str] = None,
    fields: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name (e.g., `'pagerank_results'`)
- `vertex_collection` (str): Vertex collection name (e.g., `'nodes'`). Default: `'nodes'`
- `result_filter` (Optional[str]): Optional AQL filter for results (e.g., `"r.pagerank_influence >= 0.000002"`)
- `fields` (Optional[List[str]]): Optional list of vertex fields to include. Defaults to `['full_name', 'category', 'email']`
- `limit` (Optional[int]): Optional limit on results

**Returns:**
- `List[Dict[str, Any]]`: List of result documents with vertex details:
  - `'result_id'`: Original result document ID
  - `'result_data'`: Original result document
  - Additional fields from vertex collection

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, queries

db = get_db_connection()
results = queries.get_results_with_details(
    db,
    'pagerank_results',
    fields=['full_name', 'category'],
    result_filter='r.pagerank_influence >= 0.000002',
    limit=100
)
for result in results:
    print(f"{result.get('full_name')}: {result['result_data']['pagerank_influence']}")
```

---

## Export Utilities

### `export_results_to_csv()`

Export result collection to CSV file.

**Module:** `graph_analytics_orchestrator.export`

**Signature:**
```python
def export_results_to_csv(
    db: StandardDatabase,
    result_collection: str,
    output_path: Union[str, Path],
    query: Optional[str] = None,
    fields: Optional[List[str]] = None,
    include_headers: bool = True,
    join_vertex: bool = False,
    vertex_collection: str = 'nodes',
    vertex_fields: Optional[List[str]] = None
) -> int
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name
- `output_path` (Union[str, Path]): Path to output CSV file
- `query` (Optional[str]): Optional custom AQL query. If None, exports all results
- `fields` (Optional[List[str]]): Optional list of fields to export. If None, exports all fields
- `include_headers` (bool): Whether to include CSV headers. Default: `True`
- `join_vertex` (bool): Whether to join with vertex collection for additional details. Default: `False`
- `vertex_collection` (str): Vertex collection name if joining. Default: `'nodes'`
- `vertex_fields` (Optional[List[str]]): Optional list of vertex fields to include

**Returns:**
- `int`: Number of rows exported

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()
count = export.export_results_to_csv(
    db,
    'pagerank_results',
    'top_influencers.csv',
    query="FOR r IN pagerank_results FILTER r.pagerank_influence >= 0.000002 SORT r.pagerank_influence DESC LIMIT 1000 RETURN r",
    join_vertex=True,
    vertex_fields=['full_name', 'category']
)
print(f"Exported {count} rows")
```

---

### `export_results_to_json()`

Export result collection to JSON file.

**Module:** `graph_analytics_orchestrator.export`

**Signature:**
```python
def export_results_to_json(
    db: StandardDatabase,
    result_collection: str,
    output_path: Union[str, Path],
    query: Optional[str] = None,
    pretty: bool = True,
    join_vertex: bool = False,
    vertex_collection: str = 'nodes',
    vertex_fields: Optional[List[str]] = None
) -> int
```

**Parameters:**
- `db` (StandardDatabase): ArangoDB database connection
- `result_collection` (str): Result collection name
- `output_path` (Union[str, Path]): Path to output JSON file
- `query` (Optional[str]): Optional custom AQL query. If None, exports all results
- `pretty` (bool): Whether to pretty-print JSON. Default: `True`
- `join_vertex` (bool): Whether to join with vertex collection. Default: `False`
- `vertex_collection` (str): Vertex collection name if joining. Default: `'nodes'`
- `vertex_fields` (Optional[List[str]]): Optional list of vertex fields to include

**Returns:**
- `int`: Number of documents exported

**Example:**
```python
from graph_analytics_orchestrator import get_db_connection, export

db = get_db_connection()
count = export.export_results_to_json(
    db,
    'wcc_results',
    'components.json',
    query="FOR r IN wcc_results FILTER r.component_id != null RETURN r",
    pretty=True
)
print(f"Exported {count} documents")
```

---

## Usage Examples

See [RESULT_MANAGEMENT_EXAMPLES.md](RESULT_MANAGEMENT_EXAMPLES.md) for comprehensive usage examples.

