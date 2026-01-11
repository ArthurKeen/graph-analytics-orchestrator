"""
Result Query Helpers

Provides utilities for querying and cross-referencing GAE result collections.
"""

import logging
from typing import Dict, List, Optional, Any
from arango.database import StandardDatabase

logger = logging.getLogger(__name__)


def cross_reference_results(
    db: StandardDatabase,
    collection1: str,
    collection2: str,
    filter1: Optional[str] = None,
    filter2: Optional[str] = None,
    join_fields: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Cross-reference two result collections by 'id' field.
    
    Common use case: Join PageRank and WCC results to find influential
    vertices in connected components.
    
    Args:
        db: ArangoDB database connection
        collection1: First result collection name
        collection2: Second result collection name
        filter1: Optional AQL filter expression for collection1 (e.g., "r.pagerank_influence >= 0.000002")
        filter2: Optional AQL filter expression for collection2 (e.g., "w.component_id == 'nodes/xxx'")
        join_fields: Optional dict mapping collection1 fields to collection2 fields
                    (defaults to {'id': 'id'})
        limit: Optional limit on number of results
        
    Returns:
        List of joined result documents
        
    Example:
        # Find top PageRank vertices in connected component
        results = cross_reference_results(
            db,
            'pagerank_results',
            'wcc_results',
            filter1='r.pagerank_influence >= 0.000002',
            filter2="w.component_id == 'nodes/00Hj-5Cag'",
            limit=100
        )
    """
    if join_fields is None:
        join_fields = {'id': 'id'}
    
    # Build query
    filter1_clause = f"FILTER {filter1}" if filter1 else ""
    filter2_clause = f"FILTER {filter2}" if filter2 else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Get join key fields
    key1, key2 = list(join_fields.items())[0]
    
    query = f"""
    FOR r1 IN {collection1}
      {filter1_clause}
      LET r2 = FIRST(
        FOR r2_inner IN {collection2}
          FILTER r2_inner.{key2} == r1.{key1}
          {filter2_clause}
          RETURN r2_inner
      )
      FILTER r2 != null
      RETURN {{
        id: r1.{key1},
        result1: r1,
        result2: r2
      }}
      {limit_clause}
    """
    
    return list(db.aql.execute(query))


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
) -> List[Dict[str, Any]]:
    """
    Get top influential vertices who are in the connected component.
    
    This is a specialized helper for the common pattern of finding top PageRank
    vertices who are also in the main connected WCC component.
    
    Args:
        db: ArangoDB database connection
        pagerank_collection: PageRank results collection name
        wcc_collection: WCC results collection name
        component_id: Component ID to filter by (if None, uses largest component)
        min_influence: Minimum PageRank influence score (optional)
        limit: Number of top results to return
        include_vertex_details: Whether to join with vertex collection for details
        vertex_collection: Vertex collection name if including details
        vertex_fields: Optional list of vertex fields to include
        
    Returns:
        List of influential connected vertices
    """
    # If component_id not specified, find largest component
    if component_id is None:
        largest_component_query = f"""
        FOR r IN {wcc_collection}
          FILTER r.component_id != null
          COLLECT component = r.component_id WITH COUNT INTO size
          SORT size DESC
          LIMIT 1
          RETURN component
        """
        components = list(db.aql.execute(largest_component_query))
        if not components:
            return []
        component_id = components[0]
    
    # Build filter clauses
    influence_filter = f"AND pr.pagerank_influence >= {min_influence}" if min_influence else ""
    
    # Build vertex join if requested
    if include_vertex_details:
        if vertex_fields is None:
            vertex_fields = ['full_name', 'category', 'email']
        
        vertex_selects = []
        for field in vertex_fields:
            parts = field.split('.')
            vertex_selects.append(f"{parts[-1]}: person.{field}")
        
        person_join = "LET person = DOCUMENT(pr.id)"
        person_return = ', '.join(vertex_selects) + ','
    else:
        person_join = ""
        person_return = ""
    
    query = f"""
    FOR pr IN {pagerank_collection}
      SORT pr.pagerank_influence DESC
      {person_join}
      LET wcc = FIRST(
        FOR w IN {wcc_collection}
          FILTER w.id == pr.id AND w.component_id == @component_id
          RETURN w
      )
      FILTER wcc != null {influence_filter}
      LIMIT {limit}
      RETURN {{
        vertex_id: pr.id,
        {person_return}
        pagerank_influence: pr.pagerank_influence,
        component_id: wcc.component_id,
        in_connected_network: true
      }}
    """
    
    return list(db.aql.execute(query, bind_vars={'component_id': component_id}))


def get_results_with_details(
    db: StandardDatabase,
    result_collection: str,
    vertex_collection: str = 'nodes',
    result_filter: Optional[str] = None,
    fields: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Get result collection data joined with vertex details.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name (e.g., 'pagerank_results')
        vertex_collection: Vertex collection name (e.g., 'nodes')
        result_filter: Optional AQL filter for results (e.g., "r.pagerank_influence >= 0.000002")
        fields: Optional list of vertex fields to include (if None, includes common fields)
        limit: Optional limit on results
        
    Returns:
        List of result documents with vertex details
        
    Example:
        # Get top PageRank results with vertex names
        results = get_results_with_details(
            db,
            'pagerank_results',
            fields=['full_name', 'category'],
            result_filter='r.pagerank_influence >= 0.000002',
            limit=100
        )
    """
    if fields is None:
        fields = ['full_name', 'category', 'email']
    
    # Build field accessors
    field_returns = []
    for field in fields:
        parts = field.split('.')
        accessor = 'person.' + field
        field_returns.append(f"{parts[-1]}: {accessor}")
    
    field_join = ', '.join(field_returns)
    filter_clause = f"FILTER {result_filter}" if result_filter else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
    FOR r IN {result_collection}
      {filter_clause}
      LET person = DOCUMENT(r.id)
      FILTER person != null
      RETURN {{
        result_id: r.id,
        {field_join},
        result_data: r
      }}
      {limit_clause}
    """
    
    return list(db.aql.execute(query))

