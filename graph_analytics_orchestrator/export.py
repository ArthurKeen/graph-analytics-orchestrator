"""
Result Export Utilities

Provides utilities for exporting GAE result collections to CSV and JSON formats.
"""

import csv
import json
import logging
from pathlib import Path
from typing import List, Optional, Union, Any
from arango.database import StandardDatabase

logger = logging.getLogger(__name__)


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
) -> int:
    """
    Export result collection to CSV file.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name
        output_path: Path to output CSV file
        query: Optional custom AQL query (if None, exports all results)
        fields: Optional list of fields to export (if None, exports all fields)
        include_headers: Whether to include CSV headers
        join_vertex: Whether to join with vertex collection for additional details
        vertex_collection: Vertex collection name if joining
        vertex_fields: Optional list of vertex fields to include
        
    Returns:
        Number of rows exported
        
    Example:
        # Export top PageRank results to CSV
        count = export_results_to_csv(
            db,
            'pagerank_results',
            'top_influencers.csv',
            query="FOR r IN pagerank_results FILTER r.pagerank_influence >= 0.000002 SORT r.pagerank_influence DESC LIMIT 1000 RETURN r",
            join_vertex=True,
            vertex_fields=['full_name', 'category']
        )
    """
    output_path = Path(output_path)
    
    # Build query if not provided
    if query is None:
        if join_vertex:
            vertex_selects = []
            if vertex_fields:
                for field in vertex_fields:
                    parts = field.split('.')
                    vertex_selects.append(f"{parts[-1]}: person.{field}")
            
            vertex_join = ', ' + ', '.join(vertex_selects) if vertex_selects else ''
            
            query = f"""
            FOR r IN {result_collection}
              LET person = DOCUMENT(r.id)
              RETURN {{
                vertex_id: r.id,
                {', '.join(fields) if fields else '*'},
                {vertex_join}
              }}
            """
        else:
            field_list = ', '.join(fields) if fields else '*'
            query = f"FOR r IN {result_collection} RETURN {field_list}"
    
    # Execute query
    try:
        results = list(db.aql.execute(query))
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise
    
    if not results:
        logger.warning(f"No results found for collection '{result_collection}'")
        return 0
    
    # Write to CSV
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if not results:
                return 0
            
            # Handle case where results might be strings or dicts
            if isinstance(results[0], dict):
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                if include_headers:
                    writer.writeheader()
                writer.writerows(results)
            else:
                # If results are not dicts, write as-is
                writer = csv.writer(f)
                if include_headers and results:
                    writer.writerow(results[0].keys() if isinstance(results[0], dict) else [])
                writer.writerows(results)
        
        logger.info(f"Exported {len(results)} rows to {output_path}")
        return len(results)
    except Exception as e:
        logger.error(f"Failed to write CSV file: {e}")
        raise


def export_results_to_json(
    db: StandardDatabase,
    result_collection: str,
    output_path: Union[str, Path],
    query: Optional[str] = None,
    pretty: bool = True,
    join_vertex: bool = False,
    vertex_collection: str = 'nodes',
    vertex_fields: Optional[List[str]] = None
) -> int:
    """
    Export result collection to JSON file.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name
        output_path: Path to output JSON file
        query: Optional custom AQL query
        pretty: Whether to pretty-print JSON
        join_vertex: Whether to join with vertex collection
        vertex_collection: Vertex collection name if joining
        vertex_fields: Optional list of vertex fields to include
        
    Returns:
        Number of documents exported
        
    Example:
        # Export WCC results to JSON
        count = export_results_to_json(
            db,
            'wcc_results',
            'components.json',
            query="FOR r IN wcc_results FILTER r.component_id != null RETURN r"
        )
    """
    output_path = Path(output_path)
    
    # Build query if not provided
    if query is None:
        if join_vertex:
            vertex_selects = []
            if vertex_fields:
                for field in vertex_fields:
                    parts = field.split('.')
                    vertex_selects.append(f"{parts[-1]}: person.{field}")
            
            vertex_join = ', ' + ', '.join(vertex_selects) if vertex_selects else ''
            
            query = f"""
            FOR r IN {result_collection}
              LET person = DOCUMENT(r.id)
              RETURN {{
                vertex_id: r.id,
                result: r,
                {vertex_join}
              }}
            """
        else:
            query = f"FOR r IN {result_collection} RETURN r"
    
    # Execute query
    try:
        results = list(db.aql.execute(query))
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise
    
    if not results:
        logger.warning(f"No results found for collection '{result_collection}'")
        return 0
    
    # Write to JSON
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2 if pretty else None, ensure_ascii=False)
        
        logger.info(f"Exported {len(results)} documents to {output_path}")
        return len(results)
    except Exception as e:
        logger.error(f"Failed to write JSON file: {e}")
        raise

