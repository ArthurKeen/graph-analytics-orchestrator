"""
Result Collection Management

Provides utilities for managing GAE result collections, including index management,
validation, comparison, and batch operations.
"""

import logging
from typing import Dict, List, Optional, Any
from arango.database import StandardDatabase

logger = logging.getLogger(__name__)


def ensure_result_collection_indexes(
    db: StandardDatabase,
    collection_names: Optional[List[str]] = None,
    verbose: bool = False
) -> Dict[str, int]:
    """
    Ensure indexes exist on 'id' field for result collections.
    
    GAE stores algorithm results with sequential numeric _key values and places
    the original vertex document ID in an 'id' field. Indexes on this field
    significantly improve cross-collection query performance.
    
    Args:
        db: ArangoDB database connection
        collection_names: List of collection names to index 
                         (defaults to common result collections)
        verbose: Whether to log progress messages
            
    Returns:
        Dictionary with counts: {'created': N, 'existing': M, 'missing': K}
    """
    if collection_names is None:
        collection_names = ['pagerank_results', 'wcc_results', 'label_propagation_results']
    
    indexes_created = 0
    indexes_existing = 0
    collections_missing = 0
    
    for coll_name in collection_names:
        try:
            if not db.has_collection(coll_name):
                if verbose:
                    logger.warning(f"Collection '{coll_name}' does not exist (skipping)")
                collections_missing += 1
                continue
            
            coll = db.collection(coll_name)
            existing_indexes = coll.indexes()
            
            # Check if index on 'id' field already exists
            has_id_index = any(
                idx.get('type') == 'persistent' and 
                'id' in idx.get('fields', [])
                for idx in existing_indexes
            )
            
            if has_id_index:
                if verbose:
                    logger.info(f"{coll_name}: Index on 'id' field already exists")
                indexes_existing += 1
            else:
                # Create index on 'id' field
                index_name = f"idx_{coll_name}_id"
                coll.add_persistent_index(
                    fields=['id'],
                    unique=False,
                    name=index_name
                )
                if verbose:
                    logger.info(f"{coll_name}: Created index on 'id' field")
                indexes_created += 1
                
        except Exception as e:
            if verbose:
                logger.error(f"Failed to process collection '{coll_name}': {e}")
            collections_missing += 1
    
    return {
        'created': indexes_created,
        'existing': indexes_existing,
        'missing': collections_missing
    }


def verify_result_collection(
    db: StandardDatabase,
    collection_name: str,
    check_id_field: bool = True,
    check_index: bool = True
) -> Dict[str, Any]:
    """
    Verify that a result collection has the expected structure.
    
    Validates:
    - Collection exists
    - Has 'id' field (if check_id_field=True)
    - Has index on 'id' field (if check_index=True)
    
    Args:
        db: ArangoDB database connection
        collection_name: Name of result collection to verify
        check_id_field: Whether to verify 'id' field exists
        check_index: Whether to verify index on 'id' field exists
        
    Returns:
        Dictionary with verification results: {
            'exists': bool,
            'count': int,
            'has_id_field': bool,
            'has_index': bool,
            'valid': bool
        }
    """
    result = {
        'exists': False,
        'count': 0,
        'has_id_field': False,
        'has_index': False,
        'valid': False
    }
    
    try:
        if not db.has_collection(collection_name):
            return result
        
        result['exists'] = True
        coll = db.collection(collection_name)
        result['count'] = coll.count()
        
        if check_id_field and result['count'] > 0:
            # Sample a document to check structure
            sample_query = f'FOR doc IN {collection_name} LIMIT 1 RETURN doc'
            try:
                sample = list(db.aql.execute(sample_query))
                if sample and 'id' in sample[0]:
                    result['has_id_field'] = True
                    # Check format (should be document ID like "nodes/xxx")
                    if isinstance(sample[0]['id'], str) and '/' in sample[0]['id']:
                        result['has_id_field'] = True
            except Exception:
                pass
        
        if check_index:
            indexes = coll.indexes()
            result['has_index'] = any(
                idx.get('type') == 'persistent' and 
                'id' in idx.get('fields', [])
                for idx in indexes
            )
        
        # Collection is valid if it exists and has required fields/indexes
        result['valid'] = (
            result['exists'] and
            (not check_id_field or result['has_id_field']) and
            (not check_index or result['has_index'])
        )
        
    except Exception as e:
        result['error'] = str(e)
        result['valid'] = False
    
    return result


def validate_result_schema(
    db: StandardDatabase,
    result_collection: str,
    expected_fields: Optional[List[str]] = None,
    expected_field_types: Optional[Dict[str, type]] = None,
    sample_size: int = 100
) -> Dict[str, Any]:
    """
    Validate that result collection matches expected schema.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name
        expected_fields: List of required field names (defaults to ['id'])
        expected_field_types: Dict mapping field names to expected types
        sample_size: Number of documents to sample for validation
        
    Returns:
        Validation result dictionary with:
        {
            'valid': bool,
            'has_required_fields': bool,
            'field_types_match': bool,
            'sample_count': int,
            'issues': List[str]
        }
    """
    if expected_fields is None:
        expected_fields = ['id']
    
    validation = {
        'valid': False,
        'has_required_fields': False,
        'field_types_match': False,
        'sample_count': 0,
        'issues': []
    }
    
    if not db.has_collection(result_collection):
        validation['issues'].append(f"Collection '{result_collection}' does not exist")
        return validation
    
    coll = db.collection(result_collection)
    count = coll.count()
    
    if count == 0:
        validation['issues'].append("Collection is empty")
        return validation
    
    # Sample documents
    query = f"FOR doc IN {result_collection} LIMIT {sample_size} RETURN doc"
    samples = list(db.aql.execute(query))
    validation['sample_count'] = len(samples)
    
    if not samples:
        validation['issues'].append("Could not sample any documents")
        return validation
    
    # Check required fields
    sample = samples[0]
    missing_fields = [f for f in expected_fields if f not in sample]
    if missing_fields:
        validation['issues'].append(f"Missing required fields: {missing_fields}")
        validation['has_required_fields'] = False
    else:
        validation['has_required_fields'] = True
    
    # Check field types
    if expected_field_types:
        type_mismatches = []
        for field, expected_type in expected_field_types.items():
            if field in sample:
                actual_type = type(sample[field])
                if not isinstance(sample[field], expected_type):
                    type_mismatches.append(
                        f"{field}: expected {expected_type.__name__}, got {actual_type.__name__}"
                    )
        
        if type_mismatches:
            validation['issues'].extend(type_mismatches)
            validation['field_types_match'] = False
        else:
            validation['field_types_match'] = True
    else:
        validation['field_types_match'] = True
    
    # Overall validity
    validation['valid'] = (
        validation['has_required_fields'] and
        validation['field_types_match']
    )
    
    return validation


def compare_result_collections(
    db: StandardDatabase,
    collection1: str,
    collection2: str,
    compare_fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Compare two result collections (e.g., different runs of same algorithm).
    
    Args:
        db: ArangoDB database connection
        collection1: First result collection name
        collection2: Second result collection name
        compare_fields: Optional list of fields to compare (if None, compares 'id' overlap)
        
    Returns:
        Comparison result dictionary:
        {
            'collection1_count': int,
            'collection2_count': int,
            'overlap_count': int,
            'overlap_percentage': float,
            'collection1_only': int,
            'collection2_only': int,
            'field_differences': Dict (if compare_fields specified)
        }
    """
    comparison = {
        'collection1_count': 0,
        'collection2_count': 0,
        'overlap_count': 0,
        'overlap_percentage': 0.0,
        'collection1_only': 0,
        'collection2_only': 0,
        'field_differences': {}
    }
    
    # Get counts
    if db.has_collection(collection1):
        comparison['collection1_count'] = db.collection(collection1).count()
    
    if db.has_collection(collection2):
        comparison['collection2_count'] = db.collection(collection2).count()
    
    if comparison['collection1_count'] == 0 or comparison['collection2_count'] == 0:
        return comparison
    
    # Count overlap
    overlap_query = f"""
    LET ids1 = (
      FOR r IN {collection1}
        RETURN r.id
    )
    LET ids2 = (
      FOR r IN {collection2}
        RETURN r.id
    )
    RETURN LENGTH(INTERSECTION(ids1, ids2))
    """
    
    overlap_result = list(db.aql.execute(overlap_query))
    comparison['overlap_count'] = overlap_result[0] if overlap_result else 0
    
    # Calculate percentages
    if comparison['collection1_count'] > 0:
        comparison['overlap_percentage'] = (
            comparison['overlap_count'] / comparison['collection1_count']
        ) * 100
        comparison['collection1_only'] = (
            comparison['collection1_count'] - comparison['overlap_count']
        )
    
    if comparison['collection2_count'] > 0:
        comparison['collection2_only'] = (
            comparison['collection2_count'] - comparison['overlap_count']
        )
    
    # Compare field values if specified
    if compare_fields:
        for field in compare_fields:
            diff_query = f"""
            FOR r1 IN {collection1}
              LET r2 = FIRST(FOR r2_inner IN {collection2} FILTER r2_inner.id == r1.id RETURN r2_inner)
              FILTER r2 != null AND r1.{field} != r2.{field}
              RETURN 1
            """
            differences = list(db.aql.execute(diff_query))
            comparison['field_differences'][field] = len(differences)
    
    return comparison


def bulk_update_result_metadata(
    db: StandardDatabase,
    result_collection: str,
    metadata: Dict[str, Any],
    filter_query: Optional[str] = None,
    batch_size: int = 1000
) -> int:
    """
    Add metadata fields to all results in a collection.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name
        metadata: Dictionary of metadata fields to add
        filter_query: Optional AQL filter (e.g., "r.pagerank_influence >= 0.000002")
        batch_size: Batch size for updates
        
    Returns:
        Number of documents updated
    """
    coll = db.collection(result_collection)
    
    # Build filter
    filter_clause = f"FILTER {filter_query}" if filter_query else ""
    
    # Update in batches
    updated_count = 0
    offset = 0
    
    while True:
        query = f"""
        FOR r IN {result_collection}
          {filter_clause}
          LIMIT {offset}, {batch_size}
          RETURN r._key
        """
        
        keys = list(db.aql.execute(query))
        if not keys:
            break
        
        # Update batch
        for key in keys:
            doc = coll.get(key)
            if doc:
                doc.update(metadata)
                coll.update(doc)
        
        updated_count += len(keys)
        offset += batch_size
    
    return updated_count


def copy_results(
    db: StandardDatabase,
    source_collection: str,
    target_collection: str,
    filter_query: Optional[str] = None,
    transform: Optional[str] = None,
    batch_size: int = 1000
) -> int:
    """
    Copy results from one collection to another with optional filtering/transformation.
    
    Args:
        db: ArangoDB database connection
        source_collection: Source result collection name
        target_collection: Target collection name (will be created if doesn't exist)
        filter_query: Optional AQL filter for source documents
        transform: Optional AQL transform expression (e.g., "MERGE(r, {new_field: r.pagerank_influence * 1000})")
        batch_size: Batch size for copying
        
    Returns:
        Number of documents copied
    """
    # Ensure target collection exists
    if not db.has_collection(target_collection):
        db.create_collection(target_collection)
    
    target_coll = db.collection(target_collection)
    
    filter_clause = f"FILTER {filter_query}" if filter_query else ""
    transform_clause = transform if transform else "r"
    
    # Copy in batches
    copied_count = 0
    offset = 0
    
    while True:
        query = f"""
        FOR r IN {source_collection}
          {filter_clause}
          LIMIT {offset}, {batch_size}
          RETURN {transform_clause}
        """
        
        batch = list(db.aql.execute(query))
        if not batch:
            break
        
        # Insert batch
        target_coll.import_bulk(batch)
        
        copied_count += len(batch)
        offset += batch_size
    
    return copied_count


def delete_results_by_filter(
    db: StandardDatabase,
    result_collection: str,
    filter_query: str,
    batch_size: int = 1000
) -> int:
    """
    Delete results matching a filter query.
    
    Args:
        db: ArangoDB database connection
        result_collection: Result collection name
        filter_query: AQL filter expression (e.g., "r.pagerank_influence < 0.000001")
        batch_size: Batch size for deletions
        
    Returns:
        Number of documents deleted
    """
    coll = db.collection(result_collection)
    
    deleted_count = 0
    offset = 0
    
    while True:
        query = f"""
        FOR r IN {result_collection}
          FILTER {filter_query}
          LIMIT {offset}, {batch_size}
          RETURN r._key
        """
        
        keys = list(db.aql.execute(query))
        if not keys:
            break
        
        # Delete batch
        for key in keys:
            coll.delete(key)
        
        deleted_count += len(keys)
        offset += batch_size
    
    return deleted_count

