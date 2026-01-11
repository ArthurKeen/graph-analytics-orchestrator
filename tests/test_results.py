"""Tests for results module."""

import pytest
from unittest.mock import MagicMock, Mock, patch
from pathlib import Path

from graph_analytics_orchestrator.results import (
    ensure_result_collection_indexes,
    verify_result_collection,
    validate_result_schema,
    compare_result_collections,
    bulk_update_result_metadata,
    copy_results,
    delete_results_by_filter
)


class TestEnsureResultCollectionIndexes:
    """Tests for ensure_result_collection_indexes function."""
    
    def test_create_indexes_success(self):
        """Test successful index creation."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        # Mock collection exists
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        
        # Mock no existing index
        mock_coll.indexes.return_value = []
        
        result = ensure_result_collection_indexes(
            mock_db,
            ['pagerank_results'],
            verbose=False
        )
        
        assert result['created'] == 1
        assert result['existing'] == 0
        assert result['missing'] == 0
        mock_coll.add_persistent_index.assert_called_once()
    
    def test_index_already_exists(self):
        """Test when index already exists."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        
        # Mock existing index on 'id' field
        existing_index = {
            'type': 'persistent',
            'fields': ['id']
        }
        mock_coll.indexes.return_value = [existing_index]
        
        result = ensure_result_collection_indexes(
            mock_db,
            ['pagerank_results'],
            verbose=False
        )
        
        assert result['created'] == 0
        assert result['existing'] == 1
        assert result['missing'] == 0
        mock_coll.add_persistent_index.assert_not_called()
    
    def test_collection_missing(self):
        """Test when collection doesn't exist."""
        mock_db = MagicMock()
        mock_db.has_collection.return_value = False
        
        result = ensure_result_collection_indexes(
            mock_db,
            ['missing_collection'],
            verbose=False
        )
        
        assert result['created'] == 0
        assert result['existing'] == 0
        assert result['missing'] == 1
    
    def test_default_collections(self):
        """Test with default collection names."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.indexes.return_value = []
        
        result = ensure_result_collection_indexes(mock_db, verbose=False)
        
        # Should process default collections
        assert mock_db.has_collection.call_count == 3


class TestVerifyResultCollection:
    """Tests for verify_result_collection function."""
    
    def test_verify_valid_collection(self):
        """Test verification of valid collection."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.count.return_value = 100
        
        # Mock sample document with 'id' field
        sample_doc = {'id': 'nodes/123', 'pagerank_influence': 0.5}
        mock_db.aql.execute.return_value = [sample_doc]
        
        # Mock index exists
        existing_index = {'type': 'persistent', 'fields': ['id']}
        mock_coll.indexes.return_value = [existing_index]
        
        result = verify_result_collection(mock_db, 'pagerank_results')
        
        assert result['exists'] is True
        assert result['count'] == 100
        assert result['has_id_field'] is True
        assert result['has_index'] is True
        assert result['valid'] is True
    
    def test_verify_missing_collection(self):
        """Test verification of missing collection."""
        mock_db = MagicMock()
        mock_db.has_collection.return_value = False
        
        result = verify_result_collection(mock_db, 'missing_collection')
        
        assert result['exists'] is False
        assert result['valid'] is False
    
    def test_verify_no_id_field(self):
        """Test verification when 'id' field is missing."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.count.return_value = 100
        
        # Mock sample document without 'id' field
        sample_doc = {'pagerank_influence': 0.5}
        mock_db.aql.execute.return_value = [sample_doc]
        mock_coll.indexes.return_value = []
        
        result = verify_result_collection(mock_db, 'pagerank_results')
        
        assert result['has_id_field'] is False
        assert result['valid'] is False


class TestValidateResultSchema:
    """Tests for validate_result_schema function."""
    
    def test_validate_valid_schema(self):
        """Test validation of valid schema."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.count.return_value = 100
        
        # Mock sample documents
        sample_docs = [
            {'id': 'nodes/123', 'pagerank_influence': 0.5}
        ]
        mock_db.aql.execute.return_value = sample_docs
        
        result = validate_result_schema(
            mock_db,
            'pagerank_results',
            expected_fields=['id', 'pagerank_influence'],
            expected_field_types={'pagerank_influence': float}
        )
        
        assert result['valid'] is True
        assert result['has_required_fields'] is True
        assert result['field_types_match'] is True
        assert len(result['issues']) == 0
    
    def test_validate_missing_fields(self):
        """Test validation with missing required fields."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.count.return_value = 100
        
        # Mock sample without required field
        sample_docs = [{'pagerank_influence': 0.5}]
        mock_db.aql.execute.return_value = sample_docs
        
        result = validate_result_schema(
            mock_db,
            'pagerank_results',
            expected_fields=['id', 'pagerank_influence']
        )
        
        assert result['valid'] is False
        assert result['has_required_fields'] is False
        assert len(result['issues']) > 0
    
    def test_validate_type_mismatch(self):
        """Test validation with type mismatch."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.has_collection.return_value = True
        mock_db.collection.return_value = mock_coll
        mock_coll.count.return_value = 100
        
        # Mock sample with wrong type
        sample_docs = [{'id': 'nodes/123', 'pagerank_influence': '0.5'}]
        mock_db.aql.execute.return_value = sample_docs
        
        result = validate_result_schema(
            mock_db,
            'pagerank_results',
            expected_fields=['id'],
            expected_field_types={'pagerank_influence': float}
        )
        
        assert result['valid'] is False
        assert result['field_types_match'] is False


class TestCompareResultCollections:
    """Tests for compare_result_collections function."""
    
    def test_compare_collections(self):
        """Test comparison of two collections."""
        mock_db = MagicMock()
        mock_coll1 = MagicMock()
        mock_coll2 = MagicMock()
        
        mock_db.has_collection.side_effect = lambda x: x in ['coll1', 'coll2']
        mock_db.collection.side_effect = lambda x: mock_coll1 if x == 'coll1' else mock_coll2
        mock_coll1.count.return_value = 100
        mock_coll2.count.return_value = 80
        
        # Mock overlap query
        mock_db.aql.execute.return_value = [50]  # 50 overlapping IDs
        
        result = compare_result_collections(mock_db, 'coll1', 'coll2')
        
        assert result['collection1_count'] == 100
        assert result['collection2_count'] == 80
        assert result['overlap_count'] == 50
        assert result['overlap_percentage'] == 50.0
        assert result['collection1_only'] == 50
        assert result['collection2_only'] == 30


class TestBulkUpdateResultMetadata:
    """Tests for bulk_update_result_metadata function."""
    
    def test_bulk_update_success(self):
        """Test successful bulk update."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.collection.return_value = mock_coll
        
        # Mock batch query results
        mock_db.aql.execute.side_effect = [
            ['key1', 'key2'],  # First batch
            []  # Second batch (empty, stops loop)
        ]
        
        # Mock document retrieval and update
        mock_doc1 = {'_key': 'key1', 'id': 'nodes/1'}
        mock_doc2 = {'_key': 'key2', 'id': 'nodes/2'}
        mock_coll.get.side_effect = [mock_doc1, mock_doc2]
        
        metadata = {'analysis_date': '2025-01-01', 'version': '1.0'}
        result = bulk_update_result_metadata(
            mock_db,
            'pagerank_results',
            metadata,
            batch_size=1000
        )
        
        assert result == 2
        assert mock_coll.update.call_count == 2


class TestCopyResults:
    """Tests for copy_results function."""
    
    def test_copy_results_success(self):
        """Test successful copy operation."""
        mock_db = MagicMock()
        mock_source_coll = MagicMock()
        mock_target_coll = MagicMock()
        
        mock_db.has_collection.side_effect = lambda x: x == 'source'
        mock_db.collection.side_effect = lambda x: mock_source_coll if x == 'source' else mock_target_coll
        
        # Mock batch query results
        batch1 = [{'id': 'nodes/1', 'value': 0.5}]
        batch2 = []
        mock_db.aql.execute.side_effect = [batch1, batch2]
        
        result = copy_results(
            mock_db,
            'source',
            'target',
            batch_size=1000
        )
        
        assert result == 1
        mock_db.create_collection.assert_called_once_with('target')
        mock_target_coll.import_bulk.assert_called_once()
    
    def test_copy_with_filter(self):
        """Test copy with filter query."""
        mock_db = MagicMock()
        mock_source_coll = MagicMock()
        mock_target_coll = MagicMock()
        
        mock_db.has_collection.side_effect = lambda x: x == 'source'
        mock_db.collection.side_effect = lambda x: mock_source_coll if x == 'source' else mock_target_coll
        
        mock_db.aql.execute.return_value = []
        
        copy_results(
            mock_db,
            'source',
            'target',
            filter_query='r.value >= 0.5',
            batch_size=1000
        )
        
        # Verify filter was included in query
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'FILTER r.value >= 0.5' in executed_query


class TestDeleteResultsByFilter:
    """Tests for delete_results_by_filter function."""
    
    def test_delete_results_success(self):
        """Test successful deletion."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.collection.return_value = mock_coll
        
        # Mock batch query results
        mock_db.aql.execute.side_effect = [
            ['key1', 'key2'],  # First batch
            []  # Second batch (empty, stops loop)
        ]
        
        result = delete_results_by_filter(
            mock_db,
            'pagerank_results',
            'r.value < 0.001',
            batch_size=1000
        )
        
        assert result == 2
        assert mock_coll.delete.call_count == 2
    
    def test_delete_no_matches(self):
        """Test deletion when no documents match."""
        mock_db = MagicMock()
        mock_coll = MagicMock()
        
        mock_db.collection.return_value = mock_coll
        mock_db.aql.execute.return_value = []
        
        result = delete_results_by_filter(
            mock_db,
            'pagerank_results',
            'r.value < 0.001',
            batch_size=1000
        )
        
        assert result == 0
        mock_coll.delete.assert_not_called()

