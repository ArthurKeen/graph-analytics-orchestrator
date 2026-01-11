"""Tests for queries module."""

import pytest
from unittest.mock import MagicMock

from graph_analytics_orchestrator.queries import (
    cross_reference_results,
    get_top_influential_connected,
    get_results_with_details
)


class TestCrossReferenceResults:
    """Tests for cross_reference_results function."""
    
    def test_cross_reference_success(self):
        """Test successful cross-referencing."""
        mock_db = MagicMock()
        
        # Mock query results
        mock_results = [
            {
                'id': 'nodes/1',
                'result1': {'id': 'nodes/1', 'pagerank_influence': 0.5},
                'result2': {'id': 'nodes/1', 'component_id': 'comp1'}
            }
        ]
        mock_db.aql.execute.return_value = mock_results
        
        result = cross_reference_results(
            mock_db,
            'pagerank_results',
            'wcc_results',
            filter1='r.pagerank_influence >= 0.5',
            filter2='w.component_id == "comp1"',
            limit=100
        )
        
        assert len(result) == 1
        assert result[0]['id'] == 'nodes/1'
        assert 'result1' in result[0]
        assert 'result2' in result[0]
        
        # Verify query was executed
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'pagerank_results' in executed_query
        assert 'wcc_results' in executed_query
        assert 'FILTER r.pagerank_influence >= 0.5' in executed_query
    
    def test_cross_reference_no_filters(self):
        """Test cross-referencing without filters."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        result = cross_reference_results(
            mock_db,
            'collection1',
            'collection2'
        )
        
        assert result == []
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'FILTER' not in executed_query or 'FILTER r2 != null' in executed_query
    
    def test_cross_reference_custom_join_fields(self):
        """Test cross-referencing with custom join fields."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        cross_reference_results(
            mock_db,
            'collection1',
            'collection2',
            join_fields={'vertex_id': 'id'}
        )
        
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'r1.vertex_id' in executed_query
        assert 'r2_inner.id' in executed_query


class TestGetTopInfluentialConnected:
    """Tests for get_top_influential_connected function."""
    
    def test_get_top_influential_with_component_id(self):
        """Test getting top influential with specified component ID."""
        mock_db = MagicMock()
        
        mock_results = [
            {
                'vertex_id': 'nodes/1',
                'pagerank_influence': 0.8,
                'component_id': 'comp1',
                'in_connected_network': True
            }
        ]
        mock_db.aql.execute.return_value = mock_results
        
        result = get_top_influential_connected(
            mock_db,
            'pagerank_results',
            'wcc_results',
            component_id='comp1',
            limit=10
        )
        
        assert len(result) == 1
        assert result[0]['vertex_id'] == 'nodes/1'
        assert result[0]['pagerank_influence'] == 0.8
        
        # Verify component_id was used in query
        call_args = mock_db.aql.execute.call_args
        assert call_args[1]['bind_vars']['component_id'] == 'comp1'
    
    def test_get_top_influential_find_largest_component(self):
        """Test getting top influential by finding largest component."""
        mock_db = MagicMock()
        
        # Mock largest component query
        mock_db.aql.execute.side_effect = [
            ['comp1'],  # Largest component query
            []  # Main query
        ]
        
        result = get_top_influential_connected(
            mock_db,
            'pagerank_results',
            'wcc_results',
            limit=10
        )
        
        assert result == []
        # Verify largest component query was executed first
        assert mock_db.aql.execute.call_count == 2
    
    def test_get_top_influential_with_vertex_details(self):
        """Test getting top influential with vertex details."""
        mock_db = MagicMock()
        
        mock_results = [
            {
                'vertex_id': 'nodes/1',
                'full_name': 'John Doe',
                'pagerank_influence': 0.8
            }
        ]
        mock_db.aql.execute.return_value = mock_results
        
        result = get_top_influential_connected(
            mock_db,
            'pagerank_results',
            'wcc_results',
            component_id='comp1',
            include_vertex_details=True,
            vertex_fields=['full_name']
        )
        
        assert len(result) == 1
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'LET person = DOCUMENT(pr.id)' in executed_query
        assert 'full_name: person.full_name' in executed_query
    
    def test_get_top_influential_no_components(self):
        """Test when no components found."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        result = get_top_influential_connected(
            mock_db,
            'pagerank_results',
            'wcc_results'
        )
        
        assert result == []


class TestGetResultsWithDetails:
    """Tests for get_results_with_details function."""
    
    def test_get_results_with_details_success(self):
        """Test successful retrieval with details."""
        mock_db = MagicMock()
        
        mock_results = [
            {
                'result_id': 'nodes/1',
                'full_name': 'John Doe',
                'result_data': {'id': 'nodes/1', 'pagerank_influence': 0.5}
            }
        ]
        mock_db.aql.execute.return_value = mock_results
        
        result = get_results_with_details(
            mock_db,
            'pagerank_results',
            vertex_collection='nodes',
            result_filter='r.pagerank_influence >= 0.5',
            limit=100
        )
        
        assert len(result) == 1
        assert result[0]['result_id'] == 'nodes/1'
        assert 'full_name' in result[0]
        assert 'result_data' in result[0]
        
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'FILTER r.pagerank_influence >= 0.5' in executed_query
        assert 'LIMIT 100' in executed_query
    
    def test_get_results_default_fields(self):
        """Test with default vertex fields."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        get_results_with_details(
            mock_db,
            'pagerank_results'
        )
        
        executed_query = mock_db.aql.execute.call_args[0][0]
        # Should include default fields
        assert 'full_name' in executed_query
    
    def test_get_results_custom_fields(self):
        """Test with custom vertex fields."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        get_results_with_details(
            mock_db,
            'pagerank_results',
            fields=['CustomField1', 'CustomField2']
        )
        
        executed_query = mock_db.aql.execute.call_args[0][0]
        assert 'CustomField1' in executed_query
        assert 'CustomField2' in executed_query
    
    def test_get_results_no_filter(self):
        """Test without result filter."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []
        
        get_results_with_details(
            mock_db,
            'pagerank_results'
        )
        
        executed_query = mock_db.aql.execute.call_args[0][0]
        # Should not have FILTER clause for results (only for person != null)
        assert 'FILTER person != null' in executed_query

