"""Tests for export module."""

import pytest
import json
import csv
from unittest.mock import MagicMock, mock_open, patch
from pathlib import Path

from graph_analytics_orchestrator.export import (
    export_results_to_csv,
    export_results_to_json,
)


class TestExportResultsToCSV:
    """Tests for export_results_to_csv function."""

    def test_export_csv_success(self):
        """Test successful CSV export."""
        mock_db = MagicMock()

        # Mock query results
        mock_results = [
            {"id": "nodes/1", "pagerank_influence": 0.5},
            {"id": "nodes/2", "pagerank_influence": 0.3},
        ]
        mock_db.aql.execute.return_value = mock_results

        output_path = Path("/tmp/test_output.csv")

        with patch("builtins.open", mock_open()) as mock_file:
            result = export_results_to_csv(mock_db, "pagerank_results", output_path)

        assert result == 2
        mock_db.aql.execute.assert_called_once()
        mock_file.assert_called_once()

    def test_export_csv_with_custom_query(self):
        """Test CSV export with custom query."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.csv")

        with patch("builtins.open", mock_open()):
            export_results_to_csv(
                mock_db,
                "pagerank_results",
                output_path,
                query="FOR r IN pagerank_results FILTER r.value > 0.5 RETURN r",
            )

        # Verify custom query was used
        call_args = mock_db.aql.execute.call_args[0][0]
        assert "FILTER r.value > 0.5" in call_args

    def test_export_csv_with_vertex_join(self):
        """Test CSV export with vertex join."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.csv")

        with patch("builtins.open", mock_open()):
            export_results_to_csv(
                mock_db,
                "pagerank_results",
                output_path,
                join_vertex=True,
                vertex_fields=["full_name"],
            )

        executed_query = mock_db.aql.execute.call_args[0][0]
        assert "LET person = DOCUMENT(r.id)" in executed_query
        assert "full_name: person.full_name" in executed_query

    def test_export_csv_no_results(self):
        """Test CSV export when no results."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.csv")

        result = export_results_to_csv(mock_db, "pagerank_results", output_path)

        assert result == 0

    def test_export_csv_no_headers(self):
        """Test CSV export without headers."""
        mock_db = MagicMock()
        mock_results = [{"id": "nodes/1", "value": 0.5}]
        mock_db.aql.execute.return_value = mock_results

        output_path = Path("/tmp/test_output.csv")

        with patch("builtins.open", mock_open()) as mock_file:
            export_results_to_csv(
                mock_db, "pagerank_results", output_path, include_headers=False
            )

        # Verify file was opened for writing
        mock_file.assert_called_once()

    def test_export_csv_write_error(self):
        """Test CSV export with write error."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = [{"id": "nodes/1"}]

        output_path = Path("/tmp/test_output.csv")

        with patch("builtins.open", side_effect=IOError("Permission denied")):
            with pytest.raises(IOError):
                export_results_to_csv(mock_db, "pagerank_results", output_path)


class TestExportResultsToJSON:
    """Tests for export_results_to_json function."""

    def test_export_json_success(self):
        """Test successful JSON export."""
        mock_db = MagicMock()

        mock_results = [
            {"id": "nodes/1", "pagerank_influence": 0.5},
            {"id": "nodes/2", "pagerank_influence": 0.3},
        ]
        mock_db.aql.execute.return_value = mock_results

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", mock_open()) as mock_file:
            result = export_results_to_json(mock_db, "pagerank_results", output_path)

        assert result == 2
        mock_db.aql.execute.assert_called_once()
        mock_file.assert_called_once()

    def test_export_json_pretty_print(self):
        """Test JSON export with pretty printing."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = [{"id": "nodes/1"}]

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("json.dump") as mock_json_dump:
                export_results_to_json(
                    mock_db, "pagerank_results", output_path, pretty=True
                )

        # Verify json.dump was called with indent=2
        call_args = mock_json_dump.call_args
        assert call_args[1]["indent"] == 2

    def test_export_json_no_pretty_print(self):
        """Test JSON export without pretty printing."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = [{"id": "nodes/1"}]

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", mock_open()):
            with patch("json.dump") as mock_json_dump:
                export_results_to_json(
                    mock_db, "pagerank_results", output_path, pretty=False
                )

        # Verify json.dump was called with indent=None
        call_args = mock_json_dump.call_args
        assert call_args[1]["indent"] is None

    def test_export_json_with_vertex_join(self):
        """Test JSON export with vertex join."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", mock_open()):
            export_results_to_json(
                mock_db,
                "pagerank_results",
                output_path,
                join_vertex=True,
                vertex_fields=["full_name"],
            )

        executed_query = mock_db.aql.execute.call_args[0][0]
        assert "LET person = DOCUMENT(r.id)" in executed_query

    def test_export_json_no_results(self):
        """Test JSON export when no results."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.json")

        result = export_results_to_json(mock_db, "pagerank_results", output_path)

        assert result == 0

    def test_export_json_write_error(self):
        """Test JSON export with write error."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = [{"id": "nodes/1"}]

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", side_effect=IOError("Permission denied")):
            with pytest.raises(IOError):
                export_results_to_json(mock_db, "pagerank_results", output_path)

    def test_export_json_custom_query(self):
        """Test JSON export with custom query."""
        mock_db = MagicMock()
        mock_db.aql.execute.return_value = []

        output_path = Path("/tmp/test_output.json")

        with patch("builtins.open", mock_open()):
            export_results_to_json(
                mock_db,
                "pagerank_results",
                output_path,
                query="FOR r IN pagerank_results FILTER r.value > 0.5 RETURN r",
            )

        # Verify custom query was used
        call_args = mock_db.aql.execute.call_args[0][0]
        assert "FILTER r.value > 0.5" in call_args
