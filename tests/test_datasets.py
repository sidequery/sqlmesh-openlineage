"""Tests for dataset conversion."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


class TestDatasetConversion:
    """Tests for snapshot to dataset conversion."""

    def test_snapshot_to_table_name(self, mock_snapshot):
        """Test table name extraction from snapshot."""
        from sqlmesh_openlineage.datasets import snapshot_to_table_name

        name = snapshot_to_table_name(mock_snapshot)
        assert name == "catalog.schema.test_model"

    def test_snapshot_to_table_name_no_catalog(self):
        """Test table name without catalog."""
        from sqlmesh_openlineage.datasets import snapshot_to_table_name

        snapshot = MagicMock()
        qualified_name = MagicMock()
        qualified_name.catalog = None
        qualified_name.schema_name = "schema"
        qualified_name.table = "model"
        snapshot.qualified_view_name = qualified_name

        name = snapshot_to_table_name(snapshot)
        assert name == "schema.model"

    def test_snapshot_to_schema_facet(self, mock_snapshot):
        """Test schema facet extraction."""
        from sqlmesh_openlineage.datasets import snapshot_to_schema_facet

        facet = snapshot_to_schema_facet(mock_snapshot)

        assert facet is not None
        assert len(facet.fields) == 2

    def test_snapshot_to_output_dataset(self, mock_snapshot):
        """Test output dataset creation."""
        from sqlmesh_openlineage.datasets import snapshot_to_output_dataset

        dataset = snapshot_to_output_dataset(mock_snapshot, namespace="test")

        assert dataset is not None
        assert dataset.namespace == "test"
        assert dataset.name == "catalog.schema.test_model"

    def test_snapshot_to_input_datasets(self, mock_snapshot):
        """Test input datasets from parents."""
        from sqlmesh_openlineage.datasets import snapshot_to_input_datasets

        # Add parent
        parent_id = MagicMock()
        parent_id.name = "parent_model"
        mock_snapshot.parents = [parent_id]

        datasets = snapshot_to_input_datasets(mock_snapshot, namespace="test")

        assert len(datasets) == 1
        assert datasets[0].name == "parent_model"
        assert datasets[0].namespace == "test"

    def test_snapshot_to_input_datasets_empty(self, mock_snapshot):
        """Test input datasets with no parents."""
        from sqlmesh_openlineage.datasets import snapshot_to_input_datasets

        mock_snapshot.parents = []

        datasets = snapshot_to_input_datasets(mock_snapshot, namespace="test")

        assert len(datasets) == 0
