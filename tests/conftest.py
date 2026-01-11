"""Test fixtures for sqlmesh-openlineage."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_snapshot():
    """Create a mock SQLMesh Snapshot."""
    snapshot = MagicMock()
    snapshot.name = "test_model"
    snapshot.is_model = True
    snapshot.parents = []

    # Mock qualified_view_name
    qualified_name = MagicMock()
    qualified_name.catalog = "catalog"
    qualified_name.schema_name = "schema"
    qualified_name.table = "test_model"
    snapshot.qualified_view_name = qualified_name

    # Mock model
    model = MagicMock()
    model.columns_to_types = {"id": "INT", "name": "VARCHAR"}
    snapshot.model = model

    return snapshot


@pytest.fixture
def mock_console():
    """Create a mock SQLMesh Console."""
    console = MagicMock()
    return console


@pytest.fixture
def mock_openlineage_client(mocker):
    """Mock the OpenLineage client."""
    mock_client = MagicMock()
    mocker.patch(
        "openlineage.client.OpenLineageClient",
        return_value=mock_client,
    )
    return mock_client
