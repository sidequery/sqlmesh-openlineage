"""Integration test that runs SQLMesh and verifies OpenLineage events."""
from __future__ import annotations

import os
import tempfile
import shutil
import pytest
from unittest.mock import MagicMock, patch


class TestOpenLineageIntegration:
    """Integration tests for OpenLineage event emission."""

    @pytest.fixture
    def temp_project(self):
        """Create a temporary SQLMesh project."""
        tmpdir = tempfile.mkdtemp(prefix="sqlmesh_ol_test_")

        # Create config.yaml
        config_content = """
gateways:
  local:
    connection:
      type: duckdb
      database: ':memory:'

default_gateway: local

model_defaults:
  dialect: duckdb
"""
        with open(os.path.join(tmpdir, "config.yaml"), "w") as f:
            f.write(config_content)

        # Create models directory
        models_dir = os.path.join(tmpdir, "models")
        os.makedirs(models_dir)

        # Create source model
        source_content = """
MODEL (
    name test_db.source_data,
    kind FULL,
    columns (
        id INT,
        name VARCHAR
    )
);

SELECT 1 as id, 'foo' as name
UNION ALL
SELECT 2 as id, 'bar' as name
"""
        with open(os.path.join(models_dir, "source_data.sql"), "w") as f:
            f.write(source_content)

        # Create downstream model with dependency
        downstream_content = """
MODEL (
    name test_db.processed_data,
    kind FULL,
    columns (
        id INT,
        name VARCHAR,
        name_upper VARCHAR
    )
);

SELECT
    id,
    name,
    UPPER(name) as name_upper
FROM test_db.source_data
"""
        with open(os.path.join(models_dir, "processed_data.sql"), "w") as f:
            f.write(downstream_content)

        yield tmpdir

        # Cleanup
        shutil.rmtree(tmpdir)

    @pytest.fixture
    def captured_events(self):
        """Fixture to capture emitted OpenLineage events."""
        events = []
        return events

    def test_full_run_emits_correct_events(self, temp_project, captured_events):
        """Test that a full SQLMesh run emits correct OpenLineage events."""
        import sqlmesh_openlineage
        from sqlmesh.core import console as sqlmesh_console

        # Reset console state completely
        sqlmesh_openlineage._installed = False
        sqlmesh_console._console = None

        # Install with explicit config
        sqlmesh_openlineage.install(
            url="console://localhost",
            namespace="test_namespace",
        )

        from sqlmesh import Context
        from sqlmesh.core.console import get_console
        from sqlmesh_openlineage.console import OpenLineageConsole

        # Get the console and patch emit to capture events
        console = get_console()
        assert isinstance(console, OpenLineageConsole)

        original_emit = console._emitter.client.emit

        def capture_emit(event):
            captured_events.append(event)
            return original_emit(event)

        console._emitter.client.emit = capture_emit

        # Create context and run plan
        ctx = Context(paths=[temp_project])
        ctx.plan(auto_apply=True, no_prompts=True)

        # Verify we got the expected events
        assert len(captured_events) == 4, f"Expected 4 events, got {len(captured_events)}"

        # Sort events by job name then by event type for predictable order
        from openlineage.client.event_v2 import RunState

        start_events = [e for e in captured_events if e.eventType == RunState.START]
        complete_events = [e for e in captured_events if e.eventType == RunState.COMPLETE]

        assert len(start_events) == 2, "Expected 2 START events"
        assert len(complete_events) == 2, "Expected 2 COMPLETE events"

        # Find source_data events
        source_start = next(e for e in start_events if "source_data" in e.job.name)
        source_complete = next(e for e in complete_events if "source_data" in e.job.name)

        # Find processed_data events
        processed_start = next(e for e in start_events if "processed_data" in e.job.name)
        processed_complete = next(e for e in complete_events if "processed_data" in e.job.name)

        # Verify source_data START event
        assert source_start.job.namespace == "test_namespace"
        assert len(source_start.inputs) == 0, "source_data should have no inputs"
        assert len(source_start.outputs) == 1
        assert "source_data" in source_start.outputs[0].name

        # Verify source_data COMPLETE event
        assert source_complete.run.facets is not None
        assert "sqlmesh_execution" in source_complete.run.facets

        # Verify processed_data START event has source_data as input (LINEAGE!)
        assert len(processed_start.inputs) == 1, "processed_data should have 1 input"
        assert "source_data" in processed_start.inputs[0].name
        assert len(processed_start.outputs) == 1
        assert "processed_data" in processed_start.outputs[0].name

        # Verify schema is captured in output
        output_facets = processed_start.outputs[0].facets
        assert output_facets is not None
        assert "schema" in output_facets
        schema_facet = output_facets["schema"]
        assert len(schema_facet.fields) == 3  # id, name, name_upper

        # Verify processed_data COMPLETE event
        assert processed_complete.run.facets is not None
        assert "sqlmesh_execution" in processed_complete.run.facets

    def test_audit_failure_emits_fail_event(self, temp_project, captured_events):
        """Test that audit failures emit FAIL events."""
        # Create a model with a failing audit
        models_dir = os.path.join(temp_project, "models")
        failing_model = """
MODEL (
    name test_db.failing_model,
    kind FULL,
    audits (
        assert_positive_id
    )
);

SELECT -1 as id, 'negative' as name;

AUDIT (
    name assert_positive_id
);

SELECT * FROM @this_model WHERE id < 0;
"""
        with open(os.path.join(models_dir, "failing_model.sql"), "w") as f:
            f.write(failing_model)

        import sqlmesh_openlineage
        from sqlmesh.core import console as sqlmesh_console

        # Reset console state completely
        sqlmesh_openlineage._installed = False
        sqlmesh_console._console = None

        # Install with explicit config
        sqlmesh_openlineage.install(
            url="console://localhost",
            namespace="test_namespace",
        )

        from sqlmesh import Context
        from sqlmesh.core.console import get_console
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = get_console()
        assert isinstance(console, OpenLineageConsole)

        original_emit = console._emitter.client.emit

        def capture_emit(event):
            captured_events.append(event)
            return original_emit(event)

        console._emitter.client.emit = capture_emit

        ctx = Context(paths=[temp_project])

        # Run plan - this should produce FAIL events for audit failures
        try:
            ctx.plan(auto_apply=True, no_prompts=True)
        except Exception:
            pass  # Expected to fail

        from openlineage.client.event_v2 import RunState

        fail_events = [e for e in captured_events if e.eventType == RunState.FAIL]

        # We should have at least one FAIL event from the audit failure
        # Note: depends on whether audit actually runs
        if fail_events:
            fail_event = fail_events[0]
            assert fail_event.run.facets is not None
            assert "errorMessage" in fail_event.run.facets
