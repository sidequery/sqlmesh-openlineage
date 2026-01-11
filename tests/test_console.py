"""Tests for OpenLineageConsole."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestOpenLineageConsole:
    """Tests for OpenLineageConsole wrapper."""

    def test_init(self, mock_console, mock_openlineage_client):
        """Test console initialization."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        assert console._wrapped is mock_console
        assert console._emitter.namespace == "test"
        assert len(console._active_runs) == 0

    def test_getattr_delegates(self, mock_console, mock_openlineage_client):
        """Test that unknown attributes delegate to wrapped console."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        mock_console.some_method = MagicMock(return_value="result")

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        result = console.some_method()
        assert result == "result"
        mock_console.some_method.assert_called_once()

    def test_start_snapshot_emits_start_event(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that start_snapshot_evaluation_progress emits START event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        with patch.object(console._emitter, "emit_snapshot_start") as mock_emit:
            console.start_snapshot_evaluation_progress(mock_snapshot, audit_only=False)

            mock_emit.assert_called_once()
            assert mock_snapshot.name in console._active_runs

        # Verify delegation
        mock_console.start_snapshot_evaluation_progress.assert_called_once_with(
            mock_snapshot, False
        )

    def test_update_snapshot_emits_complete_event(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that update_snapshot_evaluation_progress emits COMPLETE event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        # Simulate start first
        console._active_runs[mock_snapshot.name] = "test-run-id"

        interval = MagicMock()

        with patch.object(console._emitter, "emit_snapshot_complete") as mock_emit:
            console.update_snapshot_evaluation_progress(
                snapshot=mock_snapshot,
                interval=interval,
                batch_idx=0,
                duration_ms=1000,
                num_audits_passed=1,
                num_audits_failed=0,
            )

            mock_emit.assert_called_once()
            assert mock_snapshot.name not in console._active_runs

    def test_update_snapshot_emits_fail_on_audit_failure(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that audit failures emit FAIL event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        console._active_runs[mock_snapshot.name] = "test-run-id"

        interval = MagicMock()

        with patch.object(console._emitter, "emit_snapshot_fail") as mock_emit:
            console.update_snapshot_evaluation_progress(
                snapshot=mock_snapshot,
                interval=interval,
                batch_idx=0,
                duration_ms=1000,
                num_audits_passed=0,
                num_audits_failed=2,
            )

            mock_emit.assert_called_once()
            assert "audit" in mock_emit.call_args[1]["error"].lower()
