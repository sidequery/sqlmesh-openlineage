"""OpenLineage Console wrapper for SQLMesh."""
from __future__ import annotations

import uuid
import typing as t

if t.TYPE_CHECKING:
    from sqlmesh.core.console import Console
    from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike
    from sqlmesh.core.snapshot.definition import Interval, SnapshotId
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats
    from sqlmesh.core.environment import EnvironmentNamingInfo
    from sqlmesh.core.snapshot import Intervals


class OpenLineageConsole:
    """Console wrapper that emits OpenLineage events for snapshot evaluations.

    This class wraps an existing SQLMesh Console and intercepts lifecycle
    events to emit OpenLineage events.

    The wrapper uses delegation (not inheritance) to forward all Console
    methods to the wrapped instance while intercepting specific methods
    for OpenLineage event emission.
    """

    def __init__(
        self,
        wrapped: "Console",
        url: str,
        namespace: str = "sqlmesh",
        api_key: t.Optional[str] = None,
    ):
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        self._wrapped = wrapped
        self._emitter = OpenLineageEmitter(url=url, namespace=namespace, api_key=api_key)
        self._active_runs: t.Dict[str, str] = {}  # snapshot_name -> run_id
        self._current_snapshots: t.Dict[str, "Snapshot"] = {}  # snapshot_name -> Snapshot

    def __getattr__(self, name: str) -> t.Any:
        """Delegate all other methods to wrapped console."""
        return getattr(self._wrapped, name)

    def start_evaluation_progress(
        self,
        batched_intervals: t.Dict["Snapshot", "Intervals"],
        environment_naming_info: "EnvironmentNamingInfo",
        default_catalog: t.Optional[str],
        audit_only: bool = False,
    ) -> None:
        """Called when evaluation phase begins."""
        # Store snapshots for later use
        for snapshot in batched_intervals.keys():
            self._current_snapshots[snapshot.name] = snapshot

        # Delegate to wrapped console
        self._wrapped.start_evaluation_progress(
            batched_intervals, environment_naming_info, default_catalog, audit_only
        )

    def start_snapshot_evaluation_progress(
        self,
        snapshot: "Snapshot",
        audit_only: bool = False,
    ) -> None:
        """Called when a single snapshot evaluation starts."""
        # Generate run_id and emit START event
        run_id = str(uuid.uuid4())
        self._active_runs[snapshot.name] = run_id

        # Store snapshot for later reference
        self._current_snapshots[snapshot.name] = snapshot

        self._emitter.emit_snapshot_start(
            snapshot=snapshot,
            run_id=run_id,
        )

        # Delegate to wrapped console
        self._wrapped.start_snapshot_evaluation_progress(snapshot, audit_only)

    def update_snapshot_evaluation_progress(
        self,
        snapshot: "Snapshot",
        interval: "Interval",
        batch_idx: int,
        duration_ms: t.Optional[int],
        num_audits_passed: int,
        num_audits_failed: int,
        audit_only: bool = False,
        execution_stats: t.Optional["QueryExecutionStats"] = None,
        auto_restatement_triggers: t.Optional[t.List["SnapshotId"]] = None,
    ) -> None:
        """Called when a snapshot evaluation completes (success or audit failure)."""
        run_id = self._active_runs.pop(snapshot.name, None)

        if run_id:
            if num_audits_failed > 0:
                self._emitter.emit_snapshot_fail(
                    snapshot=snapshot,
                    run_id=run_id,
                    error=f"Audit failed: {num_audits_failed} audit(s) failed",
                )
            else:
                self._emitter.emit_snapshot_complete(
                    snapshot=snapshot,
                    run_id=run_id,
                    interval=interval,
                    duration_ms=duration_ms,
                    execution_stats=execution_stats,
                )

        # Delegate to wrapped console
        self._wrapped.update_snapshot_evaluation_progress(
            snapshot,
            interval,
            batch_idx,
            duration_ms,
            num_audits_passed,
            num_audits_failed,
            audit_only,
            execution_stats,
            auto_restatement_triggers,
        )

    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Called when evaluation phase ends."""
        # Emit FAIL for any snapshots that started but didn't complete
        for snapshot_name, run_id in list(self._active_runs.items()):
            snapshot = self._current_snapshots.get(snapshot_name)
            if snapshot and run_id:
                self._emitter.emit_snapshot_fail(
                    snapshot=snapshot,
                    run_id=run_id,
                    error="Evaluation interrupted" if not success else "Unknown error",
                )

        # Clear tracking state
        self._active_runs.clear()
        self._current_snapshots.clear()

        # Delegate to wrapped console
        self._wrapped.stop_evaluation_progress(success)

    # Forward other progress methods

    def start_creation_progress(
        self,
        snapshots: t.List["Snapshot"],
        environment_naming_info: "EnvironmentNamingInfo",
        default_catalog: t.Optional[str],
    ) -> None:
        """Forward to wrapped console."""
        self._wrapped.start_creation_progress(
            snapshots, environment_naming_info, default_catalog
        )

    def update_creation_progress(self, snapshot: "SnapshotInfoLike") -> None:
        """Forward to wrapped console."""
        self._wrapped.update_creation_progress(snapshot)

    def stop_creation_progress(self, success: bool = True) -> None:
        """Forward to wrapped console."""
        self._wrapped.stop_creation_progress(success)

    def start_promotion_progress(
        self,
        snapshots: t.List["Snapshot"],
        environment_naming_info: "EnvironmentNamingInfo",
        default_catalog: t.Optional[str],
    ) -> None:
        """Forward to wrapped console."""
        self._wrapped.start_promotion_progress(
            snapshots, environment_naming_info, default_catalog
        )

    def update_promotion_progress(
        self, snapshot: "SnapshotInfoLike", promoted: bool
    ) -> None:
        """Forward to wrapped console."""
        self._wrapped.update_promotion_progress(snapshot, promoted)

    def stop_promotion_progress(self, success: bool = True) -> None:
        """Forward to wrapped console."""
        self._wrapped.stop_promotion_progress(success)
