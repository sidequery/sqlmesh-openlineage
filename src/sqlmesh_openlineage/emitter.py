"""OpenLineage event emitter for SQLMesh."""
from __future__ import annotations

import typing as t
from datetime import datetime, timezone

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot
    from sqlmesh.core.snapshot.definition import Interval
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats


class OpenLineageEmitter:
    """Emits OpenLineage events for SQLMesh operations."""

    def __init__(
        self,
        url: str,
        namespace: str = "sqlmesh",
        api_key: t.Optional[str] = None,
    ):
        from openlineage.client import OpenLineageClient

        self.namespace = namespace
        self.url = url

        # Use console transport for console:// URLs (for testing)
        if url.startswith("console://"):
            from openlineage.client.transport.console import ConsoleTransport, ConsoleConfig

            self.client = OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))
        elif api_key:
            self.client = OpenLineageClient(
                url=url,
                options={"api_key": api_key},
            )
        else:
            self.client = OpenLineageClient(url=url)

    def emit_snapshot_start(
        self,
        snapshot: "Snapshot",
        run_id: str,
    ) -> None:
        """Emit a START event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job

        from sqlmesh_openlineage.datasets import (
            snapshot_to_output_dataset,
            snapshot_to_input_datasets,
        )

        inputs = snapshot_to_input_datasets(snapshot, self.namespace)
        output = snapshot_to_output_dataset(snapshot, self.namespace)

        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=self.namespace, name=snapshot.name),
            inputs=inputs,
            outputs=[output] if output else [],
            producer="sqlmesh-openlineage",
        )
        self.client.emit(event)

    def emit_snapshot_complete(
        self,
        snapshot: "Snapshot",
        run_id: str,
        interval: t.Optional["Interval"] = None,
        duration_ms: t.Optional[int] = None,
        execution_stats: t.Optional["QueryExecutionStats"] = None,
    ) -> None:
        """Emit a COMPLETE event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job

        from sqlmesh_openlineage.datasets import snapshot_to_output_dataset
        from sqlmesh_openlineage.facets import build_run_facets, build_output_facets

        run_facets = build_run_facets(
            duration_ms=duration_ms,
            execution_stats=execution_stats,
        )

        output = snapshot_to_output_dataset(
            snapshot,
            self.namespace,
            facets=build_output_facets(execution_stats),
        )

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=snapshot.name),
            outputs=[output] if output else [],
            producer="sqlmesh-openlineage",
        )
        self.client.emit(event)

    def emit_snapshot_fail(
        self,
        snapshot: "Snapshot",
        run_id: str,
        error: t.Union[str, Exception],
    ) -> None:
        """Emit a FAIL event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job
        from openlineage.client.facet_v2 import error_message_run

        error_msg = str(error)

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(
                runId=run_id,
                facets={
                    "errorMessage": error_message_run.ErrorMessageRunFacet(
                        message=error_msg,
                        programmingLanguage="python",
                    )
                },
            ),
            job=Job(namespace=self.namespace, name=snapshot.name),
            producer="sqlmesh-openlineage",
        )
        self.client.emit(event)
