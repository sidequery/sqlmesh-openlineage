"""Custom OpenLineage facets for SQLMesh."""
from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats


def build_run_facets(
    duration_ms: t.Optional[int] = None,
    execution_stats: t.Optional["QueryExecutionStats"] = None,
) -> t.Dict[str, t.Any]:
    """Build run facets from execution data."""
    facets: t.Dict[str, t.Any] = {}

    # Add custom SQLMesh facet with execution info
    if duration_ms is not None or execution_stats is not None:
        sqlmesh_facet = {
            "_producer": "sqlmesh-openlineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLMeshExecutionFacet.json",
        }

        if duration_ms is not None:
            sqlmesh_facet["durationMs"] = duration_ms

        if execution_stats is not None:
            if execution_stats.total_rows_processed is not None:
                sqlmesh_facet["rowsProcessed"] = execution_stats.total_rows_processed
            if execution_stats.total_bytes_processed is not None:
                sqlmesh_facet["bytesProcessed"] = execution_stats.total_bytes_processed

        facets["sqlmesh_execution"] = sqlmesh_facet

    return facets


def build_output_facets(
    execution_stats: t.Optional["QueryExecutionStats"] = None,
) -> t.Dict[str, t.Any]:
    """Build output dataset facets from execution data."""
    from openlineage.client.facet_v2 import output_statistics_output_dataset

    facets: t.Dict[str, t.Any] = {}

    if execution_stats is not None:
        if execution_stats.total_rows_processed is not None:
            facets["outputStatistics"] = (
                output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                    rowCount=execution_stats.total_rows_processed,
                    size=execution_stats.total_bytes_processed,
                )
            )

    return facets
