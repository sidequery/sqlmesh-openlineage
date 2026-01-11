"""Convert SQLMesh Snapshots to OpenLineage Datasets."""
from __future__ import annotations

import typing as t
from collections import defaultdict

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot
    from sqlmesh.core.model import Model
    from openlineage.client.event_v2 import InputDataset, OutputDataset


def snapshot_to_table_name(snapshot: "Snapshot") -> str:
    """Convert snapshot to fully qualified table name."""
    qvn = snapshot.qualified_view_name
    parts = [qvn.catalog, qvn.schema_name, qvn.table]
    return ".".join(p for p in parts if p)


def snapshot_to_schema_facet(snapshot: "Snapshot") -> t.Optional[t.Any]:
    """Extract schema facet from snapshot model."""
    from openlineage.client.facet_v2 import schema_dataset

    if not snapshot.is_model:
        return None

    model = snapshot.model
    if not model:
        return None

    columns = getattr(model, "columns_to_types", None)
    if not columns:
        return None

    fields = [
        schema_dataset.SchemaDatasetFacetFields(name=col, type=str(dtype))
        for col, dtype in columns.items()
    ]
    return schema_dataset.SchemaDatasetFacet(fields=fields)


def snapshot_to_column_lineage_facet(
    snapshot: "Snapshot",
    namespace: str,
) -> t.Optional[t.Any]:
    """Extract column-level lineage from snapshot model.

    Returns OpenLineage ColumnLineageDatasetFacet showing which upstream
    columns flow into each output column.
    """
    from openlineage.client.facet_v2 import column_lineage_dataset

    if not snapshot.is_model:
        return None

    model = snapshot.model
    if not model:
        return None

    columns = getattr(model, "columns_to_types", None)
    if not columns:
        return None

    try:
        from sqlmesh.core.lineage import lineage
        from sqlglot import exp

        fields: t.Dict[str, column_lineage_dataset.Fields] = {}

        for col_name in columns.keys():
            try:
                # Get lineage for this column
                node = lineage(col_name, model, trim_selects=False)

                # Walk the lineage tree to find source columns
                input_fields: t.List[column_lineage_dataset.InputField] = []

                for lineage_node in node.walk():
                    # Skip nodes that have downstream (not leaf nodes)
                    if lineage_node.downstream:
                        continue

                    # Find the source table
                    table = lineage_node.expression.find(exp.Table)
                    if table:
                        # Get table name
                        table_parts = [table.catalog, table.db, table.name]
                        table_name = ".".join(p for p in table_parts if p)

                        # Get column name
                        source_col = exp.to_column(lineage_node.name).name

                        input_fields.append(
                            column_lineage_dataset.InputField(
                                namespace=namespace,
                                name=table_name,
                                field=source_col,
                            )
                        )

                if input_fields:
                    fields[col_name] = column_lineage_dataset.Fields(
                        inputFields=input_fields,
                        transformationType="",
                        transformationDescription="",
                    )

            except Exception:
                # Skip columns we can't trace
                continue

        if fields:
            return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)

    except Exception:
        # If lineage extraction fails, return None
        pass

    return None


def snapshot_to_output_dataset(
    snapshot: "Snapshot",
    namespace: str,
    facets: t.Optional[t.Dict[str, t.Any]] = None,
) -> t.Optional["OutputDataset"]:
    """Convert snapshot to OpenLineage OutputDataset."""
    from openlineage.client.event_v2 import OutputDataset

    if not snapshot.is_model:
        return None

    all_facets: t.Dict[str, t.Any] = {}

    # Add schema facet
    schema_facet = snapshot_to_schema_facet(snapshot)
    if schema_facet:
        all_facets["schema"] = schema_facet

    # Add column lineage facet
    column_lineage_facet = snapshot_to_column_lineage_facet(snapshot, namespace)
    if column_lineage_facet:
        all_facets["columnLineage"] = column_lineage_facet

    # Merge additional facets
    if facets:
        all_facets.update(facets)

    return OutputDataset(
        namespace=namespace,
        name=snapshot_to_table_name(snapshot),
        facets=all_facets if all_facets else None,
    )


def snapshot_to_input_datasets(
    snapshot: "Snapshot",
    namespace: str,
) -> t.List["InputDataset"]:
    """Get upstream dependencies as input datasets."""
    from openlineage.client.event_v2 import InputDataset

    inputs: t.List["InputDataset"] = []

    # Get parent snapshot IDs
    for parent_id in snapshot.parents:
        # Parent ID contains the name we need
        inputs.append(
            InputDataset(
                namespace=namespace,
                name=parent_id.name,
            )
        )

    return inputs
