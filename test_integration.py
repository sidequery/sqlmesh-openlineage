"""Integration test with a minimal DuckDB project."""
import os
import sys
import tempfile
import shutil

# Add sqlmesh to path
sys.path.insert(0, "/Users/nico/Code/sqlmesh")

# Set up console transport for testing (prints events instead of sending to server)
os.environ["OPENLINEAGE_URL"] = "console://localhost"
os.environ["OPENLINEAGE_NAMESPACE"] = "test_namespace"

import sqlmesh_openlineage

# Install the OpenLineage console wrapper
sqlmesh_openlineage.install()

print("OpenLineage integration installed:", sqlmesh_openlineage.is_installed())

# Create a minimal temp project
tmpdir = tempfile.mkdtemp(prefix="sqlmesh_ol_test_")
print(f"\nCreating test project in: {tmpdir}")

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

# Create a simple model
model_content = """
MODEL (
    name test_db.source_data,
    kind FULL
);

SELECT 1 as id, 'foo' as name
UNION ALL
SELECT 2 as id, 'bar' as name
"""
with open(os.path.join(models_dir, "source_data.sql"), "w") as f:
    f.write(model_content)

# Create a downstream model to show lineage
downstream_content = """
MODEL (
    name test_db.processed_data,
    kind FULL
);

SELECT
    id,
    name,
    UPPER(name) as name_upper
FROM test_db.source_data
"""
with open(os.path.join(models_dir, "processed_data.sql"), "w") as f:
    f.write(downstream_content)

# Now import and run SQLMesh
from sqlmesh import Context

# Create context for our test project
ctx = Context(paths=[tmpdir])

print(f"\nLoaded {len(ctx.models)} models")
print("Models:", list(ctx.models.keys()))

# Check that our console is installed
from sqlmesh.core.console import get_console
console = get_console()
print(f"\nConsole type: {type(console).__name__}")
print(f"Is OpenLineageConsole: {type(console).__name__ == 'OpenLineageConsole'}")

# Patch emitter to print events
from sqlmesh_openlineage.console import OpenLineageConsole
console = get_console()
if isinstance(console, OpenLineageConsole):
    original_emit = console._emitter.client.emit
    def debug_emit(event):
        print(f"\n[OpenLineage Event] {event.eventType.name}: {event.job.name}")
        if event.inputs:
            print(f"  Inputs: {[i.name for i in event.inputs]}")
        if event.outputs:
            print(f"  Outputs: {[o.name for o in event.outputs]}")
        if hasattr(event, 'run') and event.run and event.run.facets:
            print(f"  Facets: {list(event.run.facets.keys())}")
        return original_emit(event)
    console._emitter.client.emit = debug_emit

# Run a plan to trigger model evaluation
print("\n--- Running SQLMesh plan (this should emit OpenLineage events) ---\n")
try:
    ctx.plan(auto_apply=True, no_prompts=True)
    print("\n--- Plan completed successfully ---")
except Exception as e:
    print(f"Error during plan: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
shutil.rmtree(tmpdir)
print(f"\nCleaned up temp directory: {tmpdir}")
print("\nIntegration test complete!")
