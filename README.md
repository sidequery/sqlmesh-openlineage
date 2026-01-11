# sqlmesh-openlineage

OpenLineage integration for SQLMesh. Emits per-model lineage events during SQLMesh runs without modifying SQLMesh itself.

## Features

- **Table-level lineage**: Track which models depend on which upstream models
- **Column-level lineage**: Track which columns flow from source to destination
- **Schema capture**: Column names and types for each model
- **Execution stats**: Duration, rows processed, bytes processed
- **Per-model events**: START/COMPLETE/FAIL events for each model evaluation

## Installation

```bash
pip install sqlmesh-openlineage
```

Or with uv:

```bash
uv add sqlmesh-openlineage
```

## Quick Start (CLI Users)

Add this to your `config.py`:

```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:5000",
    namespace="my_project",
    # api_key="...",  # optional
)

from sqlmesh.core.config import Config

config = Config(
    # ... your existing config
)
```

Then run `sqlmesh run` as normal. OpenLineage events will be emitted for each model evaluation.

## Environment Variables

You can also configure via environment variables:

```bash
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=my_project
export OPENLINEAGE_API_KEY=...  # optional
```

Then in `config.py`:

```python
import sqlmesh_openlineage
sqlmesh_openlineage.install()  # reads from env vars
```

## How It Works

This package uses SQLMesh's `set_console()` API to inject a custom Console wrapper. The wrapper intercepts per-snapshot lifecycle events and emits corresponding OpenLineage events:

- `START` event when a model evaluation begins
- `COMPLETE` event when evaluation succeeds (includes execution stats)
- `FAIL` event when evaluation fails or audits fail

## Events Emitted

| SQLMesh Event | OpenLineage Event | Data Included |
|---------------|-------------------|---------------|
| Model evaluation start | RunEvent(START) | Input datasets, output dataset with schema, column lineage |
| Model evaluation success | RunEvent(COMPLETE) | Execution stats (rows, bytes, duration) |
| Model evaluation failure | RunEvent(FAIL) | Error message |
| Audit failure | RunEvent(FAIL) | Audit failure details |

## Column-Level Lineage

The integration automatically extracts column-level lineage using SQLMesh's built-in lineage analysis. For example, if you have:

```sql
-- customers.sql
SELECT customer_id, name, email FROM raw_customers

-- customer_summary.sql
SELECT
    c.customer_id,
    c.name as customer_name,
    COUNT(o.order_id) as total_orders
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

The lineage will show that `customer_summary.customer_name` traces back to `customers.name`.

## Testing with Marquez

```bash
# Start Marquez (requires Docker)
docker compose up -d

# Configure and run SQLMesh
export OPENLINEAGE_URL=http://localhost:5001
sqlmesh run

# View lineage at http://localhost:3000
```

## Development

```bash
# Install dependencies
uv sync --dev

# Run tests (unit + integration)
uv run pytest tests/ -v

# Run Marquez integration test (requires Docker)
docker compose up -d
uv run pytest tests/test_marquez_integration.py -v -s
docker compose down
```

## License

MIT
