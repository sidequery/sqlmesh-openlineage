"""SQLMesh OpenLineage integration.

This package provides OpenLineage event emission for SQLMesh without requiring
any modifications to SQLMesh itself. It uses SQLMesh's set_console() API to
inject a custom Console wrapper that emits OpenLineage events.

## Quick Start (CLI Users)

Add this to your `config.py`:

```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:5000",
    namespace="my_project",
)

from sqlmesh.core.config import Config
config = Config(...)
```

Then run `sqlmesh run` as normal.

## Environment Variables

You can also configure via environment variables:

```bash
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=my_project
export OPENLINEAGE_API_KEY=...  # optional
```

## Programmatic Usage

```python
from sqlmesh_openlineage import OpenLineageConsole
from sqlmesh.core.console import set_console, create_console

console = OpenLineageConsole(
    wrapped=create_console(),
    url="http://localhost:5000",
    namespace="my_project",
)
set_console(console)
```
"""

from sqlmesh_openlineage.install import install, is_installed
from sqlmesh_openlineage.console import OpenLineageConsole
from sqlmesh_openlineage.emitter import OpenLineageEmitter

__version__ = "0.1.0"

__all__ = [
    "install",
    "is_installed",
    "OpenLineageConsole",
    "OpenLineageEmitter",
]
