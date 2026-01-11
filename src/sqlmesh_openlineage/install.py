"""Install OpenLineage integration for SQLMesh CLI usage."""
from __future__ import annotations

import os
import typing as t

_installed = False


def install(
    url: t.Optional[str] = None,
    namespace: str = "sqlmesh",
    api_key: t.Optional[str] = None,
) -> None:
    """
    Install OpenLineage integration for CLI usage.

    Call this in your config.py BEFORE importing Config.
    Uses SQLMesh's set_console() to inject OpenLineage event emission.

    Args:
        url: OpenLineage API URL. Falls back to OPENLINEAGE_URL env var.
        namespace: OpenLineage namespace. Falls back to OPENLINEAGE_NAMESPACE env var.
        api_key: Optional API key. Falls back to OPENLINEAGE_API_KEY env var.

    Example:
        # config.py
        import sqlmesh_openlineage

        sqlmesh_openlineage.install(
            url="http://localhost:5000",
            namespace="my_project",
        )

        from sqlmesh.core.config import Config
        config = Config(...)
    """
    global _installed

    if _installed:
        return

    from sqlmesh.core.console import set_console, create_console
    from sqlmesh_openlineage.console import OpenLineageConsole

    # Resolve config from args or env vars
    resolved_url = url or os.environ.get("OPENLINEAGE_URL")
    resolved_namespace = namespace or os.environ.get("OPENLINEAGE_NAMESPACE", "sqlmesh")
    resolved_api_key = api_key or os.environ.get("OPENLINEAGE_API_KEY")

    if not resolved_url:
        raise ValueError(
            "OpenLineage URL required. Pass url= or set OPENLINEAGE_URL env var."
        )

    # Create the default console for the current environment
    default_console = create_console()

    # Wrap it with OpenLineage emission
    ol_console = OpenLineageConsole(
        wrapped=default_console,
        url=resolved_url,
        namespace=resolved_namespace,
        api_key=resolved_api_key,
    )

    # Set as global console - SQLMesh's CLI will use this
    set_console(ol_console)

    _installed = True


def is_installed() -> bool:
    """Check if OpenLineage integration is installed."""
    return _installed
