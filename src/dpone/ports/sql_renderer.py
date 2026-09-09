"""SQL dialect rendering ports.

Connectors own transport/connection behavior. Dialect-specific SQL rendering is
kept behind this small port so base connector contracts do not pretend that one
ANSI-ish string is safe for every database.
"""

from __future__ import annotations

from typing import Any, Protocol


class SqlQueryRenderer(Protocol):
    """Minimal SQL rendering contract used by database connectors."""

    def build_max_query(self, schema: str, table: str, column: str) -> Any:
        """Build a dialect-specific ``MAX(column)`` query."""
        ...
