"""DAG services.

This package provides application-level helpers used by CLI commands.

Keep this layer thin:
- services orchestrate domain modules (dpone.dag, dpone.manifest)
- they do *not* contain output formatting (that's CLI responsibility)
"""

from __future__ import annotations

from .load_context import DagCommandContext, load_dag_context
from .path_view import group_consecutive

__all__ = [
    "DagCommandContext",
    "load_dag_context",
    "group_consecutive",
]
