"""Runtime facade for sink-dialect authority primitives.

Runtime components depend on this local seam instead of creating a separate
cross-layer dependency for every safety gate. The canonical normalization and
conflict rules remain owned by :mod:`dpone.contracts.sink_dialect`.
"""

from __future__ import annotations

from dpone.contracts.sink_dialect import (
    SinkDialectAuthorityConflictError,
    SinkDialectAuthorityMissingError,
    is_mssql_dialect,
    require_sink_dialect_authority,
    resolve_sink_dialect,
)

__all__ = [
    "SinkDialectAuthorityConflictError",
    "SinkDialectAuthorityMissingError",
    "is_mssql_dialect",
    "require_sink_dialect_authority",
    "resolve_sink_dialect",
]
