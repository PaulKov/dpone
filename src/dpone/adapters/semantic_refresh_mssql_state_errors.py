"""Public errors shared by MSSQL admission state adapters."""


class SemanticRefreshMssqlStateError(RuntimeError):
    """Base state error with no driver payload or credentials."""


class SemanticRefreshMssqlStateConflict(SemanticRefreshMssqlStateError):
    """Raised when protected control state differs from admission authority."""


__all__ = ["SemanticRefreshMssqlStateConflict", "SemanticRefreshMssqlStateError"]
