"""Stable fail-closed error for SQL Server load-strategy contracts."""

from __future__ import annotations


class MSSQLStrategyContractError(ValueError):
    """Reject an unsupported MSSQL strategy before staging is materialized."""

    code = "DPONE_MSSQL_STRATEGY_CONTRACT_BLOCKED"

    def __init__(self, blocker: str, detail: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}: {blocker}: {detail}")


__all__ = ["MSSQLStrategyContractError"]
