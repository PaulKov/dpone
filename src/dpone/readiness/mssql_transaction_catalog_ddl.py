"""Operator-facing readiness service for the external MSSQL state catalog."""

from __future__ import annotations

from dpone.runtime.state.mssql_generic_transaction_ddl import (
    render_generic_transaction_catalog_ddl,
    render_generic_transaction_catalog_v1_to_v2_ddl,
)


class MssqlTransactionCatalogDdlService:
    """Expose versioned catalog DDL without coupling CLI commands to runtime."""

    @staticmethod
    def render(*, database: str, schema: str, upgrade_from: int | None = None) -> str:
        """Render a fresh catalog or the only supported external migration."""

        if upgrade_from == 1:
            return render_generic_transaction_catalog_v1_to_v2_ddl(
                database=database,
                schema=schema,
            )
        if upgrade_from is not None:
            raise ValueError("mssql_transaction.unsupported_catalog_upgrade")
        return render_generic_transaction_catalog_ddl(database=database, schema=schema)


__all__ = ["MssqlTransactionCatalogDdlService"]
