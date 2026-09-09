"""Narrow storage capability for generic SQL Server transaction governance."""

from __future__ import annotations

from typing import Any

from dpone.runtime.state.mssql_database_authority_binding import (
    MssqlDatabaseAuthorityBindingMixin,
)
from dpone.runtime.state.mssql_generic_transaction_names import GENERIC_TRANSACTION_TABLES

MSSQL_GENERIC_TRANSACTION_STATE_CAPABILITY = "mssql_generic_transaction_state_v1"


class MssqlGenericTransactionStateStorage(MssqlDatabaseAuthorityBindingMixin):
    """Bind only the four generic fence/attempt/operation/receipt objects.

    This adapter deliberately has no XMin checkpoint, repair, audit, or runtime
    provisioning API.  External DDL is the sole authority for its catalog.
    """

    atomicity = "target_atomic"
    provisioning = "external"
    mssql_state_capability = MSSQL_GENERIC_TRANSACTION_STATE_CAPABILITY
    required_catalog_tables = GENERIC_TRANSACTION_TABLES

    def __init__(self, connector: Any, *, database: str, schema: str) -> None:
        if not str(database).strip() or not str(schema).strip():
            raise ValueError("mssql_transaction.state_location_incomplete")
        self.connector = connector
        self.database = str(database)
        self.schema = str(schema)
        self._transaction_connector: Any | None = None
        self._initialize_database_authority_binding()

    def bind_transaction_connector(self, target_connector: Any) -> MssqlGenericTransactionStateStorage:
        self._transaction_connector = target_connector
        return self


def is_mssql_generic_transaction_storage(storage: Any) -> bool:
    """Return whether a storage exposes only the generic four-object catalog."""

    return getattr(storage, "mssql_state_capability", None) == MSSQL_GENERIC_TRANSACTION_STATE_CAPABILITY


__all__ = [
    "MSSQL_GENERIC_TRANSACTION_STATE_CAPABILITY",
    "MssqlGenericTransactionStateStorage",
    "is_mssql_generic_transaction_storage",
]
