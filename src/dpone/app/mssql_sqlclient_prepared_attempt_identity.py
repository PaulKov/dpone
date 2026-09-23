"""Compatibility imports for canonical SqlClient PREPARED attempt identity."""

from dpone.contracts.mssql_sqlclient_prepared_attempt_identity import (
    attempt_number as attempt_number,
)
from dpone.contracts.mssql_sqlclient_prepared_attempt_identity import (
    operation_id as operation_id,
)
from dpone.contracts.mssql_sqlclient_prepared_attempt_identity import (
    parent_identity as parent_identity,
)

__all__ = ("attempt_number", "operation_id", "parent_identity")
