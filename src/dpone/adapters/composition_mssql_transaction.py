"""Observe one existing protected transaction without acquiring or changing it.

The configured service pin and actual database lock remain mandatory on every
call. Returning a transaction ID grants no executor and does not audit the full
catalog; the owning boundary performs that separate check before shared reads.
"""

from uuid import UUID

from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    COMPOSITION_MSSQL_SCHEMA_VERSION,
    require_control_schema,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.ports.composition_sql import CompositionSqlContext


def require_shared_transaction_in(
    context: CompositionSqlContext,
    *,
    expected_service_id: str,
    transaction_id: int | None = None,
) -> int:
    """Read the actual transaction and optionally require the same prior ID.

    Invalid/doomed transactions never call transaction-owned APPLOCK_MODE.
    CURRENT_TRANSACTION_ID observes this session without granting access to
    other sessions' DMVs. A lazy history iterator must recheck its captured ID
    between pages and after every injected proof callback.
    """
    try:
        valid_service = type(expected_service_id) is str and str(UUID(expected_service_id)) == expected_service_id
    except (ValueError, TypeError, AttributeError):
        valid_service = False
    if not valid_service:
        raise CompositionAdmissionError("control_service_id")
    try:
        schema = require_control_schema(context.schema)
    except ValueError:
        raise CompositionAdmissionError("control_schema") from None
    if transaction_id is not None and (type(transaction_id) is not int or not 1 <= transaction_id < 2**63):
        raise CompositionAdmissionError("shared_transaction_identity")
    cursor = context.cursor
    cursor.execute(
        "DECLARE @count int=@@TRANCOUNT, @state smallint=XACT_STATE(), "
        "@mode nvarchar(32)=N'NoLock', @identity bigint=NULL; "
        "IF @count>0 AND @state=1 BEGIN "
        "SET @mode=APPLOCK_MODE(N'public', ?, N'Transaction'); "
        "SET @identity=CURRENT_TRANSACTION_ID(); END; "
        "SELECT @count,@state,@mode,@identity;",
        COMPOSITION_MSSQL_LEDGER_LOCK,
    )
    rows = tuple(tuple(value) for value in cursor.fetchall())
    if (
        len(rows) != 1
        or len(rows[0]) != 4
        or type(rows[0][0]) is not int
        or rows[0][0] < 1
        or type(rows[0][1]) is not int
        or rows[0][1] != 1
        or rows[0][2] != "Exclusive"
        or type(rows[0][3]) is not int
        or not 1 <= rows[0][3] < 2**63
    ):
        raise CompositionAdmissionError("shared_transaction")
    observed_id = rows[0][3]
    if transaction_id is not None and observed_id != transaction_id:
        raise CompositionAdmissionError("shared_transaction_identity")
    cursor.execute(
        "SELECT TOP (2) singleton,schema_version,LOWER(CONVERT(char(36),service_id)) "
        f"FROM [{schema}].[composition_authority] WITH (HOLDLOCK);"
    )
    rows = tuple(tuple(value) for value in cursor.fetchall())
    if (
        rows != ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, expected_service_id),)
        or type(rows[0][0]) is not int
        or type(rows[0][1]) is not int
    ):
        raise CompositionAdmissionError("control_authority")
    return observed_id
