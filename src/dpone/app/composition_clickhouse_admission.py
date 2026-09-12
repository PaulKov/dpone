"""Protected ClickHouse enrollment callback installed by the activation root."""

from __future__ import annotations

from typing import Any

from dpone.adapters.composition_clickhouse_supervisor_schema import require_clickhouse_supervisor_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import close, rollback
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionPhysicalDomain


def require_protected_clickhouse_enrollment(
    domain: CompositionPhysicalDomain,
    context: Any,
    *,
    connections: Any,
    control_schema: str,
) -> None:
    """Prove the ClickHouse domain is enrolled before catalog observation.

    Construction is lazy: SQL opens only when physical admission invokes this
    callback. Missing exclusive supervisor enrollment rejects the parent.
    """

    domain.__post_init__()
    if domain.connector != "clickhouse":
        raise CompositionAdmissionError("clickhouse_enrollment")
    control = cursor = None
    failure: CompositionAdmissionError | None = None
    try:
        control = connections.control_connection(context)
        control.autocommit = False
        cursor = control.cursor()
        ledger = CompositionMssqlLedger(cursor, control_schema)
        transaction = ledger.begin(domain.service_id)
        require_clickhouse_supervisor_schema(cursor, control_schema)
        cursor.execute(
            "SELECT TOP (2) connector,LOWER(CONVERT(char(36),service_id)),physical_subject_sha256 "
            f"FROM {ledger.table('domains')} WITH (HOLDLOCK) WHERE guard_id=?;",
            domain.guard_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if rows != ((domain.connector, domain.service_id, domain.physical_subject_sha256),):
            raise CompositionAdmissionError("clickhouse_enrollment")
        cursor.execute(
            "SELECT TOP (2) LOWER(CONVERT(char(36),service_id)) "
            f"FROM {ledger.table('ch_supervisor_enrollments')} WITH (HOLDLOCK) "
            "WHERE service_id=?;",
            domain.service_id,
        )
        enrollments = tuple(tuple(row) for row in cursor.fetchall())
        if not enrollments or any(row != (domain.service_id,) for row in enrollments):
            raise CompositionAdmissionError("clickhouse_enrollment")
        ledger.require_transaction(transaction)
    except CompositionAdmissionError as exc:
        failure = exc
    except Exception:
        failure = CompositionAdmissionError("clickhouse_enrollment")
    finally:
        rollback(control)
        close(cursor)
        close(control)
    if failure is not None:
        raise failure from None


__all__ = ["require_protected_clickhouse_enrollment"]
