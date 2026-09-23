"""Internal SQLClient CREATE-exclusion adapter composition."""

from collections.abc import Callable
from typing import Any

from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import SqlClientCreateExclusionObserverV2, _Cursor
from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import FixedSqlClientCreateExclusionQueries


def compose_create_exclusion_observer_v2(
    cursor: _Cursor,
    *,
    admission: Any,
    observer_admission: Any,
    operation_deadline_ns: int,
    monotonic_ns: Callable[[], int],
) -> SqlClientCreateExclusionObserverV2:
    """Bind the reviewed SQL provider to the lifecycle-owning observer."""
    return SqlClientCreateExclusionObserverV2._composed(
        cursor,
        admission=admission,
        observer_admission=observer_admission,
        operation_deadline_ns=operation_deadline_ns,
        monotonic_ns=monotonic_ns,
        query_provider=FixedSqlClientCreateExclusionQueries(SqlClientCreateExclusionObserverV2._sample_kind),
    )
