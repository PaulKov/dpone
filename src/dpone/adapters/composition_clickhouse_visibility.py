"""Read the protected observer's global catalog grants on ClickHouse 24.8.

The initial observer profile requires direct global grants and no partial revoke.
Role-only or scoped grants are unavailable, never inferred as complete visibility.
The server's system.grants implementation provides grant originals; each call
uses the same bounded HTTP credential as the subsequent catalog observations.
"""

from __future__ import annotations

from uuid import uuid4

from dpone.adapters.composition_clickhouse_http import BoundedClickHouseHttp, clickhouse_http_path
from dpone.contracts.composition_snapshot_materialization import catalog_visibility_original

# Group ancestors are defined by Access/Common/AccessType.h in 24.8.14.39-lts.
# StorageSystemGrants emits SQL NULL for global database/table/column dimensions.
_REQUIRED = (
    ("SHOW DATABASES", "SHOW", "ALL"),
    ("SHOW TABLES", "SHOW", "ALL"),
    ("SHOW COLUMNS", "SHOW", "ALL"),
    ("SHOW USERS", "SHOW ACCESS", "ACCESS MANAGEMENT", "ALL"),
    ("SHOW ROLES", "SHOW ACCESS", "ACCESS MANAGEMENT", "ALL"),
    ("SHOW ROW POLICIES", "SHOW ACCESS", "ACCESS MANAGEMENT", "ALL"),
    ("SELECT", "ALL"),
)
_GLOBAL = "isNull(database) AND isNull(table) AND isNull(column) AND NOT is_partial_revoke"
_CONDITIONS = " AND ".join(
    f"countIf({_GLOBAL} AND toString(access_type) IN ({','.join(repr(value) for value in kinds)}))>0"
    for kinds in _REQUIRED
)
_QUERY = (
    "SELECT currentUser() AS observer, toString(serverUUID()) AS service, "
    "toUInt64(countIf(is_partial_revoke)) AS revokes, "
    f"toUInt64({_CONDITIONS}) AS complete FROM system.grants "
    "WHERE user_name=currentUser() FORMAT JSONCompact"
).encode()


class ClickHouseCatalogVisibility:
    """Fresh global visibility original, bounded to one small response row."""

    def __init__(self, *, http: BoundedClickHouseHttp, service_id: str, username: str) -> None:
        self._http, self._service, self._username = http, service_id, username

    def __call__(self) -> bytes:
        query_id = "dpone-visibility-" + uuid4().hex
        body: bytes | None
        try:
            body = self._http.request(
                path=clickhouse_http_path(query_id=query_id, result_rows=2), payload=_QUERY, query_id=query_id
            ).body
        except Exception:
            body = None
        return catalog_visibility_original(body, expected_service_id=self._service, expected_observer=self._username)
