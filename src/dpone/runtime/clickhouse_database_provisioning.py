"""Provision databases without requiring CREATE on verified existing databases."""

from __future__ import annotations

import re
from typing import Any


def ensure_database(connector: Any, *, database: str, statement: str, cluster: str | None = None) -> None:
    """Keep normal DDL semantics; recover only a proven redundant CREATE denial.

    A restricted account may create tables in a DBA-provisioned database but
    cannot issue even ``CREATE DATABASE IF NOT EXISTS``. On server code 497,
    verify the database through the same connection. Cluster DDL requires the
    database on every configured member, with unavailable-shard skipping off.
    Missing databases, unreadable catalogs and other DDL errors remain fatal.
    This does not waive table, audit, or data-writing permissions.
    """
    try:
        connector.execute_query(statement)
    except Exception as error:
        if not _access_denied(error):
            raise
        try:
            exists = _exists(connector, database, cluster)
        except Exception:
            exists = False
        if not exists:
            raise


def _access_denied(error: BaseException) -> bool:
    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        code = getattr(error, "code", None)
        if code is not None:
            return str(code) == "497"
        match = re.search(r"\bCode:\s*(\d+)\.", str(error))
        if match:
            return match.group(1) == "497"
        cause = error.__cause__
        if cause is None:
            return False
        error = cause
    return False


def _scalar(rows: Any) -> int:
    if not isinstance(rows, (list, tuple)) or len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError("database existence probe must return one scalar")
    value = rows[0][0]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("database existence probe must return a nonnegative integer")
    return value


def _exists(connector: Any, database: str, cluster: str | None) -> bool:
    if not cluster:
        identifier = "`" + database.replace("\\", "\\\\").replace("`", "``") + "`"
        return _scalar(connector.get_records(f"EXISTS DATABASE {identifier}")) == 1
    members = _scalar(
        connector.get_records("SELECT count() FROM system.clusters WHERE cluster = %(cluster)s", {"cluster": cluster})
    )
    if members == 0:
        return False
    present = _scalar(
        connector.get_records(
            "SELECT count() FROM clusterAllReplicas(%(cluster)s, system.databases) "
            "WHERE name = %(database)s SETTINGS skip_unavailable_shards=0",
            {"cluster": cluster, "database": database},
        )
    )
    return present == members
