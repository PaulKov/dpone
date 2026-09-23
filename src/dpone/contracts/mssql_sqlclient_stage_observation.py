"""Current empty catalog observations; no CREATE, Prepared or launch authority.

Readings bracket a real emptiness query but are sequential and do not lock the
catalog for future use. Consumers must independently retain attempt ownership,
provenance, deployment admission and later grant/launch safeguards.
"""

from collections.abc import Sequence
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation, _context_records, _record
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
)
from dpone.contracts.mssql_tds_coordinator_authority import identifier
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.mssql_tds_validation import _hash, _integer

PROFILE = "sqlclient_stage_catalog_sql16_17_v1"


def snapshot_stage_identity(value: SqlClientStageIdentity) -> SqlClientStageIdentity:
    """Validate exact original scalars before constructing a detached snapshot."""
    if type(value) is not SqlClientStageIdentity:
        raise ValueError("mssql_native.sqlclient_stage_observation_invalid")
    if type(value.columns) is not tuple:
        raise ValueError("mssql_native.sqlclient_stage_observation_invalid")
    for column in value.columns:
        if type(column) is not TdsCreateObservedColumn:
            raise ValueError("mssql_native.sqlclient_stage_observation_invalid")
        TdsCreateObservedColumn(**{f.name: getattr(column, f.name) for f in fields(TdsCreateObservedColumn)})
    SqlClientStageIdentity(**{f.name: getattr(value, f.name) for f in fields(SqlClientStageIdentity)})
    return decode_stage_identity(encode_stage_identity(value))


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientStageObservation:
    """Immutable observations from one management incarnation and fixed profile."""

    before: SqlClientStageIdentity
    after: SqlClientStageIdentity
    management_before: SqlClientObserverIncarnation
    management_after: SqlClientObserverIncarnation
    empty: int
    metadata_permissions: tuple[int, int, int]
    profile: str = PROFILE

    def __post_init__(self) -> None:
        for identity in (self.before, self.after):
            snapshot_stage_identity(identity)
        for management in (self.management_before, self.management_after):
            _record(management, SqlClientObserverIncarnation)
        db = self.management_before.authority.database
        if (
            self.before != self.after
            or self.management_before != self.management_after
            or (self.before.database_id, self.before.database_name, str(self.before.database_guid))
            != (db.database_id, db.database_name, db.database_guid)
            or type(self.empty) is not int
            or self.empty != 1
            or type(self.metadata_permissions) is not tuple
            or len(self.metadata_permissions) != 3
            or any(type(p) is not int or p != 1 for p in self.metadata_permissions)
            or type(self.profile) is not str
            or self.profile != PROFILE
            or self.management_before.visibility.server_major_version not in (16, 17)
        ):
            raise ValueError("mssql_native.sqlclient_stage_observation_invalid")


_ERROR = "mssql_native.sqlclient_stage_observation_invalid"

OBSERVED_TYPES = {
    "bigint": TdsCreateType.BIGINT,
    "float": TdsCreateType.FLOAT53,
    "nvarchar": TdsCreateType.NVARCHARMAX,
    "datetime2": TdsCreateType.DATETIME2_6,
}


def quote_stage_identifier(value: str) -> str:
    """Quote each admitted SQL identifier separately; dots remain name data."""
    identifier(value)
    return "[" + value.replace("]", "]]") + "]"


def parse_stage_object(row: Sequence[Any]) -> tuple[Any, ...]:
    """Canonical CREATE/current-stage object projection; reject unknown flags."""
    if len(row) != 9 or any(type(value) is not int or value != 0 for value in row[5:]):
        raise ValueError(_ERROR)
    _integer(row[0], 1, 2**31 - 1)
    identifier(row[1])
    _hash(row[3])
    if (
        type(row[2]) is not datetime
        or row[2].tzinfo is not None
        or row[2] < datetime(1900, 1, 1)
        or type(row[4]) is not str
        or str(UUID(row[4])) != row[4]
        or not UUID(row[4]).int
    ):
        raise ValueError(_ERROR)
    return tuple(row[:5])


def parse_stage_columns(rows: Sequence[Any]) -> tuple[TdsCreateObservedColumn, ...]:
    """Shared physical column parser; four exact types, ordered width at most100."""
    if not 1 <= len(rows) <= 100:
        raise ValueError(_ERROR)
    columns = []
    for ordinal, column in enumerate(rows, 1):
        if (
            len(column) != 11
            or type(column[2]) is not str
            or type(column[8]) is not int
            or column[8] != 0
            or column[9] is not False
            or column[10] is not False
        ):
            raise ValueError(_ERROR)
        kind = OBSERVED_TYPES.get(column[2])
        if kind is None:
            raise ValueError(_ERROR)
        observed = TdsCreateObservedColumn(column[0], column[1], kind, *column[3:8])
        if observed.ordinal != ordinal:
            raise ValueError(_ERROR)
        columns.append(observed)
    if len({c.name for c in columns}) != len(columns):
        raise ValueError(_ERROR)
    return tuple(columns)


def snapshot_stage_admission(value: SqlClientObserverAdmission) -> SqlClientObserverAdmission:
    """Validate and detach independently supplied management context before SQL."""
    if type(value) is not SqlClientObserverAdmission:
        raise ValueError(_ERROR)
    _context_records(value)
    return SqlClientObserverAdmission(
        **{
            field.name: type(item)(**{f.name: getattr(item, f.name) for f in fields(item)})
            for field in fields(value)
            for item in (getattr(value, field.name),)
        }
    )
