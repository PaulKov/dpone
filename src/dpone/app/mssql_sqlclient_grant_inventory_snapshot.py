"""Same-session grant and owned-stage catalog snapshot projection."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from dpone.adapters.mssql_sqlclient_observer_incarnation import parse_observer_incarnation_rows
from dpone.contracts.mssql_sqlclient_grant_inventory import (
    SqlClientGrantInventoryLimits,
    SqlClientGrantPrincipal,
    SqlClientPermissionRow,
    validate_inventory_original,
)
from dpone.contracts.mssql_sqlclient_observation import validate_catalog_admission
from dpone.contracts.mssql_tds_api import (
    canonical_json_bytes,
    encode_stage_identity,
    identifier,
    parse_stage_columns,
    parse_stage_object,
)
from dpone.contracts.mssql_tds_operation_models import (
    SqlClientDatabasePrincipal,
    SqlClientObserverAdmission,
    SqlClientObserverIncarnation,
    SqlClientStageIdentity,
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
)
from dpone.contracts.mssql_tds_validation import _integer
from dpone.ports.mssql_sqlclient_grant_catalog import SqlClientGrantCatalogPort

_ERROR = "mssql_native.sqlclient_grant_inventory_unknown"


def project_stage(
    catalog: SqlClientGrantCatalogPort, database: TdsDatabaseObservation, object_id: int
) -> SqlClientStageIdentity | None:
    """Build an owned stage identity, or exclude an unrelated SQL object.

    Permission inventory is deliberately lossless and therefore contains
    object permissions inherited from ``public`` as well as writer grants.
    Only a user table carrying both dpone identity markers enters the stage
    inventory; a partial or malformed marker remains an uncertain outcome.
    """
    rows = catalog.member(object_id)
    if len(rows) != 1 or len(rows[0]) != 6:
        raise ValueError(_ERROR)
    oid, schema_id, schema, table, kind, created = rows[0]
    _integer(oid, 1, 2**31 - 1)
    _integer(schema_id, 1, 2**31 - 1)
    identifier(schema)
    identifier(table)
    if oid != object_id or type(kind) is not str or type(created) is not datetime:
        raise ValueError(_ERROR)
    if kind != "USER_TABLE":
        return None
    properties = catalog.object_properties(schema_id, table)
    if len(properties) != 1:
        raise ValueError(_ERROR)
    row = properties[0]
    if type(row) not in (tuple, list) or len(row) != 9:
        raise ValueError(_ERROR)
    owner, incarnation = (row[3], row[4])
    owner_absent = owner is None or owner == ""
    incarnation_absent = incarnation is None or incarnation == ""
    if owner_absent and incarnation_absent:
        return None
    if owner_absent or incarnation_absent:
        raise ValueError(_ERROR)
    observed = parse_stage_object(row)
    if observed[:3] != (oid, table, created):
        raise ValueError(_ERROR)
    features = catalog.features(object_id)
    if (
        len(features) != 1
        or len(features[0]) != 12
        or any(type(value) is not int or value != 0 for value in features[0])
    ):
        raise ValueError(_ERROR)
    stage = SqlClientStageIdentity(
        database_guid=UUID(int=database.database_guid.int),
        database_id=database.database_id,
        database_name=database.name,
        schema_id=schema_id,
        schema_name=schema,
        table_name=table,
        object_id=object_id,
        create_date=observed[2],
        owner_binding=observed[3],
        object_nonce=UUID(observed[4]),
        columns=parse_stage_columns(catalog.columns(object_id)),
    )
    validate_inventory_original(stage, SqlClientStageIdentity)
    return stage


def project_stages(
    catalog: SqlClientGrantCatalogPort,
    database: TdsDatabaseObservation,
    object_ids: tuple[int, ...],
    *,
    member_limit: int,
) -> tuple[tuple[SqlClientStageIdentity, ...], tuple[int, ...]]:
    """Exhaustively classify a canonical bounded candidate set in catalog batches."""
    if (
        type(object_ids) is not tuple
        or not object_ids
        or tuple(sorted(set(object_ids))) != object_ids
        or any(type(value) is not int or value <= 0 for value in object_ids)
        or (type(member_limit) is not int)
        or (member_limit < 1)
    ):
        raise ValueError(_ERROR)
    member_rows = [
        row for offset in range(0, len(object_ids), 1024) for row in catalog.members(object_ids[offset : offset + 1024])
    ]
    members = _unique_rows(member_rows, expected=object_ids, width=6)
    user_tables: list[int] = []
    excluded: list[int] = []
    for object_id in object_ids:
        oid, schema_id, schema, table, kind, created = members[object_id]
        _integer(oid, 1, 2**31 - 1)
        _integer(schema_id, 1, 2**31 - 1)
        identifier(schema)
        identifier(table)
        if oid != object_id or type(kind) is not str or type(created) is not datetime:
            raise ValueError(_ERROR)
        (user_tables if kind == "USER_TABLE" else excluded).append(object_id)
    if not user_tables:
        return ((), tuple(excluded))
    object_rows = [
        row
        for offset in range(0, len(user_tables), 1024)
        for row in catalog.object_properties_batch(tuple(user_tables[offset : offset + 1024]))
    ]
    objects = _unique_rows(object_rows, expected=tuple(user_tables), width=9)
    owned: list[int] = []
    for object_id in user_tables:
        row = objects[object_id]
        owner, incarnation = (row[3], row[4])
        owner_absent = owner is None or owner == ""
        incarnation_absent = incarnation is None or incarnation == ""
        if owner_absent and incarnation_absent:
            excluded.append(object_id)
        elif owner_absent or incarnation_absent:
            raise ValueError(_ERROR)
        else:
            owned.append(object_id)
    if len(owned) > member_limit:
        raise ValueError(_ERROR)
    if not owned:
        return ((), tuple(sorted(excluded)))
    features = _unique_rows(catalog.features_batch(tuple(owned)), expected=tuple(owned), width=13)
    columns: dict[int, list[tuple]] = {object_id: [] for object_id in owned}
    for offset in range(0, len(owned), 81):
        for row in catalog.columns_batch(tuple(owned[offset : offset + 81])):
            if type(row) not in (tuple, list) or len(row) != 12 or row[0] not in columns:
                raise ValueError(_ERROR)
            columns[row[0]].append(tuple(row[1:]))
    stages = []
    for object_id in owned:
        member = members[object_id]
        obj = objects[object_id]
        feature = features[object_id]
        if any(type(value) is not int or value != 0 for value in feature[1:]):
            raise ValueError(_ERROR)
        observed = parse_stage_object(obj)
        if observed[:3] != (object_id, member[3], member[5]):
            raise ValueError(_ERROR)
        stage = SqlClientStageIdentity(
            database_guid=UUID(int=database.database_guid.int),
            database_id=database.database_id,
            database_name=database.name,
            schema_id=member[1],
            schema_name=member[2],
            table_name=member[3],
            object_id=object_id,
            create_date=observed[2],
            owner_binding=observed[3],
            object_nonce=UUID(observed[4]),
            columns=parse_stage_columns(columns[object_id]),
        )
        validate_inventory_original(stage, SqlClientStageIdentity)
        stages.append(stage)
    return (tuple(stages), tuple(sorted(excluded)))


def _unique_rows(rows: list, *, expected: tuple[int, ...], width: int) -> dict[int, tuple]:
    result: dict[int, tuple] = {}
    for row in rows:
        if type(row) not in (tuple, list) or len(row) != width or type(row[0]) is not int or (row[0] in result):
            raise ValueError(_ERROR)
        result[row[0]] = tuple(row)
    if tuple(sorted(result)) != expected:
        raise ValueError(_ERROR)
    return result


_ERROR = "mssql_native.sqlclient_grant_inventory_unknown"
CatalogObservation = tuple[
    SqlClientObserverIncarnation,
    SqlClientGrantPrincipal,
    SqlClientGrantPrincipal,
    tuple[SqlClientPermissionRow, ...],
    tuple[SqlClientStageIdentity, ...],
    tuple[int, ...],
]


class GrantInventorySnapshotMixin:
    """Project one bounded, internally consistent catalog observation."""

    catalog: SqlClientGrantCatalogPort
    management: SqlClientObserverAdmission
    writer_admission: SqlClientObserverAdmission
    writer_principal: SqlClientDatabasePrincipal
    authority: TdsCoordinatorAuthority
    limits: SqlClientGrantInventoryLimits

    def _guard(self) -> None:
        raise NotImplementedError

    def _management(self) -> SqlClientObserverIncarnation:
        rows = self.catalog.read_own_incarnation()
        validate_inventory_original(tuple(rows), tuple)
        observed = parse_observer_incarnation_rows(rows, admission=self.management)
        validate_inventory_original(observed, SqlClientObserverIncarnation)
        session = cast(Any, self.authority.session)
        if observed.visibility.server_major_version not in (16, 17) or (
            observed.connection_id,
            observed.session_id,
            observed.connect_time,
            observed.login_time,
        ) != (session.connection_id, session.session_id, session.connect_time, session.login_time):
            raise ValueError(_ERROR)
        return observed

    def _principals(self) -> tuple[SqlClientGrantPrincipal, SqlClientGrantPrincipal]:
        writer = self.writer_admission
        admission_rows = self.catalog.writer_admission(writer.database.database_id, writer.login.name)
        validate_inventory_original(tuple(admission_rows), tuple)
        validate_catalog_admission(admission_rows, writer)
        rows = self.catalog.principals(self.writer_principal.name, bytes.fromhex(writer.login.sid))
        parsed = tuple(SqlClientGrantPrincipal.from_row(row) for row in rows)
        expected = SqlClientGrantPrincipal(
            self.writer_principal.principal_id,
            self.writer_principal.name,
            self.writer_principal.sid,
            "SQL_USER",
            "INSTANCE",
        )
        public = [
            value
            for value in parsed
            if value.name == "public"
            and value.type_desc == "DATABASE_ROLE"
            and (value.authentication_type_desc == "NONE")
        ]
        if (
            len(parsed) != 2
            or parsed.count(expected) != 1
            or len(public) != 1
            or (public[0].principal_id == expected.principal_id)
        ):
            raise ValueError(_ERROR)
        return (next(value for value in parsed if value == expected), public[0])

    def _snapshot(self) -> CatalogObservation:
        self._guard()
        management = self._management()
        writer, public = self._principals()
        permissions = tuple(
            SqlClientPermissionRow(*row)
            for row in self.catalog.permissions(
                writer.principal_id, public.principal_id, limit=self.limits.permission_rows
            )
        )
        if any(row.grantee_principal_id not in (writer.principal_id, public.principal_id) for row in permissions):
            raise ValueError(_ERROR)
        candidates = sorted({row.major_id for row in permissions if row.class_id == 1 and row.major_id > 0})
        size = len(canonical_json_bytes([asdict(row) for row in permissions]))
        stages: tuple[SqlClientStageIdentity, ...] = ()
        excluded: tuple[int, ...] = ()
        if candidates:
            stages, excluded = project_stages(
                self.catalog, self.authority.database, tuple(candidates), member_limit=self.limits.members
            )
        for stage in stages:
            size += len(encode_stage_identity(stage))
            if size > self.limits.observation_bytes:
                raise ValueError(_ERROR)
        self._guard()
        return (management, writer, public, permissions, stages, excluded)
