"""Lossless direct-permission inventory; not effective rights or Prepared evidence.

This component's 8 MiB ceiling is not an IPC allowance. A production caller still
needs externally supervised SQL, deployment acceptance and authenticated original
CREATE evidence. Records neither retain a SQL lock nor authorize writer launch.
"""

from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import _name, _sid
from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation, _observer_bytes
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity, encode_stage_identity
from dpone.contracts.mssql_sqlclient_stage_locator import SqlClientStageLocatorSnapshot, encode_stage_locator
from dpone.contracts.mssql_tds_coordinator_authority import TdsCoordinatorAuthority, decode_authority, encode_authority
from dpone.contracts.mssql_tds_create import TdsCreateType
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_validation import _integer, _text
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

PROFILE = "sqlclient_direct_permissions_sql16_17_sysadmin_v2"
_ERROR = "mssql_native.sqlclient_grant_inventory_invalid"


def validate_inventory_original(value: Any, cls: type) -> None:
    """Reject original nested scalar aliases before codecs/deepcopy normalize them."""
    if type(value) is not cls:
        raise ValueError(_ERROR)

    def visit(item: Any) -> None:
        if isinstance(item, UUID):
            if type(item) is not UUID or type(item.int) is not int or not 1 <= item.int <= 2**128 - 1:
                raise ValueError(_ERROR)
        elif isinstance(item, Enum):
            if type(item) not in (TdsCreateType, TdsCoordinatorCommand):
                raise ValueError(_ERROR)
        elif isinstance(item, str):
            if type(item) is not str:
                raise ValueError(_ERROR)
        elif isinstance(item, datetime):
            if type(item) is not datetime:
                raise ValueError(_ERROR)
        elif is_dataclass(item):
            if isinstance(item, type):
                raise ValueError(_ERROR)
            for field in fields(item):
                visit(getattr(item, field.name))
            replace(item)
        elif isinstance(item, tuple):
            for child in item:
                visit(child)

    visit(value)


def snapshot_inventory_authority(value: TdsCoordinatorAuthority) -> TdsCoordinatorAuthority:
    validate_inventory_original(value, TdsCoordinatorAuthority)
    return decode_authority(encode_authority(value))


@dataclass(frozen=True, slots=True)
class SqlClientGrantInventoryLimits:
    permission_rows: int = 4096
    members: int = 1024
    observation_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        _integer(self.permission_rows, 1, 4096)
        _integer(self.members, 1, 1024)
        _integer(self.observation_bytes, 1, 8 * 1024 * 1024)


@dataclass(frozen=True, slots=True)
class SqlClientPermissionRow:
    """One unfiltered sys.database_permissions row, including negative IDs/D/R/W."""

    class_id: int
    major_id: int
    minor_id: int
    grantee_principal_id: int
    grantor_principal_id: int
    type: str
    permission_name: str
    state: str

    def __post_init__(self) -> None:
        _integer(self.class_id, 0, 255)
        _integer(self.major_id, -(2**31), 2**31 - 1)
        for value in (self.minor_id, self.grantee_principal_id, self.grantor_principal_id):
            _integer(value, 0, 2**31 - 1)
        _text(self.type, 4)
        _text(self.permission_name, 128)
        if type(self.state) is not str or self.state not in ("D", "R", "W", "G"):
            raise ValueError(_ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientGrantPrincipal:
    """Actual catalog projection; public role can have principal_id zero."""

    principal_id: int
    name: str
    sid: str
    type_desc: str
    authentication_type_desc: str

    def __post_init__(self) -> None:
        _integer(self.principal_id, 0, 2**31 - 1)
        _name(self.name)
        _sid(self.sid)
        _text(self.type_desc, 60)
        _text(self.authentication_type_desc, 60)

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> "SqlClientGrantPrincipal":
        if len(row) != 5 or type(row[2]) is not bytes or not 1 <= len(row[2]) <= 85:
            raise ValueError(_ERROR)
        return cls(row[0], row[1], row[2].hex(), row[3], row[4])


@dataclass(frozen=True, slots=True)
class SqlClientGrantMember:
    """Current table identity resolved to original journals, without CREATE proof."""

    stage: SqlClientStageIdentity
    locator: SqlClientStageLocatorSnapshot

    def __post_init__(self) -> None:
        validate_inventory_original(self.stage, SqlClientStageIdentity)
        validate_inventory_original(self.locator, SqlClientStageLocatorSnapshot)
        actual, original = self.stage, self.locator.locator
        parent, database = original.create_operation.parent, original.database
        if (actual.database_name, actual.database_id, actual.database_guid) != (
            database.name,
            database.database_id,
            database.database_guid,
        ) or (actual.schema_name, actual.table_name, actual.owner_binding, actual.object_nonce) != (
            parent.schema,
            parent.table,
            parent.owner_binding,
            original.object_nonce,
        ):
            raise ValueError(_ERROR)


def inventory_member_bytes(member: SqlClientGrantMember) -> bytes:
    validate_inventory_original(member, SqlClientGrantMember)
    return canonical_json_bytes(
        {
            "stage": strict_json_object(encode_stage_identity(member.stage)),
            "locator": strict_json_object(encode_stage_locator(member.locator.locator)),
            "locator_revision": member.locator.revision,
        }
    )


@dataclass(frozen=True, slots=True)
class SqlClientGrantInventory:
    """Opening membership with equal closing reobservation by the actual producer."""

    authority: TdsCoordinatorAuthority
    management_before: SqlClientObserverIncarnation
    management_after: SqlClientObserverIncarnation
    writer: SqlClientGrantPrincipal
    public: SqlClientGrantPrincipal
    permissions: tuple[SqlClientPermissionRow, ...]
    members: tuple[SqlClientGrantMember, ...]
    limits: SqlClientGrantInventoryLimits
    excluded_object_ids: tuple[int, ...]
    profile: str = PROFILE

    def __post_init__(self) -> None:
        for item, cls in (
            (self.authority, TdsCoordinatorAuthority),
            (self.management_before, SqlClientObserverIncarnation),
            (self.management_after, SqlClientObserverIncarnation),
            (self.writer, SqlClientGrantPrincipal),
            (self.public, SqlClientGrantPrincipal),
            (self.limits, SqlClientGrantInventoryLimits),
        ):
            validate_inventory_original(item, cls)
        if (
            type(self.profile) is not str
            or self.profile != PROFILE
            or type(self.permissions) is not tuple
            or type(self.members) is not tuple
            or type(self.excluded_object_ids) is not tuple
            or len(self.permissions) > self.limits.permission_rows
            or len(self.members) > self.limits.members
            or self.management_before != self.management_after
            or self.management_before.authority.login.is_sysadmin is not True
            or self.management_before.visibility.server_major_version not in (16, 17)
            or self.writer.principal_id <= 4
            or self.writer.type_desc != "SQL_USER"
            or self.writer.authentication_type_desc != "INSTANCE"
            or self.public.name != "public"
            or self.public.type_desc != "DATABASE_ROLE"
            or self.public.authentication_type_desc != "NONE"
            or self.public.principal_id == self.writer.principal_id
        ):
            raise ValueError(_ERROR)
        own, session, database = self.management_before, self.authority.session, self.authority.database
        if (own.connection_id, own.session_id, own.connect_time, own.login_time) != (
            session.connection_id,
            session.session_id,
            session.connect_time,
            session.login_time,
        ) or (database.name, database.database_id, str(database.database_guid)) != (
            own.authority.database.database_name,
            own.authority.database.database_id,
            own.authority.database.database_guid,
        ):
            raise ValueError(_ERROR)
        for row in self.permissions:
            validate_inventory_original(row, SqlClientPermissionRow)
            if row.grantee_principal_id not in (self.writer.principal_id, self.public.principal_id):
                raise ValueError(_ERROR)
        expected = {r.major_id for r in self.permissions if r.class_id == 1 and r.major_id > 0}
        observed = []
        for member in self.members:
            validate_inventory_original(member, SqlClientGrantMember)
            original = member.locator.locator
            if original.server != own.authority.server or original.database != database:
                raise ValueError(_ERROR)
            observed.append(member.stage.object_id)
        if len({member.locator.locator.state_domain_id for member in self.members}) > 1:
            raise ValueError(_ERROR)
        excluded = self.excluded_object_ids
        if any(type(value) is not int or value <= 0 for value in excluded):
            raise ValueError(_ERROR)
        observed_ids = set(observed)
        excluded_ids = set(excluded)
        # Every positive class-1 permission candidate is classified exactly
        # once.  This prevents a producer from silently omitting an owned stage
        # while still preserving unrelated public grants losslessly.
        if (
            len(observed_ids) != len(observed)
            or tuple(sorted(excluded)) != excluded
            or len(excluded_ids) != len(excluded)
            or observed_ids & excluded_ids
            or observed_ids | excluded_ids != expected
        ):
            raise ValueError(_ERROR)


def encode_grant_inventory(record: SqlClientGrantInventory) -> bytes:
    """Canonical component bytes only; never increases any existing IPC ceiling."""
    validate_inventory_original(record, SqlClientGrantInventory)
    data = {
        "profile": record.profile,
        "limits": asdict(record.limits),
        "authority": strict_json_object(encode_authority(record.authority)),
        "management_before": strict_json_object(_observer_bytes(record.management_before)),
        "management_after": strict_json_object(_observer_bytes(record.management_after)),
        "writer": asdict(record.writer),
        "public": asdict(record.public),
        "permissions": [asdict(row) for row in record.permissions],
        "excluded_object_ids": list(record.excluded_object_ids),
        "members": [],
    }
    used = len(canonical_json_bytes(data))
    members: list[dict[str, Any]] = []
    for member in record.members:
        body = inventory_member_bytes(member)
        used += len(body) + (1 if members else 0)
        if used > record.limits.observation_bytes:
            raise ValueError(_ERROR)
        members.append(strict_json_object(body))
    data["members"] = members
    payload = canonical_json_bytes(data)
    if len(payload) > record.limits.observation_bytes:
        raise ValueError(_ERROR)
    return payload
