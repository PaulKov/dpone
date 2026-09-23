"""Pure SQL-login catalog resolution and pregrant observation contracts.

An independently admitted identity is required. Catalog resolution is not an
execution-token or capability proof; even dbo resolution permits no mutation.
The fixed worker must compare its own initial and pre-copy principal tuple.
"""

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, require_session_nonce
from dpone.contracts.mssql_tds_worker_identity import _integer, _text
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.contracts.strict_record import canonical_uuid


def _uuid(value: Any) -> None:
    try:
        valid = canonical_uuid(value).int != 0
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("mssql_native.tds_invalid_uuid")


AUTHORITY_DOMAIN = b"dpone.sqlclient.session-authority.v1\0"
_ERROR = "mssql_native.sqlclient_observation_invalid"


def _name(value: Any) -> None:
    _text(value, 128)
    if len(value.encode("utf-16le")) > 256:
        raise ValueError(_ERROR)


def _sid(value: Any) -> None:
    if (
        type(value) is not str
        or not 2 <= len(value) <= 170
        or len(value) % 2
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError(_ERROR)


def _binary_sid(value: Any) -> str:
    if type(value) is not bytes or not 1 <= len(value) <= 85:
        raise ValueError(_ERROR)
    return value.hex()


@dataclass(frozen=True, slots=True)
class SqlClientServerAuthority:
    """Exact server instance facts, admitted independently of the child locator."""

    server_name: str
    machine_name: str
    instance_name: str
    physical_machine_name: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _name(value)


@dataclass(frozen=True, slots=True)
class SqlClientDatabaseAuthority:
    """Database incarnation and owner, not merely a recyclable numeric ID."""

    database_id: int
    database_name: str
    database_guid: str
    owner_sid: str

    def __post_init__(self) -> None:
        _integer(self.database_id, 1, 2**31 - 1)
        _name(self.database_name)
        _uuid(self.database_guid)
        _sid(self.owner_sid)


@dataclass(frozen=True, slots=True)
class SqlClientLoginAuthority:
    """Finite initial SQL-login context, excluding impersonation and containment.

    Authentication database 0 or 1 is an explicit trusted deployment expectation.
    The observed raw value must match; neither normalization nor fallback applies.
    """

    principal_id: int
    name: str
    sid: str
    original_name: str
    original_sid: str
    authenticating_database_id: int
    is_sysadmin: bool

    def __post_init__(self) -> None:
        _integer(self.principal_id, 1, 2**31 - 1)
        _name(self.name)
        _name(self.original_name)
        _sid(self.sid)
        _sid(self.original_sid)
        _integer(self.authenticating_database_id, 0, 1)
        if type(self.is_sysadmin) is not bool or self.name != self.original_name or self.sid != self.original_sid:
            raise ValueError(_ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientTransportAuthority:
    """Admitted physical SQL-auth transport and exact encryption state."""

    net_transport: str
    protocol_type: str
    auth_scheme: str
    encrypt_option: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _name(value)
        if (
            self.net_transport != "TCP"
            or self.protocol_type != "TSQL"
            or self.auth_scheme != "SQL"
            or type(self.encrypt_option) is not str
            or self.encrypt_option not in ("TRUE", "FALSE")
        ):
            raise ValueError(_ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientPrincipalResolution:
    """Catalog mapping only; db_owner membership does not imply dbo identity."""

    kind: str
    principal_id: int
    name: str
    sid: str

    def __post_init__(self) -> None:
        _name(self.kind)
        if self.kind not in ("sysadmin_dbo", "owner_dbo", "mapped_user"):
            raise ValueError(_ERROR)
        SqlClientDatabasePrincipal(self.principal_id, self.name, self.sid)
        if (self.kind == "mapped_user" and (self.principal_id <= 4 or self.name in ("dbo", "guest"))) or (
            self.kind != "mapped_user" and (self.principal_id != 1 or self.name != "dbo")
        ):
            raise ValueError(_ERROR)

    @property
    def principal(self) -> SqlClientDatabasePrincipal:
        """Tuple the fixed worker independently captures and rechecks."""
        return SqlClientDatabasePrincipal(self.principal_id, self.name, self.sid)


@dataclass(frozen=True, slots=True)
class SqlClientObserverAdmission:
    """Trusted root inputs, never constructed from the untrusted announcement."""

    server: SqlClientServerAuthority
    database: SqlClientDatabaseAuthority
    login: SqlClientLoginAuthority
    transport: SqlClientTransportAuthority

    def __post_init__(self) -> None:
        for value, cls in (
            (self.server, SqlClientServerAuthority),
            (self.database, SqlClientDatabaseAuthority),
            (self.login, SqlClientLoginAuthority),
            (self.transport, SqlClientTransportAuthority),
        ):
            if type(value) is not cls:
                raise ValueError(_ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientSessionAuthority(SqlClientObserverAdmission):
    """Closed immutable private evidence payload with catalog-resolution label."""

    principal_resolution: SqlClientPrincipalResolution
    schema_version: int = 1
    profile: str = "sql_login_initial_context_v1"

    def __post_init__(self) -> None:
        SqlClientObserverAdmission.__post_init__(self)
        _integer(self.schema_version, 1, 1)
        if (
            self.profile != "sql_login_initial_context_v1"
            or type(self.principal_resolution) is not SqlClientPrincipalResolution
        ):
            raise ValueError(_ERROR)
        kind = (
            "sysadmin_dbo"
            if self.login.is_sysadmin
            else "owner_dbo"
            if self.database.owner_sid == self.login.sid
            else "mapped_user"
        )
        if self.principal_resolution.kind != kind or (
            kind == "mapped_user" and self.principal_resolution.sid != self.login.sid
        ):
            raise ValueError(_ERROR)


def encode_session_authority(authority: SqlClientSessionAuthority) -> bytes:
    """Canonical closed JSON bytes; callers persist these as private evidence."""
    if type(authority) is not SqlClientSessionAuthority:
        raise ValueError(_ERROR)
    return canonical_json_bytes(asdict(authority))


def session_authority_digest(authority: SqlClientSessionAuthority) -> bytes:
    """Version-separated semantic digest, distinct from artifact byte hashes."""
    return sha256(AUTHORITY_DOMAIN + encode_session_authority(authority)).digest()


@dataclass(frozen=True, slots=True)
class SqlClientWriterObservation:
    """Pregrant snapshot; proves neither future continuity nor settlement."""

    remote_session: TdsRemoteSessionIdentity
    authority: SqlClientSessionAuthority

    def __post_init__(self) -> None:
        if (
            type(self.remote_session) is not TdsRemoteSessionIdentity
            or type(self.authority) is not SqlClientSessionAuthority
        ):
            raise ValueError(_ERROR)
        if self.remote_session.authority_sha256 != session_authority_digest(self.authority):
            raise ValueError(_ERROR)

    @property
    def resolved_database_principal(self) -> SqlClientDatabasePrincipal:
        """Catalog tuple, not permission to send a privileged writer."""
        return self.authority.principal_resolution.principal


def validate_visibility(rows: Sequence[Any], admission: SqlClientObserverAdmission) -> None:
    """Fail closed for unknown versions or incomplete instance/database visibility.

    Candidate query-profile SQL Server major versions are 13–17; this is
    not a live-certified support matrix. Version 16 introduced the
    PERFORMANCE STATE DMV permission. Catalog visibility is admitted separately.
    """
    if len(rows) != 1 or len(rows[0]) != 5:
        raise ValueError(_ERROR)
    major, edition, state, performance, database_id = rows[0]
    _integer(major, 13, 17)
    _integer(edition, 2, 4)
    _integer(database_id, 1, 2**31 - 1)
    required = state if major <= 15 else performance
    _integer(required, 1, 1)
    if database_id != admission.database.database_id:
        raise ValueError(_ERROR)


def validate_writer_rows(
    rows: Sequence[Any], *, session_id: int, nonce: bytes, admission: SqlClientObserverAdmission
) -> tuple[Any, ...]:
    """Validate all unfiltered target rows before querying principal candidates."""
    _integer(session_id, 1, 32767)
    require_session_nonce(nonce)
    if type(admission) is not SqlClientObserverAdmission or len(rows) != 1 or len(rows[0]) != 35:
        raise ValueError(_ERROR)
    row = tuple(rows[0])
    # Strict integral DMV values: never accept bool aliases or null counts.
    for index in (1, 8, 10, 11, 12, 13, 19, 23, 28, 29):
        _integer(row[index])
    if (
        row[1] != session_id
        or row[4] is not None
        or row[8] != 0
        or row[9] != "sleeping"
        or row[10] != 1
        or any(row[i] != 0 for i in (11, 12, 13))
        or row[14] != nonce
        or type(row[14]) is not bytes
    ):
        raise ValueError(_ERROR)
    # Validate exact connection incarnation even before principal resolution.
    TdsRemoteSessionIdentity(row[0], row[1], row[2], row[3], row[14], bytes(32))
    if type(row[21]) is not UUID or not row[21].int or row[30] != "SQL_LOGIN":
        raise ValueError(_ERROR)
    _integer(row[29], 0, 1)
    observed = SqlClientObserverAdmission(
        SqlClientServerAuthority(row[15], row[16], row[17], row[18]),
        SqlClientDatabaseAuthority(row[19], row[20], str(row[21]), _binary_sid(row[22])),
        SqlClientLoginAuthority(
            row[23], row[24], _binary_sid(row[25]), row[26], _binary_sid(row[27]), row[28], bool(row[29])
        ),
        SqlClientTransportAuthority(row[5], row[6], row[7], row[31]),
    )
    if observed != admission or row[32] != row[24] or _binary_sid(row[33]) != row[25].hex() or row[34] != row[26]:
        raise ValueError(_ERROR)
    return row


def resolve_writer_observation(
    row: tuple[Any, ...], principals: Sequence[Any], *, admission: SqlClientObserverAdmission
) -> SqlClientWriterObservation:
    """Resolve dbo precedence or one unambiguous INSTANCE SQL_USER SID mapping."""
    row = validate_writer_rows([row], session_id=row[1], nonce=row[14], admission=admission)
    resolution = resolve_database_principal(principals, admission=admission)
    authority = SqlClientSessionAuthority(
        admission.server,
        admission.database,
        admission.login,
        admission.transport,
        resolution,
    )
    remote = TdsRemoteSessionIdentity(row[0], row[1], row[2], row[3], row[14], session_authority_digest(authority))
    return SqlClientWriterObservation(remote, authority)


def validate_catalog_admission(rows: Sequence[Any], admission: SqlClientObserverAdmission) -> None:
    """Check actual scoped metadata before source acquisition, without broad grants."""
    if len(rows) != 1 or len(rows[0]) != 13:
        raise ValueError(_ERROR)
    row = rows[0]
    if type(row[6]) is not UUID or row[12] != "SQL_LOGIN":
        raise ValueError(_ERROR)
    _integer(row[11], 0, 1)
    server = SqlClientServerAuthority(*row[:4])
    database = SqlClientDatabaseAuthority(row[4], row[5], str(row[6]), _binary_sid(row[7]))
    # Catalog rows do not observe a session's authenticating database. Check
    # their actual fields; the writer snapshot separately checks that expectation.
    login = (row[8], row[9], _binary_sid(row[10]), bool(row[11]))
    expected_login = (
        admission.login.principal_id,
        admission.login.name,
        admission.login.sid,
        admission.login.is_sysadmin,
    )
    _integer(row[8], 1, 2**31 - 1)
    _name(row[9])
    if server != admission.server or database != admission.database or login != expected_login:
        raise ValueError(_ERROR)


def resolve_database_principal(
    principals: Sequence[Any], *, admission: SqlClientObserverAdmission
) -> SqlClientPrincipalResolution:
    """Resolve scoped catalog rows; also usable in source-free preflight."""
    kind = (
        "sysadmin_dbo"
        if admission.login.is_sysadmin
        else "owner_dbo"
        if admission.database.owner_sid == admission.login.sid
        else "mapped_user"
    )
    candidates = []
    for candidate in principals:
        if len(candidate) != 5:
            raise ValueError(_ERROR)
        pid, name, sid, type_desc, authentication = candidate
        principal = SqlClientDatabasePrincipal(pid, name, _binary_sid(sid))
        if type_desc != "SQL_USER" or type(authentication) is not str:
            raise ValueError(_ERROR)
        if kind != "mapped_user" and pid == 1 and name == "dbo":
            candidates.append(principal)
        elif kind == "mapped_user" and principal.sid == admission.login.sid and authentication == "INSTANCE":
            candidates.append(principal)
    if len(candidates) != 1:
        raise ValueError(_ERROR)
    principal = candidates[0]
    return SqlClientPrincipalResolution(kind, principal.principal_id, principal.name, principal.sid)
