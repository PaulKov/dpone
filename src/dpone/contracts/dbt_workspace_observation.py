"""Bounded catalog observations, never signatures or workspace activation grants."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, fields

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin


class WorkspaceObservationError(ValueError):
    """Content-free reason; raw driver exceptions and credentials are not retained."""

    code = "DPONE_DBT_WORKSPACE_PHYSICAL_OBSERVATION_UNAVAILABLE"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


def catalog_observation_fingerprint(value: Mapping[str, object]) -> str:
    """Canonical content fingerprint used by secret-free catalog DTOs."""

    return canonical_fingerprint(value)


@dataclass(frozen=True, slots=True)
class WorkspaceObservationLimits:
    """Platform budgets may be tightened per operation, never silently disabled."""

    max_slots: int = 8192
    max_nodes: int = 8192
    max_edges: int = 32768
    max_depth: int = 32
    max_json_bytes: int = 8 * 1024 * 1024
    timeout_seconds: int = 60

    def __post_init__(self) -> None:
        for descriptor in fields(self):
            value = getattr(self, descriptor.name)
            if (
                type(value) is not int
                or not isinstance(descriptor.default, int)
                or not 1 <= value <= descriptor.default
            ):
                raise WorkspaceObservationError("limits")


def require_observable_identifier(value: object, *, macro_literal: bool = False) -> str:
    """Reject truncation and unsafe pinned-macro names without folding spelling."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise WorkspaceObservationError("identifier")
    try:
        size = len(value.encode("utf-16-le"))
    except UnicodeError:
        raise WorkspaceObservationError("identifier") from None
    if size > 256 or (macro_literal and any(character in value for character in ("'", "]"))):
        raise WorkspaceObservationError("identifier")
    return value


def require_observable_database(value: object) -> str:
    """Pinned USE macro removes double quotes, so reject that target rewrite."""

    name = require_observable_identifier(value, macro_literal=True)
    if '"' in name:
        raise WorkspaceObservationError("database_identifier")
    return name


@dataclass(frozen=True, slots=True)
class MssqlWorkspaceObservationRequest:
    """One source-derived connection/DB subset; caller verifies signed context.

    A pin is expected input, not learned metadata. Subject hashes bind the
    supplied bytes/rows but do not authenticate them or authorize execution.
    """

    release_id: str
    runtime_context_sha256: str
    macro_authority_sha256: str
    pin: MssqlDatabaseAuthorityPin
    default_database: str
    invocation_databases: tuple[str, ...]
    writes: tuple[DbtRelationWrite, ...]
    limits: WorkspaceObservationLimits = field(default_factory=WorkspaceObservationLimits)

    def __post_init__(self) -> None:
        if not all(is_canonical_sha256_digest(value) for value in (self.release_id, self.runtime_context_sha256)):
            raise WorkspaceObservationError("subject")
        if self.macro_authority_sha256 != DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256:
            raise WorkspaceObservationError("macro_authority")
        if not isinstance(self.limits, WorkspaceObservationLimits):
            raise WorkspaceObservationError("limits")
        if not isinstance(self.writes, tuple) or not 1 <= len(self.writes) <= self.limits.max_slots:
            raise WorkspaceObservationError("slots")
        if not isinstance(self.pin, MssqlDatabaseAuthorityPin):
            raise WorkspaceObservationError("database_pin")
        try:
            checked = MssqlDatabaseAuthorityPin.from_raw(
                self.pin.database_name,
                {
                    "database_id": self.pin.database_id,
                    "database_guid": str(self.pin.database_guid),
                    "create_token": self.pin.create_token,
                },
                capability="target",
            )
        except Exception:
            raise WorkspaceObservationError("database_pin") from None
        if checked != self.pin:
            raise WorkspaceObservationError("database_pin")
        require_observable_database(self.pin.database_name)
        require_observable_database(self.default_database)
        if (
            not isinstance(self.invocation_databases, tuple)
            or not 1 <= len(self.invocation_databases) <= 64
            or len(set(self.invocation_databases)) != len(self.invocation_databases)
        ):
            raise WorkspaceObservationError("invocation_databases")
        for database in self.invocation_databases:
            require_observable_database(database)
        for write in self.writes:
            if not isinstance(write, DbtRelationWrite) or write.connector != "mssql":
                raise WorkspaceObservationError("connector")
            for name in (write.database or self.default_database, write.schema, write.relation):
                require_observable_identifier(name, macro_literal=write.kind != "transfer")
            if write.kind != "transfer":
                require_observable_database(write.database or self.default_database)

    @property
    def subject_sha256(self) -> str:
        payload = asdict(self)
        payload["pin"]["database_guid"] = str(self.pin.database_guid)
        return canonical_fingerprint(payload)


@dataclass(frozen=True, slots=True)
class MssqlWorkspaceHeader:
    """Observed endpoint facts; server fingerprint/GUID are not global authority."""

    database_id: int
    database_name: str
    server_facts_sha256: str
    original_login_sid_sha256: str
    effective_login_sid_sha256: str
    database_principal_sid_sha256: str
    engine_version: str
    server_collation: str
    database_collation: str
    catalog_collation: str


@dataclass(frozen=True, slots=True)
class MssqlWorkspaceSlot:
    """Equivalence classes are local to this query, including absent objects."""

    slot_id: int
    equivalence_class: int
    schema_id: int | None
    schema_name: str | None
    object_id: int | None
    object_name: str | None
    object_type: str | None
    create_token: str | None
    modify_token: str | None


@dataclass(frozen=True, slots=True)
class MssqlWorkspaceDependency:
    """One raw incoming row in the potential pinned view-drop closure."""

    database_arg: str
    parent_schema: str
    parent_relation: str
    object_id: int
    schema_name: str
    object_name: str
    create_token: str
    modify_token: str
    referenced_server: str | None
    referenced_database: str
    referenced_schema: str
    referenced_entity: str
    referenced_id: int | None
    referencing_minor_id: int
    referenced_minor_id: int


@dataclass(frozen=True, slots=True)
class MssqlWorkspaceObservation:
    """Completed read-only observation, without cross-endpoint/lifetime authority."""

    request: MssqlWorkspaceObservationRequest
    header: MssqlWorkspaceHeader
    slots: tuple[MssqlWorkspaceSlot, ...]
    dependencies: tuple[MssqlWorkspaceDependency, ...]

    @property
    def subject_sha256(self) -> str:
        return self.request.subject_sha256

    @property
    def observation_sha256(self) -> str:
        """Bind all catalog rows without treating the observation as a lifetime lease."""

        return canonical_fingerprint(
            {
                "schema": "dpone.dbt-workspace-mssql-observation.v1",
                "request_sha256": self.request.subject_sha256,
                "header": asdict(self.header),
                "slots": [asdict(item) for item in self.slots],
                "dependencies": [asdict(item) for item in self.dependencies],
            }
        )
