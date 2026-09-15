"""Immutable, internally consistent physical plans; never execution authority.

Constructors derive names and digests instead of accepting caller substitutes.
External documents must additionally pass the canonical wire decoder. All refs
remain unauthenticated locators until an admission consumer verifies originals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_validation import (
    PhysicalPlanError,
    physical_object_name,
    require_physical_identifier,
    require_physical_text,
    require_physical_timestamp,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn, normalize_mssql_physical_type
from dpone.contracts.native_delivery_json import NativeJsonValue, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef


def _digest(payload: dict[str, NativeJsonValue]) -> str:
    return "sha256:" + sha256(encode_native_delivery_json(payload)).hexdigest()


def _reference(value: OriginalRef) -> dict[str, NativeJsonValue]:
    if type(value) is not OriginalRef:
        raise PhysicalPlanError("reference requires an exact OriginalRef")
    value.__post_init__()
    return {"locator": value.locator, "sha256": value.sha256}


@dataclass(frozen=True, slots=True)
class PhysicalRelation:
    """Exact spelling only; SQL alias equivalence belongs to server admission."""

    database: str
    schema: str
    table: str

    def __post_init__(self) -> None:
        for name in ("database", "schema", "table"):
            require_physical_identifier(getattr(self, name), name)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"database": self.database, "schema": self.schema, "table": self.table}


@dataclass(frozen=True, slots=True)
class PhysicalFilegroup:
    """Plan-selected data-space identity, not a current catalog observation."""

    data_space_id: int
    name: str

    def __post_init__(self) -> None:
        require_sql_positive_integer(self.data_space_id, "data_space_id")
        require_physical_identifier(self.name, "filegroup.name")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"data_space_id": self.data_space_id, "name": self.name}


@dataclass(frozen=True, slots=True)
class AbsentPredecessor:
    """Planned absence, requiring fresh server validation before dispatch."""

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"kind": "ABSENT"}


@dataclass(frozen=True, slots=True)
class ManagedPredecessor:
    """Prior object and receipt locator; construction does not verify that receipt."""

    object_id: int
    object_create_time: str
    local_receipt: OriginalRef

    def __post_init__(self) -> None:
        require_sql_positive_integer(self.object_id, "object_id")
        require_physical_timestamp(self.object_create_time, "object_create_time")
        _reference(self.local_receipt)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {
            "kind": "MANAGED",
            "object_id": self.object_id,
            "object_create_time": self.object_create_time,
            "local_receipt": _reference(self.local_receipt),
        }


def _column(value: MssqlCatalogColumn) -> dict[str, NativeJsonValue]:
    if type(value) is not MssqlCatalogColumn:
        raise PhysicalPlanError("columns require exact catalog column records")
    require_physical_identifier(value.name, "column.name")
    dtype = require_physical_text(value.dtype, "column.dtype")
    try:
        canonical = normalize_mssql_physical_type(dtype)
    except ValueError:
        raise PhysicalPlanError("column.dtype requires canonical physical type spelling") from None
    if canonical != dtype or type(value.nullable) is not bool:
        raise PhysicalPlanError("column requires canonical dtype and exact boolean nullability")
    character = dtype.split("(", 1)[0] in {"char", "varchar", "nchar", "nvarchar"}
    if character:
        require_physical_identifier(value.collation, "column.collation")
    elif value.collation is not None:
        raise PhysicalPlanError("noncharacter column requires null collation")
    return {"name": value.name, "dtype": dtype, "nullable": value.nullable, "collation": value.collation}


@dataclass(frozen=True, slots=True)
class PhysicalModelSpec:
    """Canonical model definition; type spelling is not type qualification."""

    model_unique_id: str
    source_graph_sha256: str
    relation: PhysicalRelation
    columns: tuple[MssqlCatalogColumn, ...]
    layout: str
    filegroup: PhysicalFilegroup
    resource_bounds: OriginalRef
    model_spec_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        require_physical_text(self.model_unique_id, "model_unique_id")
        OriginalRef("graph", self.source_graph_sha256)
        if type(self.relation) is not PhysicalRelation or type(self.filegroup) is not PhysicalFilegroup:
            raise PhysicalPlanError("spec requires exact relation and filegroup records")
        self.relation.__post_init__()
        self.filegroup.__post_init__()
        if type(self.columns) is not tuple or not self.columns:
            raise PhysicalPlanError("columns require a nonempty ordered tuple")
        if type(self.layout) is not str or self.layout not in {
            "rowstore_none",
            "rowstore_row",
            "rowstore_page",
            "columnstore",
        }:
            raise PhysicalPlanError("layout requires an exact supported physical layout")
        object.__setattr__(self, "model_spec_sha256", _digest(self._unsigned()))

    def _unsigned(self) -> dict[str, NativeJsonValue]:
        return {
            "schema": "dpone.mssql-physical-model-spec.v1",
            "model_unique_id": self.model_unique_id,
            "source_graph_sha256": self.source_graph_sha256,
            "relation": self.relation.to_dict(),
            "columns": [_column(column) for column in self.columns],
            "layout": self.layout,
            "physical_policy": "sqlserver-table-physical-v1",
            "filegroup": self.filegroup.to_dict(),
            "resource_bounds": _reference(self.resource_bounds),
        }

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Return a detached projection including the derived spec digest."""
        return {**self._unsigned(), "model_spec_sha256": self.model_spec_sha256}


@dataclass(frozen=True, slots=True)
class PhysicalModelPlan:
    """Generation-specific plan with deterministic names and a derived digest."""

    generation_id: str
    spec: PhysicalModelSpec
    predecessor: AbsentPredecessor | ManagedPredecessor
    candidate_name: str = field(init=False)
    helper_name: str = field(init=False)
    backup_name: str | None = field(init=False)
    columnstore_index_name: str | None = field(init=False)
    model_plan_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        require_physical_uuid(self.generation_id, "generation_id")
        if type(self.spec) is not PhysicalModelSpec or type(self.predecessor) not in {
            AbsentPredecessor,
            ManagedPredecessor,
        }:
            raise PhysicalPlanError("plan requires exact spec and closed predecessor records")
        for attribute, role, present in (
            ("candidate_name", "CANDIDATE", True),
            ("helper_name", "HELPER", True),
            ("backup_name", "BACKUP", type(self.predecessor) is ManagedPredecessor),
            ("columnstore_index_name", "CCI", self.spec.layout == "columnstore"),
        ):
            value = physical_object_name(self.generation_id, self.spec.model_unique_id, role) if present else None
            object.__setattr__(self, attribute, value)
        object.__setattr__(self, "model_plan_sha256", _digest(self._unsigned()))

    def _unsigned(self) -> dict[str, NativeJsonValue]:
        return {
            "schema": "dpone.mssql-physical-model-plan.v1",
            "generation_id": self.generation_id,
            "spec": self.spec.to_dict(),
            "predecessor": self.predecessor.to_dict(),
            "candidate_name": self.candidate_name,
            "helper_name": self.helper_name,
            "backup_name": self.backup_name,
            "columnstore_index_name": self.columnstore_index_name,
        }

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Return the complete plan with its nested spec digest retained."""
        return {**self._unsigned(), "model_plan_sha256": self.model_plan_sha256}


@dataclass(frozen=True, slots=True)
class PhysicalPlanSet:
    """Complete ordered plans; registration ID is a locator, never authority."""

    generation_id: str
    runtime_registration_id: str
    workspace_attempt: DbtWorkspaceAttemptRequest
    guard: DbtWorkspaceGuardEpoch
    profile: OriginalRef
    model_database: MssqlDatabaseAuthorityPin
    models: tuple[PhysicalModelPlan, ...]

    def __post_init__(self) -> None:
        require_physical_uuid(self.generation_id, "generation_id")
        require_physical_uuid(self.runtime_registration_id, "runtime_registration_id")
        if (
            type(self.workspace_attempt) is not DbtWorkspaceAttemptRequest
            or type(self.guard) is not DbtWorkspaceGuardEpoch
        ):
            raise PhysicalPlanError("plan set requires exact attempt and guard records")
        self.workspace_attempt.__post_init__()
        require_physical_uuid(self.workspace_attempt.activation_id, "activation_id")
        self.guard.__post_init__()
        require_sql_positive_integer(self.guard.fencing_epoch, "fencing_epoch", bigint=True)
        if type(self.model_database) is not MssqlDatabaseAuthorityPin:
            raise PhysicalPlanError("model_database requires an exact database pin")
        pin = self.model_database
        require_physical_identifier(pin.database_name, "database_name")
        require_sql_positive_integer(pin.database_id, "database_id")
        require_physical_timestamp(pin.create_token, "create_token")
        if type(pin.database_guid) is not UUID:
            raise PhysicalPlanError("database_guid requires an exact UUID")
        if (
            type(self.models) is not tuple
            or not self.models
            or any(type(model) is not PhysicalModelPlan for model in self.models)
        ):
            raise PhysicalPlanError("models require a nonempty tuple of exact plans")
        ids = tuple(model.spec.model_unique_id.encode("utf-8") for model in self.models)
        if ids != tuple(sorted(ids)) or len(set(ids)) != len(ids):
            raise PhysicalPlanError("models require unique UTF-8 sorted identities")
        if any(
            model.generation_id != self.generation_id or model.spec.relation.database != pin.database_name
            for model in self.models
        ):
            raise PhysicalPlanError("model generation or database differs from its plan set")
        encode_native_delivery_json(self.to_dict())

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Return complete canonical-input shape without an enclosing self digest."""
        attempt, pin = self.workspace_attempt, self.model_database
        return {
            "schema": "dpone.mssql-physical-plan-set.v1",
            "generation_id": self.generation_id,
            "runtime_registration_id": self.runtime_registration_id,
            "workspace_attempt": {
                "activation_id": attempt.activation_id,
                "attempt_id": attempt.attempt_id,
                "workflow_id": attempt.workflow_id,
                "write_subjects": list(attempt.write_subjects),
                "request_sha256": attempt.request_sha256,
            },
            "guard": {"guard_id": self.guard.guard_id, "fencing_epoch": self.guard.fencing_epoch},
            "profile": _reference(self.profile),
            "model_database": {
                "database_name": pin.database_name,
                "database_id": pin.database_id,
                "create_token": pin.create_token,
                "database_guid": str(pin.database_guid),
            },
            "models": [model.to_dict() for model in self.models],
        }
