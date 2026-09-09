"""Typed durable-state locations resolved from deployment-owned connections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.ports.source_state_storage import MssqlStateLocation

if TYPE_CHECKING:  # pragma: no cover
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


class StateConfigError(ValueError):
    """Raised when state authority or location is incomplete or ambiguous."""


@dataclass(frozen=True, slots=True)
class MssqlStateDefaults:
    """Environment-independent state policy and canonical table defaults."""

    atomicity: str
    provisioning: str
    table: str
    run_table: str
    receipt_table: str
    repair_authority_table: str
    repair_consumption_table: str
    audit_table: str

    def __post_init__(self) -> None:
        _validate_mssql_state_policy(self.atomicity, self.provisioning)

    def as_table_mapping(self) -> dict[str, str]:
        return {
            "table": self.table,
            "run_table": self.run_table,
            "receipt_table": self.receipt_table,
            "repair_authority_table": self.repair_authority_table,
            "repair_consumption_table": self.repair_consumption_table,
            "audit_table": self.audit_table,
        }


@dataclass(frozen=True, slots=True)
class ResolvedMssqlStateConfig:
    """Resolved state tables and their transaction/provisioning policy."""

    location: MssqlStateLocation
    run_table: str
    audit_table: str
    atomicity: str
    provisioning: str

    def __post_init__(self) -> None:
        _validate_mssql_state_policy(self.atomicity, self.provisioning)


def resolve_mssql_state_location(
    state_cfg: Mapping[str, Any],
    connection: ResolvedBindingConnection,
) -> ResolvedMssqlStateConfig:
    """Resolve state coordinates using explicit fields then connection defaults.

    Explicit workload coordinates are authoritative. Omitted database/schema
    values inherit the environment registry snapshot, so one manifest can stay
    byte-identical across environments without making authored overrides
    ambiguous. Secrets are never inspected or copied into the returned contract.
    """

    descriptor = connection.descriptor
    properties = descriptor.properties if descriptor is not None else {}
    default_database = _text(properties.get("database")) or _text(connection.credentials.database)
    default_schema = _text(properties.get("schema")) or _text(connection.credentials.schema)
    return resolve_mssql_state_location_defaults(
        state_cfg,
        default_database=default_database,
        default_schema=default_schema,
        require_registry_defaults=False,
    )


def resolve_mssql_state_location_defaults(
    state_cfg: Mapping[str, Any],
    *,
    default_database: str | None,
    default_schema: str | None,
    require_registry_defaults: bool = False,
) -> ResolvedMssqlStateConfig:
    """Resolve a location for legacy composition roots with explicit defaults."""

    defaults = resolve_mssql_state_defaults(state_cfg)
    atomicity = defaults.atomicity
    provisioning = defaults.provisioning
    table_cfg = _table_mapping(state_cfg.get("table"), "state.table")
    database = _text(table_cfg.get("database")) or default_database
    schema = _text(table_cfg.get("schema")) or default_schema or "etl_state"
    if not database:
        raise StateConfigError(
            "MSSQL state database is required; set connection.database in the environment registry "
            "or state.table.database"
        )
    if atomicity == "target_atomic" and require_registry_defaults and (not default_database or not default_schema):
        raise StateConfigError(
            "state.atomicity=target_atomic requires deployment-owned connection.database and connection.schema"
        )

    for field, key in (
        ("state.receipt_table", "receipt_table"),
        ("state.repair_authority_table", "repair_authority_table"),
        ("state.repair_consumption_table", "repair_consumption_table"),
        ("state.run_table", "run_table"),
        ("state.audit_table", "audit_table"),
    ):
        _same_location_table(
            state_cfg.get(key),
            field_name=field,
            database=database,
            schema=schema,
        )
    source_table = defaults.table
    receipt_table = defaults.receipt_table
    repair_authority_table = defaults.repair_authority_table
    repair_consumption_table = defaults.repair_consumption_table
    run_table = defaults.run_table
    audit_table = defaults.audit_table
    for name in (
        source_table,
        receipt_table,
        repair_authority_table,
        repair_consumption_table,
        run_table,
        audit_table,
    ):
        MSSQLObjectName.from_parts(database=database, schema=schema, table=name, strict=True)
    return ResolvedMssqlStateConfig(
        location=MssqlStateLocation(
            database=database,
            schema=schema,
            table=source_table,
            receipt_table=receipt_table,
            repair_authority_table=repair_authority_table,
            repair_consumption_table=repair_consumption_table,
        ),
        run_table=run_table,
        audit_table=audit_table,
        atomicity=atomicity,
        provisioning=provisioning,
    )


def resolve_mssql_state_defaults(state_cfg: Mapping[str, Any]) -> MssqlStateDefaults:
    """Resolve policy/table defaults shared by planning and runtime location binding."""

    atomicity = _text(state_cfg.get("atomicity")) or "after_target"
    provisioning = _text(state_cfg.get("provisioning")) or ("external" if atomicity == "target_atomic" else "runtime")
    return MssqlStateDefaults(
        atomicity=atomicity,
        provisioning=provisioning,
        table=_table_name(
            _table_mapping(state_cfg.get("table"), "state.table"),
            default_name="etl_xmin_state",
            field_name="state.table",
        ),
        run_table=_table_name(
            _table_mapping(state_cfg.get("run_table"), "state.run_table"),
            default_name="etl_run_state",
            field_name="state.run_table",
            legacy_name="run_name",
        ),
        receipt_table=_default_state_table_name(state_cfg, "receipt_table", "dpone_commit_receipt"),
        repair_authority_table=_default_state_table_name(state_cfg, "repair_authority_table", "dpone_repair_authority"),
        repair_consumption_table=_default_state_table_name(
            state_cfg,
            "repair_consumption_table",
            "dpone_repair_authority_consumption",
        ),
        audit_table=_default_state_table_name(state_cfg, "audit_table", "dpone_load_audit"),
    )


def _default_state_table_name(
    state_cfg: Mapping[str, Any],
    key: str,
    default_name: str,
) -> str:
    field_name = f"state.{key}"
    return _table_name(
        _table_mapping(state_cfg.get(key), field_name),
        default_name=default_name,
        field_name=field_name,
    )


def _validate_mssql_state_policy(atomicity: str, provisioning: str) -> None:
    if atomicity not in {"after_target", "target_atomic"}:
        raise StateConfigError("state.atomicity must be after_target or target_atomic")
    if provisioning not in {"runtime", "external"}:
        raise StateConfigError("state.provisioning must be runtime or external")
    if atomicity == "target_atomic" and provisioning != "external":
        raise StateConfigError("state.atomicity=target_atomic requires state.provisioning=external")


def _table_name(
    config: Mapping[str, Any],
    *,
    default_name: str,
    field_name: str,
    legacy_name: str | None = None,
) -> str:
    modern = _text(config.get("name"))
    legacy = _text(config.get(legacy_name)) if legacy_name else None
    if modern and legacy and modern != legacy:
        raise StateConfigError(f"{field_name}.name conflicts with deprecated {field_name}.{legacy_name}")
    return modern or legacy or default_name


def _same_location_table(
    value: object,
    *,
    field_name: str,
    database: str,
    schema: str,
) -> Mapping[str, Any]:
    config = _table_mapping(value, field_name)
    declared_database = _text(config.get("database"))
    declared_schema = _text(config.get("schema"))
    if declared_database and declared_database != database:
        raise StateConfigError(f"{field_name}.database must match state.table.database")
    if declared_schema and declared_schema != schema:
        raise StateConfigError(f"{field_name}.schema must match state.table.schema")
    return config


def _table_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise StateConfigError(f"{field_name} must be an object")
    return value


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "ResolvedMssqlStateConfig",
    "StateConfigError",
    "resolve_mssql_state_location",
    "resolve_mssql_state_location_defaults",
]
