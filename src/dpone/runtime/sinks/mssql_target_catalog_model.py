"""Immutable SQL Server catalog snapshots used by governed target DDL."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.readiness.schema_evolution import ColumnDef, SchemaPlan
from dpone.runtime.sinks.mssql_target_catalog_types import (
    canonical_catalog_scalar,
    canonical_mssql_catalog_type,
    mssql_catalog_type_is_text,
    mssql_catalog_type_shape,
    render_mssql_catalog_type,
)


@dataclass(frozen=True, slots=True)
class MssqlCatalogColumnState:
    """Lossless column identity from ``sys.columns`` and related catalogs."""

    ordinal: int
    name: str
    system_type_schema: str
    system_type_name: str
    user_type_schema: str
    user_type_name: str
    max_length: int
    precision: int
    scale: int
    nullable: bool
    collation: str | None
    identity: bool
    computed: bool
    sparse: bool
    rowguidcol: bool
    generated_always_type: int
    default_definition: str | None
    computed_definition: str | None
    identity_seed: str | None
    identity_increment: str | None

    def to_dict(self) -> dict[str, object]:
        """Return a canonical, JSON-safe catalog representation."""

        return asdict(self)

    def to_column_def(self) -> ColumnDef:
        """Project the exact catalog shape to the schema-comparator contract."""

        return ColumnDef(
            self.name,
            render_mssql_catalog_type(
                self.user_type_name,
                max_length=self.max_length,
                precision=self.precision,
                scale=self.scale,
            ),
            nullable=self.nullable,
            collation=self.collation,
        )


@dataclass(frozen=True, slots=True)
class MssqlCheckConstraintState:
    """Exact table/column CHECK definition relevant to safe ALTER decisions."""

    name: str
    parent_column_ordinal: int
    definition: str
    disabled: bool
    not_trusted: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MssqlIndexState:
    """Exact enabled rowstore/columnstore index authority for the target."""

    name: str
    type_desc: str
    unique: bool
    primary_key: bool
    unique_constraint: bool
    disabled: bool
    hypothetical: bool
    ignore_dup_key: bool
    filter_definition: str | None
    key_columns: tuple[str, ...]
    included_columns: tuple[str, ...]
    descending_keys: tuple[bool, ...] = ()
    fill_factor: int = 0
    data_space_name: str | None = None
    data_space_type_desc: str | None = None
    partition_compression: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["key_columns"] = list(self.key_columns)
        payload["included_columns"] = list(self.included_columns)
        payload["descending_keys"] = list(self.descending_keys)
        payload["partition_compression"] = list(self.partition_compression)
        return payload


@dataclass(frozen=True, slots=True)
class MssqlForeignKeyState:
    """Exact foreign-key dependency that can affect governed business DML."""

    name: str
    direction: str
    parent_schema: str
    parent_table: str
    parent_columns: tuple[str, ...]
    referenced_schema: str
    referenced_table: str
    referenced_columns: tuple[str, ...]
    update_action: str
    delete_action: str
    disabled: bool
    not_trusted: bool
    not_for_replication: bool

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("mssql_transaction.target_foreign_key_name_missing")
        if self.direction not in {"inbound", "outbound", "self"}:
            raise ValueError("mssql_transaction.target_foreign_key_direction_invalid")
        if not all(
            value.strip()
            for value in (
                self.parent_schema,
                self.parent_table,
                self.referenced_schema,
                self.referenced_table,
            )
        ):
            raise ValueError("mssql_transaction.target_foreign_key_identity_invalid")
        if (
            not self.parent_columns
            or len(self.parent_columns) != len(self.referenced_columns)
            or any(not value.strip() for value in (*self.parent_columns, *self.referenced_columns))
        ):
            raise ValueError("mssql_transaction.target_foreign_key_columns_invalid")
        allowed_actions = {"NO_ACTION", "CASCADE", "SET_NULL", "SET_DEFAULT"}
        if self.update_action.upper() not in allowed_actions or self.delete_action.upper() not in allowed_actions:
            raise ValueError("mssql_transaction.target_foreign_key_action_invalid")

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["parent_columns"] = list(self.parent_columns)
        payload["referenced_columns"] = list(self.referenced_columns)
        return payload


@dataclass(frozen=True, slots=True)
class MssqlTriggerState:
    """Exact table-trigger authority relevant to target DML semantics."""

    name: str
    disabled: bool
    instead_of: bool
    not_for_replication: bool
    system_shipped: bool
    events: tuple[str, ...]
    execute_as_principal: str | None
    definition: str | None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["events"] = list(self.events)
        return payload


@dataclass(frozen=True, slots=True)
class MssqlPermissionState:
    """Exact object/column permission assignment for the target table."""

    grantee: str
    grantee_type: str
    permission_name: str
    state_desc: str
    column_name: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MssqlTableBehaviorState:
    """Exact finite table feature flags that affect target DML semantics."""

    temporal_type: int = 0
    history_schema: str | None = None
    history_table: str | None = None
    ledger_type: int = 0
    memory_optimized: bool = False
    durability_desc: str | None = None
    filetable: bool = False
    graph_node: bool = False
    graph_edge: bool = False

    @classmethod
    def ordinary_disk_table(cls) -> MssqlTableBehaviorState:
        """Return the catalog after-image produced by ordinary ``CREATE TABLE``.

        SQL Server reports ``SCHEMA_AND_DATA`` in ``sys.tables.durability_desc``
        for a regular disk-backed table. Keeping that vendor-owned default in
        the catalog model lets fresh-target planning build the same exact image
        that the post-DDL reader will observe.
        """

        return cls(durability_desc="SCHEMA_AND_DATA")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MssqlSchemaCatalogSnapshot:
    """Exact before/after schema boundary for one physical target table."""

    exists: bool
    database_collation: str
    columns: tuple[MssqlCatalogColumnState, ...] = ()
    checks: tuple[MssqlCheckConstraintState, ...] = ()
    indexes: tuple[MssqlIndexState, ...] = ()
    foreign_keys: tuple[MssqlForeignKeyState, ...] = ()
    triggers: tuple[MssqlTriggerState, ...] = ()
    permissions: tuple[MssqlPermissionState, ...] = ()
    behavior: MssqlTableBehaviorState = MssqlTableBehaviorState()
    default_filegroup: str = "PRIMARY"
    available_row_filegroups: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.database_collation.strip():
            raise ValueError("mssql_transaction.target_database_collation_missing")
        if not self.default_filegroup.strip():
            raise ValueError("mssql_transaction.target_default_filegroup_missing")
        filegroups = self.available_row_filegroups or (self.default_filegroup,)
        if any(not name.strip() for name in filegroups):
            raise ValueError("mssql_transaction.target_row_filegroup_identity_invalid")
        if len(set(filegroups)) != len(filegroups):
            raise ValueError("mssql_transaction.target_row_filegroup_identity_ambiguous")
        if self.default_filegroup not in filegroups:
            raise ValueError("mssql_transaction.target_default_filegroup_unavailable")
        object.__setattr__(self, "available_row_filegroups", tuple(sorted(filegroups)))
        ordinals = tuple(column.ordinal for column in self.columns)
        if ordinals != tuple(range(1, len(self.columns) + 1)):
            raise ValueError("mssql_transaction.target_catalog_ordinal_invalid")
        names = tuple(column.name for column in self.columns)
        if len(names) != len(set(names)) or len(names) != len({name.casefold() for name in names}):
            raise ValueError("mssql_transaction.target_catalog_identifier_ambiguous")
        if not self.exists and (
            self.columns or self.checks or self.indexes or self.foreign_keys or self.triggers or self.permissions
        ):
            raise ValueError("mssql_transaction.absent_target_has_catalog_state")

    def to_dict(self) -> dict[str, object]:
        return {
            "exists": self.exists,
            "database_collation": self.database_collation,
            "columns": [column.to_dict() for column in self.columns],
            "checks": [constraint.to_dict() for constraint in self.checks],
            "indexes": [index.to_dict() for index in self.indexes],
            "foreign_keys": [foreign_key.to_dict() for foreign_key in self.foreign_keys],
            "triggers": [trigger.to_dict() for trigger in self.triggers],
            "permissions": [permission.to_dict() for permission in self.permissions],
            "behavior": self.behavior.to_dict(),
            "default_filegroup": self.default_filegroup,
            "available_row_filegroups": list(self.available_row_filegroups),
        }

    def to_schema_dict(self) -> dict[str, object]:
        """Return the exact schema-owned catalog partition.

        Partition compression is owned by the separate physical expectation;
        excluding only that field keeps the schema after-image stable across an
        authorized physical rebuild while preserving every structural index
        attribute, dependency, trigger, and permission.
        """

        payload = self.to_dict()
        payload["indexes"] = [
            {key: value for key, value in index.to_dict().items() if key != "partition_compression"}
            for index in self.indexes
        ]
        return payload

    @property
    def column_defs(self) -> list[ColumnDef]:
        return [column.to_column_def() for column in self.columns]

    def apply_schema_plan(self, plan: SchemaPlan) -> MssqlSchemaCatalogSnapshot:
        """Derive the exact supported post-DDL shape without parsing SQL text."""

        if not self.exists:
            raise ValueError("mssql_transaction.missing_target_schema_transition_unsupported")
        columns = list(self.columns)
        by_name = {column.name.casefold(): index for index, column in enumerate(columns)}
        for change in plan.safe_changes:
            source = change.source
            if change.change_type == "add_column" and source is not None:
                columns.append(
                    catalog_column_from_definition(
                        source,
                        ordinal=len(columns) + 1,
                        database_collation=self.database_collation,
                    )
                )
            elif change.change_type == "add_generated_column" and source is not None and change.generated_column:
                columns.append(
                    catalog_column_from_definition(
                        ColumnDef(change.generated_column, source.dtype, nullable=True),
                        ordinal=len(columns) + 1,
                        database_collation=self.database_collation,
                    )
                )
            elif change.change_type in {"type_widen", "nullability_relax"} and source is not None:
                index = by_name.get(change.column.casefold())
                if index is None:
                    raise ValueError("mssql_transaction.schema_transition_column_missing")
                columns[index] = altered_catalog_column(columns[index], source)
        return replace(self, columns=tuple(columns))


def catalog_column_from_definition(
    column: ColumnDef,
    *,
    ordinal: int,
    database_collation: str,
) -> MssqlCatalogColumnState:
    """Create the exact standard SQL Server catalog shape for generated DDL."""

    dtype = normalize_mssql_physical_type(column.dtype)
    base, max_length, precision, scale = mssql_catalog_type_shape(dtype)
    collation = column.collation or (database_collation if mssql_catalog_type_is_text(base) else None)
    return MssqlCatalogColumnState(
        ordinal=ordinal,
        name=column.name,
        system_type_schema="sys",
        system_type_name=base,
        user_type_schema="sys",
        user_type_name=base,
        max_length=max_length,
        precision=precision,
        scale=scale,
        nullable=column.nullable,
        collation=collation,
        identity=False,
        computed=False,
        sparse=False,
        rowguidcol=False,
        generated_always_type=0,
        default_definition=None,
        computed_definition=None,
        identity_seed=None,
        identity_increment=None,
    )


def altered_catalog_column(
    current: MssqlCatalogColumnState,
    desired: ColumnDef,
) -> MssqlCatalogColumnState:
    """Apply an authorized ALTER COLUMN while preserving unrelated metadata."""

    dtype = normalize_mssql_physical_type(desired.dtype)
    base, max_length, precision, scale = mssql_catalog_type_shape(dtype)
    return replace(
        current,
        system_type_schema="sys",
        system_type_name=base,
        user_type_schema="sys",
        user_type_name=base,
        max_length=max_length,
        precision=precision,
        scale=scale,
        nullable=desired.nullable,
        collation=desired.collation if desired.collation is not None else current.collation,
    )


__all__ = [
    "MssqlCatalogColumnState",
    "MssqlCheckConstraintState",
    "MssqlForeignKeyState",
    "MssqlIndexState",
    "MssqlPermissionState",
    "MssqlSchemaCatalogSnapshot",
    "MssqlTableBehaviorState",
    "MssqlTriggerState",
    "altered_catalog_column",
    "canonical_mssql_catalog_type",
    "canonical_catalog_scalar",
    "catalog_column_from_definition",
    "render_mssql_catalog_type",
]
