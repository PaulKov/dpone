"""SQL Server table DDL rendering used by physical planning and runtime sinks."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from dpone.contracts.mssql_physical_design import (
    MssqlCompression,
    MssqlPhysicalDesignContract,
    MssqlTableDesign,
)


@dataclass(frozen=True, slots=True)
class MssqlTableDdlRenderer:
    """Render SQL Server CREATE TABLE DDL without opening a connection."""

    def render_create_table(
        self,
        *,
        table: str,
        column_definitions: Sequence[str],
        design: MssqlTableDesign | None = None,
    ) -> str:
        resolved = design or MssqlTableDesign()
        MssqlPhysicalDesignContract.from_sections(
            indexes={},
            storage=_storage_mapping(resolved),
            partitioning={},
        )
        return self._render_create_table(table=table, column_definitions=column_definitions, design=resolved)

    def _render_create_table(
        self,
        *,
        table: str,
        column_definitions: Sequence[str],
        design: MssqlTableDesign,
    ) -> str:
        columns = ", ".join(column_definitions)
        suffix = self._table_suffix(design)
        return f"CREATE TABLE {table} ({columns}){suffix}"

    def render_create_table_statements(
        self,
        *,
        table: str,
        column_definitions: Sequence[str],
        design: MssqlTableDesign | None = None,
        primary_key: Sequence[str] = (),
    ) -> tuple[str, ...]:
        resolved = design or MssqlTableDesign()
        contract = MssqlPhysicalDesignContract.from_sections(
            indexes={"primary_key": list(primary_key)} if primary_key else {},
            storage=_storage_mapping(resolved),
            partitioning={},
        )
        statements = [
            self._render_create_table(
                table=table,
                column_definitions=column_definitions,
                design=resolved,
            )
        ]
        if contract.primary_key:
            statements.append(self._render_primary_key(table, contract))
        if resolved.clustered_columnstore:
            statements.append(self._render_clustered_columnstore(table, resolved))
        return tuple(statements)

    def render_compression_rebuild(self, *, table: str, compression: MssqlCompression) -> str:
        if compression == "NONE":
            return f"ALTER TABLE {table} REBUILD WITH (DATA_COMPRESSION = NONE);"
        return f"ALTER TABLE {table} REBUILD WITH (DATA_COMPRESSION = {compression});"

    def _table_suffix(self, design: MssqlTableDesign) -> str:
        placement = self._filegroup_clause(design)
        options = self._table_options_clause(design)
        if placement and options:
            return f" {placement} {options}"
        if placement:
            return f" {placement}"
        if options:
            return f" {options}"
        return ""

    def _filegroup_clause(self, design: MssqlTableDesign) -> str:
        if not design.filegroup:
            return ""
        clause = f"ON {_quote_identifier(design.filegroup)}"
        if design.textimage_filegroup:
            clause += f" TEXTIMAGE_ON {_quote_identifier(design.textimage_filegroup)}"
        return clause

    def _table_options_clause(self, design: MssqlTableDesign) -> str:
        if not design.has_rowstore_compression:
            return ""
        return f"WITH (DATA_COMPRESSION = {design.compression})"

    def _render_primary_key(
        self,
        table: str,
        contract: MssqlPhysicalDesignContract,
    ) -> str:
        design = contract.storage
        name = _quote_identifier(self.primary_key_name(table, contract.primary_key))
        columns = ", ".join(_quote_identifier(item) for item in contract.primary_key)
        options = [f"DATA_COMPRESSION = {design.compression}"]
        if design.index_fillfactor is not None:
            options.append(f"FILLFACTOR = {design.index_fillfactor}")
        placement = f" ON {_quote_identifier(design.filegroup)}" if design.filegroup else ""
        return (
            f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY CLUSTERED "
            f"({columns}) WITH ({', '.join(options)}){placement};"
        )

    def _render_clustered_columnstore(self, table: str, design: MssqlTableDesign) -> str:
        name = _quote_identifier(self.clustered_columnstore_name(table))
        placement = f" ON {_quote_identifier(design.filegroup)}" if design.filegroup else ""
        return f"CREATE CLUSTERED COLUMNSTORE INDEX {name} ON {table}{placement};"

    @staticmethod
    def primary_key_name(table: str, columns: Sequence[str]) -> str:
        """Return the exact deterministic PK name used by the renderer."""

        return _safe_index_name("pk", table, "_".join(columns))

    @staticmethod
    def clustered_columnstore_name(table: str) -> str:
        """Return the exact deterministic columnstore name used by the renderer."""

        return _safe_index_name("cci", table)


def mssql_qualified_table(table: str) -> str:
    return ".".join(_quote_identifier(part) for part in table.split("."))


def mssql_column_definitions(
    columns: Mapping[str, str],
    *,
    nullable: Mapping[str, bool] | None = None,
) -> tuple[str, ...]:
    nullability = nullable or {}
    return tuple(
        f"[{name}] {dtype} {'NULL' if nullability.get(name, True) else 'NOT NULL'}" for name, dtype in columns.items()
    )


def _safe_index_name(*parts: str) -> str:
    joined = "_".join(parts).replace("][", "_").replace(".", "_")
    joined = joined.replace("[", "").replace("]", "")
    normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in joined)
    if len(normalized) <= 120:
        return normalized
    digest = sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"{normalized[:107]}_{digest}"


def _quote_identifier(value: str) -> str:
    text = str(value)
    if not text:
        raise ValueError("MSSQL identifier must not be empty")
    return "[" + text.replace("]", "]]") + "]"


def _storage_mapping(design: MssqlTableDesign) -> dict[str, object]:
    values: dict[str, object] = {
        "compression": design.compression,
        "clustered_columnstore": design.clustered_columnstore,
    }
    if design.filegroup is not None:
        values["filegroup"] = design.filegroup
    if design.textimage_filegroup is not None:
        values["textimage_filegroup"] = design.textimage_filegroup
    if design.index_fillfactor is not None:
        values["index_fillfactor"] = design.index_fillfactor
    return values


def render_load_strategy_create_table(
    *,
    options: Mapping[str, Any] | None,
    qualified_table: str,
    columns: Sequence[tuple[str, str]],
    quote_identifier: Callable[[str], str],
    to_mssql_type: Callable[[str], str],
    nullability: Mapping[str, bool] | None = None,
    collations: Mapping[str, str] | None = None,
) -> str:
    return "\n".join(
        render_load_strategy_create_statements(
            options=options,
            qualified_table=qualified_table,
            columns=columns,
            quote_identifier=quote_identifier,
            to_mssql_type=to_mssql_type,
            nullability=nullability,
            collations=collations,
        )
    )


def render_load_strategy_create_statements(
    *,
    options: Mapping[str, Any] | None,
    qualified_table: str,
    columns: Sequence[tuple[str, str]],
    quote_identifier: Callable[[str], str],
    to_mssql_type: Callable[[str], str],
    nullability: Mapping[str, bool] | None = None,
    collations: Mapping[str, str] | None = None,
    physical_contract: MssqlPhysicalDesignContract | None = None,
) -> tuple[str, ...]:
    """Render one immutable SQL statement per target mutation action.

    The joined compatibility API is convenient for legacy executors, but a
    governed finalizer must fingerprint and validate every DDL statement
    independently.  Returning the renderer's native tuple prevents an
    appended second statement from hiding behind a valid ``CREATE TABLE``
    prefix.
    """

    contract = physical_contract or MssqlPhysicalDesignContract.from_options(options)
    target_columns = {name: to_mssql_type(dtype) for name, dtype in columns}
    contract.validate_columns(target_columns)
    effective = contract if contract.active else MssqlPhysicalDesignContract(active=False)
    primary = {column.casefold() for column in effective.primary_key}
    nullable = nullability or {}
    resolved_collations = collations or {}
    column_definitions = [
        f"{quote_identifier(name)} {dtype}"
        f"{f' COLLATE {_safe_collation(resolved_collations[name])}' if name in resolved_collations else ''}"
        f"{_null_clause(name, primary=primary, nullability=nullable)}"
        for name, dtype in target_columns.items()
    ]
    return MssqlTableDdlRenderer().render_create_table_statements(
        table=qualified_table,
        column_definitions=column_definitions,
        design=effective.storage,
        primary_key=effective.primary_key,
    )


def _safe_collation(value: str) -> str:
    token = str(value).strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", token):
        raise ValueError("MSSQL column collation is invalid")
    return token


def _null_clause(name: str, *, primary: set[str], nullability: Mapping[str, bool]) -> str:
    if name.casefold() in primary or nullability.get(name) is False:
        return " NOT NULL"
    if name in nullability:
        return " NULL"
    return ""


__all__ = [
    "MssqlCompression",
    "MssqlPhysicalDesignContract",
    "MssqlTableDesign",
    "MssqlTableDdlRenderer",
    "mssql_column_definitions",
    "mssql_qualified_table",
    "render_load_strategy_create_table",
    "render_load_strategy_create_statements",
]
