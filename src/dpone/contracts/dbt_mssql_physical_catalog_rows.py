"""Immutable SQL catalog transport records, without physical-admission authority.

Construct through ``decode_catalog_result`` to enforce the wire contract. Direct
DTO construction is unchecked. Counts and property codes remain producer claims;
these records prove neither SQL visibility nor execution of COUNT_BIG. Timestamps
retain canonical seven-digit text and names retain their original spelling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID


@dataclass(frozen=True, slots=True)
class CatalogRow:
    """Common version-one identity and completeness envelope."""

    wire_version: int
    row_kind: str
    object_id: int
    row_ordinal: int
    row_count: int


@dataclass(frozen=True, slots=True)
class HeaderRow(CatalogRow):
    """Ordered HEADER catalog facts; transport acceptance is not admission."""

    database_id: int = field(metadata={"sql": "int", "nullable": False})
    database_guid: UUID = field(metadata={"sql": "uniqueidentifier", "nullable": False})
    object_create_time: str = field(metadata={"sql": "char(27)", "nullable": False})
    object_modify_time: str = field(metadata={"sql": "char(27)", "nullable": False})
    schema_name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    object_name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    object_type: str = field(metadata={"sql": "char(2)", "nullable": False})
    column_count: int = field(metadata={"sql": "int", "nullable": False})
    index_count: int = field(metadata={"sql": "int", "nullable": False})
    index_column_count: int = field(metadata={"sql": "int", "nullable": False})
    partition_count: int = field(metadata={"sql": "int", "nullable": False})
    dependency_count: int = field(metadata={"sql": "int", "nullable": False})
    forbidden_property_count: int = field(metadata={"sql": "int", "nullable": False})


@dataclass(frozen=True, slots=True)
class TableRow(CatalogRow):
    """Ordered TABLE catalog facts; transport acceptance is not admission."""

    schema_id: int = field(metadata={"sql": "int", "nullable": False})
    schema_name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    object_name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    object_type: str = field(metadata={"sql": "char(2)", "nullable": False})
    object_create_time: str = field(metadata={"sql": "char(27)", "nullable": False})
    object_modify_time: str = field(metadata={"sql": "char(27)", "nullable": False})
    is_memory_optimized: bool = field(metadata={"sql": "bit", "nullable": False})
    durability: int = field(metadata={"sql": "tinyint", "nullable": False})
    temporal_type: int = field(metadata={"sql": "tinyint", "nullable": False})
    is_filetable: bool = field(metadata={"sql": "bit", "nullable": False})
    is_node: bool = field(metadata={"sql": "bit", "nullable": False})
    is_edge: bool = field(metadata={"sql": "bit", "nullable": False})
    ledger_type: int = field(metadata={"sql": "tinyint", "nullable": False})
    lob_data_space_id: int = field(metadata={"sql": "int", "nullable": False})
    filestream_data_space_id: int | None = field(metadata={"sql": "int", "nullable": True})


@dataclass(frozen=True, slots=True)
class ColumnRow(CatalogRow):
    """Ordered COLUMN catalog facts; transport acceptance is not admission."""

    column_id: int = field(metadata={"sql": "int", "nullable": False})
    name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    system_type_id: int = field(metadata={"sql": "tinyint", "nullable": False})
    user_type_id: int = field(metadata={"sql": "int", "nullable": False})
    type_schema: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    type_name: str = field(metadata={"sql": "nvarchar(128)", "nullable": False})
    max_length: int = field(metadata={"sql": "smallint", "nullable": False})
    precision: int = field(metadata={"sql": "tinyint", "nullable": False})
    scale: int = field(metadata={"sql": "tinyint", "nullable": False})
    collation_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    is_nullable: bool = field(metadata={"sql": "bit", "nullable": False})
    is_ansi_padded: bool = field(metadata={"sql": "bit", "nullable": False})
    is_identity: bool = field(metadata={"sql": "bit", "nullable": False})
    is_computed: bool = field(metadata={"sql": "bit", "nullable": False})
    is_sparse: bool = field(metadata={"sql": "bit", "nullable": False})
    is_column_set: bool = field(metadata={"sql": "bit", "nullable": False})
    is_hidden: bool = field(metadata={"sql": "bit", "nullable": False})
    generated_always_type: int = field(metadata={"sql": "tinyint", "nullable": False})
    encryption_type: int | None = field(metadata={"sql": "int", "nullable": True})
    is_masked: bool = field(metadata={"sql": "bit", "nullable": False})
    default_object_id: int = field(metadata={"sql": "int", "nullable": False})
    rule_object_id: int = field(metadata={"sql": "int", "nullable": False})


@dataclass(frozen=True, slots=True)
class IndexRow(CatalogRow):
    """Ordered INDEX catalog facts; transport acceptance is not admission."""

    index_id: int = field(metadata={"sql": "int", "nullable": False})
    name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    type: int = field(metadata={"sql": "tinyint", "nullable": False})
    type_desc: str = field(metadata={"sql": "nvarchar(60)", "nullable": False})
    is_unique: bool = field(metadata={"sql": "bit", "nullable": False})
    is_primary_key: bool = field(metadata={"sql": "bit", "nullable": False})
    is_unique_constraint: bool = field(metadata={"sql": "bit", "nullable": False})
    is_disabled: bool = field(metadata={"sql": "bit", "nullable": False})
    is_hypothetical: bool = field(metadata={"sql": "bit", "nullable": False})
    has_filter: bool = field(metadata={"sql": "bit", "nullable": False})
    filter_definition: str | None = field(metadata={"sql": "nvarchar(max)", "nullable": True})
    data_space_id: int = field(metadata={"sql": "int", "nullable": False})
    data_space_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    data_space_type: str | None = field(metadata={"sql": "char(2)", "nullable": True})


@dataclass(frozen=True, slots=True)
class IndexColumnRow(CatalogRow):
    """Ordered INDEX_COLUMN catalog facts; transport acceptance is not admission."""

    index_id: int = field(metadata={"sql": "int", "nullable": False})
    index_column_id: int = field(metadata={"sql": "int", "nullable": False})
    column_id: int = field(metadata={"sql": "int", "nullable": False})
    key_ordinal: int = field(metadata={"sql": "tinyint", "nullable": False})
    partition_ordinal: int = field(metadata={"sql": "tinyint", "nullable": False})
    is_descending_key: bool = field(metadata={"sql": "bit", "nullable": False})
    is_included_column: bool = field(metadata={"sql": "bit", "nullable": False})
    column_store_order_ordinal: int = field(metadata={"sql": "int", "nullable": False})


@dataclass(frozen=True, slots=True)
class PartitionRow(CatalogRow):
    """Ordered PARTITION catalog facts; transport acceptance is not admission."""

    index_id: int = field(metadata={"sql": "int", "nullable": False})
    partition_number: int = field(metadata={"sql": "int", "nullable": False})
    partition_id: int = field(metadata={"sql": "bigint", "nullable": False})
    hobt_id: int = field(metadata={"sql": "bigint", "nullable": False})
    data_compression: int = field(metadata={"sql": "tinyint", "nullable": False})
    data_compression_desc: str = field(metadata={"sql": "nvarchar(60)", "nullable": False})
    data_space_id: int = field(metadata={"sql": "int", "nullable": False})
    data_space_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    data_space_type: str | None = field(metadata={"sql": "char(2)", "nullable": True})


@dataclass(frozen=True, slots=True)
class DependencyRow(CatalogRow):
    """Ordered DEPENDENCY catalog facts; transport acceptance is not admission."""

    direction: str = field(metadata={"sql": "varchar(8)", "nullable": False})
    referencing_id: int = field(metadata={"sql": "int", "nullable": False})
    referencing_minor_id: int = field(metadata={"sql": "int", "nullable": False})
    referenced_id: int | None = field(metadata={"sql": "int", "nullable": True})
    referenced_minor_id: int = field(metadata={"sql": "int", "nullable": False})
    referenced_server_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    referenced_database_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    referenced_schema_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    referenced_entity_name: str | None = field(metadata={"sql": "nvarchar(128)", "nullable": True})
    is_schema_bound_reference: bool = field(metadata={"sql": "bit", "nullable": False})
    is_caller_dependent: bool = field(metadata={"sql": "bit", "nullable": False})
    is_ambiguous: bool = field(metadata={"sql": "bit", "nullable": False})


@dataclass(frozen=True, slots=True)
class ForbiddenPropertyRow(CatalogRow):
    """Ordered FORBIDDEN_PROPERTY catalog facts; transport acceptance is not admission."""

    property_code: str = field(metadata={"sql": "varchar(40)", "nullable": False})
    related_object_id: int | None = field(metadata={"sql": "int", "nullable": True})
    related_column_id: int | None = field(metadata={"sql": "int", "nullable": True})


@dataclass(frozen=True, slots=True)
class CountRow(CatalogRow):
    """Ordered COUNT catalog facts; transport acceptance is not admission."""

    row_count_exact: int = field(metadata={"sql": "bigint", "nullable": False})
