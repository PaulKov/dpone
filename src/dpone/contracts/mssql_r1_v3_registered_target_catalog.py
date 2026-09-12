"""Registered-target catalog aggregate and catalog-bound column references."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bool,
    expect_bytes,
    expect_int,
    expect_text,
    expect_tuple,
    expect_uuid,
    require_digest,
    require_positive,
    require_sql_int,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import (
    MssqlR1ClosedTargetFeatureObservationV1,
    MssqlR1RegisteredTargetColumnV1,
    MssqlR1RegisteredTargetIndexV1,
)
from dpone.contracts.postgres_mssql_type_target_enums import MssqlR1TargetScalarFamilyV1 as Target
from dpone.contracts.postgres_mssql_type_target_enums import (
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import MssqlR1CanonicalTargetScalarShapeV1

_CATALOG = b"dpone-mssql-r1-registered-target-catalog-v1\0"
_REF = b"dpone-mssql-r1-registered-target-column-ref-v1\0"


@dataclass(frozen=True, slots=True)
class MssqlR1RegisteredTargetCatalogV1:
    catalog_version: str
    target_binding_uuid: UUID
    target_object_uuid: UUID
    physical_generation_uuid: UUID
    target_object_profile: str
    database_name: str
    schema_name: str
    object_name: str
    object_id: int
    target_contract_revision: int
    database_collation: str
    ordered_columns: tuple[MssqlR1RegisteredTargetColumnV1, ...]
    primary_key: MssqlR1RegisteredTargetIndexV1
    ordered_secondary_indexes: tuple[MssqlR1RegisteredTargetIndexV1, ...]
    feature_observation: MssqlR1ClosedTargetFeatureObservationV1

    @authority_validation
    def __post_init__(self) -> None:
        if (
            type(self.catalog_version) is not str
            or self.catalog_version != "dpone-mssql-r1-registered-target-catalog-1"
        ):
            reject("wrong_version")
        if type(self.target_object_profile) is not str or self.target_object_profile != "ordinary_disk_rowstore_v1":
            reject("target_profile_unsupported")
        if any(
            type(value) is not UUID
            for value in (self.target_binding_uuid, self.target_object_uuid, self.physical_generation_uuid)
        ):
            reject("invalid_facet")
        for name in ("database_name", "schema_name", "object_name", "database_collation"):
            if type(getattr(self, name)) is not str:
                reject("identifier_invalid")
            require_identifier_v1(getattr(self, name))
        if type(self.object_id) is not int or type(self.target_contract_revision) is not int:
            reject("invalid_facet")
        require_sql_int(self.object_id, "object ID")
        require_positive(self.target_contract_revision, "target contract revision")
        self._validate_inventory()

    def _validate_inventory(self) -> None:
        columns = self.ordered_columns
        if (
            type(columns) is not tuple
            or not columns
            or len(columns) > 1024
            or not all(type(item) is MssqlR1RegisteredTargetColumnV1 for item in columns)
        ):
            reject("catalog_order_invalid")
        if tuple(item.ordinal for item in columns) != tuple(range(1, len(columns) + 1)) or len(
            {item.name.casefold() for item in columns}
        ) != len(columns):
            reject("duplicate_column")
        key = self.primary_key
        if (
            type(key) is not MssqlR1RegisteredTargetIndexV1
            or key.ordinal != 1
            or (key.unique, key.primary_key, key.unique_constraint) != (True, True, False)
            or key.ordered_descending != (False,)
            or key.ordered_included_column_ordinals
            or len(key.ordered_key_column_ordinals) != 1
        ):
            reject("target_key_invalid")
        key_column_ordinal = key.ordered_key_column_ordinals[0]
        if not 1 <= key_column_ordinal <= len(columns):
            reject("target_key_invalid")
        key_column = columns[key_column_ordinal - 1]
        if key_column.nullable or key_column.scalar_shape.family not in {
            Target.SMALLINT,
            Target.INT,
            Target.BIGINT,
            Target.UNIQUEIDENTIFIER,
        }:
            reject("target_key_invalid")
        secondary = self.ordered_secondary_indexes
        if (
            type(secondary) is not tuple
            or len(secondary) > 999
            or not all(type(item) is MssqlR1RegisteredTargetIndexV1 for item in secondary)
        ):
            reject("catalog_order_invalid")
        if tuple(item.ordinal for item in secondary) != tuple(range(2, len(secondary) + 2)) or secondary != tuple(
            sorted(secondary, key=lambda item: item.canonical_bytes)
        ):
            reject("catalog_order_invalid")
        if len({item.name.casefold() for item in (key, *secondary)}) != len(secondary) + 1:
            reject("catalog_order_invalid")
        if (
            any(item.primary_key for item in secondary)
            or sum(item.index_kind == "clustered" for item in (key, *secondary)) > 1
        ):
            reject("catalog_behavior_mismatch", operator=True)
        ordinals = set(range(1, len(columns) + 1))
        if any(
            not set((*item.ordered_key_column_ordinals, *item.ordered_included_column_ordinals)) <= ordinals
            for item in (key, *secondary)
        ):
            reject("catalog_order_invalid")
        if type(self.feature_observation) is not MssqlR1ClosedTargetFeatureObservationV1:
            reject("target_feature_unsupported", operator=True)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CATALOG,
            (
                *tuple(getattr(self, name) for name in tuple(self.__dataclass_fields__)[:11]),
                tuple(item.canonical_bytes for item in self.ordered_columns),
                self.primary_key.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_secondary_indexes),
                self.feature_observation.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def create(cls, payload: bytes) -> MssqlR1RegisteredTargetCatalogV1:
        """Admit exact persisted catalog bytes, never a runtime snapshot object."""

        if type(payload) is not bytes:
            reject("malformed_canonical_bytes")
        value = cls.from_canonical_bytes(payload)
        if value.canonical_bytes != payload:
            reject("malformed_canonical_bytes")
        return value

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegisteredTargetCatalogV1:
        values = list(decode_canonical_bytes(payload, _CATALOG, field_count=15))
        for index in (0, 4, 5, 6, 7, 10):
            values[index] = expect_text(values[index], "catalog text")
        for index in (1, 2, 3):
            values[index] = expect_uuid(values[index], "catalog UUID")
        values[8], values[9] = expect_int(values[8], "object ID"), expect_int(values[9], "revision")
        values[11] = tuple(
            MssqlR1RegisteredTargetColumnV1.from_canonical_bytes(expect_bytes(item, "column"))
            for item in expect_tuple(values[11], "columns")
        )
        values[12] = MssqlR1RegisteredTargetIndexV1.from_canonical_bytes(expect_bytes(values[12], "primary key"))
        values[13] = tuple(
            MssqlR1RegisteredTargetIndexV1.from_canonical_bytes(expect_bytes(item, "index"))
            for item in expect_tuple(values[13], "indexes")
        )
        values[14] = MssqlR1ClosedTargetFeatureObservationV1.from_canonical_bytes(expect_bytes(values[14], "features"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True, init=False)
class MssqlR1RegisteredTargetColumnRefV1:
    ordinal: int
    name: str
    nullable: bool
    scalar_shape: MssqlR1CanonicalTargetScalarShapeV1
    target_catalog_digest: bytes
    primary_key_ordinal: int | None

    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        reject("authority_splice")

    @classmethod
    def _from_catalog(
        cls,
        ordinal: int,
        name: str,
        nullable: bool,
        scalar_shape: MssqlR1CanonicalTargetScalarShapeV1,
        target_catalog_digest: bytes,
        primary_key_ordinal: int | None,
    ) -> MssqlR1RegisteredTargetColumnRefV1:
        value = object.__new__(cls)
        for field, item in zip(
            cls.__dataclass_fields__,
            (ordinal, name, nullable, scalar_shape, target_catalog_digest, primary_key_ordinal),
            strict=True,
        ):
            object.__setattr__(value, field, item)
        value.__post_init__()
        return value

    @classmethod
    def from_catalog(
        cls, catalog: MssqlR1RegisteredTargetCatalogV1, ordinal: int
    ) -> MssqlR1RegisteredTargetColumnRefV1:
        if (
            type(catalog) is not MssqlR1RegisteredTargetCatalogV1
            or type(ordinal) is not int
            or not 1 <= ordinal <= len(catalog.ordered_columns)
        ):
            reject("ordinal_invalid")
        column = catalog.ordered_columns[ordinal - 1]
        key_ordinal = 1 if ordinal == catalog.primary_key.ordered_key_column_ordinals[0] else None
        return cls._from_catalog(
            column.ordinal, column.name, column.nullable, column.scalar_shape, catalog.digest, key_ordinal
        )

    @authority_validation
    def __post_init__(self) -> None:
        if (
            type(self.ordinal) is not int
            or not 1 <= self.ordinal <= 1024
            or type(self.nullable) is not bool
            or (
                self.primary_key_ordinal is not None
                and (type(self.primary_key_ordinal) is not int or self.primary_key_ordinal != 1)
            )
        ):
            reject("ordinal_invalid")
        if type(self.name) is not str:
            reject("identifier_invalid")
        require_identifier_v1(self.name)
        if type(self.scalar_shape) is not MssqlR1CanonicalTargetScalarShapeV1:
            reject("authority_splice")
        if type(self.target_catalog_digest) is not bytes:
            reject("invalid_facet")
        require_digest(self.target_catalog_digest, "catalog digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _REF,
            (
                self.ordinal,
                self.name,
                self.nullable,
                self.scalar_shape.canonical_bytes,
                self.target_catalog_digest,
                self.primary_key_ordinal,
            ),
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegisteredTargetColumnRefV1:
        values = decode_canonical_bytes(payload, _REF, field_count=6)
        primary_key_ordinal = values[5]
        if primary_key_ordinal is not None:
            primary_key_ordinal = expect_int(primary_key_ordinal, "primary key ordinal")
        return cls._from_catalog(
            expect_int(values[0], "ordinal"),
            expect_text(values[1], "name"),
            expect_bool(values[2], "nullable"),
            MssqlR1CanonicalTargetScalarShapeV1.from_canonical_bytes(expect_bytes(values[3], "shape")),
            require_digest(values[4], "catalog digest"),
            primary_key_ordinal,
        )

    def validate_against_catalog(self, catalog: MssqlR1RegisteredTargetCatalogV1) -> None:
        """Prove this decoded reference still belongs to the supplied catalog."""

        if type(catalog) is not MssqlR1RegisteredTargetCatalogV1 or self.target_catalog_digest != catalog.digest:
            reject("authority_splice")
        if not 1 <= self.ordinal <= len(catalog.ordered_columns):
            reject("authority_splice")
        column = catalog.ordered_columns[self.ordinal - 1]
        key_ordinal = 1 if self.ordinal == catalog.primary_key.ordered_key_column_ordinals[0] else None
        if (self.name, self.nullable, self.scalar_shape, self.primary_key_ordinal) != (
            column.name,
            column.nullable,
            column.scalar_shape,
            key_ordinal,
        ):
            reject("authority_splice")


__all__ = ["MssqlR1RegisteredTargetCatalogV1", "MssqlR1RegisteredTargetColumnRefV1"]
