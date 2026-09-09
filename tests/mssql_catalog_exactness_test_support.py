"""Synthetic SQL Server catalog rows for exactness contract tests."""

from __future__ import annotations

from copy import deepcopy

from dpone.runtime.state.mssql_catalog_integrity import MssqlTableIntegrityContract
from dpone.runtime.state.mssql_contract import MssqlColumnShape, MssqlExternalTableContract

EXACT_COLUMN_SAFETY_DRIFTS: tuple[tuple[str, object], ...] = (
    ("is_computed", True),
    ("is_sparse", True),
    ("is_rowguidcol", True),
    ("generated_always_type", 1),
    ("is_hidden", True),
    ("is_masked", True),
    ("is_encrypted", True),
    ("is_filestream", True),
    ("is_column_set", True),
    ("uses_database_default_collation", False),
    ("is_user_defined", True),
    ("is_assembly_type", True),
    ("has_bound_rule", True),
    ("has_bound_default", True),
)


class ExactCatalogExecutor:
    """Serve deterministic catalog rows to the production integrity reader."""

    def __init__(self, rows: dict[str, list[dict[str, object]]]) -> None:
        self.rows = deepcopy(rows)

    def get_records(self, query: object, params=None, as_dict: bool = False):
        del params, as_dict
        rendered = str(query)
        for catalog in ("partitions", "foreign_keys", "check_constraints", "default_constraints", "indexes"):
            if f"sys.{catalog}" in rendered:
                key = {
                    "check_constraints": "checks",
                    "default_constraints": "defaults",
                }.get(catalog, catalog)
                return deepcopy(self.rows[key])
        raise AssertionError(f"unexpected catalog query: {rendered}")

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"


class ExactShapeCatalogExecutor:
    """Serve one exact table shape, optionally with explicit identity drift."""

    def __init__(
        self,
        contract: MssqlExternalTableContract,
        *,
        column_overrides: dict[str, dict[str, object]] | None = None,
    ) -> None:
        overrides = column_overrides or {}
        self.queries: list[str] = []
        self.column_rows: list[dict[str, object]] = []
        for shape in contract.shapes:
            row = _exact_shape_row(shape)
            row.update(overrides.get(shape.name, {}))
            self.column_rows.append(row)
        self.index_rows = [
            {
                "index_id": index_id,
                "column_name": column,
                "key_ordinal": ordinal,
            }
            for index_id, columns in enumerate(contract.unique_indexes, start=1)
            for ordinal, column in enumerate(columns, start=1)
        ]

    def get_records(self, query: object, params=None, as_dict: bool = False):
        del params, as_dict
        rendered = str(query)
        self.queries.append(rendered)
        if "sys.columns AS c" in rendered and "sys.indexes AS i" not in rendered:
            return deepcopy(self.column_rows)
        if "sys.indexes AS i" in rendered:
            return deepcopy(self.index_rows)
        raise AssertionError(f"unexpected catalog query: {rendered}")

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"


def _exact_shape_row(shape: MssqlColumnShape) -> dict[str, object]:
    fields = {
        "is_identity": shape.identity,
        "is_computed": shape.is_computed,
        "is_sparse": shape.is_sparse,
        "is_rowguidcol": shape.is_rowguidcol,
        "generated_always_type": shape.generated_always_type,
        "is_hidden": shape.is_hidden,
        "is_masked": shape.is_masked,
        "is_encrypted": shape.is_encrypted,
        "is_ansi_padded": shape.is_ansi_padded,
        "is_filestream": shape.is_filestream,
        "is_column_set": shape.is_column_set,
        "uses_database_default_collation": shape.uses_database_default_collation,
        "is_user_defined": shape.is_user_defined,
        "is_assembly_type": shape.is_assembly_type,
        "has_bound_rule": shape.has_bound_rule,
        "has_bound_default": shape.has_bound_default,
    }
    missing = sorted(field for field, value in fields.items() if value is None)
    if missing:
        raise AssertionError(f"exact test shape has optional catalog metadata: {shape.name}:{','.join(missing)}")
    return {
        "column_name": shape.name,
        "type_name": shape.type_name,
        "max_length": shape.max_length,
        "precision": shape.precision,
        "scale": shape.scale,
        "is_nullable": shape.nullable,
        **fields,
    }


def catalog_rows(contract: MssqlTableIntegrityContract) -> dict[str, list[dict[str, object]]]:
    """Build one valid catalog result set from a declared integrity contract."""

    rows: dict[str, list[dict[str, object]]] = {
        "indexes": [],
        "foreign_keys": [],
        "checks": [],
        "defaults": [],
        "partitions": [],
    }
    for index_id, index in enumerate(contract.indexes, start=1):
        for ordinal, column in enumerate(index.columns, start=1):
            rows["indexes"].append(
                {
                    "index_id": index_id,
                    "index_name": f"index_{index_id}",
                    "is_unique": True,
                    "is_primary_key": index.kind == "primary_key",
                    "is_unique_constraint": index.kind == "unique_constraint",
                    "is_disabled": False,
                    "is_hypothetical": False,
                    "ignore_dup_key": False,
                    "has_filter": index.filter_expression is not None,
                    "filter_definition": index.filter_expression,
                    "type_desc": "CLUSTERED" if index.clustered else "NONCLUSTERED",
                    "column_name": column,
                    "key_ordinal": ordinal,
                    "is_included_column": False,
                    "is_descending_key": False,
                }
            )
        rows["partitions"].append(
            {
                "index_id": index_id,
                "partition_number": 1,
                "data_compression_desc": "NONE",
            }
        )
    for constraint_id, foreign_key in enumerate(contract.foreign_keys, start=1):
        for ordinal, (column, referenced_column) in enumerate(
            zip(foreign_key.columns, foreign_key.referenced_columns, strict=True),
            start=1,
        ):
            rows["foreign_keys"].append(
                {
                    "constraint_object_id": constraint_id,
                    "constraint_name": f"foreign_key_{constraint_id}",
                    "is_disabled": False,
                    "is_not_trusted": False,
                    "is_not_for_replication": False,
                    "delete_referential_action_desc": foreign_key.delete_action,
                    "update_referential_action_desc": foreign_key.update_action,
                    "referenced_schema": foreign_key.referenced_schema,
                    "referenced_table": foreign_key.referenced_table,
                    "parent_column": column,
                    "referenced_column": referenced_column,
                    "constraint_column_id": ordinal,
                }
            )
    rows["checks"] = [
        {
            "constraint_name": f"check_{offset}",
            "definition": check.expression,
            "is_disabled": False,
            "is_not_trusted": False,
            "is_not_for_replication": False,
        }
        for offset, check in enumerate(contract.checks, start=1)
    ]
    rows["defaults"] = [
        {
            "column_name": default.column,
            "definition": default.expression,
        }
        for default in contract.defaults
    ]
    return rows


def add_unexpected_catalog_object(
    rows: dict[str, list[dict[str, object]]],
    family: str,
) -> None:
    """Inject one otherwise healthy extra object into a catalog result set."""

    if family == "index":
        index_id = 1 + max((int(str(row["index_id"])) for row in rows["indexes"]), default=0)
        rows["indexes"].append(
            {
                "index_id": index_id,
                "index_name": "unexpected_index",
                "is_unique": False,
                "is_primary_key": False,
                "is_unique_constraint": False,
                "is_disabled": False,
                "is_hypothetical": False,
                "ignore_dup_key": False,
                "has_filter": False,
                "filter_definition": None,
                "type_desc": "NONCLUSTERED",
                "column_name": "unexpected_column",
                "key_ordinal": 1,
                "is_included_column": False,
                "is_descending_key": False,
            }
        )
        rows["partitions"].append(
            {
                "index_id": index_id,
                "partition_number": 1,
                "data_compression_desc": "NONE",
            }
        )
        return
    if family == "foreign_key":
        constraint_id = 1 + max(
            (int(str(row["constraint_object_id"])) for row in rows["foreign_keys"]),
            default=0,
        )
        rows["foreign_keys"].append(
            {
                "constraint_object_id": constraint_id,
                "constraint_name": "unexpected_foreign_key",
                "is_disabled": False,
                "is_not_trusted": False,
                "is_not_for_replication": False,
                "delete_referential_action_desc": "NO_ACTION",
                "update_referential_action_desc": "NO_ACTION",
                "referenced_schema": "dbo",
                "referenced_table": "unexpected_parent",
                "parent_column": "unexpected_column",
                "referenced_column": "unexpected_id",
                "constraint_column_id": 1,
            }
        )
        return
    if family == "check":
        rows["checks"].append(
            {
                "constraint_name": "unexpected_check",
                "definition": "[unexpected_column] >= 0",
                "is_disabled": False,
                "is_not_trusted": False,
                "is_not_for_replication": False,
            }
        )
        return
    if family == "default":
        rows["defaults"].append(
            {
                "column_name": "unexpected_column",
                "definition": "0",
            }
        )
        return
    raise AssertionError(f"unsupported catalog family: {family}")


def expected_exactness_error(family: str) -> str:
    return {
        "index": "index_unexpected",
        "foreign_key": "foreign_key_unexpected",
        "check": "check_unexpected",
        "default": "default_unexpected",
    }[family]


__all__ = [
    "EXACT_COLUMN_SAFETY_DRIFTS",
    "ExactCatalogExecutor",
    "ExactShapeCatalogExecutor",
    "add_unexpected_catalog_object",
    "catalog_rows",
    "expected_exactness_error",
]
