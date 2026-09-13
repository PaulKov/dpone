"""Canonical declared dbt obligations, distinct from observed physical schemas.

Only verified source readers may supply the pack and manifest at the application
boundary. These structural contracts authenticate nothing. An empty declaration
means no column obligations; an untyped column still requires that named column.
"""

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Literal

from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_relation_writes import DbtRelationWrite, selected_relation_writes
from dpone.contracts.dbt_workspace_observation import require_observable_identifier
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.contracts.strict_json import canonical_json_bytes

MAX_MATERIALIZATIONS = 128
MAX_COLUMNS = 1024
MAX_MATERIALIZATION_BYTES = 1024 * 1024


def materialization_type(value: str) -> str:
    """Use the shared strict parser and SQL Server's physical synonym families."""
    if type(value) is not str:
        raise DbtCaptureError("materialization_declared_type")
    try:
        normalized = normalize_mssql_physical_type(value)
    except ValueError:
        raise DbtCaptureError("materialization_declared_type") from None
    if normalized.startswith("numeric("):
        return "decimal" + normalized[len("numeric") :]
    floating = re.fullmatch(r"float\((\d+)\)", normalized)
    if floating:
        return "real" if int(floating.group(1)) <= 24 else "float"
    return normalized


@dataclass(frozen=True, slots=True)
class DeclaredColumn:
    """Exact name plus an optional declared physical type, never inferred."""

    name: str
    data_type: str | None

    def __post_init__(self) -> None:
        require_observable_identifier(self.name)
        if self.data_type is not None and materialization_type(self.data_type) != self.data_type:
            raise DbtCaptureError("materialization_declared_type")


@dataclass(frozen=True, slots=True)
class DbtMaterializationContract:
    """Final selected model target and explicit column/type obligations."""

    write: DbtRelationWrite
    kind: Literal["table", "view"]
    columns: tuple[DeclaredColumn, ...]

    def __post_init__(self) -> None:
        if type(self.write) is not DbtRelationWrite:
            raise DbtCaptureError("materialization_write")
        self.write.__post_init__()
        if self.write.connector != "mssql" or self.write.kind != "model" or self.write.role != "target":
            raise DbtCaptureError("materialization_write")
        for value in (self.write.database, self.write.schema, self.write.relation):
            require_observable_identifier(value)
        if self.kind not in {"table", "view"} or type(self.columns) is not tuple or len(self.columns) > MAX_COLUMNS:
            raise DbtCaptureError("materialization_columns")
        if any(type(column) is not DeclaredColumn for column in self.columns):
            raise DbtCaptureError("materialization_columns")
        names = tuple(column.name for column in self.columns)
        if names != tuple(sorted(set(names))) or len({name.casefold() for name in names}) != len(names):
            raise DbtCaptureError("materialization_ambiguous_columns")
        for column in self.columns:
            column.__post_init__()

    @property
    def schema_sha256(self) -> str:
        """Fingerprint declared obligations only; never attest an actual schema."""
        self.__post_init__()
        return (
            "sha256:"
            + sha256(
                canonical_json_bytes(
                    {
                        "schema": "dpone.dbt-declared-column-obligations.v1",
                        "columns": [asdict(column) for column in self.columns],
                    }
                )
            ).hexdigest()
        )

    @property
    def expectation(self) -> tuple[str, str, str]:
        """Existing outcome-expectation projection, without physical provenance."""
        return self.write.resource_id, self.kind, self.schema_sha256


def derive_dbt_materializations(
    *, project_path: str, pack: DbtExecutionPack, manifest: Mapping[str, Any]
) -> tuple[DbtMaterializationContract, ...]:
    """Derive exact final model coordinates through the existing write producer.

    Intermediate, helper and unit-test relations remain in the admitted write
    footprint but are not persistent model outcomes. Table and incremental
    materializations require a table; view requires a view. Extra actual columns
    are retained by observation and do not invent undeclared obligations.
    Unsupported declarative constraints fail closed rather than disappearing.
    """
    writes = selected_relation_writes(project_path=project_path, execution=pack, manifest=manifest)
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        raise DbtCaptureError("materialization_nodes")
    contracts = []
    for write in writes:
        if write.kind != "model" or write.role != "target":
            continue
        node = nodes[write.resource_id]
        if node.get("constraints"):
            raise DbtCaptureError("materialization_unsupported_constraint")
        declared = node.get("columns", {})
        if not isinstance(declared, Mapping) or len(declared) > MAX_COLUMNS:
            raise DbtCaptureError("materialization_columns")
        columns = []
        for name, column in declared.items():
            if not isinstance(column, Mapping) or column.get("name") != name or column.get("constraints"):
                raise DbtCaptureError("materialization_unsupported_constraint")
            dtype = column.get("data_type")
            columns.append(DeclaredColumn(name, None if dtype is None else materialization_type(dtype)))
        kind: Literal["table", "view"] = "view" if node["config"]["materialized"] == "view" else "table"
        contracts.append(DbtMaterializationContract(write, kind, tuple(sorted(columns, key=lambda row: row.name))))
    if not 1 <= len(contracts) <= MAX_MATERIALIZATIONS:
        raise DbtCaptureError("materialization_selection")
    return tuple(sorted(contracts, key=lambda row: row.write.resource_id))


_OBJECT_FIELDS = ("object_id", "schema", "name", "kind", "create_token", "modify_token", "module_sha256")
_COLUMN_FIELDS = (
    "column_id",
    "name",
    "system_type",
    "user_type",
    "user_defined",
    "max_length",
    "precision",
    "scale",
    "nullable",
    "collation",
    "computed",
    "identity",
    "hidden",
    "generated_always_type",
    "encryption_type",
)


def materialization_catalog_columns(object_row: tuple[Any, ...], rows: tuple[tuple[Any, ...], ...]) -> dict[str, Any]:
    """Validate and canonically project bounded physical catalog rows."""
    if not 1 <= len(rows) <= MAX_COLUMNS:
        raise DbtCaptureError("materialization_column_budget")
    prior = 0
    names = set()
    for row in rows:
        if len(row) != 15 or type(row[0]) is not int or row[0] <= prior:
            raise DbtCaptureError("materialization_catalog_columns")
        require_observable_identifier(row[1])
        if row[1].casefold() in names or any(type(row[index]) is not int for index in (4, 5, 6, 7, 8, 10, 11, 12, 13)):
            raise DbtCaptureError("materialization_catalog_columns")
        if (row[2] is not None and type(row[2]) is not str) or type(row[3]) is not str:
            raise DbtCaptureError("materialization_catalog_columns")
        if (row[9] is not None and (type(row[9]) is not str or len(row[9]) > 128)) or (
            row[14] is not None and type(row[14]) is not int
        ):
            raise DbtCaptureError("materialization_catalog_columns")
        if (
            any(row[index] not in (0, 1) for index in (4, 8, 10, 11, 12))
            or not -1 <= row[5] <= 32767
            or not 0 <= row[6] <= 255
            or not 0 <= row[7] <= 38
            or not 0 <= row[13] <= 8
            or row[14] not in (None, 1, 2)
            or len(row[3]) > 257
            or (row[2] is not None and len(row[2]) > 128)
        ):
            raise DbtCaptureError("materialization_catalog_columns")
        names.add(row[1].casefold())
        prior = row[0]
    return {
        "object": dict(
            zip(_OBJECT_FIELDS, (*object_row[:6], object_row[6].hex() if object_row[6] else None), strict=True)
        ),
        "columns": [dict(zip(_COLUMN_FIELDS, row, strict=True)) for row in rows],
    }


def require_materialization_columns(contract: DbtMaterializationContract, actual: list[dict[str, Any]]) -> None:
    columns = {row["name"]: row for row in actual}
    for declared in contract.columns:
        column = columns.get(declared.name)
        if column is None:
            raise DbtCaptureError("materialization_missing_column")
        if declared.data_type is not None and actual_materialization_type(column) != declared.data_type:
            raise DbtCaptureError("materialization_column_type")


def actual_materialization_type(column: dict[str, Any]) -> str:
    """Reconstruct exact SQL catalog dimensions, then use the shared type parser."""
    kind = column["system_type"]
    if column["user_defined"] or column["encryption_type"] is not None or type(kind) is not str:
        raise DbtCaptureError("materialization_column_type")
    if kind in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
        length = column["max_length"]
        if length != -1 and kind in {"nvarchar", "nchar"}:
            if length % 2:
                raise DbtCaptureError("materialization_column_type")
            length //= 2
        dtype = f"{kind}({'max' if length == -1 else length})"
    elif kind in {"decimal", "numeric"}:
        dtype = f"{kind}({column['precision']},{column['scale']})"
    elif kind in {"datetime2", "datetimeoffset", "time"}:
        dtype = f"{kind}({column['scale']})"
    elif kind == "float":
        dtype = f"float({column['precision']})"
    else:
        dtype = kind
    return materialization_type(dtype)


def materialization_observation_document(
    *,
    attempt_sha256: str,
    intent_sha256: str,
    invocation: str,
    projected: tuple[tuple[str, str, str], ...],
    catalog: list[dict[str, Any]],
) -> bytes:
    """Encode the complete observed outcome, keeping the existing byte budget."""
    original = canonical_json_bytes(
        {
            "schema": "dpone.composition-dbt-materialization-observation.v1",
            "attempt_sha256": attempt_sha256,
            "intent_sha256": intent_sha256,
            "invocation_id": invocation,
            "materializations": projected,
            "catalog": catalog,
        }
    )
    if len(original) > MAX_MATERIALIZATION_BYTES:
        raise DbtCaptureError("materialization_budget")
    return original
