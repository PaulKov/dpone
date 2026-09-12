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
