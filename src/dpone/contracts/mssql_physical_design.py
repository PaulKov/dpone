"""Finite SQL Server physical-design capability contract.

The contract is deliberately smaller than SQL Server's complete DDL surface.
Every accepted option has one exact renderer and catalog meaning.  Options
outside that surface are rejected instead of being silently ignored by a
planner or a runtime target-creation path.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Literal

MssqlCompression = Literal["NONE", "ROW", "PAGE"]
MssqlIndexKind = Literal["clustered", "nonclustered"]

MSSQL_MAX_INDEX_KEY_COLUMNS = 16
MSSQL_CLUSTERED_INDEX_KEY_MAX_BYTES = 900
MSSQL_NONCLUSTERED_INDEX_KEY_MAX_BYTES = 1700

_STORAGE_KEYS = frozenset(
    {
        "compression",
        "filegroup",
        "textimage_filegroup",
        "clustered_columnstore",
        "index_fillfactor",
        "fillfactor",
    }
)
_INDEX_KEYS = frozenset({"primary_key"})
_MAX_INDEX_KEY_COLUMNS = 16
_LOB_TYPES = ("varchar(max)", "nvarchar(max)", "varbinary(max)", "text", "ntext", "image", "xml")
_TARGET_TYPE_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]*"
    r"(?:\s*\(\s*(?:max|\d+)(?:\s*,\s*\d+)?\s*\))?"
    r"(?:\s+COLLATE\s+[A-Za-z0-9_]+)?$",
    re.IGNORECASE,
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CHARACTER_KEY_TYPE_RE = re.compile(r"^(n?(?:var)?char)\((max|\d+)\)$", re.IGNORECASE)
_DECIMAL_KEY_TYPE_RE = re.compile(r"^(?:decimal|numeric)\((\d+)(?:,(\d+))?\)$", re.IGNORECASE)
_FIXED_KEY_WIDTH_BYTES = {
    "bigint": 8,
    "bit": 1,
    "date": 3,
    "datetime": 8,
    "datetime2": 8,
    "datetimeoffset": 10,
    "float": 8,
    "int": 4,
    "money": 8,
    "real": 4,
    "smalldatetime": 4,
    "smallint": 2,
    "smallmoney": 4,
    "time": 5,
    "tinyint": 1,
    "uniqueidentifier": 16,
}


class MssqlIndexKeyContractError(ValueError):
    """Stable typed rejection for an impossible SQL Server index key."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class MssqlTableDesign:
    """Validated storage choices used by both planning and runtime DDL."""

    compression: MssqlCompression = "NONE"
    filegroup: str | None = None
    textimage_filegroup: str | None = None
    clustered_columnstore: bool = False
    index_fillfactor: int | None = None

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> MssqlTableDesign:
        physical = _physical_options(options)
        storage = _mapping(_mapping(physical.get("storage"), "storage").get("mssql"), "storage.mssql")
        return cls.from_storage(storage)

    @classmethod
    def from_storage(cls, storage: Mapping[str, Any] | None) -> MssqlTableDesign:
        values = dict(storage or {})
        _reject_unknown(values, _STORAGE_KEYS, "physical_design.storage.mssql")
        fillfactor = _fillfactor(values)
        design = cls(
            compression=_compression(values.get("compression")),
            filegroup=_identifier(values.get("filegroup"), "filegroup"),
            textimage_filegroup=_identifier(values.get("textimage_filegroup"), "textimage_filegroup"),
            clustered_columnstore=_boolean(values.get("clustered_columnstore"), "clustered_columnstore"),
            index_fillfactor=fillfactor,
        )
        design._validate_storage_compatibility()
        return design

    @property
    def has_rowstore_compression(self) -> bool:
        return self.compression in {"ROW", "PAGE"}

    def _validate_storage_compatibility(self) -> None:
        if self.textimage_filegroup and not self.filegroup:
            raise ValueError("physical_design.storage.mssql.textimage_filegroup requires an explicit filegroup")
        if self.clustered_columnstore and self.compression != "NONE":
            raise ValueError(
                "physical_design.storage.mssql.clustered_columnstore is incompatible with ROW/PAGE compression"
            )


@dataclass(frozen=True, slots=True)
class MssqlPhysicalDesignContract:
    """Resolved MSSQL index, storage, placement, and policy capability."""

    storage: MssqlTableDesign = MssqlTableDesign()
    primary_key: tuple[str, ...] = ()
    active: bool = True
    contract_version: str = "dpone.mssql.physical_design.v1"

    @classmethod
    def from_sections(
        cls,
        *,
        indexes: Mapping[str, Any] | None,
        storage: Mapping[str, Any] | None,
        partitioning: Mapping[str, Any] | None,
        active: bool = True,
    ) -> MssqlPhysicalDesignContract:
        index_values = dict(indexes or {})
        partition_values = dict(partitioning or {})
        _reject_unknown(index_values, _INDEX_KEYS, "physical_design.indexes")
        if partition_values:
            raise ValueError("physical_design.partitioning is not supported for the MSSQL physical-design contract")
        contract = cls(
            storage=MssqlTableDesign.from_storage(storage),
            primary_key=_primary_key(index_values.get("primary_key")),
            active=active,
        )
        contract._validate_capability_matrix()
        return contract

    @classmethod
    def from_options(
        cls,
        options: Mapping[str, Any] | None,
        *,
        active: bool | None = None,
    ) -> MssqlPhysicalDesignContract:
        physical = _physical_options(options)
        storage = _mapping(physical.get("storage"), "storage")
        resolved_active = _active_policy(physical) if active is None else active
        return cls.from_sections(
            indexes=_mapping(physical.get("indexes"), "indexes"),
            storage=_mapping(storage.get("mssql"), "storage.mssql"),
            partitioning=_mapping(physical.get("partitioning"), "partitioning"),
            active=resolved_active,
        )

    def validate_columns(self, columns: Mapping[str, str]) -> None:
        """Validate capabilities that depend on the resolved target schema."""

        for dtype in columns.values():
            _validate_target_type(dtype)
        normalized = {str(name).casefold(): str(dtype).casefold().replace(" ", "") for name, dtype in columns.items()}
        missing = [column for column in self.primary_key if column.casefold() not in normalized]
        if missing:
            raise ValueError(
                "physical_design.indexes.primary_key references missing MSSQL columns: " + ", ".join(missing)
            )
        if self.primary_key:
            require_mssql_index_key_width(columns, self.primary_key, kind="clustered")
        if self.storage.textimage_filegroup and not any(
            any(lob_type in dtype for lob_type in _LOB_TYPES) for dtype in normalized.values()
        ):
            raise ValueError("physical_design.storage.mssql.textimage_filegroup requires a LOB-capable target column")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "active": self.active,
            "primary_key": list(self.primary_key),
            "primary_key_kind": "PRIMARY KEY CLUSTERED" if self.primary_key else None,
            "storage": asdict(self.storage),
        }

    def _validate_capability_matrix(self) -> None:
        if self.storage.index_fillfactor is not None and not self.primary_key:
            raise ValueError("physical_design.storage.mssql.index_fillfactor requires indexes.primary_key")
        if self.storage.clustered_columnstore and self.primary_key:
            raise ValueError(
                "physical_design.storage.mssql.clustered_columnstore is incompatible with indexes.primary_key"
            )


def _physical_options(options: Mapping[str, Any] | None) -> Mapping[str, Any]:
    """Resolve only the authored physical-design namespace.

    Load options also contain route-level keys such as ``partitioning`` for
    parallel extraction.  Treating the whole options mapping as physical
    design makes those independent contracts collide.  Callers that already
    hold extracted sections use :meth:`MssqlPhysicalDesignContract.from_sections`
    or :meth:`MssqlTableDesign.from_storage` instead.
    """

    values = options if isinstance(options, Mapping) else {}
    if "physical_design" not in values:
        return {}
    return _mapping(values.get("physical_design"), "physical_design")


def _active_policy(physical: Mapping[str, Any]) -> bool:
    enabled = physical.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("physical_design.enabled must be boolean")
    apply_runtime = physical.get("apply_runtime", True)
    if not isinstance(apply_runtime, bool):
        raise ValueError("physical_design.apply_runtime must be boolean")
    mode = str(physical.get("mode", "auto")).strip().lower()
    if mode not in {"auto", "explicit", "off"}:
        raise ValueError("physical_design.mode must be one of: auto, explicit, off")
    apply_mode = str(physical.get("apply", "online")).strip().lower()
    if apply_mode not in {"online", "safe_window", "plan_only", "manual_approval"}:
        raise ValueError("physical_design.apply must be one of: online, safe_window, plan_only, manual_approval")
    return enabled and apply_runtime and mode != "off" and apply_mode in {"online", "safe_window"}


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"physical_design.{field} must be an object")
    return value


def _reject_unknown(values: Mapping[str, Any], supported: frozenset[str], field: str) -> None:
    unknown = sorted(str(key) for key in values if key not in supported)
    if unknown:
        raise ValueError(f"{field} contains unsupported settings: {', '.join(unknown)}")


def _compression(value: Any) -> MssqlCompression:
    if value is None:
        return "NONE"
    if not isinstance(value, str):
        raise ValueError("physical_design.storage.mssql.compression must be one of: none, row, page")
    normalized = value.strip().upper()
    if normalized not in {"NONE", "ROW", "PAGE"}:
        raise ValueError("physical_design.storage.mssql.compression must be one of: none, row, page")
    return normalized  # type: ignore[return-value]


def _boolean(value: Any, field: str) -> bool:
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ValueError(f"physical_design.storage.mssql.{field} must be boolean")
    return value


def _identifier(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value.strip()):
        raise ValueError(f"physical_design.storage.mssql.{field} must be a safe SQL Server identifier")
    return value.strip()


def _fillfactor(values: Mapping[str, Any]) -> int | None:
    canonical = values.get("index_fillfactor")
    legacy = values.get("fillfactor")
    if canonical is not None and legacy is not None and canonical != legacy:
        raise ValueError("physical_design.storage.mssql fillfactor aliases must not conflict")
    value = canonical if canonical is not None else legacy
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ValueError("physical_design.storage.mssql.index_fillfactor must be an integer between 1 and 100")
    return value


def _primary_key(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw = (value,)
    elif isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        raw = tuple(value)
    else:
        raise ValueError("physical_design.indexes.primary_key must be a string or an array of strings")
    if not raw or len(raw) > _MAX_INDEX_KEY_COLUMNS:
        raise ValueError("physical_design.indexes.primary_key must contain between 1 and 16 columns")
    columns: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not _IDENTIFIER_RE.fullmatch(item.strip()):
            raise ValueError("physical_design.indexes.primary_key columns must be safe SQL Server identifiers")
        columns.append(item.strip())
    if len({column.casefold() for column in columns}) != len(columns):
        raise ValueError("physical_design.indexes.primary_key must not contain duplicate columns")
    return tuple(columns)


def _validate_target_type(value: Any) -> None:
    if not isinstance(value, str) or not _TARGET_TYPE_RE.fullmatch(value.strip()):
        raise ValueError(f"Unsafe or unsupported MSSQL target type in physical design: {value}")


def require_mssql_index_key_width(
    columns: Mapping[str, str],
    key_columns: Sequence[str],
    *,
    kind: MssqlIndexKind,
) -> int:
    """Validate SQL Server key column-count/type/declared-byte limits."""

    if kind not in {"clustered", "nonclustered"}:
        raise MssqlIndexKeyContractError("mssql.index_key.kind_invalid")
    keys = tuple(str(column) for column in key_columns)
    if not keys or len(keys) > MSSQL_MAX_INDEX_KEY_COLUMNS:
        raise MssqlIndexKeyContractError("mssql.index_key.column_count_exceeded")
    by_name = {str(name): str(dtype) for name, dtype in columns.items()}
    missing = [column for column in keys if column not in by_name]
    if missing:
        raise MssqlIndexKeyContractError("mssql.index_key.column_missing")
    total = sum(mssql_index_key_column_bytes(by_name[column]) for column in keys)
    limit = MSSQL_CLUSTERED_INDEX_KEY_MAX_BYTES if kind == "clustered" else MSSQL_NONCLUSTERED_INDEX_KEY_MAX_BYTES
    if total > limit:
        raise MssqlIndexKeyContractError(f"mssql.index_key.width_exceeded:{kind}:{total}:{limit}")
    return total


def mssql_index_key_column_bytes(dtype: str) -> int:
    """Return the maximum declared SQL Server index-key width in bytes."""

    normalized = _normalize_key_type(dtype)
    character = _CHARACTER_KEY_TYPE_RE.fullmatch(normalized)
    if character:
        length = character.group(2)
        if length == "max":
            raise MssqlIndexKeyContractError("mssql.index_key.unbounded_type")
        return int(length) * (2 if character.group(1).lower().startswith("n") else 1)
    decimal = _DECIMAL_KEY_TYPE_RE.fullmatch(normalized)
    if decimal:
        precision = int(decimal.group(1))
        return 5 if precision <= 9 else 9 if precision <= 19 else 13 if precision <= 28 else 17
    base = normalized.split("(", 1)[0]
    if base in _FIXED_KEY_WIDTH_BYTES:
        return _FIXED_KEY_WIDTH_BYTES[base]
    binary = re.fullmatch(r"(?:var)?binary\((\d+)\)", normalized)
    if binary:
        return int(binary.group(1))
    raise MssqlIndexKeyContractError("mssql.index_key.width_unverifiable")


def _normalize_key_type(dtype: str) -> str:
    value = re.sub(r"\s+", "", str(dtype).strip().lower())
    if value.startswith("float("):
        return "float"
    for base in ("datetime2", "datetimeoffset", "time"):
        if value.startswith(f"{base}("):
            return base
    return value


__all__ = [
    "MSSQL_CLUSTERED_INDEX_KEY_MAX_BYTES",
    "MSSQL_MAX_INDEX_KEY_COLUMNS",
    "MSSQL_NONCLUSTERED_INDEX_KEY_MAX_BYTES",
    "MssqlCompression",
    "MssqlIndexKeyContractError",
    "MssqlIndexKind",
    "MssqlPhysicalDesignContract",
    "MssqlTableDesign",
    "mssql_index_key_column_bytes",
    "require_mssql_index_key_width",
]
