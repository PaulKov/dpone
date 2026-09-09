"""Portable type inference service."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.runtime.support.data_type_mapper import CanonicalType, DataTypeMapper
from dpone.type_system.models import ColumnProfile, InferredColumn, TypeInferenceOptions, TypeInferenceReport
from dpone.type_system.profiler import SampleTypeProfiler


class TypeInferenceService:
    """Resolve logical column types from contract, source metadata, and samples."""

    def infer(
        self,
        *,
        rows: Iterable[Mapping[str, Any]] | None = None,
        source_schema: Sequence[tuple[str, str]] | None = None,
        schema_contract: Any | None = None,
        options: TypeInferenceOptions | None = None,
    ) -> TypeInferenceReport:
        opts = options or TypeInferenceOptions()
        profiles = SampleTypeProfiler(opts).profile_rows(rows or []) if opts.enabled else {}
        columns: dict[str, InferredColumn] = {}
        warnings: list[str] = []
        names = _ordered_names(source_schema or [], profiles, schema_contract)
        for name in names:
            contract_column = _contract_column(schema_contract, name)
            source_dtype = _source_dtype(source_schema or [], name)
            profile = profiles.get(name)
            column = self._infer_column(
                name=name,
                contract_column=contract_column,
                source_dtype=source_dtype,
                profile=profile,
                options=opts,
            )
            if column.confidence < opts.confidence_threshold:
                warnings.append(f"{name}: inference confidence {column.confidence:.2f} below threshold")
            columns[name] = column
        return TypeInferenceReport(
            options=opts,
            columns=columns,
            profiles=profiles,
            schema_contract=_contract_dict(schema_contract),
            warnings=tuple(warnings),
        )

    def _infer_column(
        self,
        *,
        name: str,
        contract_column: Any | None,
        source_dtype: str | None,
        profile: ColumnProfile | None,
        options: TypeInferenceOptions,
    ) -> InferredColumn:
        if contract_column is not None:
            return InferredColumn(
                name=name,
                logical_type=str(getattr(contract_column, "logical_type", getattr(contract_column, "type", "string"))),
                nullable=bool(getattr(contract_column, "nullable", True)),
                confidence=1.0,
                decision_source="schema_contract",
                reason="explicit schema contract",
                precision=getattr(contract_column, "precision", None),
                scale=getattr(contract_column, "scale", None),
                timezone=getattr(contract_column, "timezone", None),
            )
        if source_dtype and options.prefer_source_metadata:
            return _from_source_dtype(name, source_dtype)
        if profile and profile.observed_types:
            logical = _logical_from_profile(profile)
            nullable = profile.null_count > 0
            confidence = 1.0 if len(profile.observed_types) == 1 else 0.75
            return InferredColumn(
                name=name,
                logical_type=logical,
                nullable=nullable,
                confidence=confidence,
                decision_source="sample_profile",
                reason="sampled row values",
                precision=_decimal_precision(profile) if logical == "decimal" else None,
                scale=_decimal_scale(profile) if logical == "decimal" else None,
            )
        return InferredColumn(
            name=name,
            logical_type="string",
            nullable=True,
            confidence=0.5,
            decision_source="safe_fallback",
            reason="no source metadata or sample profile",
        )


def _ordered_names(
    source_schema: Sequence[tuple[str, str]], profiles: Mapping[str, ColumnProfile], schema_contract: Any | None
) -> list[str]:
    names: list[str] = []
    for name, _dtype in source_schema:
        if name not in names:
            names.append(str(name))
    for name in profiles:
        if name not in names:
            names.append(str(name))
    for name in _contract_columns(schema_contract):
        if name not in names:
            names.append(str(name))
    return names


def _contract_columns(schema_contract: Any | None) -> Mapping[str, Any]:
    if schema_contract is None:
        return {}
    columns = getattr(schema_contract, "columns", {})
    return columns if isinstance(columns, Mapping) else {}


def _contract_column(schema_contract: Any | None, name: str) -> Any | None:
    columns = _contract_columns(schema_contract)
    return columns.get(name) or columns.get(name.lower())


def _contract_dict(schema_contract: Any | None) -> dict[str, Any]:
    if schema_contract is None:
        return {"enforcement": "strict", "columns": {}}
    if hasattr(schema_contract, "to_dict"):
        return dict(schema_contract.to_dict())
    return dict(schema_contract)


def _source_dtype(source_schema: Sequence[tuple[str, str]], name: str) -> str | None:
    normalized = name.lower()
    for column, dtype in source_schema:
        if str(column).lower() == normalized:
            return str(dtype)
    return None


def _from_source_dtype(name: str, source_dtype: str) -> InferredColumn:
    logical = _logical_from_dtype(source_dtype)
    precision, scale = _precision_scale(source_dtype)
    return InferredColumn(
        name=name,
        logical_type=logical,
        nullable=True,
        confidence=0.99,
        decision_source="source_metadata",
        reason=f"source metadata type {source_dtype}",
        precision=precision,
        scale=scale,
        timezone="tz" in source_dtype.lower() or "offset" in source_dtype.lower(),
    )


def _logical_from_dtype(dtype: str) -> str:
    normalized = str(dtype).strip().lower()
    if normalized.startswith(("decimal", "numeric")):
        return "decimal"
    if normalized in {"bytes", "bytea"} or "binary" in normalized:
        return "binary"
    canonical = DataTypeMapper.normalize(dtype)
    return {
        CanonicalType.INTEGER: "integer",
        CanonicalType.FLOAT: "float",
        CanonicalType.NUMERIC: "decimal",
        CanonicalType.STRING: "string",
        CanonicalType.TIMESTAMP: "timestamp",
        CanonicalType.DATE: "date",
        CanonicalType.TIME: "time",
        CanonicalType.BOOLEAN: "boolean",
        CanonicalType.JSON: "json",
        CanonicalType.ARRAY: "array",
        CanonicalType.BYTES: "binary",
        CanonicalType.UNKNOWN: "string",
    }[canonical]


def _logical_from_profile(profile: ColumnProfile) -> str:
    observed = set(profile.observed_types)
    if not observed:
        return "string"
    if observed == {"integer"}:
        return "integer"
    if observed <= {"integer", "decimal"}:
        return "decimal"
    if observed <= {"integer", "decimal", "float"}:
        return "float"
    if observed == {"boolean"}:
        return "boolean"
    if observed == {"timestamp"}:
        return "timestamp"
    if observed == {"date"}:
        return "date"
    if observed == {"time"}:
        return "time"
    if observed == {"json"}:
        return "json"
    if observed == {"array"}:
        return "array"
    if observed == {"binary"}:
        return "binary"
    return "string"


def _precision_scale(dtype: str) -> tuple[int | None, int | None]:
    match = re.search(r"\((\d+)\s*,\s*(\d+)\)", str(dtype))
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _decimal_precision(profile: ColumnProfile) -> int:
    del profile
    return 38


def _decimal_scale(profile: ColumnProfile) -> int:
    del profile
    return 9
