"""Local artifact providers for schema contract consumer lineage."""

from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_contract_sql_lineage import SqlColumnLineageExtractor


@dataclass(frozen=True, slots=True)
class DbtCompiledSqlConsumerProvider:
    manifest_path: str | Path
    compiled_sql_path: str | Path
    source: str = "dbt_compiled_sql"

    def lineage(
        self, *, contract_id: str, target_table: str, target_columns: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        manifest_file = Path(self.manifest_path)
        compiled_root = Path(self.compiled_sql_path)
        if not manifest_file.exists() or not compiled_root.exists():
            return ()
        payload = _read_mapping(manifest_file)
        sources = _dbt_sources(payload, target_table, contract_id)
        extractor = SqlColumnLineageExtractor()
        evidence: list[dict[str, Any]] = []
        for node in _mapping(payload.get("nodes")).values():
            if not isinstance(node, Mapping) or not _depends_on_source(node, sources):
                continue
            sql, sql_path = _node_sql(node, compiled_root)
            if not sql:
                evidence.append(_table_only(node, target_table, "inferred", sql_path))
                continue
            try:
                parsed = extractor.extract(
                    sql=sql,
                    consumer_id=str(node.get("unique_id") or node.get("name") or "dbt.consumer"),
                    target_table=target_table,
                    target_columns=target_columns,
                    path=str(sql_path) if sql_path else None,
                )
            except Exception:
                evidence.append(_table_only(node, target_table, "table_only", sql_path))
                continue
            evidence.extend(_with_node_metadata(item, node) for item in parsed)
        return tuple(evidence)

    def blockers(self) -> tuple[str, ...]:
        missing = []
        if not Path(self.manifest_path).exists():
            missing.append("manifest")
        if not Path(self.compiled_sql_path).exists():
            missing.append("compiled_sql")
        return tuple(f"schema_contract_lineage.source_missing:{self.source}:{item}" for item in missing)

    def warnings(self) -> tuple[str, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class DataHubConsumerProvider:
    path: str | Path
    source: str = "datahub"

    def lineage(
        self, *, contract_id: str, target_table: str, target_columns: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        del target_columns
        raw_path = Path(self.path)
        if not raw_path.exists():
            return ()
        payload = _read_mapping(raw_path)
        evidence: list[dict[str, Any]] = []
        for item in _lineage_entries(payload):
            if str(item.get("dataset") or "") not in {target_table, contract_id}:
                continue
            consumer = _mapping(item.get("consumer"))
            columns = [str(column) for column in item.get("columns", []) if str(column)]
            confidence = str(item.get("confidence") or "explicit")
            evidence.extend(
                _catalog_evidence(item, consumer, target_table, column, confidence) for column in (columns or [None])
            )
        return tuple(evidence)

    def blockers(self) -> tuple[str, ...]:
        return (f"schema_contract_lineage.source_missing:{self.source}",) if not Path(self.path).exists() else ()

    def warnings(self) -> tuple[str, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class GenericCatalogConsumerProvider:
    path: str | Path
    source: str = "generic_catalog"

    def lineage(
        self, *, contract_id: str, target_table: str, target_columns: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        del target_columns
        raw_path = Path(self.path)
        if not raw_path.exists():
            return ()
        payload = _read_mapping(raw_path)
        evidence: list[dict[str, Any]] = []
        for item in payload.get("consumers", []) if isinstance(payload.get("consumers"), list) else []:
            if not isinstance(item, Mapping) or str(item.get("dataset") or "") not in {target_table, contract_id}:
                continue
            columns = _columns(item)
            confidence = str(item.get("confidence") or ("explicit" if columns else "table_only"))
            evidence.extend(
                _catalog_evidence(item, item, target_table, column, confidence) for column in (columns or [None])
            )
        return tuple(evidence)

    def blockers(self) -> tuple[str, ...]:
        return (f"schema_contract_lineage.source_missing:{self.source}",) if not Path(self.path).exists() else ()

    def warnings(self) -> tuple[str, ...]:
        return ()


def _dbt_sources(payload: Mapping[str, Any], target_table: str, contract_id: str) -> set[str]:
    result: set[str] = set()
    for key, source in _mapping(payload.get("sources")).items():
        if not isinstance(source, Mapping):
            continue
        table = ".".join(str(source.get(item)) for item in ("schema", "name") if source.get(item))
        if table in {target_table, contract_id}:
            result.add(str(source.get("unique_id") or key))
    return result


def _depends_on_source(node: Mapping[str, Any], sources: set[str]) -> bool:
    depends = _mapping(node.get("depends_on")).get("nodes", [])
    return any(str(item) in sources for item in depends if str(item))


def _node_sql(node: Mapping[str, Any], compiled_root: Path) -> tuple[str | None, Path | None]:
    if node.get("compiled_code"):
        return str(node["compiled_code"]), None
    raw_path = node.get("compiled_path") or node.get("original_file_path")
    if not raw_path:
        return None, None
    path = Path(str(raw_path))
    candidate = path if path.is_absolute() else compiled_root / path
    return (candidate.read_text(encoding="utf-8"), candidate) if candidate.exists() else (None, candidate)


def _with_node_metadata(item: Mapping[str, Any], node: Mapping[str, Any]) -> dict[str, Any]:
    meta = _mapping(node.get("meta"))
    return {
        **dict(item),
        "consumer_type": str(node.get("resource_type") or "dbt_model"),
        "owner": meta.get("owner"),
    }


def _table_only(node: Mapping[str, Any], target_table: str, confidence: str, path: Path | None) -> dict[str, Any]:
    return {
        "consumer_id": str(node.get("unique_id") or node.get("name") or "dbt.consumer"),
        "consumer_type": str(node.get("resource_type") or "dbt_model"),
        "owner": _mapping(node.get("meta")).get("owner"),
        "dataset": target_table,
        "column": None,
        "confidence": confidence,
        "path": str(path) if path else None,
    }


def _lineage_entries(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = payload.get("lineage", payload.get("relationships", []))
    return tuple(item for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _catalog_evidence(
    item: Mapping[str, Any],
    consumer: Mapping[str, Any],
    target_table: str,
    column: str | None,
    confidence: str,
) -> dict[str, Any]:
    return {
        "consumer_id": str(consumer.get("id") or item.get("id") or "catalog.consumer"),
        "consumer_type": str(consumer.get("type") or item.get("type") or "consumer"),
        "owner": consumer.get("owner") or item.get("owner"),
        "dataset": target_table,
        "column": column,
        "confidence": confidence,
    }


def _columns(item: Mapping[str, Any]) -> list[str]:
    reads = _mapping(item.get("reads"))
    raw = reads.get("columns", item.get("columns", item.get("column", [])))
    if isinstance(raw, str):
        return [column.strip() for column in raw.split(",") if column.strip()]
    return [str(column) for column in raw if str(column)] if isinstance(raw, list) else []


def _read_mapping(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".csv":
        return {"consumers": list(csv.DictReader(text.splitlines()))}
    raw = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = ["DataHubConsumerProvider", "DbtCompiledSqlConsumerProvider", "GenericCatalogConsumerProvider"]
