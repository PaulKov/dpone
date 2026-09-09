"""Provider-neutral schema contract consumer discovery and compatibility matrix."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

import yaml

from dpone.readiness.migration_control import stable_fingerprint

CONSUMER_INVENTORY_SCHEMA = "dpone.schema_contract_consumer_inventory.v1"
CONSUMER_MATRIX_SCHEMA = "dpone.schema_contract_consumer_matrix.v1"
CONSUMER_GATE_SCHEMA = "dpone.schema_contract_consumer_gate.v1"
_MATRIX_EXPORTS = ("SchemaConsumerMatrixBuilder", "SchemaConsumerMatrixGate")


class SchemaConsumerProvider(Protocol):
    source: str

    def consumers(self, *, contract_id: str, target_table: str) -> tuple[dict[str, Any], ...]: ...

    def blockers(self) -> tuple[str, ...]: ...

    def warnings(self) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class SchemaConsumerDiscoveryOptions:
    enabled: bool
    mode: str = "gate"
    unknown_consumer: str = "warn"
    low_confidence_major_change: str = "block"
    required_sources: tuple[str, ...] = ("manual",)
    sources: Mapping[str, Any] | None = None

    @classmethod
    def from_manifest(
        cls, manifest: Mapping[str, Any], *, base_path: Path | None = None
    ) -> SchemaConsumerDiscoveryOptions:
        del base_path
        raw = _discovery(manifest)
        mode = str(raw.get("mode") or "gate")
        return cls(
            enabled=bool(raw.get("enabled", False)),
            mode=mode,
            unknown_consumer=str(raw.get("unknown_consumer") or "warn"),
            low_confidence_major_change=str(
                raw.get("low_confidence_major_change") or ("warn" if mode == "observe" else "block")
            ),
            required_sources=tuple(str(item) for item in raw.get("required_sources", ["manual"]) if str(item)),
            sources=_mapping(raw.get("sources")),
        )


@dataclass(frozen=True, slots=True)
class ManualConsumerProvider:
    manifest: Mapping[str, Any]
    source: str = "manual"

    def consumers(self, *, contract_id: str, target_table: str) -> tuple[dict[str, Any], ...]:
        del contract_id, target_table
        return tuple(_consumer_ref(item, source=self.source) for item in _manual_consumers(self.manifest))

    def blockers(self) -> tuple[str, ...]:
        return ()

    def warnings(self) -> tuple[str, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class ManifestConsumerProvider:
    paths: tuple[Path, ...]
    source: str = "manifests"

    def consumers(self, *, contract_id: str, target_table: str) -> tuple[dict[str, Any], ...]:
        consumers: list[dict[str, Any]] = []
        for path in self.paths:
            if not path.exists():
                continue
            manifest = _read_mapping(path)
            raw = _schema_contract(manifest)
            if str(raw.get("id") or _target_table(manifest)) != contract_id and _target_table(manifest) != target_table:
                continue
            for item in _manual_consumers(manifest):
                consumers.append(_consumer_ref(item, source=self.source))
        return tuple(consumers)

    def blockers(self) -> tuple[str, ...]:
        missing = [str(path) for path in self.paths if not path.exists()]
        return tuple(f"schema_contract_consumers.source_missing:manifests:{path}" for path in missing)

    def warnings(self) -> tuple[str, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class DbtManifestConsumerProvider:
    path: str | Path
    source: str = "dbt_manifest"

    def consumers(self, *, contract_id: str, target_table: str) -> tuple[dict[str, Any], ...]:
        raw_path = Path(self.path)
        if not raw_path.exists():
            return ()
        payload = _read_mapping(raw_path)
        sources = _dbt_sources(payload, target_table)
        consumers: list[dict[str, Any]] = []
        for node in _mapping(payload.get("nodes")).values():
            if not isinstance(node, Mapping):
                continue
            depends = _mapping(node.get("depends_on")).get("nodes", [])
            if not any(str(item) in sources for item in depends if str(item)):
                continue
            reads = _dbt_reads(node, target_table)
            consumers.append(
                _consumer_ref(
                    {
                        "id": str(node.get("unique_id") or node.get("name") or "dbt.consumer"),
                        "type": str(node.get("resource_type") or "dbt_model"),
                        "owner": _mapping(node.get("meta")).get("owner"),
                        "reads": {"columns": reads},
                    },
                    source=self.source,
                )
            )
        return tuple(consumers)

    def blockers(self) -> tuple[str, ...]:
        return (f"schema_contract_consumers.source_missing:{self.source}",) if not Path(self.path).exists() else ()

    def warnings(self) -> tuple[str, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class OpenLineageConsumerProvider:
    path: str | Path
    source: str = "openlineage"

    def consumers(self, *, contract_id: str, target_table: str) -> tuple[dict[str, Any], ...]:
        raw_path = Path(self.path)
        if not raw_path.exists():
            return ()
        payload = _read_mapping(raw_path)
        events = payload.get("events", [payload])
        consumers: list[dict[str, Any]] = []
        for event in events if isinstance(events, list) else []:
            if not isinstance(event, Mapping):
                continue
            inputs = [item for item in event.get("inputs", []) if isinstance(item, Mapping)]
            if not any(_dataset_name(item) in {target_table, contract_id} for item in inputs):
                continue
            job = _mapping(event.get("job"))
            columns = sorted({column for item in inputs for column in _dataset_columns(item)})
            consumers.append(
                _consumer_ref(
                    {
                        "id": "openlineage." + str(job.get("name") or "consumer"),
                        "type": "openlineage_job",
                        "owner": _mapping(job.get("facets")).get("owner"),
                        "reads": {"columns": columns},
                    },
                    source=self.source,
                )
            )
        return tuple(consumers)

    def blockers(self) -> tuple[str, ...]:
        return (f"schema_contract_consumers.source_missing:{self.source}",) if not Path(self.path).exists() else ()

    def warnings(self) -> tuple[str, ...]:
        return ()


class SchemaConsumerInventoryBuilder:
    def build(
        self,
        *,
        contract_version: Mapping[str, Any],
        options: SchemaConsumerDiscoveryOptions,
        providers: Sequence[SchemaConsumerProvider],
    ) -> dict[str, Any]:
        consumers: dict[tuple[str, str, str], dict[str, Any]] = {}
        blockers: list[str] = []
        warnings: list[str] = []
        for provider in providers:
            provider_blockers = list(provider.blockers())
            if provider.source in options.required_sources:
                blockers.extend(provider_blockers)
            else:
                warnings.extend(provider_blockers)
            warnings.extend(provider.warnings())
            for consumer in provider.consumers(
                contract_id=str(contract_version.get("contract_id")),
                target_table=str(_mapping(contract_version.get("target")).get("table") or ""),
            ):
                key = (str(consumer.get("id")), str(consumer.get("source")), ",".join(consumer["reads"]["columns"]))
                consumers.setdefault(key, consumer)
        if options.mode == "observe":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_INVENTORY_SCHEMA,
            "status": "blocked" if blockers else "discovered",
            "contract_id": contract_version.get("contract_id"),
            "contract_version_id": contract_version.get("contract_version_id"),
            "version": contract_version.get("version"),
            "consumers": sorted(consumers.values(), key=lambda item: str(item.get("id"))),
            "summary": {"consumers_count": len(consumers)},
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["consumer_inventory_id"] = stable_fingerprint(payload)
        return payload


def _consumer_ref(raw: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    reads = _mapping(raw.get("reads"))
    return {
        "id": str(raw.get("id") or "unknown"),
        "type": str(raw.get("type") or "consumer"),
        "owner": raw.get("owner"),
        "source": source,
        "version_constraint": raw.get("version_constraint"),
        "reads": {"columns": sorted(str(item) for item in reads.get("columns", []) if str(item))},
    }


def _manual_consumers(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _mapping(_schema_contract(manifest).get("consumers")).get("manual", [])
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _dbt_sources(payload: Mapping[str, Any], target_table: str) -> set[str]:
    result: set[str] = set()
    for key, source in _mapping(payload.get("sources")).items():
        if not isinstance(source, Mapping):
            continue
        table = ".".join(str(source.get(item)) for item in ("schema", "name") if source.get(item))
        if table == target_table:
            result.add(str(source.get("unique_id") or key))
    return result


def _dbt_reads(node: Mapping[str, Any], target_table: str) -> list[str]:
    meta = _mapping(node.get("meta"))
    dpone = _mapping(meta.get("dpone"))
    reads = _mapping(dpone.get("reads")).get(target_table, [])
    return sorted(str(item) for item in reads if str(item)) if isinstance(reads, list) else []


def _dataset_name(dataset: Mapping[str, Any]) -> str:
    return str(dataset.get("name") or "")


def _dataset_columns(dataset: Mapping[str, Any]) -> tuple[str, ...]:
    schema = _mapping(_mapping(dataset.get("facets")).get("schema"))
    return tuple(str(item.get("name")) for item in schema.get("fields", []) if isinstance(item, Mapping))


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _discovery(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_schema_contract(manifest).get("consumers")).get("discovery"))


def _target_table(manifest: Mapping[str, Any]) -> str:
    sink = _mapping(manifest.get("sink"))
    table = sink.get("table")
    if isinstance(table, Mapping):
        return ".".join(str(table.get(item)) for item in ("schema", "name") if table.get(item))
    return str(table or sink.get("target_table") or "")


def __getattr__(name: str) -> Any:
    if name in _MATRIX_EXPORTS:
        module = import_module("dpone.readiness.schema_contract_consumer_matrix")
        return getattr(module, name)
    raise AttributeError(name)


def _read_mapping(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    raw = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "CONSUMER_GATE_SCHEMA",
    "CONSUMER_INVENTORY_SCHEMA",
    "CONSUMER_MATRIX_SCHEMA",
    "DbtManifestConsumerProvider",
    "ManualConsumerProvider",
    "ManifestConsumerProvider",
    "OpenLineageConsumerProvider",
    "SchemaConsumerDiscoveryOptions",
    "SchemaConsumerInventoryBuilder",
]
