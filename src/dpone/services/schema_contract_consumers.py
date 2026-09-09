"""File-IO facade for schema contract consumer discovery and matrix commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class SchemaConsumerDiscoveryFacade:
    """Thin file-IO facade; business rules live in readiness modules."""

    def lineage(self, *, manifest_path: str) -> dict[str, Any]:
        manifest_file = Path(manifest_path)
        manifest = _read_mapping(manifest_file)
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        options = _consumers().SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=manifest_file.parent)
        return (
            _lineage()
            .SchemaConsumerLineageInventoryBuilder()
            .build(
                contract_version=head,
                options=options,
                providers=_lineage_providers(manifest_path=manifest_file, options=options),
            )
        )

    def discover(self, *, manifest_path: str, lineage_path: str | None = None) -> dict[str, Any]:
        manifest = _read_mapping(Path(manifest_path))
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        options = _consumers().SchemaConsumerDiscoveryOptions.from_manifest(
            manifest, base_path=Path(manifest_path).parent
        )
        inventory = (
            _consumers()
            .SchemaConsumerInventoryBuilder()
            .build(
                contract_version=head,
                options=options,
                providers=_providers(manifest=manifest, manifest_path=Path(manifest_path), options=options),
            )
        )
        return _lineage().merge_inventory_with_lineage(inventory, _optional_mapping(lineage_path))

    def matrix(
        self, *, manifest_path: str, against: str, consumers_path: str, lineage_path: str | None = None
    ) -> dict[str, Any]:
        manifest = _read_mapping(Path(manifest_path))
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _resolve_against(against=against, head=head)
        if base is None:
            return _blocked_matrix(head, f"schema_contract_registry.against_not_found:{against}")
        inventory = _read_mapping(Path(consumers_path))
        options = _consumers().SchemaConsumerDiscoveryOptions.from_manifest(
            manifest, base_path=Path(manifest_path).parent
        )
        return (
            _consumers()
            .SchemaConsumerMatrixBuilder()
            .build(
                base=base,
                head=head,
                inventory=inventory,
                lineage=_optional_mapping(lineage_path),
                unknown_consumer=options.unknown_consumer,
                low_confidence_major_change=options.low_confidence_major_change,
            )
        )

    def gate(self, *, manifest_path: str, pack_path: str, matrix_path: str) -> dict[str, Any]:
        manifest = _read_mapping(Path(manifest_path))
        pack = _read_mapping(Path(pack_path))
        matrix = _read_mapping(Path(matrix_path))
        options = _consumers().SchemaConsumerDiscoveryOptions.from_manifest(
            manifest, base_path=Path(manifest_path).parent
        )
        return _consumers().SchemaConsumerMatrixGate().evaluate(pack=pack, matrix=matrix, mode=options.mode)

    def summary(self, *, manifest_path: str) -> dict[str, Any] | None:
        manifest_file = Path(manifest_path)
        manifest = _read_mapping(manifest_file)
        options = _consumers().SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=manifest_file.parent)
        if not options.enabled:
            return None
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _store_from_version(head).latest(contract_id=str(head["contract_id"]))
        if base is None:
            return {
                "enabled": True,
                "status": "warning",
                "contract_id": head["contract_id"],
                "version": head["version"],
                "consumers_count": 0,
                "blocked_consumers": 0,
                "warnings": ["schema_contract_consumers.base_contract_not_found"],
                "blockers": [],
            }
        inventory = (
            _consumers()
            .SchemaConsumerInventoryBuilder()
            .build(
                contract_version=head,
                options=options,
                providers=_providers(manifest=manifest, manifest_path=manifest_file, options=options),
            )
        )
        lineage_payload = self.lineage(manifest_path=manifest_path) if _lineage_enabled(options) else None
        inventory = _lineage().merge_inventory_with_lineage(inventory, lineage_payload)
        matrix = (
            _consumers()
            .SchemaConsumerMatrixBuilder()
            .build(
                base=base,
                head=head,
                inventory=inventory,
                lineage=lineage_payload,
                unknown_consumer=options.unknown_consumer,
                low_confidence_major_change=options.low_confidence_major_change,
            )
        )
        summary = dict(matrix.get("summary", {})) if isinstance(matrix.get("summary"), Mapping) else {}
        return {
            "enabled": True,
            "mode": options.mode,
            "contract_id": matrix.get("contract_id"),
            "version": matrix.get("head_version"),
            "status": matrix.get("status"),
            "consumer_matrix_id": matrix.get("consumer_matrix_id"),
            "consumers_count": summary.get("consumers_count", 0),
            "blocked_consumers": summary.get("blocked_consumers", 0),
            "consumer_lineage_summary": _lineage_summary(lineage_payload),
            "blockers": matrix.get("blockers", []),
            "warnings": matrix.get("warnings", []),
        }


def _providers(
    *,
    manifest: Mapping[str, Any],
    manifest_path: Path,
    options: Any,
) -> tuple[Any, ...]:
    sources = dict(options.sources or {})
    module = _consumers()
    providers: list[Any] = []
    if sources.get("manual", True):
        providers.append(module.ManualConsumerProvider(manifest))
    if sources.get("manifests"):
        providers.append(module.ManifestConsumerProvider(_manifest_paths(sources["manifests"], manifest_path)))
    if sources.get("dbt_manifest"):
        providers.append(module.DbtManifestConsumerProvider(_resolve_path(str(sources["dbt_manifest"]), manifest_path)))
    if sources.get("openlineage"):
        providers.append(module.OpenLineageConsumerProvider(_resolve_path(str(sources["openlineage"]), manifest_path)))
    return tuple(providers)


def _lineage_providers(*, manifest_path: Path, options: Any) -> tuple[Any, ...]:
    sources = dict(options.sources or {})
    providers: list[Any] = []
    module = import_module("dpone.readiness.schema_contract_catalog_providers")
    if sources.get("dbt_compiled_sql"):
        dbt_manifest = sources.get("dbt_manifest")
        if dbt_manifest:
            providers.append(
                module.DbtCompiledSqlConsumerProvider(
                    _resolve_path(str(dbt_manifest), manifest_path),
                    _resolve_path(str(sources["dbt_compiled_sql"]), manifest_path),
                )
            )
    if sources.get("datahub"):
        providers.append(module.DataHubConsumerProvider(_resolve_path(str(sources["datahub"]), manifest_path)))
    if sources.get("generic_catalog"):
        providers.append(
            module.GenericCatalogConsumerProvider(_resolve_path(str(sources["generic_catalog"]), manifest_path))
        )
    return tuple(providers)


def _lineage_enabled(options: Any) -> bool:
    sources = dict(options.sources or {})
    return any(sources.get(key) for key in ("dbt_compiled_sql", "datahub", "generic_catalog"))


def _manifest_paths(raw: object, manifest_path: Path) -> tuple[Path, ...]:
    if raw is True:
        return (manifest_path,)
    if isinstance(raw, str):
        return (_resolve_path(raw, manifest_path),)
    if isinstance(raw, list):
        return tuple(_resolve_path(str(item), manifest_path) for item in raw if str(item))
    return ()


def _resolve_against(*, against: str, head: Mapping[str, Any]) -> dict[str, Any] | None:
    path = Path(against)
    if path.exists():
        return _read_mapping(path)
    contract_id, _, version = against.partition("@")
    if not version:
        return None
    return _store_from_version(head).get(contract_id=contract_id, version=version)


def _store_from_version(version: Mapping[str, Any]) -> Any:
    registry = version.get("registry", {})
    raw = dict(registry) if isinstance(registry, Mapping) else {}
    backend = str(raw.get("store_backend") or "local_json")
    uri = str(raw.get("store_uri") or ".dpone/schema-contracts/registry.json")
    if backend == "sqlite":
        return import_module("dpone.readiness.schema_contract_registry_sqlite").SqliteSchemaContractRegistryStore(uri)
    return import_module("dpone.readiness.schema_contract_registry_store").LocalJsonSchemaContractRegistryStore(uri)


def _blocked_matrix(head: Mapping[str, Any], blocker: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "blocked",
        "contract_id": head.get("contract_id"),
        "head_version": head.get("version"),
        "head_contract_version_id": head.get("contract_version_id"),
        "consumers": [],
        "summary": {"consumers_count": 0, "blocked_consumers": 0},
        "blockers": [blocker],
        "warnings": [],
        "reviewer_actions": ["Publish or provide the base contract version before building the consumer matrix."],
    }


def _resolve_path(raw: str, manifest_path: Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else manifest_path.parent / path


def _optional_mapping(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(Path(path)) if path else None


def _lineage_summary(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    return {
        "consumer_lineage_id": payload.get("consumer_lineage_id"),
        "status": payload.get("status"),
        "summary": payload.get("summary", {}),
        "blockers": payload.get("blockers", []),
        "warnings": payload.get("warnings", []),
    }


def _read_mapping(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    raw = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _consumers() -> Any:
    return import_module("dpone.readiness.schema_contract_consumers")


def _contracts() -> Any:
    return import_module("dpone.readiness.schema_contract_registry")


def _lineage() -> Any:
    return import_module("dpone.readiness.schema_contract_consumer_lineage")


__all__ = ["SchemaConsumerDiscoveryFacade"]
