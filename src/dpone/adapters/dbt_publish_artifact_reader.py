"""Read supported dbt manifest artifacts without importing dbt."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator
from dpone.contracts.dbt_publish_models import (
    DbtColumnArtifact,
    DbtManifestArtifact,
    DbtModelArtifact,
    DbtPublishIssue,
)
from dpone.contracts.dbt_semantic_refresh_source_proof import resolve_manifest_macro_source_closure
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

SUPPORTED_MANIFEST_VERSIONS = frozenset({10, 11, 12})
_VERSION_PATTERN = re.compile(r"/v(?P<version>\d+)\.json$")


class DbtArtifactReader:
    """Adapter from dbt's compiled manifest JSON to dpone's immutable model view."""

    def __init__(self, *, validator: DbtManifestValidatorPort | None = None) -> None:
        self._validator = validator or OfficialDbtManifestValidator()

    def read(self, path: str | Path) -> tuple[DbtManifestArtifact | None, tuple[DbtPublishIssue, ...]]:
        manifest_path = Path(path)
        try:
            manifest_bytes = manifest_path.read_bytes()
        except FileNotFoundError:
            return None, (self._issue("DPONE_DBT_MANIFEST_MISSING", "dbt manifest does not exist", manifest_path),)
        return self.read_payload(manifest_bytes, path=manifest_path)

    def read_payload(
        self, manifest_bytes: bytes, *, path: Path
    ) -> tuple[DbtManifestArtifact | None, tuple[DbtPublishIssue, ...]]:
        """Decode already acquired bytes; callers own confinement and byte limits."""

        manifest_path = path
        try:
            payload = strict_json_object(manifest_bytes)
        except (StrictJsonError, RecursionError):
            return None, (self._issue("DPONE_DBT_MANIFEST_INVALID_JSON", "invalid or ambiguous JSON", manifest_path),)
        version = _schema_version(payload)
        if version not in SUPPORTED_MANIFEST_VERSIONS:
            message = f"dbt manifest schema v{version or 'unknown'} is unsupported; expected v10-v12"
            return None, (self._issue("DPONE_DBT_MANIFEST_VERSION_UNSUPPORTED", message, manifest_path),)
        schema_issues = tuple(
            DbtPublishIssue(
                code=(
                    "DPONE_DBT_MANIFEST_ADAPTER_SCHEMA_EXTENSION"
                    if violation.severity == "warning"
                    else "DPONE_DBT_MANIFEST_INVALID"
                ),
                message=(f"dbt manifest v{version} violates official schema rule {violation.rule} at {violation.path}"),
                path=manifest_path.as_posix(),
                severity=violation.severity,
                remediation=("Use the certified dbt Core and adapter lock, run `dbt parse`, then retry."),
            )
            for violation in self._validator.validate(payload, version=version)
        )
        metadata = _mapping(payload.get("metadata"))
        adapter_type = _optional_text(metadata.get("adapter_type"))
        adapter_issues: tuple[DbtPublishIssue, ...] = ()
        if adapter_type != "sqlserver":
            adapter_issues = (
                DbtPublishIssue(
                    code="DPONE_DBT_SQLSERVER_RUNTIME_POLICY_INVALID",
                    message=("dbt manifest metadata.adapter_type must equal 'sqlserver' for the native v1 preview"),
                    path=manifest_path.as_posix(),
                    remediation=("Run `dbt parse` with the pinned dbt-sqlserver toolchain, then retry."),
                ),
            )
        models, model_issues = self._models(payload, manifest_path=manifest_path)
        artifact = DbtManifestArtifact(
            path=manifest_path.as_posix(),
            sha256="sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
            schema_version=version,
            dbt_version=_optional_text(metadata.get("dbt_version")),
            invocation_id=_optional_text(metadata.get("invocation_id")),
            project_name=_optional_text(metadata.get("project_name")),
            models=models,
        )
        return artifact, (*schema_issues, *adapter_issues, *model_issues)

    def _models(
        self,
        payload: Mapping[str, Any],
        *,
        manifest_path: Path,
    ) -> tuple[tuple[DbtModelArtifact, ...], tuple[DbtPublishIssue, ...]]:
        nodes = _mapping(payload.get("nodes"))
        macros = _mapping(payload.get("macros"))
        tests_by_model = _tests_by_model(nodes)
        models = []
        issues = []
        for unique_id, raw_node in sorted(nodes.items()):
            node = _mapping(raw_node)
            if node.get("resource_type") != "model":
                continue
            config = _mapping(node.get("config"))
            contract = _mapping(config.get("contract")) or _mapping(node.get("contract"))
            contract_enforced = contract.get("enforced", False)
            if not isinstance(contract_enforced, bool):
                issues.append(
                    self._issue(
                        "DPONE_DBT_MANIFEST_INVALID",
                        f"{unique_id}.config.contract.enforced must be a JSON boolean",
                        manifest_path,
                    )
                )
                contract_enforced = False
            column_contracts = _column_contracts(_mapping(node.get("columns")))
            columns = tuple(item.name for item in column_contracts)
            direct_macro_sources, semantic_macro_sources, semantic_macro_closure_complete = _model_macro_sources(
                node=node,
                macros=macros,
            )
            models.append(
                DbtModelArtifact(
                    unique_id=str(unique_id),
                    name=str(node.get("name") or str(unique_id).rsplit(".", 1)[-1]),
                    original_file_path=str(node.get("original_file_path") or ""),
                    database=_optional_text(node.get("database")),
                    schema=str(node.get("schema") or ""),
                    alias=str(node.get("alias") or node.get("name") or ""),
                    materialized=str(config.get("materialized") or "view"),
                    contract_enforced=contract_enforced,
                    columns=columns,
                    column_contracts=column_contracts,
                    group=_optional_text(node.get("group") or config.get("group")),
                    tags=_string_tuple(node.get("tags") or config.get("tags")),
                    meta=_mapping(config.get("meta")),
                    unique_key=_unique_key_tuple(config.get("unique_key")),
                    depends_on=_string_tuple(_mapping(node.get("depends_on")).get("nodes")),
                    test_ids=tests_by_model.get(str(unique_id), ()),
                    fqn=_string_tuple(node.get("fqn")),
                    incremental_strategy=_optional_text(config.get("incremental_strategy")),
                    raw_code=str(node.get("raw_code") or ""),
                    compiled_code_by_target=_string_mapping(node.get("compiled_code_by_target")),
                    macro_sources=direct_macro_sources,
                    semantic_refresh_macro_sources=semantic_macro_sources,
                    semantic_refresh_macro_closure_complete=semantic_macro_closure_complete,
                )
            )
        return tuple(models), tuple(issues)

    @staticmethod
    def _issue(code: str, message: str, path: Path) -> DbtPublishIssue:
        return DbtPublishIssue(
            code=code,
            message=message,
            path=path.as_posix(),
            remediation="Run `dbt parse` with a supported dbt version, then retry.",
        )


def _schema_version(payload: Mapping[str, Any]) -> int | None:
    raw = str(_mapping(payload.get("metadata")).get("dbt_schema_version") or "")
    match = _VERSION_PATTERN.search(raw)
    return int(match.group("version")) if match else None


def _tests_by_model(nodes: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    result: dict[str, list[str]] = {}
    for unique_id, raw_node in nodes.items():
        node = _mapping(raw_node)
        if node.get("resource_type") != "test":
            continue
        for dependency in _string_tuple(_mapping(node.get("depends_on")).get("nodes")):
            if dependency.startswith("model."):
                result.setdefault(dependency, []).append(str(unique_id))
    return {key: tuple(sorted(value)) for key, value in result.items()}


def _column_contracts(columns: Mapping[str, Any]) -> tuple[DbtColumnArtifact, ...]:
    result = []
    for name, raw in columns.items():
        config = _mapping(raw)
        constraints = tuple(
            str(item.get("type"))
            for item in config.get("constraints", ())
            if isinstance(item, Mapping) and item.get("type")
        )
        result.append(
            DbtColumnArtifact(
                name=str(name),
                data_type=str(config.get("data_type") or "").strip(),
                nullable="not_null" not in constraints,
                constraints=constraints,
            )
        )
    return tuple(result)


def _model_macro_sources(
    *,
    node: Mapping[str, Any],
    macros: Mapping[str, Any],
) -> tuple[dict[str, str], dict[str, str], bool]:
    roots = _string_tuple(_mapping(node.get("depends_on")).get("macros"))
    direct = {
        macro_id: source
        for macro_id in roots
        if isinstance(source := _mapping(macros.get(macro_id)).get("macro_sql"), str)
    }
    try:
        semantic = resolve_manifest_macro_source_closure(root_macro_ids=roots, macros=macros)
    except ValueError:
        return direct, {}, False
    return direct, semantic, True


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items() if isinstance(item, str)}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, list | tuple):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _unique_key_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, list | tuple):
        return tuple(item for item in value if isinstance(item, str))
    return ()


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


class DbtManifestSchemaViolationPort(Protocol):
    path: str
    rule: str
    severity: str


class DbtManifestValidatorPort(Protocol):
    def validate(
        self,
        payload: Mapping[str, Any],
        *,
        version: int,
    ) -> tuple[DbtManifestSchemaViolationPort, ...]: ...


__all__ = ["DbtArtifactReader", "SUPPORTED_MANIFEST_VERSIONS"]
