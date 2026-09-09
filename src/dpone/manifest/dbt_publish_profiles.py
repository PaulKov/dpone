"""Trusted profile registry for dbt-authored publishing intents."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from dpone.contracts.dbt_publish_models import (
    DbtPublishIssue,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
    DbtRouteCertificationProfile,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy

PROFILE_KIND = "dpone.dbt-publish-policy.v1"
PROFILE_V2_KIND = "dpone.dbt-publish-policy.v2"
PROFILE_V3_KIND = "dpone.dbt-publish-policy.v3"
LEGACY_PROFILE_KIND = "dpone.dbt_publish_profiles.v1"
DEFAULT_PROFILE_PATHS = ("dpone/dbt-publish-profiles.yml", ".dpone/dbt-publish-profiles.yml")
_SAFE_DEFAULT_STRATEGIES = ("incremental_merge", "partition_replace")


class DbtPublishProfileRegistry:
    """Loads trusted connection/runtime defaults outside dbt model metadata."""

    def __init__(
        self,
        profiles: Mapping[str, DbtPublishProfile],
        workflows: Mapping[str, DbtWorkflowProfile],
        *,
        source_path: str,
        strategy_policies: Mapping[str, DbtPublishStrategyPolicy] | None = None,
    ) -> None:
        self._profiles = dict(profiles)
        self._workflows = dict(workflows)
        provided = dict(strategy_policies or {})
        self._strategy_policies = {name: provided.get(name, _default_strategy_policy()) for name in self._profiles}
        self.source_path = source_path

    @classmethod
    def load(
        cls, manifest_path: str | Path, explicit_path: str | Path | None = None
    ) -> tuple[DbtPublishProfileRegistry | None, tuple[DbtPublishIssue, ...]]:
        path, discovery_issues = _resolve_path(Path(manifest_path), explicit_path)
        if discovery_issues:
            return None, discovery_issues
        if path is None:
            return None, (
                DbtPublishIssue(
                    code="DPONE_DBT_PROFILES_MISSING",
                    message="No trusted dbt publish profile registry was found",
                    path=str(explicit_path or DEFAULT_PROFILE_PATHS[0]),
                    remediation="Create dpone/dbt-publish-profiles.yml or pass --profiles.",
                ),
            )
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            return None, (_issue("DPONE_DBT_PROFILES_INVALID", str(exc), path),)
        return cls.from_mapping(payload, source_path=path)

    @classmethod
    def from_mapping(
        cls, payload: object, *, source_path: str | Path
    ) -> tuple[DbtPublishProfileRegistry | None, tuple[DbtPublishIssue, ...]]:
        """Validate acquired policy data without resolving paths or environment."""

        path = Path(source_path)
        if not isinstance(payload, Mapping):
            return None, (_issue("DPONE_DBT_PROFILES_INVALID", "Policy root must be a JSON object", path),)
        discriminator = payload.get("schema", payload.get("kind"))
        if not isinstance(discriminator, str) or discriminator not in {
            PROFILE_KIND,
            PROFILE_V2_KIND,
            PROFILE_V3_KIND,
            LEGACY_PROFILE_KIND,
        }:
            return None, (_issue("DPONE_DBT_PROFILES_KIND_INVALID", f"Expected schema: {PROFILE_KIND}", path),)
        validation_issues = _validation_issues(payload, path)
        if validation_issues:
            return None, validation_issues
        try:
            profile_payloads = {str(name): _mapping(raw) for name, raw in _mapping(payload.get("profiles")).items()}
            profiles = {
                name: _profile(
                    name,
                    raw,
                    legacy=discriminator == LEGACY_PROFILE_KIND,
                )
                for name, raw in profile_payloads.items()
            }
            strategy_policies = {
                name: _strategy_policy(
                    raw,
                    legacy=discriminator == LEGACY_PROFILE_KIND,
                )
                for name, raw in profile_payloads.items()
            }
            workflows = {
                str(name): _workflow(str(name), _mapping(raw))
                for name, raw in _mapping(payload.get("workflows")).items()
            }
        except (KeyError, TypeError, ValueError) as exc:
            return None, (_issue("DPONE_DBT_PROFILES_INVALID", str(exc), path),)
        return (
            cls(
                profiles,
                workflows,
                source_path=path.as_posix(),
                strategy_policies=strategy_policies,
            ),
            (),
        )

    def profile(self, name: str) -> DbtPublishProfile | None:
        return self._profiles.get(name)

    def workflow(self, name: str) -> DbtWorkflowProfile | None:
        return self._workflows.get(name)

    def strategy_policy(self, name: str) -> DbtPublishStrategyPolicy | None:
        return self._strategy_policies.get(name)


def _resolve_path(
    manifest_path: Path,
    explicit_path: str | Path | None,
) -> tuple[Path | None, tuple[DbtPublishIssue, ...]]:
    env_path = os.environ.get("DPONE_DBT_PUBLISH_PROFILES")
    if explicit_path or env_path:
        candidate = Path(explicit_path or env_path or "")
        return (candidate if candidate.exists() else None), ()
    project_root = manifest_path.parent.parent if manifest_path.parent.name == "target" else manifest_path.parent
    matches = [project_root / relative for relative in DEFAULT_PROFILE_PATHS if (project_root / relative).exists()]
    if len(matches) > 1:
        relative_paths = ", ".join(DEFAULT_PROFILE_PATHS)
        return None, (
            DbtPublishIssue(
                code="DPONE_DBT_PROFILES_AMBIGUOUS",
                message=f"Multiple default dbt publish profile registries were found: {relative_paths}",
                path="dbt publish profile registry",
                remediation=(
                    "Keep exactly one default registry or pass --profiles with one explicit project-relative path."
                ),
            ),
        )
    return (matches[0] if matches else None), ()


def _profile(
    name: str,
    raw: Mapping[str, Any],
    *,
    legacy: bool,
) -> DbtPublishProfile:
    source = _mapping(raw["source"])
    sink = _mapping(raw["sink"])
    runtime = _normalized_runtime(_mapping(raw["runtime"]), legacy=legacy)
    certification = _mapping(raw.get("certification"))
    return DbtPublishProfile(
        name=name,
        source_type=_text(source["type"]),
        source_connection_ref=_text(source["connection_ref"]),
        sink_type=_text(sink["type"]),
        sink_connection_ref=_text(sink["connection_ref"]),
        target_schema=_text(sink["target_schema"]),
        staging_schema=_optional_text(sink.get("staging_schema")),
        runtime_image=_text(runtime["image"]),
        toolchain_id=_text(runtime.pop("toolchain")),
        certification=(
            DbtRouteCertificationProfile(
                transport=_text(certification["transport"]),
                schema_evolution=_text(certification["schema_evolution"]),
                airflow_runtime_mode=_text(certification["airflow_runtime_mode"]),
            )
            if certification
            else None
        ),
        state=_mapping(raw.get("state")),
        source_options=_mapping(source.get("options")),
        sink_options=_mapping(sink.get("options")),
        runtime=runtime,
        physical_design=_mapping(raw.get("physical_design")),
        execution=_mapping(raw.get("execution")),
        quality=_mapping(raw.get("quality")),
        lineage=_mapping(raw.get("lineage")),
        semantic_refresh=(
            SemanticRefreshProfilePolicy.from_mapping(_mapping(raw["refresh"])) if "refresh" in raw else None
        ),
    )


def _normalized_runtime(
    value: Mapping[str, Any],
    *,
    legacy: bool,
) -> dict[str, Any]:
    runtime = dict(value)
    expected = DBT_SQLSERVER_1_12_CERTIFIED
    if legacy:
        observed = (
            runtime.pop("dbt_core_version", expected.dbt_core_version),
            runtime.pop("dbt_adapter", expected.adapter_name),
            runtime.pop("dbt_adapter_version", expected.adapter_version),
        )
        required = (
            expected.dbt_core_version,
            expected.adapter_name,
            expected.adapter_version,
        )
        if observed != required:
            raise ValueError("legacy dbt runtime versions differ from the certified toolchain")
        runtime.setdefault("toolchain", expected.contract_id)
    if runtime.get("toolchain") != expected.contract_id:
        raise ValueError("dbt runtime toolchain is not production-certified")
    return runtime


def _workflow(name: str, raw: Mapping[str, Any]) -> DbtWorkflowProfile:
    schedule = raw.get("schedule")
    return DbtWorkflowProfile(
        name=name,
        schedule=None if schedule is None else _text(schedule),
        start_date=_text(raw.get("start_date")) or "2026-01-01",
        timezone=_text(raw.get("timezone")) or "UTC",
        owner=_text(raw["owner"]),
        tags=tuple(raw.get("tags") or ()),
        catchup=raw.get("catchup", False),
        max_active_runs=raw.get("max_active_runs", 1),
    )


def _strategy_policy(
    raw: Mapping[str, Any],
    *,
    legacy: bool,
) -> DbtPublishStrategyPolicy:
    value = raw.get("strategy_policy")
    if value is None:
        if legacy:
            return _default_strategy_policy()
        raise ValueError("canonical dbt publish profile requires strategy_policy")
    policy = _mapping(value)
    allowed = tuple(policy["allowed_strategies"])
    full_refresh = _mapping(policy.get("full_refresh"))
    authorized = full_refresh.get("authorized", False)
    max_source_bytes = full_refresh.get("max_source_bytes")
    if "full_refresh" in allowed:
        if not authorized or max_source_bytes is None:
            raise ValueError(
                "strategy_policy.full_refresh requires authorized: true and "
                "positive max_source_bytes when full_refresh is allowlisted"
            )
    elif authorized or max_source_bytes is not None:
        raise ValueError("strategy_policy.full_refresh cannot grant a strategy absent from allowed_strategies")
    partition = _mapping(policy.get("partition_replace"))
    return DbtPublishStrategyPolicy(
        allowed_strategies=allowed,
        full_refresh_authorized=authorized,
        full_refresh_max_source_bytes=max_source_bytes,
        partition_replace_requires_atomic_capability=partition.get("require_atomic_capability", True),
    )


def _default_strategy_policy() -> DbtPublishStrategyPolicy:
    return DbtPublishStrategyPolicy(allowed_strategies=_SAFE_DEFAULT_STRATEGIES)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _validation_issues(
    payload: Mapping[str, Any],
    path: Path,
) -> tuple[DbtPublishIssue, ...]:
    discriminator = payload.get("schema", payload.get("kind"))
    contract_id = (
        str(discriminator) if discriminator in {PROFILE_KIND, PROFILE_V2_KIND, PROFILE_V3_KIND} else PROFILE_KIND
    )
    schema = dbt_schema_contracts()[contract_id]
    errors = sorted(
        Draft202012Validator(schema).iter_errors(payload),
        key=lambda error: tuple(str(item) for item in error.absolute_path),
    )
    return tuple(
        _issue(
            "DPONE_DBT_PROFILES_INVALID",
            "Invalid policy value at "
            + (".".join(str(item) for item in error.absolute_path) or "<root>")
            + f" ({error.validator})",
            path,
        )
        for error in errors
    )


def _issue(code: str, message: str, path: Path) -> DbtPublishIssue:
    return DbtPublishIssue(code=code, message=message, path=path.as_posix())


__all__ = [
    "DbtPublishProfileRegistry",
    "DbtPublishStrategyPolicy",
    "LEGACY_PROFILE_KIND",
    "PROFILE_KIND",
    "PROFILE_V2_KIND",
    "PROFILE_V3_KIND",
]
