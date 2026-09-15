"""Offline, explicit-policy authoring for the installed native dbt starter.

Init creates source files only. It does not run dbt, acquire credentials, provision
databases, qualify execution, or publish data. Existing authoring serialization
and confined scaffold receipts own write/recovery semantics.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Protocol

from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.native_delivery_json import decode_native_delivery_json
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file_snapshot
from dpone.manifest.project_root import (
    ProjectRootError,
    ensure_project_root,
    inspect_project_root,
    project_root_candidate,
    verify_project_root,
)
from dpone.ports.dbt_publish_compiler import DbtPublishProfileRegistryPort
from dpone.ports.project_authoring_lock import AuthoringLockFactory, ProjectAuthoringLockError
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldApplyPlan, ScaffoldFile
from dpone.readiness.airflow_scaffold_result import scaffold_result
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix
from dpone.readiness.workload_init_scaffold_rollback import scaffold_rollback_journal

_STAGE = "init_dbt"
_POLICY_OUTPUT = Path("dpone/dbt-publish-profiles.yml")
_MARKER = re.compile(r"@@DPONE_([A-Z_]+)@@")
# JSON permits literal C1 controls and Unicode line separators, but YAML treats
# some as invalid characters or folded line breaks. Keep those escaped while
# retaining supplementary Unicode as actual scalars, not surrogate escapes.
_YAML_LITERAL_ESCAPES = {
    codepoint: f"\\u{codepoint:04x}" for codepoint in (*range(0x7F, 0xA0), 0x2028, 0x2029, 0xFFFE, 0xFFFF)
}
_TEMPLATE_MARKERS = {
    "dbt_project.yml": {"PROJECT_NAME", "DBT_PROFILE"},
    "profiles/profiles.yml": {"DBT_PROFILE", "DBT_TARGET", "INVOCATION_DATABASE", "INVOCATION_SCHEMA"},
    "models/orders.sql": {"PROFILE", "WORKFLOW"},
    "models/schema.yml": {"SOURCE_DATABASE", "SOURCE_SCHEMA", "SOURCE_NAME"},
    "README.md": set(),
    ".gitignore": set(),
}


class DbtStarterResourcesPort(Protocol):
    """Acquire the fixed installed inventory with intended output paths."""

    def files(self) -> tuple[ScaffoldFile, ...]: ...


class DbtStarterProfileParser(Protocol):
    """Validate the captured mapping without a second read or discovery."""

    def __call__(
        self, payload: object, *, source_path: str | Path
    ) -> tuple[DbtPublishProfileRegistryPort | None, tuple[DbtPublishIssue, ...]]: ...


class DbtStarterService:
    """Plan or create a complete starter without overwriting existing files."""

    def __init__(
        self,
        *,
        resources: DbtStarterResourcesPort,
        parse_profiles: DbtStarterProfileParser,
        authoring_lock: AuthoringLockFactory,
    ) -> None:
        self._resources = resources
        self._parse_profiles = parse_profiles
        self._authoring_lock = authoring_lock

    def init(
        self,
        path: str | Path,
        *,
        profiles: str | Path,
        profile: str,
        workflow: str,
        dry_run: bool = False,
    ) -> SelfServiceResult:
        """Return a content-suppressed plan/apply receipt for 23 project files.

        The parent directory must exist. An absent-root dry-run never creates
        the project, but the injected lock may use bounded external lock state.
        Identical files are no-ops and conflicts reject the entire write plan.
        """
        if not profile or not workflow or not str(path) or not str(profiles) or type(dry_run) is not bool:
            return _failure(
                "DPONE_PROJECT_CONFIG_INVALID", "Explicit path, policy, profile and workflow are required.", 2
            )
        try:
            policy_path = Path(profiles).absolute()
            policy_root = inspect_project_root(policy_path.parent)
            assert policy_root is not None
            snapshot = read_confined_file_snapshot(
                policy_root.path,
                policy_path.name,
                max_bytes=BoundedYamlLimits().max_bytes,
                root_identity=policy_root,
            )
            payload = load_bounded_yaml(snapshot.content)
        except (OSError, BoundedYamlError):
            return _failure("DPONE_DBT_PROFILES_INVALID", "The explicit policy could not be read safely.", 1)
        registry, issues = self._parse_profiles(payload, source_path=policy_path)
        if issues:
            return SelfServiceResult(
                passed=False,
                errors=tuple(
                    dpone_error(issue.code, "The explicit policy failed validation.", stage=_STAGE, path=issue.path)
                    for issue in issues
                ),
                exit_code=1,
            )
        if registry is None:
            return _failure("DPONE_DBT_PROFILES_INVALID", "No validated policy was returned.", 1)
        values = _selection(registry, profile=profile, workflow=workflow)
        if isinstance(values, SelfServiceResult):
            return values
        try:
            files = _render(self._resources.files(), values)
        except (OSError, UnicodeError, ValueError):
            return _failure(
                "DPONE_DBT_STARTER_RESOURCES_INVALID", "Installed starter resources are incomplete or invalid.", 2
            )
        files = (*files, ScaffoldFile(_POLICY_OUTPUT, snapshot.content.decode("utf-8")))
        details = {"policy_sha256": snapshot.sha256, "dry_run": dry_run, "file_count": len(files)}
        try:
            root = project_root_candidate(path)
            with self._authoring_lock(root):
                identity = inspect_project_root(root, allow_missing=True)
                if identity is None and dry_run:
                    plan = ScaffoldApplyPlan(
                        changes=tuple(Change("create", file.path.as_posix()) for file in files),
                        rollback_journal=scaffold_rollback_journal(()),
                    )
                    if inspect_project_root(root, allow_missing=True) is not None:
                        raise ProjectRootError("Project appeared during dry-run")
                else:
                    identity = identity or ensure_project_root(root)
                    applier = ScaffoldApplier(root, root_identity=identity)

                    def unchanged() -> bool:
                        verify_project_root(identity)
                        return True

                    if dry_run:
                        plan = applier.plan(files)
                        verify_project_root(identity)
                    else:
                        try:
                            plan = applier.apply(files, precondition=unchanged, postcondition=unchanged)
                        except Exception as exc:  # noqa: BLE001 - retain the confined applier's rollback receipt
                            receipt = getattr(exc, "scaffold_receipt", None)
                            if not isinstance(receipt, ScaffoldApplyPlan):
                                raise
                            plan = receipt
                return _public_result(plan, details)
        except ProjectAuthoringLockError:
            return _failure(
                "DPONE_PROJECT_AUTHORING_LOCK_FAILED", "The project authoring lock could not be held safely.", 4
            )
        except ProjectRootError:
            return _failure("DPONE_PROJECT_CONFIG_INVALID", "The project root could not be used safely.", 2)


def _selection(
    registry: DbtPublishProfileRegistryPort, *, profile: str, workflow: str
) -> dict[str, str] | SelfServiceResult:
    selected = registry.profile(profile)
    if selected is None:
        return _failure("DPONE_DBT_PROFILE_UNKNOWN", "The selected profile does not exist.", 1)
    if registry.workflow(workflow) is None:
        return _failure("DPONE_DBT_WORKFLOW_UNKNOWN", "The selected workflow does not exist.", 1)
    template = selected.authoring_template
    policy = registry.strategy_policy(profile)
    if (
        selected.native_policy_schema != "dpone.dbt-publish-policy.v4"
        or selected.native_profile_payload is None
        or template is None
        or policy is None
        or not policy.allows("full_refresh")
        or policy.full_refresh_serialized_payload_max_bytes is None
    ):
        return _failure("DPONE_DBT_PROFILES_INVALID", "The selected profile does not admit the native starter.", 1)
    member = decode_native_delivery_json(selected.native_profile_payload)
    physical = member.get("dbt_model_physical_design")
    layouts = physical.get("allowed_layouts") if isinstance(physical, dict) else None
    if not isinstance(layouts, list) or "rowstore_none" not in layouts:
        return _failure("DPONE_DBT_PROFILES_INVALID", "The starter requires explicit rowstore_none admission.", 1)
    values = {
        "PROJECT_NAME": template.project_name,
        "DBT_PROFILE": selected.runtime.get("dbt_profile"),
        "DBT_TARGET": selected.runtime.get("dbt_target"),
        "INVOCATION_DATABASE": template.invocation_target.database,
        "INVOCATION_SCHEMA": template.invocation_target.schema,
        "SOURCE_DATABASE": template.source_relation.database,
        "SOURCE_SCHEMA": template.source_relation.schema,
        "SOURCE_NAME": template.source_relation.name,
        "PROFILE": profile,
        "WORKFLOW": workflow,
    }
    if any(
        not isinstance(value, str) or any(mark in value for mark in ("{{", "{%", "{#", "@@DPONE_"))
        for value in values.values()
    ):
        return _failure("DPONE_DBT_PROFILES_INVALID", "Starter scalar values must not contain template expressions.", 1)
    return {
        name: json.dumps(value, ensure_ascii=False).translate(_YAML_LITERAL_ESCAPES) for name, value in values.items()
    }


def _render(files: tuple[ScaffoldFile, ...], values: dict[str, str]) -> tuple[ScaffoldFile, ...]:
    paths = {file.path.as_posix() for file in files}
    if (
        len(files) != 22
        or len(paths) != 22
        or not set(_TEMPLATE_MARKERS).issubset(paths)
        or _POLICY_OUTPUT.as_posix() in paths
    ):
        raise ValueError("Starter requires the closed installed inventory")
    rendered: list[ScaffoldFile] = []
    for file in files:
        if not file.path.parts or file.path.is_absolute() or ".." in file.path.parts or "\\" in str(file.path):
            raise ValueError("Starter output path is unsafe")
        expected = _TEMPLATE_MARKERS.get(file.path.as_posix())
        if expected is None:
            rendered.append(file)
            continue
        if set(_MARKER.findall(file.text)) != expected:
            raise ValueError("Starter template placeholders differ from their contract")
        text = _MARKER.sub(lambda match: values[match.group(1)], file.text)
        if "@@DPONE_" in text:
            raise ValueError("Starter contains an unresolved placeholder")
        rendered.append(replace(file, text=text))
    return tuple(rendered)


def _public_result(plan: ScaffoldApplyPlan, details: dict[str, object]) -> SelfServiceResult:
    private = replace(
        plan,
        changes=tuple(replace(change, diff="", message="") for change in plan.changes),
        rollback_issues=tuple(
            "Rollback could not complete safely; inspect the recovery journal." for _ in plan.rollback_issues
        ),
    )
    result = scaffold_result(private, stage=_STAGE, details=details)
    return replace(result, exit_code=result.exit_code if result.exit_code is not None else 0 if result.passed else 1)


def _failure(code: str, message: str, exit_code: int) -> SelfServiceResult:
    resource_failure = code == "DPONE_DBT_STARTER_RESOURCES_INVALID"
    documented = resource_failure or code == "DPONE_PROJECT_AUTHORING_LOCK_FAILED"
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage=_STAGE,
                docs_url=error_docs_url(code) if documented else None,
                fixes=[manual_fix("repair_installed_distribution")] if resource_failure else [],
            ),
        ),
        exit_code=exit_code,
    )


__all__ = ["DbtStarterProfileParser", "DbtStarterResourcesPort", "DbtStarterService"]
