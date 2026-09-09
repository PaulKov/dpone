"""Dedicated runtime policy for an activated semantic-refresh dbt invocation."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.dbt_execution_policy import (
    DbtExecutionOutputPaths,
    build_dbt_ls_command,
    build_dbt_parse_command,
)
from dpone.runtime.dbt_semantic_refresh_command_authority import (
    SemanticRefreshDbtCommandRunner,
    SemanticRefreshDbtExecutionVariables,
    SemanticRefreshDbtScopeMapLoaderPort,
    load_semantic_refresh_scope_map,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtImmutableProofRecheckPort,
    SemanticRefreshDbtProofComparator,
    SemanticRefreshDbtStaticProjectionIdentity,
    semantic_refresh_expected_proof_digests,
)

MAX_DBT_PREFLIGHT_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_DBT_PREFLIGHT_SECONDS = 120


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtPreflightResult:
    """Observed exact V2 selection returned to the shared execution engine."""

    graph_contract_sha256: str
    selected_graph_unique_ids: tuple[str, ...]
    expected_run_result_unique_ids: tuple[str, ...]


class SemanticRefreshDbtRuntimePreflight:
    """Prove exact V2 mutation closure immediately before the mutating build."""

    def __init__(
        self,
        *,
        command_runner: Any,
        artifact_reader: Any,
        manifest_validator: Any,
        mutation_prover: Callable[..., Any],
        immutable_proof_rechecker: SemanticRefreshDbtImmutableProofRecheckPort,
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        error_factory: Callable[[str, str], Exception],
    ) -> None:
        self._command_runner = command_runner
        self._artifact_reader = artifact_reader
        self._manifest_validator = manifest_validator
        self._mutation_prover = mutation_prover
        self._immutable_proof_rechecker = immutable_proof_rechecker
        self._plan_bundle = plan_bundle
        self._projection_identity = projection_identity
        self._error_factory = error_factory

    def verify(
        self,
        pack: Any,
        *,
        project_dir: Path,
        profile_path: Path,
        output_paths: DbtExecutionOutputPaths,
        interval_vars_json: str,
        redactions: tuple[str, ...],
    ) -> SemanticRefreshDbtPreflightResult:
        timeout = min(pack.timeout_seconds, MAX_DBT_PREFLIGHT_SECONDS)
        parsed = self._command_runner.run(
            build_dbt_parse_command(
                pack,
                project_dir=project_dir,
                profile_path=profile_path,
                target_path=output_paths.preflight_target,
                log_path=output_paths.preflight_logs,
                interval_vars_json=interval_vars_json,
            ),
            cwd=project_dir,
            timeout_seconds=timeout,
            redactions=redactions,
        )
        if parsed.exit_code != 0:
            self._fail("dbt parse preflight failed")
        selected_ids = tuple(pack.selection_lock.selected_graph_unique_ids)
        model_ids = tuple(pack.selection_lock.publish_model_unique_ids)
        if selected_ids != model_ids:
            self._fail("semantic-refresh selection includes non-operation nodes")
        self._recheck_immutable_sources(
            self._manifest(pack, output_paths),
            selected_model_ids=model_ids,
        )
        compiled = self._command_runner.run(
            _build_dbt_compile_command(
                pack,
                project_dir=project_dir,
                profile_path=profile_path,
                target_path=output_paths.preflight_target,
                log_path=output_paths.preflight_logs,
                interval_vars_json=interval_vars_json,
            ),
            cwd=project_dir,
            timeout_seconds=timeout,
            redactions=redactions,
        )
        if compiled.exit_code != 0:
            self._fail("dbt compile preflight failed")
        manifest = self._manifest(pack, output_paths)
        proof = self._mutation_prover(
            manifest,
            selected_graph_unique_ids=selected_ids,
            selected_mutating_node_ids=model_ids,
        )
        if proof.status != "PROVEN" or proof.selected_mutating_node_ids != model_ids:
            self._fail("runtime mutation closure is not proven")
        _validate_logical_target(
            manifest,
            model_ids,
            expected=(pack.profile.database, pack.profile.schema),
            fail=self._fail,
        )
        selected = self._command_runner.run(
            build_dbt_ls_command(
                pack,
                project_dir=project_dir,
                profile_path=profile_path,
                target_path=output_paths.preflight_target,
                log_path=output_paths.preflight_logs,
                interval_vars_json=interval_vars_json,
            ),
            cwd=project_dir,
            timeout_seconds=timeout,
            redactions=redactions,
        )
        observed = _selection_ids(selected.stdout, fail=self._fail)
        if selected.exit_code != 0 or selected.stdout_truncated or observed != selected_ids:
            self._fail("runtime dbt selection differs from the exact operation closure")
        self._recheck_immutable_proofs(
            manifest,
            selected_model_ids=model_ids,
        )
        return SemanticRefreshDbtPreflightResult(
            graph_contract_sha256=pack.selection_lock.graph_contract_sha256,
            selected_graph_unique_ids=observed,
            expected_run_result_unique_ids=model_ids,
        )

    def _recheck_immutable_sources(
        self,
        manifest: Mapping[str, Any],
        *,
        selected_model_ids: tuple[str, ...],
    ) -> None:
        try:
            self._immutable_proof_rechecker.recheck_sources(
                manifest=manifest,
                plan_bundle=self._plan_bundle,
                projection_identity=self._projection_identity,
                selected_model_unique_ids=selected_model_ids,
            )
        except Exception as exc:
            if getattr(exc, "code", None) in {
                "DPONE_DBT_V2_PROOF_DRIFT",
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
            }:
                raise
            raise self._error_factory(
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
                "runtime raw model/macro proof recheck is unavailable",
            ) from exc

    def _recheck_immutable_proofs(
        self,
        manifest: Mapping[str, Any],
        *,
        selected_model_ids: tuple[str, ...],
    ) -> None:
        try:
            expected = semantic_refresh_expected_proof_digests(
                self._plan_bundle,
                selected_model_unique_ids=selected_model_ids,
            )
            observed = self._immutable_proof_rechecker.recheck(
                manifest=manifest,
                plan_bundle=self._plan_bundle,
                projection_identity=self._projection_identity,
                selected_model_unique_ids=selected_model_ids,
            )
            result = SemanticRefreshDbtProofComparator().verify(
                expected_selected_unique_ids=selected_model_ids,
                observed_selected_unique_ids=observed.observed_selected_unique_ids,
                expected_proof_digests=expected,
                observed_proof_digests=observed.observed_proof_digests,
                proof_statuses=observed.proof_statuses,
            )
        except Exception as exc:
            if getattr(exc, "code", None) in {
                "DPONE_DBT_V2_PROOF_DRIFT",
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
            }:
                raise
            raise self._error_factory(
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
                "runtime immutable proof recheck is unavailable",
            ) from exc
        if not result.can_execute:
            code = "DPONE_DBT_V2_PROOF_DRIFT" if result.status == "NONCONFORMANT" else "DPONE_DBT_V2_PROOF_UNVERIFIED"
            raise self._error_factory(
                code,
                "runtime immutable proof differs from the protected deployment plan",
            )

    def _manifest(self, pack: Any, paths: DbtExecutionOutputPaths) -> Mapping[str, Any]:
        try:
            manifest = self._artifact_reader.read(
                paths.preflight_target / "manifest.json",
                root=paths.root,
                max_bytes=MAX_DBT_PREFLIGHT_MANIFEST_BYTES,
            )
            diagnostics = self._manifest_validator.validate(
                manifest,
                version=int(pack.manifest_schema_version.removeprefix("v")),
            )
            if any(item.severity != "warning" for item in diagnostics):
                self._fail("runtime dbt manifest does not satisfy the official schema")
            return manifest
        except Exception as exc:
            if getattr(exc, "code", None) == "DPONE_DBT_V2_GRAPH_UNVERIFIED":
                raise
            raise self._error_factory(
                "DPONE_DBT_V2_GRAPH_UNVERIFIED",
                "runtime dbt manifest is unavailable or invalid",
            ) from exc

    def _fail(self, message: str) -> None:
        raise self._error_factory("DPONE_DBT_V2_GRAPH_UNVERIFIED", message)


def _build_dbt_compile_command(
    pack: Any,
    *,
    project_dir: Path,
    profile_path: Path,
    target_path: Path,
    log_path: Path,
    interval_vars_json: str,
) -> tuple[str, ...]:
    """Build the exact non-mutating runtime compilation probe."""

    return (
        "dbt",
        "--quiet",
        "--no-use-colors",
        "compile",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profile_path.parent),
        "--profile",
        pack.profile.profile_name,
        "--target",
        pack.profile.target_name,
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--indirect-selection",
        pack.invocation_context.indirect_selection,
        "--select",
        *pack.selection_lock.selectors,
        "--vars",
        interval_vars_json,
    )


def _validate_logical_target(
    manifest: Mapping[str, Any],
    model_ids: tuple[str, ...],
    *,
    expected: tuple[str, str],
    fail: Callable[[str], None],
) -> None:
    nodes = _mapping(manifest.get("nodes"), "manifest nodes")
    for model_id in model_ids:
        node = _mapping(nodes.get(model_id), model_id)
        if (node.get("database"), node.get("schema")) != expected:
            fail("runtime dbt target differs from the activated execution pack")


def _selection_ids(stdout: str, *, fail: Callable[[str], None]) -> tuple[str, ...]:
    values: list[str] = []
    try:
        for line in stdout.splitlines():
            if not line.strip():
                continue
            payload = json.loads(
                line,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
            unique_id = payload.get("unique_id") if isinstance(payload, Mapping) else None
            if not isinstance(unique_id, str) or not unique_id.startswith("model."):
                fail("runtime dbt selection returned an invalid model identity")
                continue
            values.append(unique_id)
    except (TypeError, ValueError, json.JSONDecodeError):
        fail("runtime dbt selection returned invalid JSON")
    result = tuple(sorted(values))
    if not result or len(result) != len(set(result)):
        fail("runtime dbt selection returned empty or duplicate identities")
    return result


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"semantic-refresh {field} must be an object")
    return value


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


__all__ = [
    "SemanticRefreshDbtCommandRunner",
    "SemanticRefreshDbtExecutionVariables",
    "SemanticRefreshDbtPreflightResult",
    "SemanticRefreshDbtRuntimePreflight",
    "SemanticRefreshDbtScopeMapLoaderPort",
    "load_semantic_refresh_scope_map",
]
