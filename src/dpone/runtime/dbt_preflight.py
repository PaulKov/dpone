"""Non-mutating dbt graph and logical-target verification."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_publishing import DbtExecutionPack, DbtPublishingError
from dpone.contracts.dbt_selected_graph_observation import (
    DBT_SELECTION_SEMANTIC_ERROR_CODES,
    observe_dbt_selected_graph,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.runtime.dbt_execution_policy import (
    DbtExecutionOutputPaths,
    build_dbt_ls_command,
    build_dbt_parse_command,
)

if TYPE_CHECKING:
    from dpone.ports.dbt_publishing import (
        DbtCommandRunner,
        DbtManifestSchemaValidator,
        DbtRunResultsReader,
    )

MAX_DBT_PREFLIGHT_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_DBT_PREFLIGHT_SECONDS = 120


@dataclass(frozen=True, slots=True)
class DbtPreflightResult:
    """Observed identities proven before the mutating build."""

    graph_contract_sha256: str
    selected_graph_unique_ids: tuple[str, ...]
    expected_run_result_unique_ids: tuple[str, ...]


class DbtRuntimePreflight:
    """Run parse and ls under the exact runtime context before mutation."""

    def __init__(
        self,
        *,
        command_runner: DbtCommandRunner,
        artifact_reader: DbtRunResultsReader,
        manifest_validator: DbtManifestSchemaValidator,
    ) -> None:
        self._command_runner = command_runner
        self._artifact_reader = artifact_reader
        self._manifest_validator = manifest_validator

    def verify(
        self,
        pack: DbtExecutionPack,
        *,
        project_dir: Path,
        profile_path: Path,
        output_paths: DbtExecutionOutputPaths,
        interval_vars_json: str,
        redactions: tuple[str, ...],
    ) -> DbtPreflightResult:
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
            raise _selection_drift("dbt parse preflight failed")
        try:
            manifest = self._artifact_reader.read(
                output_paths.preflight_target / "manifest.json",
                root=output_paths.root,
                max_bytes=MAX_DBT_PREFLIGHT_MANIFEST_BYTES,
            )
            manifest_version = int(pack.manifest_schema_version.removeprefix("v"))
            diagnostics = self._manifest_validator.validate(
                manifest,
                version=manifest_version,
            )
            if any(item.severity != "warning" for item in diagnostics):
                raise _selection_drift("dbt parse preflight manifest does not satisfy the official schema")
            observation = observe_dbt_selected_graph(
                manifest, lock=pack.selection_lock, logical_target=(pack.profile.database, pack.profile.schema)
            )
        except DbtPublishingError as exc:
            if exc.code in DBT_SELECTION_SEMANTIC_ERROR_CODES:
                raise
            raise _selection_drift("dbt parse preflight manifest is unavailable or invalid") from exc
        except (TypeError, ValueError) as exc:
            raise _selection_drift("dbt parse preflight manifest identity is invalid") from exc
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
        if selected.exit_code != 0 or selected.stdout_truncated:
            raise _selection_drift("dbt selection preflight failed")
        observed_graph = _selection_ids(selected.stdout)
        observation.require_matches(pack.selection_lock, observed_graph)
        return DbtPreflightResult(
            graph_contract_sha256=observation.graph_contract_sha256,
            selected_graph_unique_ids=observed_graph,
            expected_run_result_unique_ids=observation.expected_run_result_unique_ids,
        )


def _selection_ids(stdout: str) -> tuple[str, ...]:
    values = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        try:
            payload = strict_json_object(line.encode("utf-8"))
        except StrictJsonError as exc:
            raise _selection_drift("dbt selection preflight returned invalid JSON") from exc
        unique_id = payload.get("unique_id")
        if not isinstance(unique_id, str) or not unique_id.startswith(
            ("model.", "seed.", "snapshot.", "test.", "unit_test.")
        ):
            raise _selection_drift("dbt selection preflight returned an invalid unique_id")
        values.append(unique_id)
    result = tuple(sorted(values))
    if not result or len(result) != len(set(result)):
        raise _selection_drift("dbt selection preflight returned empty or duplicate identities")
    return result


def _selection_drift(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_SELECTION_DRIFT", message)


__all__ = ["DbtPreflightResult", "DbtRuntimePreflight"]
