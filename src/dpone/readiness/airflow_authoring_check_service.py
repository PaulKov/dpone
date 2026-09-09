"""Application service for one compiled Airflow self-service source."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompilation, AuthoringCompiler
    from dpone.readiness.airflow_live_preflight import AirflowLivePreflightRunner
    from dpone.readiness.project_selection_loader import ProjectSelectionOutcome


from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.confined_files import (
    ConfinedFileError,
    sha256_confined_file,
)
from dpone.readiness.airflow_authoring_migration_plan import airflow_authoring_migration_plan
from dpone.readiness.airflow_authoring_source_io import (
    load_pinned_pipeline_source,
    missing_source_result,
)
from dpone.readiness.airflow_authoring_validation import authoring_check_view, compile_pipeline_source
from dpone.readiness.airflow_connection_checks import validate_connection_configuration
from dpone.readiness.airflow_pipeline_source_reader import (
    PipelineSourcePathError,
    fallback_pipeline_identity,
    resolve_pipeline_source_reference,
)
from dpone.readiness.airflow_self_service_live_check import live_preflight_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url
from dpone.readiness.project_selection import ProjectSelectionService, SelectionError, selection_error_result


@dataclass(frozen=True, slots=True)
class CheckedPipelineSource:
    """One source read and compilation shared by check and preview consumers."""

    source_path: Path
    payload: dict[str, Any] | None
    compilation: AuthoringCompilation | None
    result: SelfServiceResult
    source_bytes: bytes | None = None
    source_sha256: str | None = None
    source_label: str | None = None
    workload_fingerprint: str | None = None
    consumed_files: tuple[tuple[str, str], ...] = ()


class CheckedPipelineSourceChangedError(ValueError):
    """The primary source no longer matches the checked snapshot."""

    code = "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"


class AirflowAuthoringCheckService:
    """Compile one primary source and apply the selected readiness checks."""

    def __init__(
        self,
        *,
        root: Path,
        live_preflight_runner: AirflowLivePreflightRunner | None = None,
        authoring_compiler: AuthoringCompiler | None = None,
    ) -> None:
        self._root = root
        self._live_preflight_runner = live_preflight_runner
        self._authoring_compiler = authoring_compiler or default_authoring_compiler()

    def inspect(
        self,
        target: str | Path,
        *,
        mode: str = "static",
        environment: str = "dev",
    ) -> CheckedPipelineSource:
        try:
            source_path, source_reference = resolve_pipeline_source_reference(self._root, target)
        except PipelineSourcePathError as exc:
            source_path = self._root / "pipeline.yaml"
            entity_id = exc.entity_id or fallback_pipeline_identity(target)
            result = SelfServiceResult(
                passed=False,
                errors=(
                    dpone_error(
                        exc.code,
                        str(exc),
                        stage="self_service",
                        entity={"kind": "pipeline", "id": entity_id},
                        path=exc.path,
                        docs_url=error_docs_url(exc.code),
                    ),
                ),
                details=_check_details(mode),
                exit_code=exc.exit_code,
            )
            return CheckedPipelineSource(source_path, None, None, result)
        pinned_source, load_error = load_pinned_pipeline_source(self._root, source_path)
        if load_error is not None:
            if load_error["code"] == "DPONE_PIPELINE_SOURCE_NOT_FOUND":
                return CheckedPipelineSource(
                    source_path,
                    None,
                    None,
                    missing_source_result(source_path, root=self._root),
                )
            exit_code = 4 if load_error["code"] == "DPONE_PIPELINE_SOURCE_PATH_INVALID" else None
            result = SelfServiceResult(
                passed=False,
                errors=(load_error,),
                details=_check_details(mode),
                exit_code=exit_code,
            )
            return CheckedPipelineSource(source_path, None, None, result)
        assert pinned_source is not None
        payload = pinned_source.payload

        compilation, errors = compile_pipeline_source(
            payload,
            source_path,
            root=self._root,
            compiler=self._authoring_compiler,
        )
        if (
            not errors
            and source_reference.requested_id is not None
            and (compilation is None or compilation.pipeline_id != source_reference.requested_id)
        ):
            errors.append(
                dpone_error(
                    "DPONE_PIPELINE_ID_MISMATCH",
                    "Requested pipeline id does not match metadata.id in the primary source.",
                    stage="self_service",
                    path=pinned_source.source_label,
                    entity={"kind": "pipeline", "id": str(source_reference.requested_id)},
                    docs_url=error_docs_url("DPONE_PIPELINE_ID_MISMATCH"),
                )
            )
        if not errors and compilation is not None:
            authority_error = _domain_first_authority_error(
                self._root,
                source_reference=source_reference,
                source_sha256=pinned_source.sha256,
                compilation=compilation,
            )
            if authority_error is not None:
                errors.append(authority_error)
        details = _check_details(mode)
        authoring_details, pipeline_view = authoring_check_view(payload, compilation)
        details.update(authoring_details)
        migration_plan = airflow_authoring_migration_plan(payload=payload, source_path=source_path, root=self._root)
        if migration_plan is not None:
            details["airflow_authoring_migration_plan"] = migration_plan
        if mode in {"connections", "live"} and not errors:
            connection_details = validate_connection_configuration(
                root=self._root,
                pipeline_source=pipeline_view,
                environment=environment,
            )
            errors.extend(connection_details.pop("errors"))
            for key in ("mode", "network", "secrets", "source_queries"):
                connection_details.pop(key, None)
            details.update(connection_details)
        if mode == "live" and not errors:
            result = live_preflight_result(
                details=details,
                errors=errors,
                source_path=source_path,
                source_label=pinned_source.source_label,
                environment=environment,
                runner=self._live_preflight_runner,
            )
        else:
            result = SelfServiceResult(passed=not errors, errors=tuple(errors), details=details)
        return CheckedPipelineSource(
            source_path,
            payload,
            compilation,
            result,
            source_bytes=pinned_source.content,
            source_sha256=pinned_source.sha256,
            source_label=pinned_source.source_label,
            workload_fingerprint=source_reference.workload_fingerprint,
            consumed_files=tuple(sorted(source_reference.consumed_files.items())),
        )


class ProjectSelectionCheckService:
    """Aggregate canonical authoring checks for one explained selected set."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        live_preflight_runner: AirflowLivePreflightRunner | None = None,
    ) -> None:
        self._root = Path(root).resolve(strict=True)
        self._selection = ProjectSelectionService(root=self._root)
        self._authoring = AirflowAuthoringCheckService(
            root=self._root,
            live_preflight_runner=live_preflight_runner,
        )

    def check(
        self,
        *,
        target: str | Path,
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        state_path: str | Path | None,
        selectors_path: str,
        max_selected: int,
        mode: str,
        environment: str,
    ) -> SelfServiceResult:
        try:
            outcome = self._selection.select(
                target=target,
                select=select,
                exclude=exclude,
                state_path=state_path,
                selectors_path=selectors_path,
                max_selected=max_selected,
            )
        except SelectionError as exc:
            return selection_error_result(exc, stage="selection_check")
        reports, errors, exit_codes = inspect_selected_sources(
            outcome,
            authoring=self._authoring,
            mode=mode,
            environment=environment,
        )
        try:
            self._selection.verify_consumed_files(dict(outcome.consumed_files))
        except SelectionError as exc:
            return selection_error_result(exc, stage="selection_check")
        aggregate_details = _selected_check_details(mode, reports)
        aggregate_details.update(
            {
                "selection": outcome.report.to_jsonable(),
                "selected_checks": reports,
            }
        )
        return SelfServiceResult(
            passed=not errors,
            errors=tuple(errors),
            details=aggregate_details,
            exit_code=max(exit_codes) if exit_codes else None,
        )


def verify_checked_pipeline_source(root: str | Path, checked: CheckedPipelineSource) -> None:
    """Revalidate source and domain-first authority before a durable handoff."""

    if checked.source_label is None or checked.source_sha256 is None:
        raise CheckedPipelineSourceChangedError("Checked pipeline source has no exact digest.")
    root_path = Path(root).resolve(strict=True)
    try:
        current_sha256 = sha256_confined_file(root_path, checked.source_label)
        _, reference = resolve_pipeline_source_reference(root_path, checked.source_label)
    except (ConfinedFileError, OSError, PipelineSourcePathError) as exc:
        raise CheckedPipelineSourceChangedError("Checked pipeline source is no longer readable.") from exc
    if (
        current_sha256 != checked.source_sha256
        or reference.workload_fingerprint != checked.workload_fingerprint
        or tuple(sorted(reference.consumed_files.items())) != checked.consumed_files
    ):
        raise CheckedPipelineSourceChangedError("Checked pipeline authority changed after validation.")


def checked_pipeline_source_unchanged(root: str | Path, checked: CheckedPipelineSource) -> bool:
    """Return whether the checked source still represents the same complete authority."""

    try:
        verify_checked_pipeline_source(root, checked)
    except CheckedPipelineSourceChangedError:
        return False
    return True


def inspect_selected_sources(
    outcome: ProjectSelectionOutcome,
    *,
    authoring: AirflowAuthoringCheckService,
    mode: str,
    environment: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[int]]:
    """Run the canonical authoring check for every selected workload."""

    reports: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    exit_codes: list[int] = []
    for entry in outcome.report.selected:
        checked = outcome.checked_sources[entry.node.node_id]
        result = authoring.inspect(
            checked.source_path,
            mode=mode,
            environment=environment,
        ).result
        reports.append(
            {
                "workload_id": entry.node.node_id,
                "passed": result.passed,
                "details": result.details or {},
            }
        )
        errors.extend(result.errors)
        if result.exit_code is not None:
            exit_codes.append(result.exit_code)
    return reports, errors, exit_codes


def _selected_check_details(mode: str, reports: list[dict[str, Any]]) -> dict[str, Any]:
    child_details = [item_details for report in reports if isinstance((item_details := report.get("details")), dict)]
    source_query_values = [details.get("source_queries", False) for details in child_details]
    source_queries: bool | str = (
        True
        if any(value is True for value in source_query_values)
        else "bounded_probes"
        if any(value == "bounded_probes" for value in source_query_values)
        else False
    )
    aggregate: dict[str, Any] = {
        "mode": mode,
        "network": any(item.get("network") is True for item in child_details),
        "secrets": any(item.get("secrets") is True for item in child_details),
        "source_queries": source_queries,
    }
    if any(item.get("planned_network") is True for item in child_details):
        aggregate["planned_network"] = True
    if any(item.get("planned_secrets") is True for item in child_details):
        aggregate["planned_secrets"] = True
    if any(item.get("planned_source_queries") == "bounded_probes" for item in child_details):
        aggregate["planned_source_queries"] = "bounded_probes"
    return aggregate


def _domain_first_authority_error(
    root: Path,
    *,
    source_reference: Any,
    source_sha256: str,
    compilation: AuthoringCompilation,
) -> dict[str, Any] | None:
    expected = source_reference.checked_source
    if expected is None:
        return None
    expected_dependencies = tuple((item.kind, item.path, item.sha256) for item in expected.compilation.dependencies)
    actual_dependencies = tuple((item.kind, item.path, item.sha256) for item in compilation.dependencies)
    changed = (
        source_sha256 != expected.source_sha256
        or compilation.semantic_fingerprint != expected.compilation.semantic_fingerprint
        or actual_dependencies != expected_dependencies
    )
    if not changed:
        try:
            for path, digest in source_reference.consumed_files.items():
                if sha256_confined_file(root, path) != digest:
                    changed = True
                    break
        except (ConfinedFileError, OSError):
            changed = True
    if not changed:
        return None
    return dpone_error(
        "DPONE_SELECTION_STATE_CHANGED",
        "Domain-first authoring inputs changed during validation.",
        stage="self_service",
        path=expected.source_label,
        entity={"kind": "pipeline", "id": expected.workload_id},
        docs_url=error_docs_url("DPONE_SELECTION_STATE_CHANGED"),
    )


def _check_details(mode: str) -> dict[str, Any]:
    details: dict[str, Any] = {"mode": mode, "network": False, "secrets": False, "source_queries": False}
    if mode == "live":
        details.update(
            {
                "planned_network": True,
                "planned_secrets": True,
                "planned_source_queries": "bounded_probes",
            }
        )
    return details


__all__ = [
    "AirflowAuthoringCheckService",
    "CheckedPipelineSource",
    "CheckedPipelineSourceChangedError",
    "ProjectSelectionCheckService",
    "checked_pipeline_source_unchanged",
    "inspect_selected_sources",
    "verify_checked_pipeline_source",
]
