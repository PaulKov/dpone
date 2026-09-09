"""Build the parse-safe Airflow explain read model."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_authoring_check_service import (
        AirflowAuthoringCheckService,
        CheckedPipelineSource,
    )


import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import is_sha256_digest
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.readiness.airflow_active_index import (
    MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
    ActiveAirflowIndexSnapshot,
    load_active_index_snapshot,
)
from dpone.readiness.airflow_explain_scope import scope_active_index_snapshot
from dpone.readiness.airflow_pipeline_scaffold import safe_pipeline_id
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.security_redaction import redact_absolute_paths, redact_text


class AirflowExplainService:
    """Explain source and materialized state without remote or secret I/O."""

    def __init__(self, *, root: Path, authoring_check: AirflowAuthoringCheckService) -> None:
        self._root = root
        self._authoring_check = authoring_check

    def explain(self, pipeline_ref: str) -> SelfServiceResult:
        checked = self._authoring_check.inspect(pipeline_ref)
        return self.explain_checked(pipeline_ref, checked)

    def explain_checked(
        self,
        pipeline_ref: str,
        checked: CheckedPipelineSource,
    ) -> SelfServiceResult:
        """Explain one immutable authoring snapshot without reading it twice."""
        from dpone.readiness.airflow_operator_diagnostics import (
            build_operator_diagnostics,
            failed_operator_checks,
        )

        source_path = checked.source_path
        public_source_ref = checked.source_label or _project_relative_source(self._root, source_path)
        pipeline_id = (
            _pipeline_id_from_source(checked.payload, source_path)
            if checked.payload is not None
            else _pipeline_id_from_ref(pipeline_ref)
        )
        if checked.result.passed:
            snapshot = scope_active_index_snapshot(
                self._root,
                load_active_index_snapshot(self._root),
                pipeline_id=pipeline_id,
            )
            diagnostics = build_operator_diagnostics(self._root, snapshot=snapshot)
            artifact_state = _artifact_state(
                root=self._root,
                source_path=source_path,
                pipeline_id=pipeline_id,
                semantic_fingerprint=(
                    checked.compilation.semantic_fingerprint if checked.compilation is not None else None
                ),
                diagnostics=diagnostics,
                snapshot=snapshot,
            )
            operator_errors = _operator_diagnostic_errors(failed_operator_checks(diagnostics))
            identity_error = _snapshot_identity_error(
                diagnostics=diagnostics,
                artifact_state=artifact_state,
            )
            if identity_error is not None:
                operator_errors = (*operator_errors, identity_error)
        else:
            diagnostics = _not_applicable_operator_diagnostics()
            artifact_state = _unavailable_artifact_state(source_path)
            operator_errors = ()
        passed = checked.result.passed and not operator_errors
        return SelfServiceResult(
            passed=passed,
            errors=(*checked.result.errors, *operator_errors),
            exit_code=_aggregate_exit_code(
                authoring_result=checked.result,
                operator_failed=bool(operator_errors),
            ),
            details={
                "kind": "dpone.airflow-explain.v1",
                "pipeline_ref": public_source_ref,
                "source_path": public_source_ref,
                "artifact_state": artifact_state,
                "operator_diagnostics": diagnostics,
                "hint": _explain_hint(passed=passed, artifact_state=artifact_state),
            },
        )


def _not_applicable_operator_diagnostics() -> dict[str, Any]:
    return {
        "kind": "dpone.airflow-operator-diagnostics.v1",
        "status": "not_applicable",
        "operator_pinning": "not_applicable",
        "parse_side_effects": {
            "network": False,
            "metadata_db": False,
            "airflow_variables": False,
            "airflow_connections": False,
            "vault": False,
            "kubernetes": False,
            "cache_refresh": False,
        },
        "checks": [],
        "summary": {"passed": 0, "warning": 0, "failed": 0},
        "next_actions": [],
    }


def _unavailable_artifact_state(source_path: Path) -> dict[str, Any]:
    return {
        "manifest": "invalid" if source_path.exists() else "missing",
        "dag_spec": "unavailable",
        "airflow_pack": "unavailable",
        "published_deployment_id": None,
        "published_generation": None,
    }


def _explain_hint(
    *,
    passed: bool,
    artifact_state: Mapping[str, Any],
) -> str:
    if not passed:
        return "Correct or initialize the pipeline source before continuing."
    if artifact_state.get("dag_spec") == "materialized":
        return "The pipeline is ready for its hermetic contract test."
    return "The pipeline is ready for a local non-runnable Airflow preview."


def _artifact_state(
    *,
    root: Path,
    source_path: Path,
    pipeline_id: str,
    semantic_fingerprint: str | None,
    diagnostics: Mapping[str, Any],
    snapshot: ActiveAirflowIndexSnapshot,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "manifest": (
            "source"
            if source_path.exists() and semantic_fingerprint is not None
            else ("invalid" if source_path.exists() else "missing")
        ),
        "dag_spec": "planned",
        "airflow_pack": "planned",
        "published_deployment_id": None,
        "published_generation": None,
    }
    if diagnostics.get("status") != "materialized":
        return state
    index = snapshot.index
    if index is None:
        return state
    dag_present = bool(_artifact_ids(index.get("dag_specs")))
    pack_present = pipeline_id in _artifact_ids(index.get("workload_packs"))
    if not dag_present and not pack_present:
        return state
    deployment_id = index.get("deployment_id")
    if isinstance(deployment_id, str) and deployment_id.startswith("sha256:"):
        state["published_deployment_id"] = deployment_id
        state["published_generation"] = deployment_id
    if not _release_matches_semantic_fingerprint(
        root,
        release_id=index.get("release_id"),
        semantic_fingerprint=semantic_fingerprint,
    ):
        state["dag_spec"] = "stale" if dag_present else "planned"
        state["airflow_pack"] = "stale" if pack_present else "planned"
        return state
    state["dag_spec"] = "materialized" if dag_present else "planned"
    state["airflow_pack"] = "materialized" if pack_present else "planned"
    return state


def _release_matches_semantic_fingerprint(
    root: Path,
    *,
    release_id: object,
    semantic_fingerprint: str | None,
) -> bool:
    if not is_sha256_digest(release_id) or semantic_fingerprint is None:
        return False
    release_ref = (
        Path(".dpone-cache") / "releases" / str(release_id).replace(":", "-") / "release-set.json"
    ).as_posix()
    release = _bounded_json_mapping(root, release_ref)
    if release is None:
        return False
    provenance = release.get("provenance")
    return isinstance(provenance, Mapping) and provenance.get("semantic_fingerprint") == semantic_fingerprint


def _artifact_ids(raw: object) -> set[str]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return set()
    return {
        str(item["id"]) for item in raw if isinstance(item, Mapping) and isinstance(item.get("id"), str) and item["id"]
    }


def _operator_diagnostic_errors(
    failed_checks: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    errors: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for check in failed_checks:
        raw_code = str(check.get("code") or "").strip()
        code = raw_code if raw_code.startswith("DPONE_") else "DPONE_AIRFLOW_OPERATOR_DIAGNOSTIC_FAILED"
        entity = _operator_check_entity(check, code=code)
        identity = (code, entity["kind"], entity["id"])
        if identity in seen:
            continue
        message = redact_absolute_paths(redact_text(str(check.get("message") or "Airflow operator diagnostic failed.")))
        errors.append(
            dpone_error(
                code,
                message,
                stage="airflow_operator_diagnostics",
                entity=entity,
            )
        )
        seen.add(identity)
    return tuple(errors)


def _operator_check_entity(check: Mapping[str, Any], *, code: str) -> dict[str, str]:
    raw_entity = check.get("entity")
    entity = raw_entity if isinstance(raw_entity, Mapping) else {}
    kind = str(entity.get("kind") or "airflow_operator_check")
    identifier = str(entity.get("id") or code)
    return {
        "kind": redact_absolute_paths(redact_text(kind)),
        "id": redact_absolute_paths(redact_text(identifier)),
    }


def _snapshot_identity_error(
    *,
    diagnostics: Mapping[str, Any],
    artifact_state: Mapping[str, Any],
) -> dict[str, Any] | None:
    diagnosed = diagnostics.get("deployment_id")
    published = artifact_state.get("published_deployment_id")
    if not isinstance(diagnosed, str) or not isinstance(published, str) or diagnosed == published:
        return None
    return dpone_error(
        "DPONE_AIRFLOW_EXPLAIN_SNAPSHOT_MISMATCH",
        "Airflow explain projections did not retain one active deployment identity.",
        stage="airflow_operator_diagnostics",
        entity={"kind": "airflow_deployment", "id": "active"},
    )


def _aggregate_exit_code(
    *,
    authoring_result: SelfServiceResult,
    operator_failed: bool,
) -> int | None:
    if authoring_result.exit_code is not None:
        return authoring_result.exit_code
    if authoring_result.passed and operator_failed:
        return 1
    return None


def _bounded_json_mapping(root: Path, relative_path: str) -> dict[str, Any] | None:
    try:
        raw = read_confined_file(
            root,
            relative_path,
            max_bytes=MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
        )
    except ConfinedFileError:
        return None
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _pipeline_id_from_source(payload: dict[str, Any], source_path: Path) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("id"), str):
        return safe_pipeline_id(metadata["id"])
    return safe_pipeline_id(source_path.parent.name)


def _pipeline_id_from_ref(pipeline_ref: str) -> str:
    candidate = Path(pipeline_ref)
    raw = candidate.parent.name if candidate.name == "pipeline.yaml" else candidate.name
    try:
        return safe_pipeline_id(raw)
    except ValueError:
        return raw or pipeline_ref


def _project_relative_source(root: Path, source_path: Path) -> str:
    try:
        return source_path.relative_to(root).as_posix()
    except ValueError:
        return source_path.name or "pipeline.yaml"


__all__ = ["AirflowExplainService"]
