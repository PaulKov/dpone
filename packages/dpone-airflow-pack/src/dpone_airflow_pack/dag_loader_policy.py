"""Duplicate, failure, and diagnostic policies shared by DAG loaders."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_activation_contract import CacheReadLease, cache_read_lease
from dpone_airflow_pack.cache_artifact_contract import infer_cache_root
from dpone_airflow_pack.dag_spec_loader import (
    compute_dag_spec_fingerprint,
    redact_parse_error_message,
)
from dpone_airflow_pack.deployment_index import LoadReport, load_report
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

DUPLICATE_POLICIES = frozenset({"skip_and_report", "fail_all", "replace_if_same_fingerprint"})
INVALID_DAG_POLICIES = frozenset({"skip_and_report", "fail_all", "create_diagnostic_dag"})


@contextmanager
def indexed_cache_read_lease(index_path: Path) -> Iterator[CacheReadLease]:
    """Lease managed caches while preserving standalone local index loading."""

    cache_root = infer_cache_root(index_path)
    index_parent = index_path.parent.absolute()
    if cache_root == index_parent and cache_root.name != ".dpone-cache":
        yield CacheReadLease(root_available=True)
        return
    with cache_read_lease(cache_root) as lease:
        yield lease


def validate_policy(name: str, value: str, allowed: frozenset[str]) -> None:
    if value not in allowed:
        choices = ", ".join(sorted(allowed))
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_POLICY_INVALID",
            f"{name} must be one of: {choices}",
            path=name,
        )


def handle_dag_error(
    globals_dict: MutableMapping[str, Any],
    *,
    invalid_dag_policy: str,
    dag_id: str,
    message: str,
    source: str,
    error_code: str = "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED",
) -> None:
    message = redact_parse_error_message(message)
    source = redact_parse_error_message(source)
    if invalid_dag_policy == "fail_all":
        raise AirflowDeploymentIndexError(error_code, message, path=source)
    if invalid_dag_policy == "create_diagnostic_dag" and dag_id not in globals_dict:
        globals_dict[dag_id] = _diagnostic_dag(
            dag_id=dag_id,
            message=message,
            source=source,
        )


def duplicate_skip_reason(
    globals_dict: Mapping[str, Any],
    *,
    dag_id: str,
    spec: Mapping[str, Any] | None,
    duplicate_policy: str,
) -> str | None:
    existing = globals_dict.get(dag_id)
    if existing is None or _is_dpone_diagnostic_dag(existing):
        return None
    if duplicate_policy == "fail_all":
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_DAG_DUPLICATE",
            f"Duplicate DAG id already exists in globals: {dag_id}",
            path=dag_id,
        )
    if duplicate_policy == "replace_if_same_fingerprint":
        if spec is None:
            return None
        incoming = _spec_fingerprint(spec)
        current = _existing_spec_fingerprint(existing)
        if current == incoming:
            return None
        if current is None:
            return "duplicate_dag_id_fingerprint_missing"
        return "duplicate_dag_id_fingerprint_mismatch"
    return "duplicate_dag_id"


def deployment_index_load_error(
    exc: AirflowDeploymentIndexError,
    *,
    dag_id: str | None = None,
) -> dict[str, str]:
    payload = exc.to_jsonable()
    payload["message"] = redact_parse_error_message(payload["message"])
    if "path" in payload:
        payload["path"] = redact_parse_error_message(payload["path"])
    if dag_id is not None:
        payload["dag_id"] = dag_id
    return payload


def redacted_deployment_index_error(
    exc: AirflowDeploymentIndexError,
) -> AirflowDeploymentIndexError:
    payload = deployment_index_load_error(exc)
    return AirflowDeploymentIndexError(
        exc.code,
        payload["message"],
        path=payload.get("path"),
    )


def failed_index_report(
    started_at: float,
    error: AirflowDeploymentIndexError,
) -> LoadReport:
    """Render one bounded fatal report for an unavailable deployment index."""

    return load_report(
        started_at=started_at,
        release_id=None,
        deployment_id=None,
        errors=[deployment_index_load_error(error)],
        fatal=True,
    )


def frozen_missing_cache_error(index_path: Path) -> AirflowDeploymentIndexError:
    """Preserve the public missing-index error for one frozen absent cache."""

    return AirflowDeploymentIndexError(
        "DPONE_AIRFLOW_INDEX_NOT_FOUND",
        "airflow deployment index does not exist",
        path=index_path.as_posix(),
    )


def fatal_report_error(
    lexical_index: Path,
    report: LoadReport,
) -> AirflowDeploymentIndexError:
    """Rebuild the already-redacted error that aborted one index load."""

    aborting: Mapping[str, str] = report.errors[-1] if report.errors else {}
    return AirflowDeploymentIndexError(
        str(aborting.get("code") or "DPONE_AIRFLOW_INDEX_READ_FAILED"),
        str(aborting.get("message") or "airflow deployment index could not be loaded"),
        path=aborting.get("path") or lexical_index.as_posix(),
    )


def _spec_fingerprint(spec: Mapping[str, Any]) -> str:
    value = spec.get("spec_fingerprint")
    if isinstance(value, str) and value.startswith("sha256:"):
        return value
    return compute_dag_spec_fingerprint(spec)


def _existing_spec_fingerprint(dag: Any) -> str | None:
    value = getattr(dag, "_dpone_spec_fingerprint", None)
    return value if isinstance(value, str) and value.startswith("sha256:") else None


def _diagnostic_dag(*, dag_id: str, message: str, source: str) -> Any:
    from dpone_airflow_pack.dag_materializer import _quarantine_dag

    dag = _quarantine_dag(
        dag_id=dag_id,
        message=redact_parse_error_message(message),
        source=redact_parse_error_message(source),
    )
    setattr(dag, "_dpone_diagnostic_error", True)
    return dag


def _is_dpone_diagnostic_dag(dag: Any) -> bool:
    return getattr(dag, "_dpone_diagnostic_error", None) is True


__all__ = [
    "DUPLICATE_POLICIES",
    "INVALID_DAG_POLICIES",
    "deployment_index_load_error",
    "duplicate_skip_reason",
    "failed_index_report",
    "fatal_report_error",
    "frozen_missing_cache_error",
    "handle_dag_error",
    "indexed_cache_read_lease",
    "redacted_deployment_index_error",
    "validate_policy",
]
