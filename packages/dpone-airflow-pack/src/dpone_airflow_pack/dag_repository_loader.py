"""Load declarative DAG specs directly from one repository tree."""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any

from dpone_airflow_pack.dag_loader_policy import (
    duplicate_skip_reason,
    handle_dag_error,
)
from dpone_airflow_pack.dag_spec_cache_paths import pinned_dag_specs
from dpone_airflow_pack.dag_spec_loader import (
    _dag_spec_load_error,
    load_dag_spec_file,
    redact_parse_error_message,
)
from dpone_airflow_pack.deployment_index import LoadReport, load_report


def load_repository_dags(
    globals_dict: MutableMapping[str, Any],
    *,
    root: Path,
    domains: Sequence[str] | None,
    operator_overrides: Mapping[str, Any] | None,
    duplicate_policy: str,
    invalid_dag_policy: str,
    started_at: float,
    materialize_dag_spec: Callable[..., Any],
) -> LoadReport:
    """Materialize repository DAG specs without deployment-index semantics."""

    atomic = "fail_all" in {duplicate_policy, invalid_dag_policy}
    target_globals = dict(globals_dict) if atomic else globals_dict
    merged_overrides = dict(operator_overrides or {})
    loaded_ids: list[str] = []
    skipped: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    selected_domains = {str(domain) for domain in domains or ()}
    with pinned_dag_specs(root) as specs:
        for spec in specs:
            path = spec.path
            payload, issues = load_dag_spec_file(
                path,
                expected_sha256=spec.expected_sha256,
                expected_bytes=spec.expected_bytes,
                expected_dag_id=spec.expected_dag_id,
                confined_root=spec.confined_root,
            )
            if issues:
                dag_id = path.name.removesuffix(".dag-spec.json")
                issue_payloads = [{**issue.to_jsonable(), "dag_id": dag_id} for issue in issues]
                errors.extend(issue_payloads)
                message = "; ".join(issue["message"] for issue in issue_payloads)
                handle_dag_error(
                    target_globals,
                    invalid_dag_policy=invalid_dag_policy,
                    dag_id=dag_id,
                    message=message,
                    source=path.as_posix(),
                    error_code=issues[0].code,
                )
                continue
            assert payload is not None
            domain = payload.get("domain")
            if selected_domains and domain and str(domain) not in selected_domains:
                continue
            dag_id = str(payload["dag_id"])
            duplicate_reason = duplicate_skip_reason(
                target_globals,
                dag_id=dag_id,
                spec=payload,
                duplicate_policy=duplicate_policy,
            )
            if duplicate_reason:
                skipped.append({"dag_id": dag_id, "reason": duplicate_reason})
                continue
            try:
                target_globals[dag_id] = materialize_dag_spec(
                    payload,
                    repo_root=root,
                    operator_overrides=merged_overrides,
                )
                loaded_ids.append(dag_id)
            except Exception as exc:  # noqa: BLE001 - isolate one invalid DAG spec.
                message = redact_parse_error_message(exc)
                errors.append(
                    _dag_spec_load_error(
                        dag_id=dag_id,
                        message=message,
                        source=path.as_posix(),
                    )
                )
                handle_dag_error(
                    target_globals,
                    invalid_dag_policy=invalid_dag_policy,
                    dag_id=dag_id,
                    message=message,
                    source=path.as_posix(),
                )
    report = load_report(
        started_at=started_at,
        release_id=None,
        deployment_id=None,
        loaded=loaded_ids,
        skipped=skipped,
        errors=errors,
    )
    if atomic:
        globals_dict.update(target_globals)
    return report


__all__ = ["load_repository_dags"]
