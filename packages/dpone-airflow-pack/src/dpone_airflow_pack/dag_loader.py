"""Scheduler-side loader for declarative dpone DAG specs."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.dag_loader_policy import (
    DUPLICATE_POLICIES as _DUPLICATE_POLICIES,
)
from dpone_airflow_pack.dag_loader_policy import (
    INVALID_DAG_POLICIES as _INVALID_DAG_POLICIES,
)
from dpone_airflow_pack.dag_loader_policy import (
    deployment_index_load_error as _deployment_index_load_error,
)
from dpone_airflow_pack.dag_loader_policy import (
    duplicate_skip_reason as _duplicate_skip_reason,
)
from dpone_airflow_pack.dag_loader_policy import (
    failed_index_report as _failed_index_report,
)
from dpone_airflow_pack.dag_loader_policy import fatal_report_error as _fatal_report_error
from dpone_airflow_pack.dag_loader_policy import frozen_missing_cache_error as _frozen_missing_cache_error
from dpone_airflow_pack.dag_loader_policy import (
    handle_dag_error as _handle_dag_error,
)
from dpone_airflow_pack.dag_loader_policy import (
    indexed_cache_read_lease,
)
from dpone_airflow_pack.dag_loader_policy import (
    redacted_deployment_index_error as _redacted_deployment_index_error,
)
from dpone_airflow_pack.dag_loader_policy import (
    validate_policy as _validate_policy,
)
from dpone_airflow_pack.dag_materializer import (
    _materialize_dag_spec,
    _resolve_node_pack_refs,
)
from dpone_airflow_pack.dag_repository_loader import load_repository_dags
from dpone_airflow_pack.dag_spec_loader import (
    _dag_spec_load_error,
    load_dag_spec_file,
    redact_parse_error_message,
)
from dpone_airflow_pack.deployment_index import (
    INDEX_SCHEMA_V2,
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    CacheResolver,
    LoadReport,
    load_report,
    load_report_started,
)
from dpone_airflow_pack.deployment_index_contract import (
    _load_airflow_deployment_index_descriptor,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.init_fetch_preflight import (
    load_preflighted_init_fetch_dags,
)
from dpone_airflow_pack.loader_ack import (
    AcknowledgedDagLoad,
    write_dpone_loader_ack,
)
from dpone_airflow_pack.run_identity import build_dag_run_identity_context

if TYPE_CHECKING:
    from dpone_airflow_pack.semantic_refresh_airflow import (
        SemanticRefreshAirflowCallables,
    )


def load_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    repo_root: str | Path | None = None,
    *,
    index_path: str | Path | None = None,
    domains: Sequence[str] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    duplicate_policy: str = "skip_and_report",
    invalid_dag_policy: str = "skip_and_report",
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> LoadReport:
    """Materialize DAG objects from cached ``*.dag-spec.json`` artifacts.

    Collision-safe: dag ids already present in ``globals_dict`` are skipped so
    legacy hand-written DAG modules can coexist during migration.
    """

    started_at = load_report_started()
    _validate_policy("duplicate_policy", duplicate_policy, _DUPLICATE_POLICIES)
    _validate_policy("invalid_dag_policy", invalid_dag_policy, _INVALID_DAG_POLICIES)
    if index_path is not None:
        lexical_index = Path(index_path).absolute()
        try:
            with indexed_cache_read_lease(lexical_index) as lease:
                if not lease.root_available:
                    error = _frozen_missing_cache_error(lexical_index)
                    if invalid_dag_policy == "fail_all":
                        raise _redacted_deployment_index_error(error)
                    return _failed_index_report(started_at, error)
                return _load_indexed_dags_unleased(
                    globals_dict,
                    lexical_index=lexical_index,
                    operator_overrides=operator_overrides,
                    duplicate_policy=duplicate_policy,
                    invalid_dag_policy=invalid_dag_policy,
                    semantic_refresh_callables=semantic_refresh_callables,
                    started_at=started_at,
                )
        except AirflowDeploymentIndexError as exc:
            if exc.code != "DPONE_CACHE_READ_LEASE_FAILED":
                raise
            if invalid_dag_policy == "fail_all":
                raise _redacted_deployment_index_error(exc) from None
            return _failed_index_report(started_at, exc)
    if repo_root is None:
        raise TypeError("repo_root is required when index_path is not provided")
    root = Path(repo_root).resolve(strict=False)
    return load_repository_dags(
        globals_dict,
        root=root,
        domains=domains,
        operator_overrides=operator_overrides,
        duplicate_policy=duplicate_policy,
        invalid_dag_policy=invalid_dag_policy,
        started_at=started_at,
        materialize_dag_spec=_materialize_dag_spec,
    )


def load_and_acknowledge_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    duplicate_policy: str = "skip_and_report",
    invalid_dag_policy: str = "skip_and_report",
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> AcknowledgedDagLoad:
    """Load one exact cache occurrence and persist its ACK under one lease."""

    started_at = load_report_started()
    _validate_policy("duplicate_policy", duplicate_policy, _DUPLICATE_POLICIES)
    _validate_policy("invalid_dag_policy", invalid_dag_policy, _INVALID_DAG_POLICIES)
    lexical_index = Path(index_path).absolute()
    try:
        with indexed_cache_read_lease(lexical_index) as lease:
            if not lease.root_available:
                raise _redacted_deployment_index_error(_frozen_missing_cache_error(lexical_index))
            report = _load_indexed_dags_unleased(
                globals_dict,
                lexical_index=lexical_index,
                operator_overrides=operator_overrides,
                duplicate_policy=duplicate_policy,
                invalid_dag_policy=invalid_dag_policy,
                semantic_refresh_callables=semantic_refresh_callables,
                started_at=started_at,
            )
            if report.fatal and report.release_id is None:
                # A fatal report without deployment identity cannot be
                # acknowledged; ack validation would only report the
                # misleading "loader report release_id is invalid". Surface
                # the recorded index error instead. Identity-bearing fatal
                # reports still persist their ack with fatal=True.
                raise _fatal_report_error(lexical_index, report)
            acknowledgement = write_dpone_loader_ack(
                report,
                index_path=lexical_index,
                ack_path=ack_path,
                ack_root=ack_root,
            )
            return AcknowledgedDagLoad(report=report, acknowledgement=acknowledgement)
    except AirflowDeploymentIndexError as exc:
        if exc.code != "DPONE_CACHE_READ_LEASE_FAILED":
            raise
        if invalid_dag_policy == "fail_all":
            raise _redacted_deployment_index_error(exc) from None
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_LOADER_ACK_UNAVAILABLE",
            "exact DAG load could not be acknowledged",
            path=lexical_index.as_posix(),
        ) from exc


def _load_indexed_dags_unleased(
    globals_dict: MutableMapping[str, Any],
    *,
    lexical_index: Path,
    operator_overrides: Mapping[str, Any] | None,
    duplicate_policy: str,
    invalid_dag_policy: str,
    started_at: float,
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None,
) -> LoadReport:
    try:
        index = _load_airflow_deployment_index_descriptor(lexical_index)
    except AirflowDeploymentIndexError as exc:
        if invalid_dag_policy == "fail_all":
            raise _redacted_deployment_index_error(exc) from None
        return _failed_index_report(started_at, exc)
    return load_dpone_dags_from_index(
        globals_dict,
        index=index,
        operator_overrides=operator_overrides,
        duplicate_policy=duplicate_policy,
        invalid_dag_policy=invalid_dag_policy,
        started_at=started_at,
        semantic_refresh_callables=semantic_refresh_callables,
    )


def load_dpone_dags_from_index(
    globals_dict: MutableMapping[str, Any],
    *,
    index: AirflowDeploymentIndex,
    operator_overrides: Mapping[str, Any] | None,
    duplicate_policy: str,
    invalid_dag_policy: str,
    started_at: float | None = None,
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> LoadReport:
    """Materialize from one already-pinned deployment snapshot."""

    _validate_policy("duplicate_policy", duplicate_policy, _DUPLICATE_POLICIES)
    _validate_policy("invalid_dag_policy", invalid_dag_policy, _INVALID_DAG_POLICIES)
    report_started_at = started_at if started_at is not None else load_report_started()
    if index.schema == INDEX_SCHEMA_V2:
        return load_preflighted_init_fetch_dags(
            globals_dict,
            index=index,
            operator_overrides=operator_overrides,
            duplicate_policy=duplicate_policy,
            invalid_dag_policy=invalid_dag_policy,
            started_at=report_started_at,
            load_dag_spec=load_dag_spec_file,
            resolve_node_pack_refs=_resolve_node_pack_refs,
            materialize_dag_spec=_materialize_dag_spec,
            semantic_refresh_callables=semantic_refresh_callables,
        )
    loaded_ids: list[str] = []
    skipped: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    target_globals = dict(globals_dict)
    merged_overrides = dict(operator_overrides or {})
    cache_resolver = CacheResolver(index)
    for artifact in index.dag_specs:
        duplicate_reason = _duplicate_skip_reason(
            target_globals,
            dag_id=artifact.id,
            spec=None,
            duplicate_policy=duplicate_policy,
        )
        if duplicate_reason:
            skipped.append({"dag_id": artifact.id, "reason": duplicate_reason})
            continue
        try:
            payload, issues = load_dag_spec_file(
                artifact.path,
                expected_sha256=artifact.sha256,
                expected_bytes=artifact.bytes,
                max_bytes=index.max_artifact_bytes,
                confined_root=index.cache_root,
            )
        except Exception as exc:  # noqa: BLE001 - one unreadable spec must not break valid DAGs.
            message = redact_parse_error_message(exc)
            errors.append(
                _dag_spec_load_error(
                    dag_id=artifact.id,
                    message=message,
                    source=artifact.path.as_posix(),
                )
            )
            _handle_dag_error(
                target_globals,
                invalid_dag_policy=invalid_dag_policy,
                dag_id=artifact.id,
                message=message,
                source=artifact.path.as_posix(),
            )
            continue
        if issues:
            issue_payloads = [
                {
                    **issue.to_jsonable(),
                    "dag_id": artifact.id,
                }
                for issue in issues
            ]
            errors.extend(issue_payloads)
            message = "; ".join(issue.get("message", "") for issue in issue_payloads if issue.get("message"))
            _handle_dag_error(
                target_globals,
                invalid_dag_policy=invalid_dag_policy,
                dag_id=artifact.id,
                message=message or "dag-spec validation failed",
                source=artifact.path.as_posix(),
                error_code="DPONE_AIRFLOW_DAG_SPEC_INVALID",
            )
            continue
        assert payload is not None
        dag_id = str(payload["dag_id"])
        if dag_id != artifact.id:
            message = "dag-spec dag_id does not match deployment index artifact id"
            errors.append(
                _dag_spec_load_error(
                    dag_id=artifact.id,
                    message=message,
                    source=artifact.path.as_posix(),
                    error_code="DPONE_AIRFLOW_DAG_SPEC_ID_MISMATCH",
                )
            )
            _handle_dag_error(
                target_globals,
                invalid_dag_policy=invalid_dag_policy,
                dag_id=artifact.id,
                message=message,
                source=artifact.path.as_posix(),
                error_code="DPONE_AIRFLOW_DAG_SPEC_ID_MISMATCH",
            )
            continue
        dag_id = artifact.id
        try:
            payload = _resolve_node_pack_refs(payload, cache_resolver=cache_resolver)
        except AirflowDeploymentIndexError as exc:
            if invalid_dag_policy == "fail_all":
                raise _redacted_deployment_index_error(exc) from None
            errors.append(_deployment_index_load_error(exc, dag_id=dag_id))
            if index.delivery_context is not None:
                return load_report(
                    started_at=report_started_at,
                    release_id=index.release_id,
                    deployment_id=index.deployment_id,
                    airflow_index_sha256=index.airflow_index_sha256,
                    cache_root=index.cache_root.as_posix(),
                    activation_id=index.activation_id,
                    skipped=skipped,
                    errors=errors,
                    fatal=True,
                )
            _handle_dag_error(
                target_globals,
                invalid_dag_policy=invalid_dag_policy,
                dag_id=dag_id,
                message=redact_parse_error_message(exc),
                source=artifact.path.as_posix(),
            )
            continue
        duplicate_reason = _duplicate_skip_reason(
            target_globals,
            dag_id=dag_id,
            spec=payload,
            duplicate_policy=duplicate_policy,
        )
        if duplicate_reason:
            skipped.append({"dag_id": dag_id, "reason": duplicate_reason})
            continue
        try:
            target_globals[dag_id] = _materialize_dag_spec(
                payload,
                repo_root=index.cache_root,
                operator_overrides=merged_overrides,
                runtime_artifact_delivery=index.runtime_artifact_delivery,
                delivery_context=index.delivery_context,
                run_identity_context=build_dag_run_identity_context(
                    index,
                    dag_spec_id=artifact.id,
                    dag_spec_sha256=artifact.sha256,
                ),
            )
            loaded_ids.append(dag_id)
        except InitFetchProviderError as exc:
            if invalid_dag_policy == "fail_all":
                raise _redacted_deployment_index_error(exc) from None
            errors.append(_deployment_index_load_error(exc, dag_id=dag_id))
            return load_report(
                started_at=report_started_at,
                release_id=index.release_id,
                deployment_id=index.deployment_id,
                airflow_index_sha256=index.airflow_index_sha256,
                cache_root=index.cache_root.as_posix(),
                activation_id=index.activation_id,
                skipped=skipped,
                errors=errors,
                fatal=True,
            )
        except Exception as exc:  # noqa: BLE001 - parse isolation is a public provider contract.
            message = redact_parse_error_message(exc)
            errors.append(
                _dag_spec_load_error(
                    dag_id=dag_id,
                    message=message,
                    source=artifact.path.as_posix(),
                )
            )
            _handle_dag_error(
                target_globals,
                invalid_dag_policy=invalid_dag_policy,
                dag_id=dag_id,
                message=message,
                source=artifact.path.as_posix(),
            )
    report = load_report(
        started_at=report_started_at,
        release_id=index.release_id,
        deployment_id=index.deployment_id,
        airflow_index_sha256=index.airflow_index_sha256,
        cache_root=index.cache_root.as_posix(),
        activation_id=index.activation_id,
        loaded=loaded_ids,
        skipped=skipped,
        errors=errors,
    )
    globals_dict.update(target_globals)
    return report


__all__ = ["load_and_acknowledge_dpone_dags", "load_dpone_dags"]
