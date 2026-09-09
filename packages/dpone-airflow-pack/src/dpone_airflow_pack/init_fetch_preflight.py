"""Two-phase global preflight for strict indexed init-fetch DAGs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.cache_artifact_resolver import CacheResolver
from dpone_airflow_pack.dag_loader_policy import (
    deployment_index_load_error,
    duplicate_skip_reason,
    redacted_deployment_index_error,
)
from dpone_airflow_pack.dag_spec_loader import redact_parse_error_message
from dpone_airflow_pack.deployment_index import LoadReport, load_report
from dpone_airflow_pack.deployment_index_artifacts import (
    verify_airflow_index_artifact,
)
from dpone_airflow_pack.deployment_index_contract import AirflowDeploymentIndex
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.run_identity import build_dag_run_identity_context

if TYPE_CHECKING:
    from dpone_airflow_pack.semantic_refresh_airflow import (
        SemanticRefreshAirflowCallables,
    )


@dataclass(frozen=True, slots=True)
class PreflightedIndexedDag:
    """One fully validated DAG held outside the Airflow globals namespace."""

    dag_id: str
    payload: Mapping[str, Any]
    dag: Any


@dataclass(frozen=True, slots=True)
class PreparedIndexedDag:
    """One integrity-checked DAG payload not yet materialized as Airflow objects."""

    dag_id: str
    payload: Mapping[str, Any]
    artifact_sha256: str
    artifact_path: str


def load_preflighted_init_fetch_dags(
    globals_dict: MutableMapping[str, Any],
    *,
    index: AirflowDeploymentIndex,
    operator_overrides: Mapping[str, Any] | None,
    duplicate_policy: str,
    invalid_dag_policy: str,
    started_at: float,
    load_dag_spec: Callable[..., tuple[dict[str, Any] | None, Any]],
    resolve_node_pack_refs: Callable[..., dict[str, Any]],
    materialize_dag_spec: Callable[..., Any],
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> LoadReport:
    """Preflight all strict artifacts, then atomically apply duplicate policy."""

    try:
        preflighted = preflight_init_fetch_index(
            index,
            operator_overrides=dict(operator_overrides or {}),
            load_dag_spec=load_dag_spec,
            resolve_node_pack_refs=resolve_node_pack_refs,
            materialize_dag_spec=materialize_dag_spec,
            semantic_refresh_callables=semantic_refresh_callables,
        )
    except AirflowDeploymentIndexError as exc:
        if invalid_dag_policy == "fail_all":
            raise redacted_deployment_index_error(exc) from None
        return load_report(
            started_at=started_at,
            release_id=index.release_id,
            deployment_id=index.deployment_id,
            airflow_index_sha256=index.airflow_index_sha256,
            cache_root=index.cache_root.as_posix(),
            activation_id=index.activation_id,
            errors=[deployment_index_load_error(exc)],
            fatal=True,
        )

    target_globals = dict(globals_dict)
    loaded_ids: list[str] = []
    skipped: list[dict[str, str]] = []
    for item in preflighted:
        duplicate_reason = duplicate_skip_reason(
            target_globals,
            dag_id=item.dag_id,
            spec=item.payload,
            duplicate_policy=duplicate_policy,
        )
        if duplicate_reason:
            skipped.append({"dag_id": item.dag_id, "reason": duplicate_reason})
            continue
        target_globals[item.dag_id] = item.dag
        loaded_ids.append(item.dag_id)
    report = load_report(
        started_at=started_at,
        release_id=index.release_id,
        deployment_id=index.deployment_id,
        airflow_index_sha256=index.airflow_index_sha256,
        cache_root=index.cache_root.as_posix(),
        activation_id=index.activation_id,
        loaded=loaded_ids,
        skipped=skipped,
    )
    globals_dict.update(target_globals)
    return report


def preflight_init_fetch_index(
    index: AirflowDeploymentIndex,
    *,
    operator_overrides: Mapping[str, Any],
    load_dag_spec: Callable[..., tuple[dict[str, Any] | None, Any]],
    resolve_node_pack_refs: Callable[..., dict[str, Any]],
    materialize_dag_spec: Callable[..., Any],
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> tuple[PreflightedIndexedDag, ...]:
    """Validate every strict v2 artifact before duplicate/install decisions."""

    context = index.delivery_context
    if context is None:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            "wire v2 requires a complete immutable init-fetch delivery context",
            path=index.path.as_posix(),
        )
    cache_resolver = CacheResolver(index)
    prepared = tuple(
        _prepare_dag(
            index,
            artifact=artifact,
            cache_resolver=cache_resolver,
            load_dag_spec=load_dag_spec,
            resolve_node_pack_refs=resolve_node_pack_refs,
        )
        for artifact in index.dag_specs
    )
    for artifact in index.workload_packs:
        verify_airflow_index_artifact(
            artifact,
            max_artifact_bytes=index.max_artifact_bytes,
        )
    generic = tuple(
        _materialize_prepared_dag(
            index,
            item=item,
            operator_overrides=operator_overrides,
            materialize_dag_spec=materialize_dag_spec,
        )
        for item in prepared
    )
    semantic = _materialize_semantic_refresh_dags(
        index,
        semantic_refresh_callables=semantic_refresh_callables,
    )
    return (*generic, *semantic)


def _materialize_semantic_refresh_dags(
    index: AirflowDeploymentIndex,
    *,
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None,
) -> tuple[PreflightedIndexedDag, ...]:
    artifacts = index.semantic_refresh_dag_projections
    if not artifacts:
        return ()
    if semantic_refresh_callables is None:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_SEMANTIC_REFRESH_RUNTIME_REQUIRED",
            "semantic-refresh DAG sidecars require the protected worker runtime",
            path=index.path.as_posix(),
        )
    from dpone_airflow_pack.semantic_refresh_airflow import (  # noqa: PLC0415
        materialize_semantic_refresh_dag,
    )
    from dpone_airflow_pack.semantic_refresh_index_artifacts import (  # noqa: PLC0415
        load_semantic_refresh_dag_projection_artifact,
    )

    result: list[PreflightedIndexedDag] = []
    for artifact in artifacts:
        try:
            projection = load_semantic_refresh_dag_projection_artifact(
                artifact,
                max_artifact_bytes=index.max_artifact_bytes,
            )
            dag = materialize_semantic_refresh_dag(
                dag_projection=projection.to_mapping(),
                dag_projection_authority=artifact.authority,
                callables=semantic_refresh_callables,
            )
            if (
                getattr(dag, "dag_id", None) != artifact.dag_id
                and getattr(dag, "kwargs", {}).get("dag_id") != artifact.dag_id
            ):
                raise ValueError("materialized semantic-refresh DAG identity differs")
            setattr(dag, "_dpone_spec_fingerprint", artifact.dag_projection_sha256)
        except AirflowDeploymentIndexError:
            raise
        except Exception as exc:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED",
                "semantic-refresh DAG sidecar could not be authenticated or materialized",
                path=artifact.path.as_posix(),
            ) from exc
        result.append(
            PreflightedIndexedDag(
                dag_id=artifact.dag_id,
                payload={
                    "dag_id": artifact.dag_id,
                    "spec_fingerprint": artifact.dag_projection_sha256,
                },
                dag=dag,
            )
        )
    return tuple(result)


def _prepare_dag(
    index: AirflowDeploymentIndex,
    *,
    artifact: Any,
    cache_resolver: CacheResolver,
    load_dag_spec: Callable[..., tuple[dict[str, Any] | None, Any]],
    resolve_node_pack_refs: Callable[..., dict[str, Any]],
) -> PreparedIndexedDag:
    try:
        payload, issues = load_dag_spec(
            artifact.path,
            expected_sha256=artifact.sha256,
            expected_bytes=artifact.bytes,
            max_bytes=index.max_artifact_bytes,
            confined_root=index.cache_root,
        )
        if issues:
            raise _invalid_dag_spec(artifact.id, artifact.path.as_posix(), issues)
        if payload is None:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_DAG_SPEC_INVALID",
                "dag-spec validation returned no payload",
                path=artifact.path.as_posix(),
            )
        dag_id = str(payload["dag_id"])
        if dag_id != artifact.id:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_DAG_SPEC_ID_MISMATCH",
                "dag-spec dag_id does not match deployment index artifact id",
                path=artifact.path.as_posix(),
            )
        resolved_payload = resolve_node_pack_refs(
            payload,
            cache_resolver=cache_resolver,
        )
    except AirflowDeploymentIndexError:
        raise
    except Exception as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED",
            redact_parse_error_message(exc),
            path=artifact.path.as_posix(),
        ) from exc
    return PreparedIndexedDag(
        dag_id=artifact.id,
        payload=resolved_payload,
        artifact_sha256=artifact.sha256,
        artifact_path=artifact.path.as_posix(),
    )


def _materialize_prepared_dag(
    index: AirflowDeploymentIndex,
    *,
    item: PreparedIndexedDag,
    operator_overrides: Mapping[str, Any],
    materialize_dag_spec: Callable[..., Any],
) -> PreflightedIndexedDag:
    try:
        dag = materialize_dag_spec(
            item.payload,
            repo_root=index.cache_root,
            operator_overrides=operator_overrides,
            runtime_artifact_delivery=index.runtime_artifact_delivery,
            delivery_context=index.delivery_context,
            run_identity_context=build_dag_run_identity_context(
                index,
                dag_spec_id=item.dag_id,
                dag_spec_sha256=item.artifact_sha256,
            ),
        )
    except AirflowDeploymentIndexError:
        raise
    except Exception as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED",
            redact_parse_error_message(exc),
            path=item.artifact_path,
        ) from exc
    return PreflightedIndexedDag(dag_id=item.dag_id, payload=item.payload, dag=dag)


def _invalid_dag_spec(
    dag_id: str,
    path: str,
    issues: Any,
) -> AirflowDeploymentIndexError:
    payloads = [issue.to_jsonable() for issue in issues]
    message = "; ".join(str(payload.get("message") or "") for payload in payloads if payload.get("message"))
    code = str(payloads[0].get("code") or "DPONE_AIRFLOW_DAG_SPEC_INVALID")
    return AirflowDeploymentIndexError(
        code,
        redact_parse_error_message(message or f"dag-spec is invalid: {dag_id}"),
        path=path,
    )


__all__ = [
    "PreflightedIndexedDag",
    "PreparedIndexedDag",
    "load_preflighted_init_fetch_dags",
    "preflight_init_fetch_index",
]
