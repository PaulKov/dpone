"""Pure scheduler and artifact identity contracts for dbt runtime execution."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    AIRFLOW_RUN_IDENTITY_ENV,
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
    parse_airflow_deployment_identity_json,
    parse_airflow_run_identity_json,
)
from dpone.contracts.dbt_contract_validation import (
    DbtPublishingError,
    canonical_fingerprint,
    require_digest,
)
from dpone.contracts.dbt_execution_evidence import (
    DbtCredentialVersion,
    DbtExecutionEvidence,
    DbtNodeOutcome,
)
from dpone.contracts.dbt_execution_pack import (
    DBT_EXECUTION_PACK_SCHEMA_V2,
    DbtExecutionPack,
    dbt_target_identity_sha256,
)
from dpone.contracts.dbt_release import dbt_release_runtime_wire_contract
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V1,
    DBT_RUNTIME_WIRE_V2,
    dbt_runtime_payload_reference,
    dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_runtime_release_binding import bind_dbt_runtime_workload
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_semantic_refresh_selection import prove_mutation_closure
from dpone.contracts.dbt_sqlserver_policy import (
    MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
)
from dpone.contracts.dbt_workspace_attempt import (
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    require_workspace_authority_connection_ref,
)
from dpone.contracts.run_interval import (
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    TRY_NUMBER_ENV,
    parse_interval_datetime,
    run_interval_from_env,
)
from dpone.contracts.strict_json import strict_json_object


@dataclass(frozen=True, slots=True)
class DbtExecutionInterval:
    """Immutable scheduler window passed to dbt as locked variables."""

    start: str
    end: str

    def __post_init__(self) -> None:
        start = parse_interval_datetime(self.start)
        end = parse_interval_datetime(self.end)
        if start is None or end is None or start.tzinfo is None or end.tzinfo is None or start >= end:
            raise DbtPublishingError(
                "DPONE_DBT_EXECUTION_FAILED",
                "Airflow data interval is missing or invalid",
            )

    def dbt_vars_json(self) -> str:
        """Serialize only the replay-defining data interval variables."""

        return json.dumps(
            {
                "dpone_data_interval_end": self.end,
                "dpone_data_interval_start": self.start,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )


def required_runtime_environment(environment: Mapping[str, str], name: str) -> str:
    """Return one required scheduler value using the canonical runtime error."""

    value = environment.get(name)
    if not isinstance(value, str) or not value:
        raise DbtPublishingError("DPONE_DBT_EXECUTION_FAILED", f"{name} is required")
    return value


def airflow_attempt_from_environment(
    environment: Mapping[str, str],
    run_identity: AirflowRunIdentity,
) -> AirflowAttemptCorrelation:
    """Project the exact scheduler task attempt that owns this execution.

    The task identity is derived from the verified workload pack rather than the
    environment, so a task cannot claim another workload's attempt correlation.
    """

    try:
        try_number = int(required_runtime_environment(environment, TRY_NUMBER_ENV))
    except ValueError as exc:
        raise DbtPublishingError("DPONE_DBT_EXECUTION_FAILED", "Airflow try number is invalid") from exc
    return AirflowAttemptCorrelation(
        dag_id=required_runtime_environment(environment, DAG_ID_ENV),
        task_id=f"{run_identity.workload_pack.id}__dpone_runtime",
        run_id=required_runtime_environment(environment, DAG_RUN_ID_ENV),
        try_number=try_number,
        map_index=-1,
    )


def dbt_execution_interval_from_environment(environment: Mapping[str, str]) -> DbtExecutionInterval:
    """Project the replay-defining data interval from the scheduler environment."""

    interval = run_interval_from_env(environment)
    return DbtExecutionInterval(
        start=str(interval.interval_start or ""),
        end=str(interval.interval_end or ""),
    )


def validate_dbt_runtime_release_identity(
    *,
    execution_pack_payload: bytes,
    selection_lock_payload: bytes,
    project_bundle_sha256: str,
    manifest_sha256: str,
    workload_id: str,
    airflow_runtime_payload_ids: object,
    runtime_payload_ids: tuple[str, ...],
    wire_contract: str = DBT_RUNTIME_WIRE_V1,
) -> None:
    """Bind one fetched dbt release projection to its embedded execution pack."""

    try:
        execution = DbtExecutionPack.from_mapping(_dbt_contract_object(execution_pack_payload))
        execution.require_wire_contract(wire_contract)
        selection = DbtSelectionLock.from_mapping(_dbt_contract_object(selection_lock_payload))
        expected_ids = dbt_runtime_payload_trio(
            workflow_id=execution.workflow_id,
            project_sha256=project_bundle_sha256,
            manifest_sha256=manifest_sha256,
            selection_lock_payload=selection_lock_payload,
            wire_contract=wire_contract,
        )
    except (
        DbtPublishingError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
    ):
        raise _identity_error() from None
    # Details name only the mismatched field: identifier values are derived
    # from fetched artifact content and must not cross the runtime CLI boundary.
    if workload_id != f"dbt__{execution.workflow_id}":
        raise _identity_error()
    if not isinstance(airflow_runtime_payload_ids, list):
        raise _identity_error()
    if tuple(airflow_runtime_payload_ids) != expected_ids:
        raise _identity_error()
    if runtime_payload_ids != expected_ids:
        raise _identity_error()
    if project_bundle_sha256 != execution.project_bundle_sha256:
        raise _identity_error("DPONE_DBT_PROJECT_BUNDLE_INVALID")
    if manifest_sha256 != execution.selection_lock.manifest_sha256 or selection != execution.selection_lock:
        raise _identity_error()


def dbt_runtime_plan_payload_order(
    *,
    airflow_runtime_payload_ids: object,
    runtime_payload_ids: tuple[str, ...],
    wire_contract: str,
) -> tuple[str, ...]:
    """Apply legacy compact-plan order compatibility, not identity validation.

    Historical v1 materialization sorted plan references while packs retained
    canonical dbt order. Only equal-membership v1 lists may supply that order.
    V2 is never repaired. The caller must still validate the returned IDs against
    the exact execution pack and source bytes; this policy authorizes no payload.
    """

    if (
        wire_contract == DBT_RUNTIME_WIRE_V1
        and isinstance(airflow_runtime_payload_ids, list)
        and tuple(airflow_runtime_payload_ids) != runtime_payload_ids
        and set(airflow_runtime_payload_ids) == set(runtime_payload_ids)
    ):
        return tuple(str(item) for item in airflow_runtime_payload_ids)
    return runtime_payload_ids


def validate_dbt_runtime_source_projection(
    *,
    release_payload: bytes,
    release_id: str,
    workload_id: str,
    payload_refs: tuple[tuple[str, str], ...],
    artifact_bytes: Mapping[str, bytes],
) -> str:
    """Return the verified wire after checking the selected v2 source projection.

    Generic receipt verification must already bind the release/deployment and
    plan. This pure dbt policy adds canonical payload metadata and exact selected
    bytes; it neither fetches other projects nor certifies whole-source closure.
    """

    release = strict_json_object(release_payload)
    if release.get("schema") == "dpone.release-set.v3":
        from dpone.contracts.release_composition_policy import composition_native_release

        release = composition_native_release(release, workload_id=workload_id)
    wire = dbt_release_runtime_wire_contract(release)
    if wire == DBT_RUNTIME_WIRE_V2:
        descriptors = bind_dbt_runtime_workload(
            release, workload_id=workload_id, payload_ids=tuple(item_id for item_id, _ in payload_refs)
        )
        prefix = f"cache://releases/{release_id.replace(':', '-')}/"
        for (item_id, artifact_ref), descriptor in zip(payload_refs, descriptors, strict=True):
            reference = dbt_runtime_payload_reference(item_id, wire_contract=wire)
            if artifact_ref != prefix + reference.path or descriptor != reference.descriptor(
                artifact_bytes.get(artifact_ref, b"")
            ):
                raise ValueError("dbt runtime artifact differs from canonical release binding")
    return wire


def validate_dbt_workload_identity(
    pack: DbtExecutionPack,
    run_identity: AirflowRunIdentity,
) -> None:
    """Reject a verified workload that does not own the requested dbt workflow."""

    if run_identity.workload_pack.id != f"dbt__{pack.workflow_id}":
        raise DbtPublishingError(
            "DPONE_DBT_SELECTION_DRIFT",
            "dbt workflow identity does not match the verified workload",
        )


def dbt_attempt_id(
    run_identity: AirflowRunIdentity,
    airflow_attempt: AirflowAttemptCorrelation,
) -> str:
    """Return a stable, path-safe output identity for one Airflow attempt."""

    digest = canonical_fingerprint(
        {
            "release_id": run_identity.release_id,
            "deployment_id": run_identity.deployment_id,
            "airflow_attempt": airflow_attempt.to_dict(),
        }
    )
    return digest.removeprefix("sha256:")[:32]


def dbt_target_binding_sha256(
    pack: DbtExecutionPack,
    run_identity: AirflowRunIdentity,
) -> str:
    """Bind logical dbt target identity to immutable deployment references."""

    return dbt_target_binding_identity_sha256(
        logical_target_sha256=dbt_target_identity_sha256(pack.profile),
        run_identity=run_identity,
    )


def dbt_target_binding_identity_sha256(
    *,
    logical_target_sha256: str,
    run_identity: AirflowRunIdentity,
) -> str:
    """Bind a release-derived logical target to one verified deployment."""

    return canonical_fingerprint(
        {
            "logical_target_sha256": require_digest(
                logical_target_sha256,
                "logical target identity",
                "DPONE_DBT_TARGET_IDENTITY_MISMATCH",
            ),
            "deployment_id": run_identity.deployment_id,
            "binding_set_ref": run_identity.binding_set_ref,
            "connection_registry_ref": run_identity.connection_registry_ref,
            "credential_runtime_ref": run_identity.credential_runtime_ref,
        }
    )


def _dbt_contract_object(payload: bytes) -> Mapping[str, object]:
    value = json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=_unique_json_object,
        parse_constant=_reject_json_constant,
    )
    if not isinstance(value, Mapping):
        raise ValueError("dbt contract must be an object")
    return value


def _unique_json_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("unsupported JSON constant")


def _identity_error(code: str = "DPONE_DBT_SELECTION_DRIFT") -> DbtPublishingError:
    return DbtPublishingError(code, "verified dbt runtime identity is inconsistent")


__all__ = [
    "AIRFLOW_DEPLOYMENT_IDENTITY_ENV",
    "AIRFLOW_RUN_IDENTITY_ENV",
    "AirflowDeploymentIdentity",
    "AirflowAttemptCorrelation",
    "AirflowRunIdentity",
    "DAG_ID_ENV",
    "DAG_RUN_ID_ENV",
    "TRY_NUMBER_ENV",
    "DbtCredentialVersion",
    "DbtExecutionEvidence",
    "DbtExecutionInterval",
    "DbtExecutionPack",
    "DBT_EXECUTION_PACK_SCHEMA_V2",
    "DBT_RUNTIME_WIRE_V1",
    "DBT_RUNTIME_WIRE_V2",
    "DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV",
    "DbtNodeOutcome",
    "DbtPublishingError",
    "MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES",
    "airflow_attempt_from_environment",
    "dbt_attempt_id",
    "dbt_execution_interval_from_environment",
    "dbt_runtime_plan_payload_order",
    "dbt_target_binding_identity_sha256",
    "dbt_target_binding_sha256",
    "dbt_target_identity_sha256",
    "parse_airflow_deployment_identity_json",
    "parse_airflow_run_identity_json",
    "prove_mutation_closure",
    "required_runtime_environment",
    "run_interval_from_env",
    "validate_dbt_runtime_release_identity",
    "validate_dbt_runtime_source_projection",
    "validate_dbt_workload_identity",
    "require_workspace_authority_connection_ref",
]
