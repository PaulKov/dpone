"""Secret-free runtime evidence contracts for governed dbt execution."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime

from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.dbt_contract_validation import (
    contract_error,
    require_digest,
    require_strict_mapping,
    require_text,
    require_token,
)
from dpone.contracts.dbt_sqlserver_policy import DbtSqlServerRuntimePolicy

DBT_EXECUTION_EVIDENCE_SCHEMA = "dpone.dbt-execution-evidence.v1"
DBT_WARNING_POLICIES = frozenset({"allow", "fail"})
DBT_RESULT_STATUSES = frozenset(
    {
        "error",
        "fail",
        "no-op",
        "partial success",
        "pass",
        "runtime error",
        "skipped",
        "success",
        "warn",
    }
)
_RUN_RESULTS_ERROR = "DPONE_DBT_RUN_RESULTS_INVALID"


@dataclass(frozen=True, slots=True)
class DbtCredentialVersion:
    connection_ref: str
    resolver: str
    resolved_version: str | None

    def __post_init__(self) -> None:
        if not is_valid_connection_ref(self.connection_ref):
            raise contract_error(
                "DPONE_DBT_PROFILE_INVALID",
                "credential connection_ref is invalid",
            )
        require_token(
            self.resolver,
            "resolver",
            "DPONE_DBT_PROFILE_INVALID",
        )
        if self.resolved_version is not None:
            require_text(
                self.resolved_version,
                "resolved_version",
                "DPONE_DBT_PROFILE_INVALID",
            )

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DbtNodeOutcome:
    unique_id: str
    status: str
    execution_time: float

    def __post_init__(self) -> None:
        require_token(self.unique_id, "node unique_id", _RUN_RESULTS_ERROR)
        if require_text(self.status, "node status", _RUN_RESULTS_ERROR) not in DBT_RESULT_STATUSES:
            raise contract_error(_RUN_RESULTS_ERROR, "node status is unsupported")
        if isinstance(self.execution_time, bool) or not isinstance(
            self.execution_time,
            (int, float),
        ):
            raise contract_error(
                _RUN_RESULTS_ERROR,
                "node execution_time must be numeric",
            )
        if not math.isfinite(float(self.execution_time)):
            raise contract_error(
                _RUN_RESULTS_ERROR,
                "node execution_time must be finite",
            )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DbtExecutionEvidence:
    status: str
    code: str
    workflow_id: str
    release_id: str
    deployment_id: str
    workload_pack_sha256: str
    project_bundle_sha256: str
    manifest_sha256: str
    selection_sha256: str
    toolchain_sha256: str
    invocation_context_sha256: str
    logical_target_sha256: str
    target_binding_sha256: str
    adapter_runtime: DbtSqlServerRuntimePolicy
    adapter_policy_sha256: str
    graph_policy_sha256: str
    preflight_status: str
    build_started: bool
    dbt_exit_code: int | None
    dbt_warning_policy: str
    dbt_warning_count: int
    dbt_schema_version: str | None
    dbt_version: str | None
    invocation_id: str | None
    started_at: str
    finished_at: str
    airflow: Mapping[str, object]
    credential_versions: tuple[DbtCredentialVersion, ...]
    nodes: tuple[DbtNodeOutcome, ...]
    recovery: Mapping[str, object] | None = None
    schema: str = DBT_EXECUTION_EVIDENCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_EXECUTION_EVIDENCE_SCHEMA or self.status not in {"passed", "failed"}:
            raise contract_error(
                _RUN_RESULTS_ERROR,
                "dbt evidence status or schema is invalid",
            )
        for value in (
            self.release_id,
            self.deployment_id,
            self.workload_pack_sha256,
            self.project_bundle_sha256,
            self.manifest_sha256,
            self.selection_sha256,
            self.toolchain_sha256,
            self.invocation_context_sha256,
            self.logical_target_sha256,
            self.target_binding_sha256,
            self.adapter_policy_sha256,
            self.graph_policy_sha256,
        ):
            require_digest(value, "evidence digest", _RUN_RESULTS_ERROR)
        if not isinstance(self.adapter_runtime, DbtSqlServerRuntimePolicy):
            raise contract_error(
                _RUN_RESULTS_ERROR,
                "dbt evidence adapter runtime policy is invalid",
            )
        for value in (self.started_at, self.finished_at):
            _aware_timestamp(value)
        _validate_result_summary(self)
        require_strict_mapping(
            self.airflow,
            "airflow",
            frozenset(
                {
                    "dag_id",
                    "task_id",
                    "run_id",
                    "try_number",
                    "map_index",
                }
            ),
            _RUN_RESULTS_ERROR,
        )
        object.__setattr__(
            self,
            "credential_versions",
            tuple(self.credential_versions),
        )
        object.__setattr__(self, "nodes", tuple(self.nodes))
        object.__setattr__(
            self,
            "recovery",
            None if self.recovery is None else dict(self.recovery),
        )
        _validate_node_outcomes(self)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        if self.recovery is None:
            payload.pop("recovery")
        return payload


def _validate_result_summary(evidence: DbtExecutionEvidence) -> None:
    if evidence.preflight_status not in {"not_started", "passed", "failed"}:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence preflight status is invalid",
        )
    if not isinstance(evidence.build_started, bool):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence build_started must be a boolean",
        )
    if evidence.dbt_exit_code is not None and (
        isinstance(evidence.dbt_exit_code, bool) or not isinstance(evidence.dbt_exit_code, int)
    ):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence exit code must be an integer or null",
        )
    if not evidence.build_started and evidence.dbt_exit_code is not None:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence without a started build cannot contain an exit code",
        )
    if evidence.build_started and evidence.preflight_status != "passed":
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt build cannot start before preflight passes",
        )
    if evidence.dbt_warning_policy not in DBT_WARNING_POLICIES:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence warning policy is invalid",
        )
    if (
        isinstance(evidence.dbt_warning_count, bool)
        or not isinstance(evidence.dbt_warning_count, int)
        or evidence.dbt_warning_count < 0
    ):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence warning count is invalid",
        )
    if evidence.status == "passed" and (
        evidence.code != "DPONE_DBT_EXECUTION_PASSED"
        or evidence.dbt_exit_code != 0
        or evidence.preflight_status != "passed"
        or not evidence.build_started
    ):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "passed dbt evidence requires the success code and exit code zero",
        )
    if evidence.status == "passed":
        for value, field in (
            (evidence.dbt_schema_version, "dbt_schema_version"),
            (evidence.dbt_version, "dbt_version"),
            (evidence.invocation_id, "invocation_id"),
        ):
            require_text(value, field, _RUN_RESULTS_ERROR)
    if evidence.status == "failed" and evidence.code == "DPONE_DBT_EXECUTION_PASSED":
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "failed dbt evidence cannot use the success code",
        )
    _validate_recovery(evidence)


def _validate_recovery(evidence: DbtExecutionEvidence) -> None:
    if evidence.code != "COMMIT_UNKNOWN":
        if evidence.recovery is not None:
            raise contract_error(
                _RUN_RESULTS_ERROR,
                "dbt evidence recovery is reserved for COMMIT_UNKNOWN",
            )
        return
    if evidence.status != "failed" or not evidence.build_started:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "COMMIT_UNKNOWN requires a failed started build",
        )
    recovery = require_strict_mapping(
        evidence.recovery,
        "recovery",
        frozenset(
            {
                "status",
                "failure_boundary",
                "target_state",
                "checkpoint_state",
                "source_state",
                "safe_to_retry",
                "operator_verification_required",
                "recovery_action",
            }
        ),
        _RUN_RESULTS_ERROR,
    )
    expected = {
        "status": "COMMIT_UNKNOWN",
        "failure_boundary": "target_invocation",
        "target_state": "unknown",
        "checkpoint_state": "not_advanced",
        "source_state": "not_advanced",
        "safe_to_retry": False,
        "operator_verification_required": True,
        "recovery_action": "operator_verification_required",
    }
    if recovery != expected:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "COMMIT_UNKNOWN recovery contract is invalid",
        )


def _validate_node_outcomes(evidence: DbtExecutionEvidence) -> None:
    unique_ids = tuple(item.unique_id for item in evidence.nodes)
    if len(unique_ids) != len(set(unique_ids)):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence contains duplicate node outcomes",
        )
    observed_warnings = sum(item.status == "warn" for item in evidence.nodes)
    if evidence.dbt_warning_count != observed_warnings:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "dbt evidence warning count differs from node outcomes",
        )
    if evidence.status != "passed":
        return
    allowed = {"success", "pass", "no-op"}
    if evidence.dbt_warning_policy == "allow":
        allowed.add("warn")
    if not evidence.nodes or any(item.status not in allowed for item in evidence.nodes):
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "passed dbt evidence contains a non-passing node",
        )


def canonical_dbt_execution_evidence_bytes(
    payload: Mapping[str, object],
) -> bytes:
    """Serialize exact dbt evidence identically in runtime and XCom metadata."""

    return (
        json.dumps(
            dict(payload),
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _aware_timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "evidence timestamps must be ISO-8601",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise contract_error(
            _RUN_RESULTS_ERROR,
            "evidence timestamps must be offset-aware",
        )


__all__ = [
    "DBT_EXECUTION_EVIDENCE_SCHEMA",
    "DBT_RESULT_STATUSES",
    "DBT_WARNING_POLICIES",
    "canonical_dbt_execution_evidence_bytes",
    "DbtCredentialVersion",
    "DbtExecutionEvidence",
    "DbtNodeOutcome",
]
