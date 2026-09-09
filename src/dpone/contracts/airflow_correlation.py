"""Canonical non-secret correlation for one dpone Airflow workload attempt."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.airflow_run_identity import AirflowRunIdentity

AIRFLOW_CORRELATION_SCHEMA = "dpone.airflow-correlation.v1"
MAX_AIRFLOW_CORRELATION_BYTES = 32 * 1024
_MAX_TEXT_LENGTH = 1024
_MAX_RESOURCE_TEXT_LENGTH = 253
_ROOT_FIELDS = frozenset({"schema", "correlation_id", "attempt_ref", "airflow", "dpone", "artifacts", "pod"})
_ATTEMPT_FIELDS = frozenset({"dag_id", "task_id", "run_id", "try_number", "map_index"})
_DPONE_FIELDS = frozenset({"run_id", "process"})
_ARTIFACT_FIELDS = frozenset(
    {"release_id", "deployment_id", "workload_id", "workload_pack_sha256", "runtime_evidence_sha256"}
)
_POD_FIELDS = frozenset({"name", "uid", "namespace", "image_digest"})


class AirflowCorrelationError(ValueError):
    """Correlation input is malformed, incomplete for its use, or contradictory."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class AirflowAttemptCorrelation:
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    map_index: int

    @classmethod
    def from_mapping(cls, value: object) -> AirflowAttemptCorrelation:
        payload = _mapping(value, field="airflow")
        _reject_unknown(payload, _ATTEMPT_FIELDS, field="airflow")
        return cls(
            dag_id=_required_text(payload.get("dag_id"), field="airflow.dag_id"),
            task_id=_required_text(payload.get("task_id"), field="airflow.task_id"),
            run_id=_required_text(payload.get("run_id"), field="airflow.run_id"),
            try_number=_integer(payload.get("try_number"), field="airflow.try_number", minimum=1),
            map_index=_integer(payload.get("map_index"), field="airflow.map_index", minimum=-1),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "try_number": self.try_number,
            "map_index": self.map_index,
        }


@dataclass(frozen=True, slots=True)
class DponeRunCorrelation:
    run_id: str | None = None
    process: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> DponeRunCorrelation:
        if value is None:
            return cls()
        payload = _mapping(value, field="dpone")
        _reject_unknown(payload, _DPONE_FIELDS, field="dpone")
        return cls(
            run_id=_optional_text(payload.get("run_id"), field="dpone.run_id"),
            process=_optional_text(payload.get("process"), field="dpone.process", maximum=_MAX_RESOURCE_TEXT_LENGTH),
        )

    def to_dict(self) -> dict[str, str | None]:
        return {"run_id": self.run_id, "process": self.process}


@dataclass(frozen=True, slots=True)
class AirflowCorrelationArtifacts:
    release_id: str
    deployment_id: str
    workload_id: str
    workload_pack_sha256: str
    runtime_evidence_sha256: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> AirflowCorrelationArtifacts:
        payload = _mapping(value, field="artifacts")
        _reject_unknown(payload, _ARTIFACT_FIELDS, field="artifacts")
        return cls(
            release_id=_digest(payload.get("release_id"), field="artifacts.release_id"),
            deployment_id=_digest(payload.get("deployment_id"), field="artifacts.deployment_id"),
            workload_id=_required_text(
                payload.get("workload_id"),
                field="artifacts.workload_id",
                maximum=_MAX_RESOURCE_TEXT_LENGTH,
            ),
            workload_pack_sha256=_digest(
                payload.get("workload_pack_sha256"),
                field="artifacts.workload_pack_sha256",
            ),
            runtime_evidence_sha256=_optional_digest(
                payload.get("runtime_evidence_sha256"),
                field="artifacts.runtime_evidence_sha256",
            ),
        )

    def to_dict(self) -> dict[str, str | None]:
        return {
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "workload_id": self.workload_id,
            "workload_pack_sha256": self.workload_pack_sha256,
            "runtime_evidence_sha256": self.runtime_evidence_sha256,
        }


@dataclass(frozen=True, slots=True)
class AirflowPodCorrelation:
    name: str | None = None
    uid: str | None = None
    namespace: str | None = None
    image_digest: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> AirflowPodCorrelation:
        if value is None:
            return cls()
        payload = _mapping(value, field="pod")
        _reject_unknown(payload, _POD_FIELDS, field="pod")
        return cls(
            name=_optional_text(payload.get("name"), field="pod.name", maximum=_MAX_RESOURCE_TEXT_LENGTH),
            uid=_optional_text(payload.get("uid"), field="pod.uid"),
            namespace=_optional_text(
                payload.get("namespace"),
                field="pod.namespace",
                maximum=_MAX_RESOURCE_TEXT_LENGTH,
            ),
            image_digest=_optional_digest(payload.get("image_digest"), field="pod.image_digest"),
        )

    def to_dict(self) -> dict[str, str | None]:
        return {
            "name": self.name,
            "uid": self.uid,
            "namespace": self.namespace,
            "image_digest": self.image_digest,
        }


@dataclass(frozen=True, slots=True)
class AirflowCorrelation:
    correlation_id: str
    attempt_ref: str
    airflow: AirflowAttemptCorrelation
    dpone: DponeRunCorrelation
    artifacts: AirflowCorrelationArtifacts
    pod: AirflowPodCorrelation
    schema: str = AIRFLOW_CORRELATION_SCHEMA

    @property
    def missing_fields(self) -> tuple[str, ...]:
        values = {
            "dpone.run_id": self.dpone.run_id,
            "dpone.process": self.dpone.process,
            "artifacts.runtime_evidence_sha256": self.artifacts.runtime_evidence_sha256,
            "pod.name": self.pod.name,
            "pod.uid": self.pod.uid,
            "pod.namespace": self.pod.namespace,
            "pod.image_digest": self.pod.image_digest,
        }
        return tuple(sorted(field for field, value in values.items() if value is None))

    @property
    def complete(self) -> bool:
        return not self.missing_fields

    @property
    def openlineage_run_id(self) -> str:
        """Return a standards-compatible deterministic UUID for OpenLineage."""

        return str(uuid5(NAMESPACE_URL, f"urn:dpone:airflow-correlation:{self.correlation_id}"))

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "correlation_id": self.correlation_id,
            "attempt_ref": self.attempt_ref,
            "airflow": self.airflow.to_dict(),
            "dpone": self.dpone.to_dict(),
            "artifacts": self.artifacts.to_dict(),
            "pod": self.pod.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def build_airflow_correlation(
    *,
    run_identity: AirflowRunIdentity,
    attempt: Mapping[str, object] | AirflowAttemptCorrelation,
    dpone_run_id: str | None = None,
    dpone_process: str | None = None,
    runtime_evidence_sha256: str | None = None,
    pod: Mapping[str, object] | AirflowPodCorrelation | None = None,
) -> AirflowCorrelation:
    """Build one deterministic attempt correlation from verified observations."""

    attempt_model = (
        attempt if isinstance(attempt, AirflowAttemptCorrelation) else AirflowAttemptCorrelation.from_mapping(attempt)
    )
    pod_model = pod if isinstance(pod, AirflowPodCorrelation) else AirflowPodCorrelation.from_mapping(pod)
    _validate_cross_identity(run_identity=run_identity, attempt=attempt_model, pod=pod_model)
    artifacts = AirflowCorrelationArtifacts(
        release_id=run_identity.release_id,
        deployment_id=run_identity.deployment_id,
        workload_id=run_identity.workload_pack.id,
        workload_pack_sha256=run_identity.workload_pack.sha256,
        runtime_evidence_sha256=(
            None
            if runtime_evidence_sha256 is None
            else _optional_digest(runtime_evidence_sha256, field="artifacts.runtime_evidence_sha256")
        ),
    )
    correlation_id = _correlation_id(attempt_model, artifacts)
    correlation = AirflowCorrelation(
        correlation_id=correlation_id,
        attempt_ref=correlation_id,
        airflow=attempt_model,
        dpone=DponeRunCorrelation(
            run_id=_optional_text(dpone_run_id, field="dpone.run_id"),
            process=_optional_text(dpone_process, field="dpone.process", maximum=_MAX_RESOURCE_TEXT_LENGTH),
        ),
        artifacts=artifacts,
        pod=pod_model,
    )
    _require_bounded(correlation.to_json())
    return correlation


def parse_airflow_correlation(value: object) -> AirflowCorrelation:
    """Parse, bound and verify a serialized or mapping correlation object."""

    if isinstance(value, str):
        _require_bounded(value)
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise _invalid("correlation JSON is invalid") from exc
    payload = _mapping(value, field="correlation")
    _require_bounded(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    _reject_unknown(payload, _ROOT_FIELDS, field="correlation")
    if payload.get("schema") != AIRFLOW_CORRELATION_SCHEMA:
        raise _invalid(f"schema must be {AIRFLOW_CORRELATION_SCHEMA}")
    airflow = AirflowAttemptCorrelation.from_mapping(payload.get("airflow"))
    artifacts = AirflowCorrelationArtifacts.from_mapping(payload.get("artifacts"))
    expected = _correlation_id(airflow, artifacts)
    correlation_id = _digest(payload.get("correlation_id"), field="correlation_id")
    attempt_ref = _digest(payload.get("attempt_ref"), field="attempt_ref")
    if correlation_id != expected or attempt_ref != expected:
        raise _mismatch("correlation fingerprint does not match its canonical attempt identity")
    return AirflowCorrelation(
        correlation_id=correlation_id,
        attempt_ref=attempt_ref,
        airflow=airflow,
        dpone=DponeRunCorrelation.from_mapping(payload.get("dpone")),
        artifacts=artifacts,
        pod=AirflowPodCorrelation.from_mapping(payload.get("pod")),
    )


def _correlation_id(
    attempt: AirflowAttemptCorrelation,
    artifacts: AirflowCorrelationArtifacts,
) -> str:
    return canonical_fingerprint(
        {
            "schema": AIRFLOW_CORRELATION_SCHEMA,
            "airflow": attempt.to_dict(),
            "release_id": artifacts.release_id,
            "deployment_id": artifacts.deployment_id,
            "workload_id": artifacts.workload_id,
            "workload_pack_sha256": artifacts.workload_pack_sha256,
        }
    )


def _validate_cross_identity(
    *,
    run_identity: AirflowRunIdentity,
    attempt: AirflowAttemptCorrelation,
    pod: AirflowPodCorrelation,
) -> None:
    if run_identity.dag_spec is not None and run_identity.dag_spec.id != attempt.dag_id:
        raise _mismatch("Airflow DAG does not match the pinned DAG specification")
    if (
        pod.image_digest is not None
        and run_identity.runtime_image_digest is not None
        and pod.image_digest != run_identity.runtime_image_digest
    ):
        raise _mismatch("observed pod image does not match the pinned runtime image")


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"{field} must be an object")
    return value


def _reject_unknown(payload: Mapping[str, Any], allowed: frozenset[str], *, field: str) -> None:
    unknown = sorted(str(key) for key in payload if key not in allowed)
    if unknown:
        raise _invalid(f"{field} contains unsupported fields: {', '.join(unknown)}")


def _required_text(value: object, *, field: str, maximum: int = _MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(f"{field} must be a non-empty string")
    text = value.strip()
    if len(text) > maximum or any(char in text for char in ("\x00", "\n", "\r")):
        raise _invalid(f"{field} is outside the public text bounds")
    return text


def _optional_text(value: object, *, field: str, maximum: int = _MAX_TEXT_LENGTH) -> str | None:
    if value is None or value == "":
        return None
    return _required_text(value, field=field, maximum=maximum)


def _integer(value: object, *, field: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _invalid(f"{field} must be an integer >= {minimum}")
    return value


def _digest(value: object, *, field: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise _invalid(f"{field} must be a canonical sha256 digest")
    return str(value)


def _optional_digest(value: object, *, field: str) -> str | None:
    if value is None or value == "":
        return None
    return _digest(value, field=field)


def _require_bounded(value: str) -> None:
    if len(value.encode("utf-8")) > MAX_AIRFLOW_CORRELATION_BYTES:
        raise _invalid("serialized correlation exceeds 32 KiB")


def _invalid(message: str) -> AirflowCorrelationError:
    return AirflowCorrelationError("DPONE_AIRFLOW_CORRELATION_INVALID", message)


def _mismatch(message: str) -> AirflowCorrelationError:
    return AirflowCorrelationError("DPONE_AIRFLOW_CORRELATION_MISMATCH", message)


__all__ = [
    "AIRFLOW_CORRELATION_SCHEMA",
    "MAX_AIRFLOW_CORRELATION_BYTES",
    "AirflowAttemptCorrelation",
    "AirflowCorrelation",
    "AirflowCorrelationArtifacts",
    "AirflowCorrelationError",
    "AirflowPodCorrelation",
    "DponeRunCorrelation",
    "build_airflow_correlation",
    "parse_airflow_correlation",
]
