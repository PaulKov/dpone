"""Pure interpretation of verified dbt workload archives and workflow DAGs.

File acquisition and framework pack fingerprint checks belong to the service
wrapper. This contract receives bytes/mappings, never reads a path or imports
Airflow/dbt SDKs.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import io
import tarfile
import zlib
from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import DbtPublishingError, sha256_bytes
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

_MAX_EXECUTION_ARCHIVE_BYTES = 2 * 1024 * 1024
_MAX_EXECUTION_PACK_BYTES = 1024 * 1024
_DBT_EXECUTION_PACK_PATH = "runtime/dbt-execution-pack.json"


class DbtDevEvidenceReleaseError(ValueError):
    """The compiled release cannot define trustworthy evidence expectations."""


def dbt_execution_from_pack(
    pack: Mapping[str, object],
    *,
    expected_runtime_payload_ids: tuple[str, ...] | None = None,
    wire_contract: str = DBT_RUNTIME_WIRE_V1,
) -> DbtExecutionPack:
    """Bind an already fingerprint-verified workload to its execution contract."""

    if expected_runtime_payload_ids is not None and pack.get("runtime_payload_ids") != list(
        expected_runtime_payload_ids
    ):
        raise DbtDevEvidenceReleaseError("embedded workload trio differs from release sources")
    execution_bytes = runtime_payload_member(
        pack,
        expected_path=_DBT_EXECUTION_PACK_PATH,
        max_archive_bytes=_MAX_EXECUTION_ARCHIVE_BYTES,
        max_member_bytes=_MAX_EXECUTION_PACK_BYTES,
        label="execution pack",
    )
    try:
        execution = DbtExecutionPack.from_mapping(release_object(execution_bytes, "dbt execution pack"))
    except DbtPublishingError as exc:
        raise DbtDevEvidenceReleaseError("dbt execution pack contract is invalid") from exc
    try:
        execution.require_wire_contract(wire_contract)
    except DbtPublishingError as exc:
        raise DbtDevEvidenceReleaseError("execution pack differs from release wire version") from exc
    return execution


def runtime_payload_member(
    pack: Mapping[str, object],
    *,
    expected_path: str,
    max_archive_bytes: int,
    max_member_bytes: int,
    label: str,
) -> bytes:
    """Read one exact bounded regular member from an embedded runtime archive."""

    if (
        not expected_path
        or not label
        or isinstance(max_archive_bytes, bool)
        or isinstance(max_member_bytes, bool)
        or not isinstance(max_archive_bytes, int)
        or not isinstance(max_member_bytes, int)
        or max_archive_bytes <= 0
        or max_member_bytes <= 0
    ):
        raise ValueError("runtime payload member bounds and identity must be explicit")
    payload = release_mapping(pack.get("runtime_payload"), "runtime payload")
    if set(payload) != {"schema", "archive"} or payload.get("schema") != "dpone.airflow-runtime-payload.v1":
        raise DbtDevEvidenceReleaseError("runtime payload contract is invalid")
    archive = release_mapping(payload.get("archive"), "runtime payload archive")
    if (
        set(archive) != {"encoding", "format", "sha256", "bytes", "data"}
        or archive.get("encoding") != "base64"
        or archive.get("format") != "tar+gzip"
    ):
        raise DbtDevEvidenceReleaseError("runtime payload archive contract is invalid")
    raw_data = archive.get("data")
    declared_bytes = archive.get("bytes")
    if (
        not isinstance(raw_data, str)
        or len(raw_data) > (max_archive_bytes * 4 // 3) + 8
        or isinstance(declared_bytes, bool)
        or not isinstance(declared_bytes, int)
        or declared_bytes <= 0
        or declared_bytes > max_archive_bytes
    ):
        raise DbtDevEvidenceReleaseError("runtime payload archive exceeds its bounded contract")
    try:
        compressed = base64.b64decode(raw_data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise DbtDevEvidenceReleaseError("runtime payload archive encoding is invalid") from exc
    if base64.b64encode(compressed).decode("ascii") != raw_data:
        raise DbtDevEvidenceReleaseError("runtime payload archive encoding is noncanonical")
    if len(compressed) != declared_bytes or sha256_bytes(compressed) != archive.get("sha256"):
        raise DbtDevEvidenceReleaseError("runtime payload archive identity is invalid")
    try:
        # Bound *all* expanded bytes before tarfile interprets GNU/PAX headers.
        # One member needs its body, header/end blocks and at most record padding.
        expanded_limit = max_member_bytes + 2 * tarfile.RECORDSIZE
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
            expanded = stream.read(expanded_limit + 1)
        if len(expanded) > expanded_limit:
            raise DbtDevEvidenceReleaseError("runtime payload expanded archive exceeds its bounded contract")
        with tarfile.open(fileobj=io.BytesIO(expanded), mode="r:") as archive:
            member = archive.next()
            if (
                member is None
                or member.name != expected_path
                or not member.isreg()
                or member.size <= 0
                or member.size > max_member_bytes
                or member.pax_headers
            ):
                raise DbtDevEvidenceReleaseError(f"runtime payload contains an invalid {label} member")
            stream = archive.extractfile(member)
            if stream is None:
                raise DbtDevEvidenceReleaseError(f"runtime payload {label} is unavailable")
            body = stream.read(max_member_bytes + 1)
            if archive.next() is not None:
                raise DbtDevEvidenceReleaseError(f"runtime payload contains multiple {label} members")
    except DbtDevEvidenceReleaseError:
        raise
    except (OSError, EOFError, tarfile.TarError, zlib.error) as exc:
        raise DbtDevEvidenceReleaseError("runtime payload archive is invalid") from exc
    if len(body) != member.size:
        raise DbtDevEvidenceReleaseError(f"runtime payload {label} size differs")
    return body


@dataclass(frozen=True, slots=True)
class ExpectedWorkflowDag:
    dag_id: str
    workload_ids: tuple[str, ...]
    terminal_task_ids: tuple[str, ...]


def decode_dbt_workflow_dag(
    payload_bytes: bytes,
    *,
    dag_id: str,
    workload_packs: Mapping[str, Mapping[str, object]],
) -> tuple[str, ExpectedWorkflowDag]:
    """Validate one DAG's workflow, complete node inventory and terminal tasks."""

    payload = release_object(payload_bytes, "DAG spec")
    source = payload.get("source")
    nodes = payload.get("nodes")
    outcome = payload.get("workflow_outcome")
    workflow = source.get("workflow") if isinstance(source, Mapping) else None
    if (
        payload.get("dag_id") != dag_id
        or not isinstance(workflow, str)
        or not workflow
        or not isinstance(nodes, list)
        or not isinstance(outcome, Mapping)
        or set(outcome)
        != {
            "schema",
            "task_id",
            "workflow_id",
            "expected_terminal_task_ids",
        }
        or outcome.get("schema") != "dpone.dbt-workflow-outcome.v1"
        or outcome.get("workflow_id") != workflow
        or not release_text(outcome.get("task_id"), "workflow outcome task id")
    ):
        raise DbtDevEvidenceReleaseError("DAG spec workflow identity is invalid")
    workload_ids = tuple(
        sorted(
            {
                str(item.get("workload_id"))
                for item in nodes
                if isinstance(item, Mapping) and isinstance(item.get("workload_id"), str)
            }
        )
    )
    if not workload_ids or len(workload_ids) != len(nodes) or any(item not in workload_packs for item in workload_ids):
        raise DbtDevEvidenceReleaseError("DAG spec workload inventory is invalid")
    return workflow, ExpectedWorkflowDag(
        dag_id=dag_id,
        workload_ids=workload_ids,
        terminal_task_ids=_terminal_task_ids(outcome.get("expected_terminal_task_ids")),
    )


def require_dbt_workflow_dag(
    workflow_dags: Mapping[str, ExpectedWorkflowDag],
    workflow_id: str,
    dbt_workload_id: str,
) -> ExpectedWorkflowDag:
    workflow_dag = workflow_dags.get(workflow_id)
    if workflow_dag is None or dbt_workload_id not in workflow_dag.workload_ids:
        raise DbtDevEvidenceReleaseError("dbt workflow pack is not bound to exactly one DAG spec")
    return workflow_dag


def _terminal_task_ids(value: object) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item.strip() for item in value)
        or len(set(value)) != len(value)
    ):
        raise DbtDevEvidenceReleaseError("DAG spec terminal task identities are invalid")
    return tuple(sorted(value))


def release_object(value: bytes, field: str) -> Mapping[str, object]:
    try:
        payload = strict_json_object(value)
    except (StrictJsonError, RecursionError) as exc:
        raise DbtDevEvidenceReleaseError(f"{field} is invalid JSON") from exc
    return payload


def release_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DbtDevEvidenceReleaseError(f"{field} must be an object")
    return value


def release_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise DbtDevEvidenceReleaseError(f"{field} is invalid")
    return value


def release_digest(value: object, field: str) -> str:
    text = release_text(value, field)
    if not is_canonical_sha256_digest(text):
        raise DbtDevEvidenceReleaseError(f"{field} is invalid")
    return text
