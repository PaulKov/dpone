"""Canonical request and provenance helpers for dbt dev evidence bundles."""

from __future__ import annotations

import re
import stat
from pathlib import Path

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_dev_evidence_campaign import (
    DBT_DEV_EVIDENCE_REQUEST_FILENAME,
    campaign_request_bytes,
    canonical_json_bytes,
)
from dpone.contracts.dbt_dev_evidence_campaign import (
    campaign_request_sha256 as _campaign_request_sha256,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
    DbtDevEvidenceRequestError,
)

DBT_DEV_EVIDENCE_PROVENANCE_FILENAME = "provenance.json"
_FULL_COMMIT = re.compile(r"[0-9a-f]{40}")
_SAFE_PROVENANCE_TEXT = re.compile(r"[^\x00-\x1f\x7f]{1,2048}")
_PROVENANCE_FIELDS_V1 = frozenset(
    {
        "schema",
        "release_id",
        "deployment_id",
        "producer_repository",
        "producer_workflow",
        "source_commit",
    }
)
_PROVENANCE_FIELDS_V2 = frozenset(
    {
        "schema",
        "release_id",
        "deployment_id",
        "evidence_set_id",
        "campaign_request_sha256",
        "campaign_controller_repository",
        "campaign_controller_workflow",
        "campaign_source_commit",
        "orchestration_run_id",
        "orchestration_run_attempt",
        "finalizer_repository",
        "finalizer_workflow",
        "finalizer_source_commit",
    }
)


class DbtDevEvidenceProvenanceError(ValueError):
    """Evidence provenance or its authorizing request is not trustworthy."""


def write_provenance(
    root: Path,
    *,
    release_id: str,
    deployment_id: str,
    evidence_set_id: str | None,
    campaign_request: DbtDevEvidenceRequest | None,
    producer_repository: str,
    producer_workflow: str,
    source_commit: str,
) -> None:
    """Write versioned provenance without overloading controller identity."""

    if campaign_request is None:
        payload: dict[str, object] = {
            "schema": "dpone.dbt-dev-evidence-provenance.v1",
            "release_id": release_id,
            "deployment_id": deployment_id,
            "producer_repository": producer_repository,
            "producer_workflow": producer_workflow,
            "source_commit": source_commit,
        }
    else:
        payload = {
            "schema": "dpone.dbt-dev-evidence-provenance.v2",
            "release_id": release_id,
            "deployment_id": deployment_id,
            "evidence_set_id": evidence_set_id,
            "campaign_request_sha256": campaign_request_sha256(campaign_request),
            "campaign_controller_repository": campaign_request.producer_repository,
            "campaign_controller_workflow": campaign_request.producer_workflow,
            "campaign_source_commit": campaign_request.source_commit,
            "orchestration_run_id": campaign_request.orchestration_run_id,
            "orchestration_run_attempt": campaign_request.orchestration_run_attempt,
            "finalizer_repository": producer_repository,
            "finalizer_workflow": producer_workflow,
            "finalizer_source_commit": source_commit,
        }
    (root / DBT_DEV_EVIDENCE_PROVENANCE_FILENAME).write_bytes(json_bytes(payload))


def read_provenance(root: Path) -> dict[str, object]:
    """Read strict bounded provenance."""

    try:
        raw = read_confined_file(
            root,
            DBT_DEV_EVIDENCE_PROVENANCE_FILENAME,
            max_bytes=64 * 1024,
        )
        return strict_json_object(raw)
    except (ConfinedFileError, StrictJsonError) as exc:
        raise DbtDevEvidenceProvenanceError("dev evidence provenance is missing, unsafe, or invalid") from exc


def require_provenance_identity(
    payload: dict[str, object],
    *,
    expected_release_id: str,
    expected_deployment_id: str,
    expected_evidence_set_id: str | None,
    campaign_request: DbtDevEvidenceRequest | None,
) -> None:
    """Require provenance to bind the expected release, deployment and request."""

    if campaign_request is None:
        repository = payload.get("producer_repository")
        workflow = payload.get("producer_workflow")
        commit = payload.get("source_commit")
        valid = (
            set(payload) == _PROVENANCE_FIELDS_V1
            and payload.get("schema") == "dpone.dbt-dev-evidence-provenance.v1"
            and isinstance(repository, str)
            and isinstance(workflow, str)
            and isinstance(commit, str)
        )
    else:
        repository = payload.get("finalizer_repository")
        workflow = payload.get("finalizer_workflow")
        commit = payload.get("finalizer_source_commit")
        valid = (
            set(payload) == _PROVENANCE_FIELDS_V2
            and payload.get("schema") == "dpone.dbt-dev-evidence-provenance.v2"
            and payload.get("evidence_set_id") == expected_evidence_set_id
            and payload.get("campaign_request_sha256") == campaign_request_sha256(campaign_request)
            and payload.get("campaign_controller_repository") == campaign_request.producer_repository
            and payload.get("campaign_controller_workflow") == campaign_request.producer_workflow
            and payload.get("campaign_source_commit") == campaign_request.source_commit
            and payload.get("orchestration_run_id") == campaign_request.orchestration_run_id
            and payload.get("orchestration_run_attempt") == campaign_request.orchestration_run_attempt
            and isinstance(repository, str)
            and isinstance(workflow, str)
            and isinstance(commit, str)
        )
    if (
        not valid
        or payload.get("release_id") != expected_release_id
        or payload.get("deployment_id") != expected_deployment_id
    ):
        raise DbtDevEvidenceProvenanceError("dev evidence provenance identity differs")
    if not isinstance(repository, str) or not isinstance(workflow, str) or not isinstance(commit, str):
        raise DbtDevEvidenceProvenanceError("dev evidence provenance identity differs")
    validate_provenance(repository, workflow, commit)


def read_campaign_request(
    root: Path,
    *,
    required: bool,
) -> DbtDevEvidenceRequest | None:
    """Read one optional request without confusing missing files with unsafe files."""

    path = root / DBT_DEV_EVIDENCE_REQUEST_FILENAME
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if required:
            raise DbtDevEvidenceProvenanceError("dev evidence campaign request is missing") from None
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise DbtDevEvidenceProvenanceError("dev evidence campaign request is unsafe")
    try:
        payload = read_confined_file(
            root,
            DBT_DEV_EVIDENCE_REQUEST_FILENAME,
            max_bytes=1024 * 1024,
        )
        return DbtDevEvidenceRequest.from_mapping(strict_json_object(payload))
    except (
        ConfinedFileError,
        DbtDevEvidenceRequestError,
        StrictJsonError,
    ) as exc:
        raise DbtDevEvidenceProvenanceError("dev evidence campaign request is invalid") from exc


def validate_campaign_argument(
    request: DbtDevEvidenceRequest | None,
    *,
    release_id: str,
    deployment_id: str,
    evidence_set_id: str | None,
) -> None:
    """Reject evidence-set identities that are not authorized by a request."""

    if request is None:
        if evidence_set_id is not None:
            raise DbtDevEvidenceProvenanceError("an evidence set requires its campaign request")
        return
    if (
        request.release_id != release_id
        or request.deployment_id != deployment_id
        or request.evidence_set_id != evidence_set_id
    ):
        raise DbtDevEvidenceProvenanceError("dev evidence campaign request identity differs")


def campaign_request_sha256(request: DbtDevEvidenceRequest) -> str:
    """Compatibility export for the canonical campaign request digest."""

    return _campaign_request_sha256(request)


def json_bytes(payload: dict[str, object]) -> bytes:
    """Return bounded deterministic JSON bytes."""

    return canonical_json_bytes(payload)


def validate_identity(value: str, field: str) -> None:
    if not is_canonical_sha256_digest(value):
        raise DbtDevEvidenceProvenanceError(f"{field} identity is invalid")


def validate_provenance(
    repository: str,
    workflow: str,
    source_commit: str,
) -> None:
    if (
        repository.strip() != repository
        or _SAFE_PROVENANCE_TEXT.fullmatch(repository) is None
        or workflow.strip() != workflow
        or _SAFE_PROVENANCE_TEXT.fullmatch(workflow) is None
        or _FULL_COMMIT.fullmatch(source_commit) is None
    ):
        raise DbtDevEvidenceProvenanceError("dev evidence provenance is invalid")


__all__ = [
    "DBT_DEV_EVIDENCE_PROVENANCE_FILENAME",
    "DBT_DEV_EVIDENCE_REQUEST_FILENAME",
    "DbtDevEvidenceProvenanceError",
    "campaign_request_bytes",
    "campaign_request_sha256",
    "json_bytes",
    "read_campaign_request",
    "read_provenance",
    "require_provenance_identity",
    "validate_campaign_argument",
    "validate_identity",
    "validate_provenance",
    "write_provenance",
]
