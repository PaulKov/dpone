"""Validate persisted campaign authority before provider evidence export."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    DevEvidenceStoreError,
)
from dpone_airflow_pack.strict_json import loads_strict_json_object

CAMPAIGN_REQUEST_FILENAME = "campaign-request.json"
CAMPAIGN_REQUEST_SCHEMA = "dpone.dbt-dev-evidence-request.v1"
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_REQUEST_FIELDS = frozenset(
    {
        "schema",
        "evidence_set_id",
        "release_id",
        "deployment_id",
        "producer_repository",
        "producer_workflow",
        "source_commit",
        "orchestration_run_id",
        "orchestration_run_attempt",
        "workflows",
    }
)
_WORKFLOW_FIELDS = frozenset({"workflow_id", "dag_id", "dag_run_id"})


class DevEvidenceAuthorityError(ValueError):
    """Persisted request does not authorize the current exact DAG run."""


def require_persisted_campaign_authority(
    store: DevEvidenceConfinedStore,
    *,
    base: tuple[str, ...],
    authority: Mapping[str, Any],
    workflow_id: str,
    release_id: str,
    deployment_id: str,
    dag_id: str,
    dag_run_id: str,
) -> str:
    """Prove conf against a create-only request stored before trigger."""

    try:
        request = loads_strict_json_object(
            store.read_payload(
                directory_parts=base,
                filename=CAMPAIGN_REQUEST_FILENAME,
            ).decode("utf-8")
        )
        evidence_set_id = _request_identity(request)
        expected_workflow = {
            "workflow_id": workflow_id,
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
        }
        workflows = request["workflows"]
        if (
            request.get("release_id") != release_id
            or request.get("deployment_id") != deployment_id
            or expected_workflow not in workflows
            or len([item for item in workflows if item.get("workflow_id") == workflow_id]) != 1
            or dict(authority)
            != {
                "schema": "dpone.dbt-dev-evidence-authority.v1",
                "request_id": evidence_set_id,
                "evidence_set_id": evidence_set_id,
                "release_id": release_id,
                "deployment_id": deployment_id,
                "workflow_id": workflow_id,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
            }
        ):
            raise DevEvidenceAuthorityError("dev evidence authority differs from persisted request")
        return evidence_set_id
    except DevEvidenceAuthorityError:
        raise
    except (
        DevEvidenceStoreError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise DevEvidenceAuthorityError("persisted dev evidence campaign request is invalid") from exc


def _request_identity(request: Mapping[str, Any]) -> str:
    workflows = request.get("workflows")
    if (
        set(request) != _REQUEST_FIELDS
        or request.get("schema") != CAMPAIGN_REQUEST_SCHEMA
        or not isinstance(workflows, list)
        or not workflows
        or any(not isinstance(item, Mapping) or set(item) != _WORKFLOW_FIELDS for item in workflows)
        or workflows
        != sorted(
            workflows,
            key=lambda item: (
                str(item["workflow_id"]),
                str(item["dag_id"]),
            ),
        )
    ):
        raise DevEvidenceAuthorityError("persisted dev evidence campaign request is malformed")
    evidence_set_id = request.get("evidence_set_id")
    if not isinstance(evidence_set_id, str) or _DIGEST.fullmatch(evidence_set_id) is None:
        raise DevEvidenceAuthorityError("persisted dev evidence set identity is invalid")
    unsigned = {key: value for key, value in request.items() if key not in {"evidence_set_id", "workflows"}}
    unsigned["workflows"] = [
        {
            "workflow_id": item["workflow_id"],
            "dag_id": item["dag_id"],
        }
        for item in workflows
    ]
    if _canonical_sha256(unsigned) != evidence_set_id:
        raise DevEvidenceAuthorityError("persisted dev evidence request fingerprint is invalid")
    for item in workflows:
        if item["dag_run_id"] != _dag_run_id(
            evidence_set_id,
            str(item["workflow_id"]),
        ):
            raise DevEvidenceAuthorityError("persisted dev evidence DAG-run identity is invalid")
    return evidence_set_id


def _dag_run_id(evidence_set_id: str, workflow_id: str) -> str:
    workflow_suffix = hashlib.sha256(workflow_id.encode("utf-8")).hexdigest()[:12]
    return f"dpone_evidence__{evidence_set_id.removeprefix('sha256:')[:20]}__{workflow_suffix}"


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


__all__ = [
    "CAMPAIGN_REQUEST_FILENAME",
    "DevEvidenceAuthorityError",
    "require_persisted_campaign_authority",
]
