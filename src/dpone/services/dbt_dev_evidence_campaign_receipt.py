"""Terminal closure contract for one dbt dev-evidence campaign."""

from __future__ import annotations

import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.dbt_dev_evidence_campaign import (
    DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
    campaign_request_sha256,
    canonical_json_bytes,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest

DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_SCHEMA = "dpone.dbt-dev-evidence-campaign-outcome.v1"
_FIELDS = frozenset(
    {
        "schema",
        "closed",
        "status",
        "passed",
        "code",
        "evidence_set_id",
        "release_id",
        "deployment_id",
        "campaign_request_sha256",
        "workflow_states",
    }
)
_WORKFLOW_FIELDS = frozenset({"workflow_id", "state"})
_STATES = frozenset({"queued", "running", "success", "failed", "unknown"})
_STATUSES = frozenset({"passed", "failed", "abandoned"})


class DbtDevEvidenceCampaignReceiptError(ValueError):
    """Campaign closure cannot prove one complete immutable attempt."""


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceCampaignReceipt:
    """Deterministic aggregate terminal state for one authorized campaign."""

    evidence_set_id: str
    release_id: str
    deployment_id: str
    campaign_request_sha256: str
    status: str
    code: str
    workflow_states: tuple[tuple[str, str], ...]
    schema: str = DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_SCHEMA

    @property
    def passed(self) -> bool:
        return (
            self.status == "passed"
            and bool(self.workflow_states)
            and all(state == "success" for _, state in self.workflow_states)
        )

    @classmethod
    def build(
        cls,
        *,
        request: DbtDevEvidenceRequest,
        status: str,
        code: str,
        workflow_states: Mapping[str, str],
    ) -> DbtDevEvidenceCampaignReceipt:
        receipt = cls(
            evidence_set_id=request.evidence_set_id,
            release_id=request.release_id,
            deployment_id=request.deployment_id,
            campaign_request_sha256=campaign_request_sha256(request),
            status=status,
            code=code,
            workflow_states=tuple(sorted(workflow_states.items())),
        )
        receipt._validate(request=request)
        return receipt

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        request: DbtDevEvidenceRequest,
    ) -> DbtDevEvidenceCampaignReceipt:
        try:
            if (
                set(value) != _FIELDS
                or value.get("schema") != DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_SCHEMA
                or value.get("closed") is not True
                or isinstance(value.get("passed"), str)
            ):
                raise ValueError("campaign outcome fields are invalid")
            raw_states = value.get("workflow_states")
            if not isinstance(raw_states, list) or not raw_states:
                raise ValueError("campaign outcome states are invalid")
            states = tuple(_state_from_mapping(item) for item in raw_states)
            receipt = cls(
                evidence_set_id=_text(value.get("evidence_set_id")),
                release_id=_text(value.get("release_id")),
                deployment_id=_text(value.get("deployment_id")),
                campaign_request_sha256=_text(value.get("campaign_request_sha256")),
                status=_text(value.get("status")),
                code=_text(value.get("code")),
                workflow_states=states,
            )
            receipt._validate(request=request)
            if value.get("passed") is not receipt.passed:
                raise ValueError("campaign outcome passed flag differs")
            return receipt
        except (TypeError, ValueError) as exc:
            raise DbtDevEvidenceCampaignReceiptError("dev evidence campaign outcome is invalid") from exc

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "closed": True,
            "status": self.status,
            "passed": self.passed,
            "code": self.code,
            "evidence_set_id": self.evidence_set_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "campaign_request_sha256": self.campaign_request_sha256,
            "workflow_states": [
                {"workflow_id": workflow_id, "state": state} for workflow_id, state in self.workflow_states
            ],
        }

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    def _validate(self, *, request: DbtDevEvidenceRequest) -> None:
        request_workflows = tuple(sorted(item.workflow_id for item in request.workflows))
        actual_workflows = tuple(item[0] for item in self.workflow_states)
        if (
            self.status not in _STATUSES
            or not self.code.startswith("DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_")
            or self.evidence_set_id != request.evidence_set_id
            or self.release_id != request.release_id
            or self.deployment_id != request.deployment_id
            or self.campaign_request_sha256 != campaign_request_sha256(request)
            or actual_workflows != request_workflows
            or len(set(actual_workflows)) != len(actual_workflows)
            or any(state not in _STATES for _, state in self.workflow_states)
            or (self.status == "passed" and not self.passed)
            or (self.status != "passed" and self.passed)
        ):
            raise DbtDevEvidenceCampaignReceiptError("dev evidence campaign outcome identity differs")


def read_campaign_receipt(
    root: Path,
    *,
    request: DbtDevEvidenceRequest,
) -> DbtDevEvidenceCampaignReceipt:
    """Read the required aggregate closure from a confined evidence set."""

    path = root / DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise DbtDevEvidenceCampaignReceiptError("dev evidence campaign outcome is missing") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise DbtDevEvidenceCampaignReceiptError("dev evidence campaign outcome is unsafe")
    try:
        payload = strict_json_object(
            read_confined_file(
                root,
                DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
                max_bytes=1024 * 1024,
            )
        )
        return DbtDevEvidenceCampaignReceipt.from_mapping(
            payload,
            request=request,
        )
    except (
        ConfinedFileError,
        StrictJsonError,
        DbtDevEvidenceCampaignReceiptError,
    ) as exc:
        raise DbtDevEvidenceCampaignReceiptError("dev evidence campaign outcome is invalid") from exc


def _state_from_mapping(value: object) -> tuple[str, str]:
    if not isinstance(value, Mapping) or set(value) != _WORKFLOW_FIELDS:
        raise ValueError("campaign workflow state is invalid")
    return _text(value.get("workflow_id")), _text(value.get("state"))


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or len(value.encode("utf-8")) > 2048:
        raise ValueError("campaign outcome text is invalid")
    return value


__all__ = [
    "DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME",
    "DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_SCHEMA",
    "DbtDevEvidenceCampaignReceipt",
    "DbtDevEvidenceCampaignReceiptError",
    "read_campaign_receipt",
]
