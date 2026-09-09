"""Layer-neutral contracts shared by dbt evidence campaign ports and adapters."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Protocol

DBT_DEV_EVIDENCE_REQUEST_FILENAME = "campaign-request.json"
DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME = "campaign-outcome.json"
MAX_DBT_DEV_EVIDENCE_WORKFLOWS = 200
MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY = 500
MAX_DBT_DEV_EVIDENCE_SOURCE_FILES = 3 * MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY
MAX_DBT_DEV_EVIDENCE_BUNDLE_FILES = MAX_DBT_DEV_EVIDENCE_SOURCE_FILES + 2


class DbtDevEvidenceRequestContract(Protocol):
    """Structural request contract required by persistence adapters."""

    @property
    def evidence_set_id(self) -> str:
        """Return the immutable campaign identity."""

    @property
    def release_id(self) -> str:
        """Return the immutable release identity."""

    @property
    def deployment_id(self) -> str:
        """Return the immutable deployment identity."""

    def to_dict(self) -> dict[str, object]:
        """Return the complete deterministic request payload."""


class DbtDevEvidenceCampaignReceiptContract(Protocol):
    """Structural terminal-receipt contract required by persistence adapters."""

    @property
    def evidence_set_id(self) -> str:
        """Return the immutable campaign identity."""

    @property
    def release_id(self) -> str:
        """Return the immutable release identity."""

    @property
    def deployment_id(self) -> str:
        """Return the immutable deployment identity."""

    def to_bytes(self) -> bytes:
        """Return the immutable deterministic receipt bytes."""


def canonical_json_bytes(payload: Mapping[str, object]) -> bytes:
    """Serialize a bounded contract payload deterministically."""

    return (
        json.dumps(
            dict(payload),
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def campaign_request_bytes(
    request: DbtDevEvidenceRequestContract,
) -> bytes:
    """Return exact deterministic bytes for one campaign request."""

    return canonical_json_bytes(request.to_dict())


def campaign_request_sha256(
    request: DbtDevEvidenceRequestContract,
) -> str:
    """Return the content identity of one canonical campaign request."""

    return "sha256:" + hashlib.sha256(campaign_request_bytes(request)).hexdigest()


__all__ = [
    "DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME",
    "DBT_DEV_EVIDENCE_REQUEST_FILENAME",
    "MAX_DBT_DEV_EVIDENCE_BUNDLE_FILES",
    "MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY",
    "MAX_DBT_DEV_EVIDENCE_SOURCE_FILES",
    "MAX_DBT_DEV_EVIDENCE_WORKFLOWS",
    "DbtDevEvidenceCampaignReceiptContract",
    "DbtDevEvidenceRequestContract",
    "campaign_request_bytes",
    "campaign_request_sha256",
    "canonical_json_bytes",
]
