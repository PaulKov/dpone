"""Canonical identities for the ephemeral workload-index projection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.project_discovery import DISCOVERY_WORKLOAD_IDENTITY_FIELDS
from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.project_identity import (
    is_sha256_digest,
    project_identity_fingerprint,
)

_WORKLOAD_IDENTITY_SCHEMA = "dpone.discovery-workload-identity.v1"
_PROJECT_IDENTITY_SCHEMA = "dpone.discovery-project-identity.v1"


def normalized_pipeline_id(value: object) -> str | None:
    """Return one canonical pipeline id without exposing parser exceptions."""

    if not isinstance(value, str):
        return None
    try:
        return str(PipelineId.parse(value))
    except PipelineIdError:
        return None


def normalized_domain_id(value: object) -> str | None:
    """Return one canonical domain id without exposing parser exceptions."""

    if not isinstance(value, str):
        return None
    try:
        return str(DomainId.parse(value))
    except DomainIdError:
        return None


def is_canonical_sha256(value: object) -> bool:
    """Return whether a value is one lowercase canonical SHA-256 digest."""

    return isinstance(value, str) and value == value.lower() and is_sha256_digest(value)


def workload_fingerprint(item: Mapping[str, Any]) -> str:
    """Fingerprint every semantic field of one closed workload entry."""

    return project_identity_fingerprint(
        {
            "schema": _WORKLOAD_IDENTITY_SCHEMA,
            "workload": {field: item[field] for field in DISCOVERY_WORKLOAD_IDENTITY_FIELDS},
        }
    )


def project_fingerprint(
    *,
    layout_mode: str,
    layout_root: str,
    pipeline_id_scope: str,
    workloads: Sequence[Mapping[str, Any]],
) -> str:
    """Fingerprint project layout identity and sorted workload identities."""

    identities = sorted(
        (
            {
                "pipeline_id": item["pipeline_id"],
                "workload_fingerprint": item["workload_fingerprint"],
            }
            for item in workloads
        ),
        key=lambda item: str(item["pipeline_id"]),
    )
    return project_identity_fingerprint(
        {
            "schema": _PROJECT_IDENTITY_SCHEMA,
            "layout": {
                "mode": layout_mode,
                "root": layout_root,
                "pipeline_id_scope": pipeline_id_scope,
            },
            "workloads": identities,
        }
    )


__all__ = [
    "is_canonical_sha256",
    "normalized_domain_id",
    "normalized_pipeline_id",
    "project_fingerprint",
    "workload_fingerprint",
]
