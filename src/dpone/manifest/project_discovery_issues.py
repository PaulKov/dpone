"""Structured issue construction for domain-first discovery."""

from __future__ import annotations

from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.project_discovery_models import ProjectDiscoveryIssue


def discovery_issue(
    code: str,
    message: str,
    path: str | None = None,
    *,
    pipeline_id: str | None = None,
    domain: str | None = None,
) -> ProjectDiscoveryIssue:
    """Build one safe discovery issue."""

    return ProjectDiscoveryIssue(code, message, path, pipeline_id, domain)


def parse_domain(value: str, root: str) -> tuple[str, ProjectDiscoveryIssue | None]:
    """Parse one path-owned domain id without echoing unsafe source data."""

    try:
        return str(DomainId.parse(value)), None
    except DomainIdError:
        return value, discovery_issue(
            "DPONE_DOMAIN_ID_INVALID",
            "Domain directory name is not a canonical domain id.",
            f"{root}/{value}",
            domain=value,
        )


def state_changed_issue(
    path: str,
    *,
    pipeline_id: str | None = None,
    domain: str | None = None,
) -> ProjectDiscoveryIssue:
    """Report that a pinned discovery input changed before commit."""

    return discovery_issue(
        "DPONE_SELECTION_STATE_CHANGED",
        "A discovery input changed while the snapshot was being built.",
        path,
        pipeline_id=pipeline_id,
        domain=domain,
    )


__all__ = ["discovery_issue", "parse_domain", "state_changed_issue"]
