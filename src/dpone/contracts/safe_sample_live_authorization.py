"""Pure path contract for deployment-scoped safe-sample authorization."""

from __future__ import annotations

import re
from pathlib import PurePosixPath

AUTHORIZATION_OVERLAY_PROFILE = "deployment_scoped_v1"
AUTHORIZATION_FILE_NAMES = (
    ("route_attestation", "route-attestation.json"),
    ("route_attestation_bundle", "route-attestation.sigstore.json"),
    ("route_certification_bundle", "route-certification-bundle.json"),
    ("route_attestation_policy", "route-attestation-policy.json"),
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PATH_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_RESERVED_PATH_COMPONENTS = frozenset({"current"})


class SafeSampleLiveAuthorizationPathError(ValueError):
    """Reject an identity that cannot form one bounded cache path."""


def authorization_overlay_relative_path(deployment_id: str, pipeline_id: str) -> PurePosixPath:
    """Return a `current`-free path derived only from pinned safe identities."""

    deployment = str(deployment_id or "")
    pipeline = str(pipeline_id or "")
    if not _DIGEST_RE.fullmatch(deployment):
        raise SafeSampleLiveAuthorizationPathError("deployment_id must be a complete lowercase sha256 digest")
    if not _PATH_COMPONENT_RE.fullmatch(pipeline) or pipeline.lower() in _RESERVED_PATH_COMPONENTS:
        raise SafeSampleLiveAuthorizationPathError("pipeline_id must be one safe path component")
    return PurePosixPath("route-authorizations", deployment.replace(":", "-"), pipeline)


def authorization_overlay_files(deployment_id: str, pipeline_id: str) -> dict[str, PurePosixPath]:
    """Return the four existing trust artifacts beneath one pinned overlay."""

    root = authorization_overlay_relative_path(deployment_id, pipeline_id)
    return {field: root / filename for field, filename in AUTHORIZATION_FILE_NAMES}


__all__ = [
    "AUTHORIZATION_FILE_NAMES",
    "AUTHORIZATION_OVERLAY_PROFILE",
    "SafeSampleLiveAuthorizationPathError",
    "authorization_overlay_files",
    "authorization_overlay_relative_path",
]
