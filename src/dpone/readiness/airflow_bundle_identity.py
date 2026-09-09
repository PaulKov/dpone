"""Parse-safe Airflow DAG Bundle identity helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_FULL_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class AirflowBundleIdentity:
    backend: str
    ref: str
    versioned: bool
    version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "ref": self.ref,
            "versioned": self.versioned,
            "version": self.version,
        }


def airflow_bundle_identity(ref: object) -> AirflowBundleIdentity | None:
    """Classify a deployment-index Airflow DAG Bundle reference without IO."""

    text = str(ref or "").strip()
    if not text:
        return None
    if text.startswith("git:"):
        candidate = text.removeprefix("git:").strip() or None
        versioned = bool(candidate and _FULL_GIT_COMMIT.fullmatch(candidate))
        return AirflowBundleIdentity(
            backend="git",
            ref=text,
            versioned=versioned,
            version=candidate if versioned else None,
        )
    if text.startswith("s3://"):
        return AirflowBundleIdentity(backend="s3", ref=text, versioned=False)
    if text.startswith(("gs://", "gcs://")):
        return AirflowBundleIdentity(backend="gcs", ref=text, versioned=False)
    if text.startswith("local:") or text.startswith("/"):
        return AirflowBundleIdentity(backend="local", ref=text, versioned=False)
    return AirflowBundleIdentity(backend="unknown", ref=text, versioned=False)


def airflow_bundle_versioning_check(identity: AirflowBundleIdentity | None) -> dict[str, str] | None:
    if identity is None:
        return None
    if identity.versioned:
        return {
            "code": "DPONE_AIRFLOW_BUNDLE_VERSIONED",
            "status": "passed",
            "message": "Airflow DAG Bundle reference is versioned and can participate in reproducible reruns.",
        }
    return {
        "code": "DPONE_RERUN_NOT_REPRODUCIBLE",
        "status": "warning",
        "message": (
            "Airflow DAG Bundle backend is not versioned; reproducible rerun requires a versioned backend "
            "or a dpone-managed content-addressed snapshot."
        ),
    }


__all__ = ["AirflowBundleIdentity", "airflow_bundle_identity", "airflow_bundle_versioning_check"]
