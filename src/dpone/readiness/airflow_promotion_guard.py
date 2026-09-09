"""File-backed precondition for fenced Airflow cache promotion."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from dpone.readiness.airflow_self_service_cache_sync import PromotionPrecondition
from dpone.runtime.deployment_cache import DeploymentCacheError

_CANONICAL_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_GUARD_BYTES = 64 * 1024


class PromotionPreconditionError(ValueError):
    """Raised when a promotion precondition cannot be constructed safely."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def file_digest_precondition(
    path: str | Path,
    *,
    expected_sha256: str,
) -> PromotionPrecondition:
    """Require one bounded control file to retain its exact reviewed bytes."""

    guard_path = Path(path)
    if not _CANONICAL_SHA256.fullmatch(expected_sha256):
        raise PromotionPreconditionError(
            "DPONE_PROMOTION_PRECONDITION_INVALID",
            "promotion guard digest must be a canonical SHA-256 identity",
        )

    def check() -> bool:
        try:
            with guard_path.open("rb") as handle:
                payload = handle.read(_MAX_GUARD_BYTES + 1)
        except OSError as exc:
            raise DeploymentCacheError(
                "DPONE_PROMOTION_PRECONDITION_UNAVAILABLE",
                "promotion guard file is unavailable",
                path=guard_path.as_posix(),
            ) from exc
        if len(payload) > _MAX_GUARD_BYTES:
            raise DeploymentCacheError(
                "DPONE_PROMOTION_PRECONDITION_INVALID",
                "promotion guard file exceeds 64 KiB",
                path=guard_path.as_posix(),
            )
        actual = "sha256:" + hashlib.sha256(payload).hexdigest()
        return actual == expected_sha256

    return PromotionPrecondition(
        check=check,
        code="DPONE_PROMOTION_PRECONDITION_CHANGED",
        message="promotion guard changed before current activation",
    )


__all__ = ["PromotionPreconditionError", "file_digest_precondition"]
