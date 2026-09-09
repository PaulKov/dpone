"""Confined JSON input for dbt dev-evidence CLI commands."""

from __future__ import annotations

import stat
from pathlib import Path

from dpone.contracts.strict_json import StrictJsonError, strict_json_object

_MAX_REQUEST_BYTES = 1024 * 1024


class DevEvidenceRequestFileError(ValueError):
    """A request path or JSON body is unsafe for local CLI consumption."""


def read_dev_evidence_request(path: Path) -> dict[str, object]:
    """Read one bounded regular request file with strict JSON semantics."""

    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_REQUEST_BYTES:
        raise DevEvidenceRequestFileError("dev evidence request file is unsafe")
    try:
        return strict_json_object(path.read_bytes())
    except StrictJsonError:
        raise DevEvidenceRequestFileError("dev evidence request file is invalid JSON") from None


__all__ = ["DevEvidenceRequestFileError", "read_dev_evidence_request"]
