"""Stable DAG-spec identity, issue, and redaction contracts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DAG_SPEC_KIND = "gitops.airflow_dag_spec"
DAG_SPEC_SCHEMA_VERSION = "1"
DEFAULT_MAX_DAG_SPEC_BYTES = 8 * 1024 * 1024
_ADVISORY_FIELDS = frozenset({"spec_fingerprint", "warnings"})
_SENSITIVE_KEY = r"password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key|private[_-]?key|authorization"
_SENSITIVE_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>\b(?:{_SENSITIVE_KEY})\b[\"']?\s*[:=]\s*)"
    r"(?:(?P<quote>[\"'])(?P<quoted>[^\r\n]*?)(?P=quote)|(?P<bare>[^\s,;}\]\r\n]+))"
)
_URI_USERINFO_RE = re.compile(r"(?i)(?P<prefix>\b[a-z][a-z0-9+.-]*://)(?P<userinfo>[^/\s?#]*@)")
_BEARER_RE = re.compile(r"(?i)(?P<prefix>\bauthorization\s*[:=]\s*bearer\s+|\bbearer\s+)(?P<token>[^\s,;]+)")


@dataclass(frozen=True)
class DagSpecLoadIssue:
    code: str
    message: str
    path: str

    def to_jsonable(self) -> dict[str, str]:
        return {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "airflow_parse",
            "severity": "error",
            "message": redact_parse_error_message(self.message),
            "path": self.path,
        }


def compute_dag_spec_fingerprint(payload: Mapping[str, Any]) -> str:
    """Return the compatibility fingerprint for one declarative DAG spec."""

    canonical = {key: value for key, value in payload.items() if key not in _ADVISORY_FIELDS}
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def redact_parse_error_message(message: object) -> str:
    """Remove common credential material before scheduler-visible output."""

    redacted = _URI_USERINFO_RE.sub(lambda match: f"{match.group('prefix')}[REDACTED]@", str(message))
    redacted = _BEARER_RE.sub(lambda match: f"{match.group('prefix')}[REDACTED]", redacted)
    return _SENSITIVE_VALUE_RE.sub(_redacted_sensitive_value, redacted)


def _redacted_sensitive_value(match: re.Match[str]) -> str:
    quote = match.group("quote") or ""
    return f"{match.group('prefix')}{quote}[REDACTED]{quote}"


__all__ = [
    "DAG_SPEC_KIND",
    "DAG_SPEC_SCHEMA_VERSION",
    "DEFAULT_MAX_DAG_SPEC_BYTES",
    "DagSpecLoadIssue",
    "compute_dag_spec_fingerprint",
    "redact_parse_error_message",
]
