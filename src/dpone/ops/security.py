"""Security audit checks for manifests, logs, and ops artifacts."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

SECRET_KEY_PARTS = (
    "access_key",
    "api_key",
    "client_secret",
    "password",
    "private_key",
    "sasl_password",
    "schema_registry_password",
    "secret",
    "token",
)
SAFE_SECRET_PREFIXES = ("${", "{{", "env:", "vault:", "airflow:")
SAFE_SECRET_VALUES = {"", "[REDACTED]", "***", "****", "<redacted>", "redacted"}
ALLOWED_CONNECTION_TYPES = {"env", "airflow", "vault", "params"}


@dataclass(frozen=True, slots=True)
class SecurityFinding:
    code: str
    severity: str
    passed: bool
    message: str
    action: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SecurityAuditReport:
    findings: tuple[SecurityFinding, ...]

    @property
    def passed(self) -> bool:
        return not any(not finding.passed and finding.severity == "fail" for finding in self.findings)

    def to_dict(self) -> dict[str, object]:
        return {"passed": self.passed, "findings": [finding.to_dict() for finding in self.findings]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops security audit",
            "",
            f"- Passed: `{self.passed}`",
            "",
            "| check | status | action |",
            "|---|---|---|",
        ]
        for finding in self.findings:
            status = "pass" if finding.passed else finding.severity
            lines.append(f"| `{finding.code}` | {status} | {finding.action} |")
        return "\n".join(lines) + "\n"


class OpsSecurityAuditService:
    """Runs credential and redaction checks without connector dependencies."""

    def audit(
        self,
        *,
        manifest: Mapping[str, Any] | None = None,
        log_text: str | None = None,
    ) -> SecurityAuditReport:
        findings: list[SecurityFinding] = []
        if manifest is not None:
            self._check_manifest(manifest, findings)
        if log_text:
            self._check_logs(log_text, findings)
        if not findings:
            findings.append(
                SecurityFinding(
                    code="security.no_findings",
                    severity="pass",
                    passed=True,
                    message="No inline secrets, unsafe connection types, or unredacted log tokens were found.",
                    action="Keep using provider-backed credentials and redacted logs.",
                )
            )
        return SecurityAuditReport(findings=tuple(findings))

    def _check_manifest(self, manifest: Mapping[str, Any], findings: list[SecurityFinding]) -> None:
        for path, value in self._walk(manifest):
            key = path[-1] if path else ""
            if key == "connection_type":
                self._check_connection_type(path, value, findings)
            if self._is_secret_key(key) and self._is_unsafe_secret_value(value):
                dotted = ".".join(path)
                findings.append(
                    SecurityFinding(
                        code=f"manifest.inline_secret.{dotted}",
                        severity="fail",
                        passed=False,
                        message=f"Manifest contains an inline secret at {dotted}.",
                        action=(
                            "Move secret to env, Vault, Airflow, or external credential provider "
                            f"and replace `{dotted}` with a reference."
                        ),
                    )
                )

    def _check_connection_type(self, path: tuple[str, ...], value: Any, findings: list[SecurityFinding]) -> None:
        if str(value) not in ALLOWED_CONNECTION_TYPES:
            dotted = ".".join(path)
            findings.append(
                SecurityFinding(
                    code=f"manifest.unsafe_connection_type.{dotted}",
                    severity="fail",
                    passed=False,
                    message=f"Unsupported connection_type `{value}`.",
                    action=f"Use one of {', '.join(sorted(ALLOWED_CONNECTION_TYPES))} for `{dotted}`.",
                )
            )

    def _check_logs(self, log_text: str, findings: list[SecurityFinding]) -> None:
        patterns = {
            "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{16,}\b"),
            "pypi_token": re.compile(r"\bpypi-[A-Za-z0-9_.-]{16,}\b"),
            "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
            "password_assignment": re.compile(
                r"(?i)\b(password|token|secret|api[_-]?key)\s*[:=]\s*(?!\[REDACTED\])[^\\s,;]+"
            ),
            "url_credentials": re.compile(r"://[^\\s/:]+:[^\\s/@]+@"),
        }
        for name, pattern in patterns.items():
            if pattern.search(log_text):
                findings.append(
                    SecurityFinding(
                        code=f"log.unredacted_secret.{name}",
                        severity="fail",
                        passed=False,
                        message=f"Log text appears to contain an unredacted {name}.",
                        action="Redact logs before storing artifacts, CI output, or support bundles.",
                    )
                )

    def _walk(self, value: Any, prefix: tuple[str, ...] = ()) -> tuple[tuple[tuple[str, ...], Any], ...]:
        items: list[tuple[tuple[str, ...], Any]] = []
        if isinstance(value, Mapping):
            for key, child in value.items():
                path = (*prefix, str(key))
                items.append((path, child))
                items.extend(self._walk(child, path))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                items.extend(self._walk(child, (*prefix, str(index))))
        return tuple(items)

    @staticmethod
    def _is_secret_key(key: str) -> bool:
        normalized = key.lower()
        return any(part in normalized for part in SECRET_KEY_PARTS)

    @staticmethod
    def _is_unsafe_secret_value(value: Any) -> bool:
        if value is None:
            return False
        if not isinstance(value, str):
            return bool(value)
        stripped = value.strip()
        if stripped.lower() in SAFE_SECRET_VALUES:
            return False
        return not stripped.startswith(SAFE_SECRET_PREFIXES)
