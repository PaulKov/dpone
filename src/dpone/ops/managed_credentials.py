"""Credential readiness evidence without exposing credential values."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_VENDOR_LIVE_ENV = (
    "APPSFLYER_API_TOKEN",
    "GOOGLE_ADS_DEVELOPER_TOKEN",
    "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON",
    "MINDBOX_SECRET_KEY",
    "SIMILARWEB_API_KEY",
    "YANDEX_WEBMASTER_OAUTH_TOKEN",
    "OPENEXCHANGERATES_APP_ID",
)


@dataclass(frozen=True, slots=True)
class ManagedCredentialReadinessReport:
    profile: str
    passed: bool
    present: tuple[str, ...]
    missing: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str
    schema_version: str = "dpone.managed_credentials.readiness.v1"

    @property
    def blockers(self) -> tuple[str, ...]:
        return ("missing_credentials",) if self.missing else tuple()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "profile": self.profile,
            "passed": self.passed,
            "present": list(self.present),
            "missing": list(self.missing),
            "blockers": list(self.blockers),
            "redaction": "values_not_collected",
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Managed credential readiness",
            "",
            f"- Profile: `{self.profile}`",
            f"- Passed: `{self.passed}`",
            "- Redaction: `values_not_collected`",
            "",
            "| credential env | status |",
            "|---|---|",
        ]
        for name in self.present:
            lines.append(f"| `{name}` | present |")
        for name in self.missing:
            lines.append(f"| `{name}` | missing |")
        lines.append("")
        return "\n".join(lines)


class ManagedCredentialReadinessService:
    """Build readiness evidence from environment shape only."""

    def check(
        self,
        *,
        output_dir: str | Path,
        profile: str = "vendor_live",
        required_env: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ManagedCredentialReadinessReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        required = _required_env(profile=profile, required_env=required_env)
        source = env if env is not None else os.environ
        present = tuple(name for name in required if str(source.get(name) or "").strip())
        missing = tuple(name for name in required if name not in set(present))
        json_path = directory / "managed_credentials_readiness.json"
        markdown_path = directory / "managed_credentials_readiness.md"
        report = ManagedCredentialReadinessReport(
            profile=profile,
            passed=not missing,
            present=present,
            missing=missing,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _required_env(*, profile: str, required_env: Sequence[str] | None) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(str(item).strip() for item in (required_env or ()) if str(item).strip()))
    if values:
        return values
    if profile == "vendor_live":
        return DEFAULT_VENDOR_LIVE_ENV
    return tuple()


__all__ = [
    "DEFAULT_VENDOR_LIVE_ENV",
    "ManagedCredentialReadinessReport",
    "ManagedCredentialReadinessService",
]
