"""Production maturity evidence gate.

The service intentionally aggregates already-produced evidence instead of
running heavy checks itself. CDC, benchmark, security, supply-chain,
governance, and docs gates stay independently reproducible; route production
authority remains a separate cryptographically verified decision.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed, production_artifact_payload_passed
from dpone.ops.checksums import sha256_file

DEFAULT_REQUIRED_DOMAINS: tuple[str, ...] = (
    "cdc",
    "performance",
    "security",
    "supply_chain",
    "governance",
    "docs",
)


@dataclass(frozen=True, slots=True)
class ProductionMaturityItem:
    domain: str
    path: str
    required: bool
    missing: bool
    passed: bool
    sha256: str
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ProductionMaturityReport:
    release: str
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    required_domains: tuple[str, ...]
    items: tuple[ProductionMaturityItem, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "required_domains": list(self.required_domains),
            "items": [item.to_dict() for item in self.items],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone production maturity gate",
            "",
            f"- Release: `{self.release}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            f"- Required domains: `{', '.join(self.required_domains)}`",
            "",
            "| domain | required | status | sha256 | summary | path |",
            "|---|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "missing" if item.missing else ("pass" if item.passed else "fail")
            lines.append(
                f"| `{item.domain}` | `{item.required}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        if self.blockers:
            lines.extend(["", "## Fix production maturity blockers", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        if self.warnings:
            lines.extend(["", "## Warnings", ""])
            lines.extend(f"- `{warning}`" for warning in self.warnings)
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Re-run the red upstream gate first; do not edit this aggregate report by hand.",
                "2. If a required artifact is missing, regenerate the upstream evidence and re-run this command.",
                "3. If a domain is red, open that domain artifact and follow its dedicated runbook.",
                "4. Attach `production_maturity.json` and `production_maturity.md` to release review.",
                "",
            ]
        )
        return "\n".join(lines)


class ProductionMaturityService:
    """Aggregates industrial readiness evidence into one go/no-go report."""

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        release: str,
        artifacts: Mapping[str, str | Path],
        required_domains: Sequence[str] = DEFAULT_REQUIRED_DOMAINS,
    ) -> ProductionMaturityReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        required = tuple(dict.fromkeys(required_domains))
        domains = tuple(dict.fromkeys((*required, *sorted(artifacts))))
        items = tuple(self._item(domain, artifacts.get(domain), required=domain in required) for domain in domains)
        blockers = tuple(self._blocker(item) for item in items if item.required and (item.missing or not item.passed))
        warnings = tuple(
            self._warning(item) for item in items if not item.required and (item.missing or not item.passed)
        )
        score = self._score(items)
        json_path = directory / "production_maturity.json"
        markdown_path = directory / "production_maturity.md"
        report = ProductionMaturityReport(
            release=release,
            passed=not blockers,
            level=self._level(score, blockers),
            score=score,
            blockers=blockers,
            warnings=warnings,
            required_domains=required,
            items=items,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, domain: str, path_value: str | Path | None, *, required: bool) -> ProductionMaturityItem:
        if path_value is None:
            return self._missing_item(domain, required=required, path="")
        path = Path(path_value)
        if not path.is_file():
            return self._missing_item(domain, required=required, path=str(path))
        payload = self._payload(path)
        return ProductionMaturityItem(
            domain=domain,
            path=str(path),
            required=required,
            missing=False,
            passed=production_artifact_payload_passed(payload, name=domain),
            sha256=sha256_file(path),
            summary=self._summary(payload),
        )

    @staticmethod
    def _missing_item(domain: str, *, required: bool, path: str) -> ProductionMaturityItem:
        return ProductionMaturityItem(
            domain=domain,
            path=path,
            required=required,
            missing=True,
            passed=False,
            sha256="0" * 64,
            summary="artifact missing",
        )

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if path.suffix.lower() != ".json":
            return {"passed": True, "summary": "non-json artifact exists"}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"passed": False, "blockers": ["invalid_json"]}
        return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}

    @staticmethod
    def _passed(payload: Mapping[str, Any]) -> bool:
        return artifact_payload_passed(payload)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in (
            "blockers",
            "violations",
            "findings",
            "results",
            "items",
            "strategy_rows",
            "artifacts",
            "cases",
        ):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        for key in ("case_count", "evidence_count", "package_count", "links_checked", "status"):
            if key in payload:
                return f"{key}={payload[key]}"
        return "passed" if artifact_payload_passed(payload) else "failed"

    @staticmethod
    def _blocker(item: ProductionMaturityItem) -> str:
        suffix = "missing" if item.missing else "not_passed"
        return f"{item.domain}.{suffix}"

    @staticmethod
    def _warning(item: ProductionMaturityItem) -> str:
        suffix = "missing" if item.missing else "not_passed"
        return f"{item.domain}.{suffix}"

    @staticmethod
    def _score(items: tuple[ProductionMaturityItem, ...]) -> float:
        required = [item for item in items if item.required]
        if not required:
            return 100.0
        passed = sum(1 for item in required if not item.missing and item.passed)
        return round((passed / len(required)) * 100.0, 2)

    @staticmethod
    def _level(score: float, blockers: tuple[str, ...]) -> str:
        if not blockers:
            return "ga_ready"
        if score >= 80.0:
            return "release_candidate"
        return "blocked"
