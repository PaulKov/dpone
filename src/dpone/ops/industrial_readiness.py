"""Industrial readiness evidence aggregation for release-grade dpone operation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import production_artifact_payload_passed
from dpone.ops.checksums import sha256_file

DEFAULT_INDUSTRIAL_DOMAINS: tuple[str, ...] = (
    "local_matrix",
    "correctness",
    "reliability",
    "performance_lab",
    "ux",
    "governance",
    "schema_evolution",
)


@dataclass(frozen=True, slots=True)
class IndustrialDomainItem:
    domain: str
    path: str | None
    passed: bool
    sha256: str
    summary: str
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IndustrialMatrixSummary:
    total_cases: int
    min_row_count: int
    max_row_count: int
    max_column_count: int
    required_cases: tuple[str, ...]
    covered_cases: tuple[str, ...]
    missing_cases: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.missing_cases

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IndustrialReadinessReport:
    release: str
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    items: tuple[IndustrialDomainItem, ...]
    matrix: IndustrialMatrixSummary
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
            "items": [item.to_dict() for item in self.items],
            "matrix": self.matrix.to_dict(),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone industrial readiness report",
            "",
            f"- Release: `{self.release}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Evidence domains",
            "",
            "| domain | status | artifact | summary | blockers |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "passed" if item.passed else "failed"
            artifact = f"`{item.path}`" if item.path else "missing"
            blockers = ", ".join(f"`{blocker}`" for blocker in item.blockers) if item.blockers else "-"
            lines.append(f"| `{item.domain}` | {status} | {artifact} | {item.summary} | {blockers} |")
        lines.extend(
            [
                "",
                "## Local matrix summary",
                "",
                f"- Total cases: `{self.matrix.total_cases}`",
                f"- Min row count: `{self.matrix.min_row_count}`",
                f"- Max row count: `{self.matrix.max_row_count}`",
                f"- Max column count: `{self.matrix.max_column_count}`",
                f"- Required cases: `{len(self.matrix.required_cases)}`",
                f"- Missing cases: `{len(self.matrix.missing_cases)}`",
            ]
        )
        if self.blockers:
            lines.extend(["", "## Fix industrial readiness blockers", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Re-run the specialized workflow that produced each failed domain artifact.",
                "2. For missing matrix cases, run the focused source -> sink -> strategy case locally or in the manual matrix workflow.",
                "3. For correctness failures, inspect row-count, checksum, NULL/empty-string, and type-fidelity evidence before approving a release.",
                "4. For reliability failures, verify locks, retries, resumability, idempotency, and state commit ordering.",
                "5. Re-run `dpone ops industrial-readiness` only after the specialized evidence is updated.",
                "",
            ]
        )
        return "\n".join(lines)


class IndustrialReadinessService:
    """Aggregates local matrix and production maturity evidence into one industrial gate."""

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        release: str,
        artifacts: Mapping[str, str | Path],
        required_domains: tuple[str, ...] = DEFAULT_INDUSTRIAL_DOMAINS,
        required_matrix_cases: tuple[str, ...] = (),
    ) -> IndustrialReadinessReport:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        payloads = {domain: _read_payload(Path(path)) for domain, path in artifacts.items()}
        items = tuple(
            _domain_item(domain=domain, artifact_path=artifacts.get(domain), payload=payloads.get(domain))
            for domain in dict.fromkeys((*required_domains, *artifacts.keys()))
        )
        matrix = _matrix_summary(payloads.get("local_matrix") or {}, required_matrix_cases)
        blockers = _blockers(items, matrix)
        passed = not blockers
        report = IndustrialReadinessReport(
            release=release,
            passed=passed,
            level=_level(score=_score(items, required_domains), blockers=blockers),
            score=_score(items, required_domains),
            blockers=blockers,
            items=items,
            matrix=matrix,
            output_dir=str(output_path),
            json_path=str(output_path / "industrial_readiness.json"),
            markdown_path=str(output_path / "industrial_readiness.md"),
        )
        (output_path / "industrial_readiness.json").write_text(report.to_json(), encoding="utf-8")
        (output_path / "industrial_readiness.md").write_text(report.to_markdown(), encoding="utf-8")
        return report


def _domain_item(
    *, domain: str, artifact_path: str | Path | None, payload: dict[str, Any] | None
) -> IndustrialDomainItem:
    if artifact_path is None or payload is None:
        return IndustrialDomainItem(
            domain=domain,
            path=None,
            passed=False,
            sha256="0" * 64,
            summary="Required evidence artifact is missing.",
            blockers=(f"{domain}.missing",),
        )
    path = Path(artifact_path)
    passed = _passed(domain, payload)
    blockers = tuple(str(item) for item in payload.get("blockers", []) if str(item))
    return IndustrialDomainItem(
        domain=domain,
        path=str(path),
        passed=passed,
        sha256=sha256_file(path),
        summary=_summary(payload),
        blockers=blockers,
    )


def _read_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {"passed": False, "blockers": ["artifact.unreadable"]}
    return payload if isinstance(payload, dict) else {"passed": False, "blockers": ["artifact.non_object"]}


def _passed(domain: str, payload: dict[str, Any]) -> bool:
    return production_artifact_payload_passed(payload, name=domain)


def _summary(payload: dict[str, Any]) -> str:
    value = payload.get("summary") or payload.get("message")
    if isinstance(value, str) and value.strip():
        return value.strip()
    if "cases" in payload:
        cases = payload.get("cases")
        if isinstance(cases, list):
            return f"{len(cases)} matrix cases reported."
    return "Evidence artifact parsed."


def _matrix_summary(payload: dict[str, Any], required_cases: tuple[str, ...]) -> IndustrialMatrixSummary:
    cases = payload.get("cases", [])
    normalized_cases = (
        [_normalize_case(item) for item in cases if isinstance(item, dict)] if isinstance(cases, list) else []
    )
    covered = tuple(sorted({case_id for case_id, _, _ in normalized_cases if case_id}))
    missing = tuple(case for case in required_cases if case not in covered)
    row_counts = [row_count for _, row_count, _ in normalized_cases if row_count is not None]
    column_counts = [column_count for _, _, column_count in normalized_cases if column_count is not None]
    return IndustrialMatrixSummary(
        total_cases=len(normalized_cases),
        min_row_count=min(row_counts) if row_counts else 0,
        max_row_count=max(row_counts) if row_counts else 0,
        max_column_count=max(column_counts) if column_counts else 0,
        required_cases=required_cases,
        covered_cases=covered,
        missing_cases=missing,
    )


def _normalize_case(payload: dict[str, Any]) -> tuple[str, int | None, int | None]:
    source = str(payload.get("source", "")).strip()
    sink = str(payload.get("sink", "")).strip()
    strategy = str(payload.get("strategy", "")).strip()
    return (
        f"{source}:{sink}:{strategy}",
        _optional_int(payload.get("row_count")),
        _optional_int(payload.get("wide_columns")),
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str | int | float | bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _blockers(items: tuple[IndustrialDomainItem, ...], matrix: IndustrialMatrixSummary) -> tuple[str, ...]:
    result: list[str] = []
    for item in items:
        if not item.passed:
            if item.path is None:
                result.append(f"{item.domain}.missing")
            else:
                result.append(f"{item.domain}.not_passed")
    result.extend(f"local_matrix.case_missing:{case}" for case in matrix.missing_cases)
    return tuple(dict.fromkeys(result))


def _score(items: tuple[IndustrialDomainItem, ...], required_domains: tuple[str, ...]) -> float:
    required = tuple(dict.fromkeys(required_domains))
    if not required:
        return 100.0
    by_domain = {item.domain: item for item in items}
    passed = sum(1 for domain in required if by_domain.get(domain) and by_domain[domain].passed)
    return round((passed / len(required)) * 100, 2)


def _level(*, score: float, blockers: tuple[str, ...]) -> str:
    if not blockers:
        return "industrial_ready"
    if score >= 80:
        return "release_candidate"
    return "blocked"
