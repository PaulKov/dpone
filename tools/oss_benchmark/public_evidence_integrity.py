"""Public artifact redaction and claim-evidence integrity for benchmarks."""

from __future__ import annotations

import copy
import re
from typing import Any

from tools.oss_benchmark.config import ROOT

_FORBIDDEN_PATTERNS = ("/Users/", "/private/tmp", "/tmp/data-platform-dpone", "data-platform-dpone")
_PUBLIC_PATH_TOKENS = ("$WORKSPACE", "$BENCHMARK_CACHE", "$TMP")


def sanitize_public_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a public-safe copy with machine-specific paths replaced by stable tokens."""

    return _sanitize_value(copy.deepcopy(payload))


def find_public_redaction_violations(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return any machine-specific strings still present in public evidence."""

    violations: list[dict[str, Any]] = []
    _collect_violations(payload, path="$", violations=violations)
    return violations


def build_public_evidence_integrity(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a public evidence-integrity score and claim evidence ledger."""

    violations = find_public_redaction_violations(payload)
    claims = _claim_evidence(payload)
    covered = sum(1 for claim in claims if claim["status"] == "covered")
    coverage = int(round(covered / len(claims) * 100)) if claims else 100
    violation_penalty = min(70, len(violations) * 20)
    missing_penalty = max(0, 100 - coverage)
    score = max(0, 100 - violation_penalty - missing_penalty)
    status = "verified" if score >= 95 and not violations else "watch" if score >= 70 else "blocked"
    return {
        "schema_version": 1,
        "status": status,
        "score": score,
        "redaction_violation_count": len(violations),
        "redaction_violations": violations[:20],
        "claim_coverage_percent": coverage,
        "claim_evidence": claims,
        "redaction_policy": {
            "forbidden_pattern_labels": [
                "home-directory paths",
                "temporary-directory paths",
                "workspace-specific repository names",
            ],
            "public_path_tokens": list(_PUBLIC_PATH_TOKENS),
            "policy": "Public benchmark artifacts must not expose local filesystem, temp, or workspace-specific paths.",
        },
    }


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, str):
        return _sanitize_string(value)
    return value


def _sanitize_string(value: str) -> str:
    root = ROOT.resolve().as_posix()
    cache_root = (ROOT / ".cache" / "oss-code-quality-benchmark").resolve().as_posix()
    sanitized = value.replace(f"{cache_root}/", "$BENCHMARK_CACHE/")
    sanitized = sanitized.replace(cache_root, "$BENCHMARK_CACHE")
    sanitized = sanitized.replace(root, "$WORKSPACE")
    sanitized = re.sub(
        r"/Users/[^\s/`\"']+/data-platform-dpone/\.cache/oss-code-quality-benchmark/", "$BENCHMARK_CACHE/", sanitized
    )
    sanitized = re.sub(r"/Users/[^\s/`\"']+/data-platform-dpone", "$WORKSPACE", sanitized)
    sanitized = re.sub(r"/private/tmp/[^\s`\"']*data-platform-dpone[^\s`\"']*", "$TMP", sanitized)
    sanitized = re.sub(r"/tmp/[^\s`\"']*data-platform-dpone[^\s`\"']*", "$TMP", sanitized)
    sanitized = sanitized.replace("$WORKSPACE/.cache/oss-code-quality-benchmark/", "$BENCHMARK_CACHE/")
    return sanitized


def _collect_violations(value: Any, *, path: str, violations: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _collect_violations(item, path=f"{path}.{key}", violations=violations)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _collect_violations(item, path=f"{path}[{index}]", violations=violations)
        return
    if not isinstance(value, str):
        return
    for pattern in _FORBIDDEN_PATTERNS:
        if pattern in value:
            violations.append({"path": path, "pattern": pattern, "value": value[:160]})


def _claim_evidence(payload: dict[str, Any]) -> list[dict[str, Any]]:
    checked_at = str(payload.get("generated_at") or (payload.get("run_context") or {}).get("generated_at") or "")
    return [
        _claim(
            "feature_parity_public_sources",
            "Feature Parity Matrix",
            "Feature claims are backed by public source references.",
            "vendor-public",
            "feature_parity.entries[].sources",
            checked_at,
            bool(payload.get("feature_parity")),
        ),
        _claim(
            "closed_core_comparator_scope",
            "Comparable OSS corpus",
            "Closed-core competitors are labeled as not code-comparable.",
            "vendor-public",
            "closed_core_notes[].url",
            checked_at,
            bool(payload.get("closed_core_notes")),
        ),
        _claim(
            "customer_trust_center_verified",
            "Customer Trust Center Snapshot",
            "Trust-center status is generated from benchmark evidence.",
            "derived",
            "trust_center.status",
            checked_at,
            bool(payload.get("trust_center")),
        ),
        _claim(
            "quality_gates_passed",
            "Quality Gates",
            "dpone quality gates are evaluated from merged benchmark evidence.",
            "derived",
            "quality_gates.status",
            checked_at,
            bool(payload.get("quality_gates")),
        ),
        _claim(
            "external_analyzer_execution",
            "Independent Analyzer Cross-Validation",
            "External analyzer execution metadata is retained in raw evidence.",
            "measured",
            "external_analyzer_results[]",
            checked_at,
            bool(payload.get("external_analyzer_results") or payload.get("independent_validation")),
        ),
        _claim(
            "dpone_position_static_quality",
            "dpone position",
            "dpone positioning is grounded in static quality, dependency and size metrics.",
            "derived",
            "projects[].quality, projects[].coupling, industrial_maintainability",
            checked_at,
            bool(payload.get("projects")),
        ),
    ]


def _claim(
    claim_id: str,
    section: str,
    claim: str,
    confidence: str,
    source: str,
    checked_at: str,
    covered: bool,
) -> dict[str, Any]:
    return {
        "claim_id": claim_id,
        "section": section,
        "claim": claim,
        "confidence": confidence,
        "source": source,
        "last_checked_at": checked_at,
        "status": "covered" if covered else "missing",
    }
