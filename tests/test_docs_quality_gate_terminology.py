from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
LEGACY_TERM = re.compile(r"\bquality checks\b", re.IGNORECASE)

PUBLIC_ENTRYPOINTS = (
    DOCS / "index.md",
    DOCS / "architecture.md",
    DOCS / "cli-examples.md",
    DOCS / "load-strategies.md",
    DOCS / "ux.md",
    DOCS / "nested-normalization.md",
    DOCS / "testing" / "integration-matrix.md",
    DOCS / "testing" / "nested" / "index.md",
    DOCS / "getting-started" / "manifest-basics.md",
    DOCS / "google-ads.md",
    DOCS / "appsflyer.md",
    DOCS / "similarweb-rollout.md",
    DOCS / "strategy-intelligence.md",
    DOCS / "connector-sdk.md",
    DOCS / "cdc.md",
)


def test_user_facing_data_quality_language_uses_canonical_gates() -> None:
    paths = (*PUBLIC_ENTRYPOINTS, *sorted((DOCS / "source-sink").glob("*.md")))
    violations: list[str] = []

    for path in paths:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if LEGACY_TERM.search(line):
                violations.append(f"{path.relative_to(ROOT)}:{line_number}")

    assert violations == [], (
        "Use 'quality gates' in user-facing prose. Keep `quality.checks`, "
        "`quality_checks`, and literal CI job names only in their explicit "
        f"compatibility contexts: {violations}"
    )
