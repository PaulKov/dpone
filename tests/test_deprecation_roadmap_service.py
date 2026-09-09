from __future__ import annotations

from pathlib import Path

from dpone.services.docs.compatibility_registry import CompatibilityEntry
from dpone.services.docs.deprecation_roadmap import (
    ROADMAP_END,
    ROADMAP_START,
    analyze_deprecation_roadmap,
    is_deprecation_roadmap_doc_in_sync,
    render_deprecation_roadmap_block,
    sync_deprecation_roadmap_doc,
)


def test_analyze_deprecation_roadmap_detects_canonical_usage(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    (pkg / "deprecated_pkg").mkdir(parents=True)
    (pkg / "canonical_pkg").mkdir(parents=True)
    (pkg / "consumer").mkdir(parents=True)
    for path in [
        pkg / "__init__.py",
        pkg / "deprecated_pkg" / "__init__.py",
        pkg / "canonical_pkg" / "__init__.py",
        pkg / "consumer" / "__init__.py",
    ]:
        path.write_text("", encoding="utf-8")
    (pkg / "consumer" / "user.py").write_text("import dpone.deprecated_pkg\n", encoding="utf-8")
    entries = [
        CompatibilityEntry(
            deprecated="dpone.deprecated_pkg",
            canonical="dpone.canonical_pkg",
            scope="package",
            status="deprecated-shim",
            removal="later",
        )
    ]
    rows = analyze_deprecation_roadmap(entries, package_dir=pkg)
    assert rows[0].canonical_usage_count == 1
    assert rows[0].readiness.startswith("blocked-by-usage")


def test_deprecation_roadmap_sync_roundtrip(tmp_path: Path) -> None:
    row = CompatibilityEntry(
        deprecated="dpone.old",
        canonical="dpone.new",
        scope="module",
        status="transitional-shim",
        removal="later",
    )
    pkg = tmp_path / "src" / "dpone"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    rows = analyze_deprecation_roadmap([row], package_dir=pkg)
    rendered = render_deprecation_roadmap_block(rows)
    assert ROADMAP_START in rendered and ROADMAP_END in rendered
    doc = tmp_path / "deprecation-roadmap.md"
    doc.write_text(
        f"# Deprecation roadmap\n\n{ROADMAP_START}\nold\n{ROADMAP_END}\n",
        encoding="utf-8",
    )
    changed, _ = sync_deprecation_roadmap_doc(doc, rendered_block=rendered)
    assert changed is True
    assert is_deprecation_roadmap_doc_in_sync(doc, rendered_block=rendered) is True
