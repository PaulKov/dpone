from __future__ import annotations

import subprocess
from pathlib import Path

from dpone.metrics.module_size import ModuleSizeThresholds, analyze_module_sizes
from dpone.metrics.module_size_snapshot import load_module_size_head_snapshot


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def test_exact_snapshot_includes_tracked_python_under_ignored_name_directory(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    package = repo / "src/dpone"
    governed = package / "test/oversized.py"
    governed.parent.mkdir(parents=True)
    governed.write_text("\n".join(f"value_{index} = {index}" for index in range(701)) + "\n", encoding="utf-8")
    baseline = repo / "docs/module_size_baseline.json"
    baseline.parent.mkdir(parents=True)
    baseline.write_text('{"debt": {}, "schema_version": 2}\n', encoding="utf-8")
    budgets = repo / "docs/benchmarks/quality_budgets.yml"
    budgets.parent.mkdir(parents=True)
    budgets.write_text("global:\n  max_loc: 600\n  max_sloc: 400\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "tracked production module")
    head = _git(repo, "rev-parse", "HEAD")

    snapshot = load_module_size_head_snapshot(
        repo_root=repo,
        package_dir=package,
        baseline_path=baseline,
        head_sha=head,
    )
    report = analyze_module_sizes(
        package,
        repo_root=repo,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
        source_bytes_by_path=snapshot.source_bytes_by_path,
    )

    assert "src/dpone/test/oversized.py" in snapshot.source_bytes_by_path
    assert report.ok is False
    assert report.issues[0].message.startswith("Module violates hard max")
