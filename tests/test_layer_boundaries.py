from __future__ import annotations

from pathlib import Path

from dpone.metrics.import_rules import evaluate_import_rules, format_import_rule_report_text

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dpone"


def test_default_import_rules_have_no_violations() -> None:
    report = evaluate_import_rules(SRC)
    assert report.ok, format_import_rule_report_text(report, package_dir=SRC)


def test_import_rules_detect_manifest_runtime_violation(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    (pkg / "manifest").mkdir(parents=True)
    (pkg / "runtime").mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "manifest" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "runtime" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "manifest" / "bad.py").write_text(
        "from dpone.runtime.connectors import base\n",
        encoding="utf-8",
    )

    report = evaluate_import_rules(pkg)
    assert not report.ok
    assert any(v.rule_id == "manifest-no-runtime-or-cli" for v in report.violations)
    assert any(v.imported.startswith("dpone.runtime") for v in report.violations)


def test_import_rules_detect_commands_importing_ops_internals(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    (pkg / "commands").mkdir(parents=True)
    (pkg / "ops").mkdir(parents=True)
    (pkg / "services" / "ops").mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "commands" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "ops" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "ops" / "release_gate.py").write_text("class ReleaseGateService: ...\n", encoding="utf-8")
    (pkg / "services" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "services" / "ops" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "services" / "ops" / "facade.py").write_text("class OpsCommandService: ...\n", encoding="utf-8")
    (pkg / "commands" / "bad.py").write_text(
        "from dpone.ops.release_gate import ReleaseGateService\n",
        encoding="utf-8",
    )
    (pkg / "commands" / "good.py").write_text(
        "from dpone.services.ops.facade import OpsCommandService\n",
        encoding="utf-8",
    )

    report = evaluate_import_rules(pkg)

    assert not report.ok
    assert any(v.rule_id == "commands-no-ops-internals" for v in report.violations)
    assert all("good.py" not in v.source_path for v in report.violations)
