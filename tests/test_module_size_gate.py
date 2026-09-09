from __future__ import annotations

import argparse
import logging
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import dpone.metrics.module_size_write as module_size_writer
import dpone.services.docs.check_module_size_service as module_size_service
from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.project_authoring_lock import project_authoring_lock
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.docs.check_module_size_cmd import register_parser
from dpone.metrics.module_size import (
    ModuleSizeBaselineEntry,
    ModuleSizeReport,
    ModuleSizeThresholds,
    analyze_module_sizes,
    format_module_size_report_jsonable,
)
from dpone.metrics.module_size import (
    load_module_size_baseline as load_legacy_baseline,
)
from dpone.metrics.module_size import (
    write_module_size_baseline as write_legacy_baseline,
)
from dpone.metrics.module_size_policy import (
    AUDITED_BOOTSTRAP_COMMIT,
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    ModuleSizeGitContext,
    write_module_size_baseline,
)
from dpone.metrics.module_size_write import ratchet_module_size_baseline
from dpone.services.docs.check_module_size_service import CheckModuleSizeService


def _write(path: Path, line_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(f"value_{i} = {i}" for i in range(line_count)) + "\n", encoding="utf-8")


def _entry(path: str, *, lines: int, sloc: int, commit: str = AUDITED_BOOTSTRAP_COMMIT) -> ModuleSizeDebtEntry:
    return ModuleSizeDebtEntry(
        path=path,
        max_lines=lines,
        max_sloc=sloc,
        owner="architecture",
        reason="audited legacy orchestration debt",
        target_sloc=min(350, sloc - 1),
        target_date=date(2099, 12, 31),
        accepted_adr=None,
        baseline_commit=commit,
    )


def _ctx(repo_root: Path) -> AppContext:
    return AppContext(
        settings=Settings(
            repo_root=repo_root,
            project_dir=repo_root,
            manifest_dir=repo_root / "etl-process-manifest",
            sources_registry_paths=(),
        ),
        logger=logging.getLogger("test.module_size"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def _write_budgets(repo: Path) -> None:
    budgets = repo / "docs/benchmarks/quality_budgets.yml"
    budgets.parent.mkdir(parents=True, exist_ok=True)
    budgets.write_text(
        "global:\n  warn_loc: 450\n  max_loc: 600\n  warn_sloc: 350\n  max_sloc: 400\n",
        encoding="utf-8",
    )


def _args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "format": "json",
        "package": "src/dpone",
        "baseline": "docs/module_size_baseline.json",
        "no_baseline": False,
        "write_baseline": False,
        "base_ref": None,
        "head_ref": None,
        "warn_lines": 450,
        "max_lines": 600,
        "warn_sloc": 350,
        "max_sloc": 400,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _init_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _write(repo / "src/dpone/legacy.py", 360)
    _write_budgets(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def _advance_head(repo: Path, message: str = "module-size comparison head") -> str:
    marker = repo / "README.md"
    marker.write_text(f"{message}\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-qm", message)
    return _git(repo, "rev-parse", "HEAD")


def test_hard_limits_cannot_be_waived_and_warning_debt_requires_exact_cap(tmp_path: Path) -> None:
    package = tmp_path / "src/dpone"
    _write(package / "hard.py", 401)
    _write(package / "grown.py", 361)
    _write(package / "shrunk.py", 359)
    _write(package / "unknown.py", 351)
    baseline = (
        _entry("src/dpone/hard.py", lines=401, sloc=401),
        _entry("src/dpone/grown.py", lines=360, sloc=360),
        _entry("src/dpone/shrunk.py", lines=360, sloc=360),
    )

    report = analyze_module_sizes(
        package,
        repo_root=tmp_path,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
        baseline=baseline,
        warning_debt_requires_baseline=True,
    )

    assert report.ok is False
    messages = {issue.path: issue.message for issue in report.issues}
    assert "hard max" in messages["src/dpone/hard.py"]
    assert "grew beyond exact baseline cap" in messages["src/dpone/grown.py"]
    assert "ratchet update required" in messages["src/dpone/shrunk.py"]
    assert "unbaselined warning debt" in messages["src/dpone/unknown.py"]


def test_legacy_public_baseline_api_round_trips_v1_and_remains_analyzable(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    entry = ModuleSizeBaselineEntry("src/dpone/legacy.py", 500, "architecture", "legacy debt", "below 450")
    write_legacy_baseline(path, (entry,))

    assert load_legacy_baseline(path) == (entry,)
    assert load_legacy_baseline(tmp_path / "missing.json") == ()
    _write(tmp_path / entry.path, 500)
    report = analyze_module_sizes(
        tmp_path / "src/dpone",
        repo_root=tmp_path,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=None, max_sloc=None),
        baseline=(entry,),
    )
    payload = format_module_size_report_jsonable(report)
    assert report.ok is True
    assert payload["schema_version"] == "dpone.module-size-report.v2"
    assert payload["debt_model"] == "legacy-v1"
    assert payload["allowlisted"] == [entry.__dict__]


def test_public_analyzer_rejects_mixed_debt_models_before_reading_sources(tmp_path: Path) -> None:
    legacy = ModuleSizeBaselineEntry("src/dpone/legacy.py", 500, "architecture", "legacy debt", "below 450")
    ratchet = _entry("src/dpone/ratchet.py", lines=360, sloc=360)

    with pytest.raises(ValueError, match="exactly one debt model"):
        analyze_module_sizes(
            tmp_path / "missing",
            repo_root=tmp_path,
            thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
            baseline=(legacy, ratchet),
        )


def test_module_size_thresholds_reject_invalid_public_values() -> None:
    invalid = (
        {"warn_lines": 0, "max_lines": 600, "warn_sloc": 350, "max_sloc": 400},
        {"warn_lines": 600, "max_lines": 600, "warn_sloc": 350, "max_sloc": 400},
        {"warn_lines": 450, "max_lines": 600, "warn_sloc": -1, "max_sloc": 400},
        {"warn_lines": 450, "max_lines": 600, "warn_sloc": 400, "max_sloc": 400},
    )
    for values in invalid:
        with pytest.raises(ValueError, match="threshold"):
            ModuleSizeThresholds(**values)


def test_public_analyzer_retains_advisory_warning_default(tmp_path: Path) -> None:
    package = tmp_path / "src/dpone"
    _write(package / "warning.py", 351)

    report = analyze_module_sizes(
        package,
        repo_root=tmp_path,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
    )

    assert report.ok is True
    assert report.issues[0].severity == "warn"
    assert report.debt_model == "none"


@pytest.mark.parametrize(
    "option,value",
    [("--baseline", ""), ("--warn-lines", "0"), ("--warn-lines", "-1"), ("--max-sloc", "invalid")],
)
def test_cli_parser_rejects_empty_baseline_and_invalid_thresholds(option: str, value: str) -> None:
    parser = argparse.ArgumentParser()
    register_parser(parser.add_subparsers(dest="command"))
    with pytest.raises(SystemExit) as raised:
        parser.parse_args(["check-module-size", option, value])
    assert raised.value.code == 2


def test_module_size_runbook_distinguishes_parser_errors_from_json_envelopes() -> None:
    guide = Path("docs/module-size-ratchet.md").read_text(encoding="utf-8")

    assert "non-positive thresholds such as `--warn-lines -1`" in guide
    assert "use argparse usage/error text on stderr" in guide
    assert "neither\n`dpone.module-size-error.v2` nor `dpone.error.v1` envelopes" in guide


def test_analyzer_rejects_clean_tracked_python_symlink_outside_repository(tmp_path: Path) -> None:
    repo, _head = _init_repo(tmp_path)
    outside = tmp_path / "outside.py"
    _write(outside, 10)
    link = repo / "src/dpone/nested/linked.py"
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)
    _git(repo, "add", "src/dpone/nested/linked.py")
    _git(repo, "commit", "-qm", "track unsafe symlink")
    assert _git(repo, "status", "--porcelain") == ""

    report = analyze_module_sizes(
        repo / "src/dpone",
        repo_root=repo,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
        baseline=(),
    )

    assert any(issue.path.endswith("nested/linked.py") and "symlink" in issue.message for issue in report.issues)


def test_analyzer_reports_invalid_utf8_as_bounded_failure(tmp_path: Path) -> None:
    package = tmp_path / "src/dpone"
    package.mkdir(parents=True)
    (package / "invalid.py").write_bytes(b"\xff\xfe")

    report = analyze_module_sizes(
        package,
        repo_root=tmp_path,
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
        baseline=(),
    )

    assert report.ok is False
    assert report.issues[0].message == "Module cannot be read as trusted UTF-8 source: UnicodeDecodeError"


def test_service_rejects_baseline_and_parent_symlinks_before_read(tmp_path: Path) -> None:
    _write(tmp_path / "src/dpone/ok.py", 10)
    _write_budgets(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    write_module_size_baseline(outside / "baseline.json", ModuleSizeBaseline(entries=()))
    link = tmp_path / "config"
    link.symlink_to(outside, target_is_directory=True)
    code, payload = CheckModuleSizeService(ctx=_ctx(tmp_path)).run(_args(baseline="config/baseline.json"))
    assert code == 2 and isinstance(payload, dict)
    assert "symlink" in payload["configuration_error"]


def test_check_module_size_service_requires_explicit_refs_for_v2(tmp_path: Path) -> None:
    package = tmp_path / "src/dpone"
    _write(package / "ok.py", 20)
    baseline = tmp_path / "docs/module_size_baseline.json"
    _write_budgets(tmp_path)
    write_module_size_baseline(baseline, ModuleSizeBaseline(entries=()))

    svc = CheckModuleSizeService(ctx=_ctx(tmp_path))
    code, payload = svc.run(_args())
    assert code == 2
    assert isinstance(payload, dict)
    assert payload["schema_version"] == "dpone.module-size-error.v2"
    assert "--base-ref and --head-ref" in payload["configuration_error"]


def test_v2_service_rejects_equal_refs_without_side_effects(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact baseline")
    head = _git(repo, "rev-parse", "HEAD")
    before = baseline_path.read_bytes()

    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=head, head_ref=head, write_baseline=True))

    assert code == 2 and isinstance(payload, dict)
    assert payload["schema_version"] == "dpone.module-size-error.v2"
    assert "must identify distinct commits" in payload["configuration_error"]
    assert baseline_path.read_bytes() == before
    assert list(baseline_path.parent.glob(f".{baseline_path.name}.*.tmp")) == []

    text_code, text_payload = CheckModuleSizeService(ctx=_ctx(repo)).run(
        _args(base_ref=head, head_ref=head, write_baseline=True, format="text")
    )
    assert text_code == 2 and isinstance(text_payload, str)
    assert "must identify distinct commits" in text_payload
    assert baseline_path.read_bytes() == before


def test_missing_package_error_does_not_expose_workstation_path(tmp_path: Path) -> None:
    code, payload = CheckModuleSizeService(ctx=_ctx(tmp_path)).run(
        _args(package="missing/package", baseline=None, no_baseline=True)
    )

    assert code == 2 and isinstance(payload, dict)
    assert "missing/package" in payload["configuration_error"]
    assert str(tmp_path) not in payload["configuration_error"]


def test_service_reports_verified_exact_base_and_head(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline")
    base = _git(repo, "rev-parse", "HEAD")
    head = _advance_head(repo)
    code, payload = CheckModuleSizeService(ctx=_ctx(repo), clock=lambda: date(2026, 8, 8)).run(
        _args(base_ref=base, head_ref=head)
    )
    assert code == 0
    assert isinstance(payload, dict)
    assert payload["base_sha"] == base
    assert payload["head_sha"] == head
    _write(repo / "src/dpone/legacy.py", 361)
    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head))
    assert code == 2
    assert isinstance(payload, dict)
    assert "differ from checked-out HEAD" in payload["configuration_error"]
    _write(repo / "src/dpone/legacy.py", 360)
    budget = repo / "docs/benchmarks/quality_budgets.yml"
    budget.write_text(budget.read_text(encoding="utf-8") + "# dirty\n", encoding="utf-8")
    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head))
    assert code == 2
    assert isinstance(payload, dict)
    assert "differ from checked-out HEAD" in payload["configuration_error"]
    _write_budgets(repo)
    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head, max_lines=601))
    assert code == 2
    assert isinstance(payload, dict)
    assert "cannot exceed authoritative" in payload["configuration_error"]


def test_service_consumes_exact_head_tree_not_scanner_visible_ignored_python(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    (repo / ".gitignore").write_text("src/dpone/.dpone/\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline and ignore policy")
    base = _git(repo, "rev-parse", "HEAD")
    head = _advance_head(repo)
    _write(repo / "src/dpone/.dpone/probe.py", 10)
    assert _git(repo, "status", "--porcelain", "--untracked-files=all") == ""

    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head))

    assert code == 0
    assert isinstance(payload, dict)
    assert all(item["path"] != "src/dpone/.dpone/probe.py" for item in payload["largest"])


def test_v2_service_rejects_warning_thresholds_looser_than_authoritative_budgets(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact baseline")
    base = _git(repo, "rev-parse", "HEAD")
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=()))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "attempt to hide warning debt")
    base = _git(repo, "rev-parse", "HEAD")
    head = _advance_head(repo)

    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(
        _args(base_ref=base, head_ref=head, warn_lines=599, warn_sloc=399)
    )

    assert code == 2 and isinstance(payload, dict)
    assert "warning thresholds cannot exceed authoritative" in payload["configuration_error"]


def test_service_rejects_deleted_tracked_module_missing_from_scanner_set(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    _write(repo / "src/dpone/unbaselined.py", 20)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline with ordinary module")
    base = _git(repo, "rev-parse", "HEAD")
    head = _advance_head(repo)
    (repo / "src/dpone/unbaselined.py").unlink()

    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head))

    assert code == 2
    assert isinstance(payload, dict)
    assert "inputs differ from checked-out HEAD" in payload["configuration_error"]


def test_service_analyzes_immutable_head_bytes_after_worktree_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact baseline")
    base = _git(repo, "rev-parse", "HEAD")
    head = _advance_head(repo)
    analyze = module_size_service.analyze_module_sizes

    def mutate_then_analyze(*args: Any, **kwargs: Any) -> ModuleSizeReport:
        _write(repo / "src/dpone/legacy.py", 401)
        return analyze(*args, **kwargs)

    monkeypatch.setattr(module_size_service, "analyze_module_sizes", mutate_then_analyze)
    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(_args(base_ref=base, head_ref=head))

    assert code == 0
    assert isinstance(payload, dict)
    legacy = next(item for item in payload["largest"] if item["path"] == "src/dpone/legacy.py")
    assert legacy == {"path": "src/dpone/legacy.py", "lines": 360, "sloc": 360}


def test_write_baseline_reports_noop_and_changed_candidate_without_false_success(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact baseline")
    base = _git(repo, "rev-parse", "HEAD")
    comparison_head = _advance_head(repo)
    before = baseline_path.read_bytes()
    code, payload = CheckModuleSizeService(ctx=_ctx(repo)).run(
        _args(base_ref=base, head_ref=comparison_head, write_baseline=True)
    )
    assert code == 2 and isinstance(payload, dict)
    assert (payload["status"], payload["changed"], baseline_path.read_bytes()) == ("NO_CHANGE", False, before)

    _write(repo / "src/dpone/legacy.py", 359)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "shrink debt")
    head = _git(repo, "rev-parse", "HEAD")
    code, payload = CheckModuleSizeService(ctx=_ctx(repo), authoring_lock=project_authoring_lock).run(
        _args(base_ref=comparison_head, head_ref=head, write_baseline=True)
    )
    assert code == 2 and isinstance(payload, dict)
    assert payload["schema_version"] == "dpone.module-size-baseline-write.v2"
    assert (payload["status"], payload["changed"]) == ("CANDIDATE_WRITTEN", True)
    assert baseline_path.read_bytes() != before


def test_service_does_not_overwrite_concurrent_baseline_edit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    original = ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),))
    write_module_size_baseline(baseline_path, original)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact baseline")
    base = _git(repo, "rev-parse", "HEAD")
    _write(repo / "src/dpone/legacy.py", 359)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "shrink debt")
    head = _git(repo, "rev-parse", "HEAD")
    concurrent = ModuleSizeBaseline(entries=(replace(original.entries[0], owner="concurrent-owner"),))
    real_write = module_size_writer.write_baseline_if_unchanged

    def race_then_write(**kwargs: Any) -> None:
        write_module_size_baseline(baseline_path, concurrent)
        real_write(**kwargs)

    monkeypatch.setattr(module_size_writer, "write_baseline_if_unchanged", race_then_write)
    code, payload = CheckModuleSizeService(ctx=_ctx(repo), authoring_lock=project_authoring_lock).run(
        _args(base_ref=base, head_ref=head, write_baseline=True)
    )

    assert code == 2 and isinstance(payload, dict)
    assert "baseline bytes changed" in payload["configuration_error"]
    assert baseline_path.read_text(encoding="utf-8").count("concurrent-owner") == 1


def test_baseline_writer_retires_missing_entry_only_after_exact_git_deletion(tmp_path: Path) -> None:
    package = tmp_path / "src/dpone"
    package.mkdir(parents=True)
    entry = _entry("src/dpone/deleted.py", lines=360, sloc=360)
    thresholds = ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400)
    report = analyze_module_sizes(package, repo_root=tmp_path, thresholds=thresholds, baseline=(entry,))
    context = ModuleSizeGitContext(
        base_sha=entry.baseline_commit,
        head_sha=entry.baseline_commit,
        previous_baseline=ModuleSizeBaseline(entries=(entry,)),
        exact_renames=(),
        deleted_paths=(),
        rename_only=False,
    )
    with pytest.raises(ModuleSizeBaselineError, match="without an exact Git deletion"):
        ratchet_module_size_baseline(
            ModuleSizeBaseline(entries=(entry,)),
            report=report,
            thresholds=thresholds,
            bootstrap=False,
            bootstrap_commit=AUDITED_BOOTSTRAP_COMMIT,
            git_context=context,
            policy_issues=(),
        )
    retired = ratchet_module_size_baseline(
        ModuleSizeBaseline(entries=(entry,)),
        report=report,
        thresholds=thresholds,
        bootstrap=False,
        bootstrap_commit=AUDITED_BOOTSTRAP_COMMIT,
        git_context=replace(context, deleted_paths=(entry.path,)),
        policy_issues=(),
    )
    assert retired.entries == ()


def test_no_baseline_mode_remains_available_for_scoped_hard_gates(tmp_path: Path) -> None:
    tools_dir = tmp_path / "tools/agent_policy"
    _write_budgets(tmp_path)
    _write(tools_dir / "oversized.py", 12)
    service = CheckModuleSizeService(ctx=_ctx(tmp_path))
    _write(tmp_path / "src/dpone/canonical.py", 1)
    code, payload = service.run(_args(baseline="", no_baseline=True))
    assert code == 2 and isinstance(payload, dict)
    assert "cannot scan canonical package src/dpone" in payload["configuration_error"]
    code, payload = service.run(_args(package="tools/agent_policy", baseline="", no_baseline=True, base_ref="a" * 40))
    assert code == 2 and isinstance(payload, dict)
    assert "cannot be used with --no-baseline" in payload["configuration_error"]
    code, payload = service.run(
        _args(
            package="tools/agent_policy",
            baseline="",
            no_baseline=True,
            warn_lines=5,
            max_lines=10,
            warn_sloc=None,
            max_sloc=None,
        )
    )
    assert code == 2
    assert isinstance(payload, dict)
    assert payload["issues"][0]["path"] == "tools/agent_policy/oversized.py"
    code, payload = service.run(
        _args(
            package="tools/agent_policy",
            baseline="",
            no_baseline=True,
            warn_lines=9998,
            max_lines=9999,
            warn_sloc=9998,
            max_sloc=9999,
        )
    )
    assert code == 2
    assert isinstance(payload, dict)
    assert "cannot exceed authoritative" in payload["configuration_error"]


def test_current_repo_has_no_unbaselined_hard_errors() -> None:
    report = analyze_module_sizes(
        Path("src/dpone"),
        repo_root=Path.cwd(),
        thresholds=ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400),
        baseline=(),
    )
    hard = [issue for issue in report.issues if "hard max" in issue.message]
    assert [f"{issue.path}: {issue.lines} LOC / {issue.sloc} SLOC" for issue in hard] == []


@pytest.mark.parametrize("package", ["tools/agent_policy", "tests/agent_policy"])
def test_agent_policy_code_stays_within_strict_hard_budget(package: str) -> None:
    report = analyze_module_sizes(
        Path(package),
        repo_root=Path.cwd(),
        thresholds=ModuleSizeThresholds(warn_lines=350, max_lines=400, warn_sloc=300, max_sloc=350),
        baseline=(),
    )
    hard = [issue for issue in report.issues if "hard max" in issue.message]
    assert [f"{issue.path}: {issue.lines} LOC / {issue.sloc} SLOC" for issue in hard] == []
