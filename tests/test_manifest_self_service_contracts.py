from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest

from dpone.contracts.errors import ETLConfigurationError
from dpone.manifest.registry_lint import lint_registry
from dpone.manifest.validation import Severity, get_profile
from dpone.manifest.verify import (
    DiffEntry,
    VerificationIssue,
    VerificationReport,
    _find_process,
    _resolve_batch_path,
    deep_diff,
)


def write_yaml(path: Path, text: str) -> Path:
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def test_registry_lint_requires_registry_paths(tmp_path: Path) -> None:
    manifest = write_yaml(
        tmp_path / "batch.yaml",
        """
        kind: dpone.batch.v1
        vars:
          src_system: demo_source
          src_database: demo_db
        """,
    )

    with pytest.raises(ETLConfigurationError, match="registry_paths is required"):
        lint_registry([manifest], registry_paths=[])


def test_registry_lint_reports_missing_required_registry_fields(tmp_path: Path) -> None:
    manifest = write_yaml(
        tmp_path / "batch.yaml",
        """
        kind: dpone.batch.v1
        vars:
          src_system: demo_source
          src_database: demo_db
        """,
    )
    registry = write_yaml(
        tmp_path / "sources.yaml",
        """
        version: 1
        entries:
          - src_system: demo_source
            src_database: demo_db
            host: demo-host
        """,
    )

    issues = lint_registry([manifest], registry_paths=[registry], require_fields=["host", "type"])

    assert len(issues) == 1
    assert issues[0].severity is Severity.ERROR
    assert issues[0].code == "REGISTRY_MISSING_FIELD"
    assert "required field 'type'" in issues[0].message


def test_registry_lint_infers_legacy_landing_dataset_and_reports_missing_entry(tmp_path: Path) -> None:
    manifest = write_yaml(
        tmp_path / "legacy.yaml",
        """
        sink:
          table:
            schema: landing__demo_source__demo_db__archive
        """,
    )
    registry = write_yaml(
        tmp_path / "sources.yaml",
        """
        version: 1
        entries: []
        """,
    )

    issues = lint_registry([manifest], registry_paths=[registry])

    assert len(issues) == 1
    assert issues[0].code == "REGISTRY_MISSING_ENTRY"
    assert issues[0].src_system == "demo_source"
    assert issues[0].src_database == "demo_db__archive"


def test_registry_lint_reports_yaml_parse_errors_without_stopping(tmp_path: Path) -> None:
    broken = write_yaml(tmp_path / "broken.yaml", "kind: [")
    registry = write_yaml(
        tmp_path / "sources.yaml",
        """
        version: 1
        entries: []
        """,
    )

    issues = lint_registry([broken], registry_paths=[registry])

    assert len(issues) == 1
    assert issues[0].code == "YAML_PARSE_ERROR"
    assert issues[0].manifest_path == broken


def test_validation_profile_exposes_landing_raw_contract() -> None:
    profile = get_profile("landing")

    assert profile.name == "landing_raw_v1"
    assert profile.required_labels == ("layer", "src", "db", "schema", "host", "type", "ingest")
    assert profile.missing_labels_severity is Severity.ERROR
    assert profile.missing_description_severity is Severity.ERROR
    assert profile.require_source_path_in_description is True


def test_validation_profile_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="Unknown validation profile"):
        get_profile("enterprise_only")


def test_verification_report_is_json_ready_and_stringifies_paths(tmp_path: Path) -> None:
    issue = VerificationIssue(
        code="MISMATCH",
        message="Different process config",
        legacy_path=tmp_path / "legacy.yaml",
        batch_ref="batch.yaml#public.orders",
        selector="public.orders",
        diffs=(DiffEntry(path="sink.table.name", legacy="orders", batch="orders_v2"),),
    )
    report = VerificationReport(total_legacy=1, ok=0, failed=1, issues=(issue,))

    assert report.to_jsonable() == {
        "total_legacy": 1,
        "ok": 0,
        "failed": 1,
        "issues": [
            {
                "code": "MISMATCH",
                "message": "Different process config",
                "legacy_path": str(tmp_path / "legacy.yaml"),
                "batch_ref": "batch.yaml#public.orders",
                "selector": "public.orders",
                "diffs": [{"path": "sink.table.name", "legacy": "orders", "batch": "orders_v2"}],
            }
        ],
    }


def test_verify_helpers_resolve_batch_paths_and_process_selectors(tmp_path: Path) -> None:
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    planned_inside = batch_dir / "orders.batch.yaml"
    planned_outside = tmp_path / "planned" / "users.batch.yaml"

    processes = [
        SimpleNamespace(selector="public.orders", name="orders"),
        SimpleNamespace(selector=None, name="users"),
    ]

    assert _resolve_batch_path(planned_inside, batch_dir) == planned_inside
    assert _resolve_batch_path(planned_outside, batch_dir) == batch_dir / "users.batch.yaml"
    assert _find_process(processes, selector="public.orders") is processes[0]
    assert _find_process(processes, selector="users") is processes[1]
    assert _find_process(processes, selector="missing") is None


def test_deep_diff_reports_missing_values_and_honors_ignored_paths() -> None:
    legacy = {
        "name": "orders",
        "sink": {"table": {"name": "orders", "description": "legacy"}},
        "depends_on": [{"path": "extract.yaml"}],
    }
    batch = {
        "name": "orders",
        "sink": {"table": {"name": "orders_v2", "description": "generated"}},
        "depends_on": [],
        "labels": {"layer": "landing"},
    }

    diffs = deep_diff(legacy, batch, ignore_paths=["sink.table.description"])

    assert {diff.path for diff in diffs} == {"depends_on (len)", "labels", "sink.table.name"}
