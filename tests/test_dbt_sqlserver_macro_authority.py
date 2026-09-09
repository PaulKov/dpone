from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.dbt_sqlserver_macro_authority import (
    DBT_SQLSERVER_MACRO_AUTHORITY_INVALID,
    DBT_SQLSERVER_MACRO_AUTHORITY_POLICY_PAYLOAD,
    evaluate_dbt_sqlserver_macro_authority,
)
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import (
    DBT_DPONE_PUBLISH_HELPER_RECORD,
    DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS,
    DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS,
    DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS,
    DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
    DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION,
    DBT_SQLSERVER_PROTECTED_MACRO_NAMES,
    DBT_SQLSERVER_TRUSTED_ROOTS,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json"
HELPER_PATH = ROOT / "packages" / "dbt-dpone" / "macros" / "dpone_publish.sql"
PRODUCER_PATH = ROOT / "tools" / "dbt_self_service" / "generate_sqlserver_macro_authority.py"

_UNIQUE_ID = 0
_PACKAGE_NAME = 1
_NAME = 2
_BODY_SHA256 = 3
_DEPENDENCIES = 4


@pytest.fixture
def manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_generated_baseline_freezes_exact_framework_and_invocation_sets() -> None:
    framework_ids = tuple(record[_UNIQUE_ID] for record in DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS)
    extension_ids = tuple(record[_UNIQUE_ID] for record in DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS)

    assert len(DBT_SQLSERVER_TRUSTED_ROOTS) == 11
    assert len(framework_ids) == len(set(framework_ids)) == 153
    assert len(extension_ids) == len(set(extension_ids)) == 7
    assert len(set(framework_ids) | set(extension_ids)) == 160
    assert framework_ids == tuple(sorted(framework_ids))
    assert extension_ids == tuple(sorted(extension_ids))
    assert {record[_PACKAGE_NAME] for record in DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS} == {"dbt", "dbt_sqlserver"}
    assert {record[_PACKAGE_NAME] for record in DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS} == {"dbt"}
    assert all(record[_BODY_SHA256].startswith("sha256:") for record in _all_records())
    assert all(record[_DEPENDENCIES] == tuple(sorted(record[_DEPENDENCIES])) for record in _all_records())
    assert DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256.startswith("sha256:")
    assert DBT_SQLSERVER_MACRO_AUTHORITY_POLICY_PAYLOAD == {
        "generator_version": DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION,
        "baseline_sha256": DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
    }


def test_exact_manifest_and_selected_graph_are_admitted_deterministically(
    manifest: dict[str, Any],
) -> None:
    selected = tuple(reversed(tuple(manifest["nodes"])))
    before = copy.deepcopy(manifest)

    first = evaluate_dbt_sqlserver_macro_authority(manifest, selected)
    second = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(reversed(selected)))

    assert first.passed
    assert first.issues == ()
    assert first.baseline_sha256 == DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
    assert first.projection_sha256 is not None
    assert second.projection_sha256 == first.projection_sha256
    assert manifest == before


@pytest.mark.parametrize(
    ("unique_id", "field", "replacement"),
    [
        (
            "macro.dbt_sqlserver.materialization_table_sqlserver",
            "macro_sql",
            "{% macro drifted() %}{% endmacro %}",
        ),
        (
            "macro.dbt.test_relationships",
            "depends_on",
            {"macros": []},
        ),
        (
            "macro.dbt.default__test_unique",
            "package_name",
            "foreign_package",
        ),
    ],
)
def test_framework_or_invocation_record_drift_fails_closed(
    manifest: dict[str, Any],
    unique_id: str,
    field: str,
    replacement: object,
) -> None:
    manifest["macros"][unique_id][field] = replacement

    report = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))

    assert not report.passed
    assert report.projection_sha256 is None
    assert {issue.code for issue in report.issues} == {DBT_SQLSERVER_MACRO_AUTHORITY_INVALID}
    assert any(issue.unique_id == unique_id for issue in report.issues)


def test_dependency_order_is_canonical_but_missing_root_and_cycles_fail(
    manifest: dict[str, Any],
) -> None:
    root = DBT_SQLSERVER_TRUSTED_ROOTS[0]
    dependencies = manifest["macros"][root]["depends_on"]["macros"]
    manifest["macros"][root]["depends_on"]["macros"] = list(reversed(dependencies))
    reordered = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))
    assert reordered.passed

    manifest["macros"].pop(root)
    missing = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))
    assert not missing.passed
    assert any(issue.unique_id == root and issue.field == "manifest.macros" for issue in missing.issues)

    manifest = _manifest()
    dependency = manifest["macros"][root]["depends_on"]["macros"][0]
    manifest["macros"][dependency]["depends_on"]["macros"].append(root)
    cyclic = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))
    assert not cyclic.passed
    assert any(issue.field == "depends_on.macros" and "acyclic" in issue.expectation for issue in cyclic.issues)


@pytest.mark.parametrize(
    "shadow_name",
    [
        "get_create_table_as_sql",
        "default__get_create_table_as_sql",
        "sqlserver__get_create_table_as_sql",
    ],
)
def test_project_or_package_dispatch_family_shadow_fails_closed(
    manifest: dict[str, Any],
    shadow_name: str,
) -> None:
    assert shadow_name in DBT_SQLSERVER_PROTECTED_MACRO_NAMES
    unique_id = f"macro.analytics_package.{shadow_name}"
    manifest["macros"][unique_id] = _macro(unique_id, "analytics_package", shadow_name)

    report = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))

    assert not report.passed
    assert any(issue.unique_id == unique_id and issue.field == "name" for issue in report.issues)


@pytest.mark.parametrize("package_name", ["dbt", "dbt_sqlserver"])
def test_trusted_package_name_cannot_add_dispatch_candidate(
    manifest: dict[str, Any],
    package_name: str,
) -> None:
    shadow_name = "sqlserver__get_create_table_as_sql"
    unique_id = f"macro.{package_name}.{shadow_name}"
    assert unique_id not in DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS
    manifest["macros"][unique_id] = _macro(unique_id, package_name, shadow_name)

    report = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))

    assert not report.passed
    assert any(issue.unique_id == unique_id and issue.field == "name" for issue in report.issues)


def test_missing_pinned_dispatch_candidate_fails_closed(
    manifest: dict[str, Any],
) -> None:
    candidate = next(
        unique_id
        for unique_id in DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS
        if unique_id
        not in {
            record[_UNIQUE_ID]
            for record in (*DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS, *DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS)
        }
    )
    manifest["macros"].pop(candidate)

    report = evaluate_dbt_sqlserver_macro_authority(manifest, tuple(manifest["nodes"]))

    assert not report.passed
    assert any(issue.unique_id == candidate and issue.field == "manifest.macros" for issue in report.issues)


def test_unused_unrelated_custom_macro_does_not_change_projection(
    manifest: dict[str, Any],
) -> None:
    selected = tuple(manifest["nodes"])
    baseline = evaluate_dbt_sqlserver_macro_authority(manifest, selected)
    unique_id = "macro.analytics_package.unrelated_metadata"
    manifest["macros"][unique_id] = _macro(unique_id, "analytics_package", "unrelated_metadata")

    observed = evaluate_dbt_sqlserver_macro_authority(manifest, selected)

    assert observed.passed
    assert observed.projection_sha256 == baseline.projection_sha256


def test_selected_node_rejects_foreign_macro_dependency(
    manifest: dict[str, Any],
) -> None:
    selected_id = next(iter(manifest["nodes"]))
    unique_id = "macro.analytics_package.runtime_sql"
    manifest["macros"][unique_id] = _macro(unique_id, "analytics_package", "runtime_sql")
    manifest["nodes"][selected_id]["depends_on"]["macros"].append(unique_id)

    report = evaluate_dbt_sqlserver_macro_authority(manifest, (selected_id,))

    assert not report.passed
    assert any(issue.unique_id == selected_id and issue.field == "depends_on.macros" for issue in report.issues)


def test_selected_id_collision_cannot_hide_node_macro_dependency(
    manifest: dict[str, Any],
) -> None:
    selected_id = next(iter(manifest["nodes"]))
    unique_id = "macro.analytics_package.runtime_sql"
    manifest["macros"][unique_id] = _macro(unique_id, "analytics_package", "runtime_sql")
    manifest["nodes"][selected_id]["depends_on"]["macros"].append(unique_id)
    manifest["unit_tests"][selected_id] = {
        "unique_id": selected_id,
        "resource_type": "unit_test",
        "depends_on": {"macros": []},
    }

    report = evaluate_dbt_sqlserver_macro_authority(manifest, (selected_id,))

    assert not report.passed
    assert any(issue.field == "manifest.unit_tests" for issue in report.issues)
    assert any(issue.field == "depends_on.macros" for issue in report.issues)


def test_exact_metadata_helper_is_admitted_but_cannot_add_authority(
    manifest: dict[str, Any],
) -> None:
    selected_id = next(iter(manifest["nodes"]))
    helper = _helper_macro()
    manifest["macros"][helper["unique_id"]] = helper
    manifest["nodes"][selected_id]["depends_on"]["macros"] = [helper["unique_id"]]

    admitted = evaluate_dbt_sqlserver_macro_authority(manifest, (selected_id,))
    assert admitted.passed

    helper["macro_sql"] += " "
    body_rejected = evaluate_dbt_sqlserver_macro_authority(manifest, (selected_id,))
    assert not body_rejected.passed
    assert any(
        issue.unique_id == helper["unique_id"] and issue.field == "macro_sql_sha256" for issue in body_rejected.issues
    )

    helper["macro_sql"] = HELPER_PATH.read_text(encoding="utf-8").rstrip("\r\n")
    helper["depends_on"]["macros"] = ["macro.analytics_package.runtime_sql"]
    rejected = evaluate_dbt_sqlserver_macro_authority(manifest, (selected_id,))
    assert not rejected.passed
    assert any(issue.unique_id == helper["unique_id"] for issue in rejected.issues)


def test_helper_body_is_pinned_to_exact_checked_in_utf8() -> None:
    helper_source = HELPER_PATH.read_text(encoding="utf-8")

    assert DBT_DPONE_PUBLISH_HELPER_RECORD[_UNIQUE_ID] == "macro.dbt_dpone.dpone_publish"
    assert helper_source.endswith("\n")
    assert "run_query" not in helper_source
    assert "adapter.dispatch" not in helper_source
    assert "{% do return({'dpone': {'publish': publish}}) %}" in helper_source


def test_producer_check_is_hermetic_and_current() -> None:
    completed = subprocess.run(
        [sys.executable, str(PRODUCER_PATH), "--check"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_candidate_comparison_reports_authority_and_execution_capability_delta(tmp_path: Path) -> None:
    candidate = _manifest()
    candidate["metadata"]["dbt_version"] = "1.12.3"
    root = "macro.dbt_sqlserver.materialization_table_sqlserver"
    introduced = "macro.dbt_sqlserver.candidate_run_query"
    candidate["macros"][root]["depends_on"]["macros"].append(introduced)
    candidate["macros"][introduced] = {
        "unique_id": introduced,
        "resource_type": "macro",
        "package_name": "dbt_sqlserver",
        "name": "candidate_run_query",
        "macro_sql": "{% macro candidate_run_query() %}{{ run_query('select 1') }}{% endmacro %}",
        "depends_on": {"macros": []},
    }
    candidate_path = tmp_path / "candidate-manifest.json"
    output_path = tmp_path / "macro-authority-diff.json"
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(PRODUCER_PATH),
            "--candidate-manifest",
            str(candidate_path),
            "--diff-output",
            str(output_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["schema"] == "dpone.dbt-sqlserver-macro-authority-diff.v1"
    assert report["baseline"]["framework_macro_record_count"] == 153
    assert report["candidate"]["dbt_core_version"] == "1.12.3"
    assert report["candidate"]["framework_macro_record_count"] == 154
    assert [record["unique_id"] for record in report["framework_macro_records"]["added"]] == [introduced]
    assert report["new_or_changed_execution_capable_macros"] == [
        {"unique_id": introduced, "tokens": ["run_query"]},
        {
            "unique_id": root,
            "tokens": ["statement(", "adapter.", "{% call"],
        },
    ]
    assert {
        "trusted_root": root,
        "target_unique_id": introduced,
        "path": [root, introduced],
    } in report["trusted_root_paths_to_new_or_changed_execution_capable_macros"]


def _all_records() -> tuple[tuple[object, ...], ...]:
    return (*DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS, *DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS)


def _manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _macro(unique_id: str, package_name: str, name: str) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "macro",
        "package_name": package_name,
        "name": name,
        "macro_sql": "{% macro unrelated_metadata() %}{{ return({}) }}{% endmacro %}",
        "depends_on": {"macros": []},
    }


def _helper_macro() -> dict[str, Any]:
    return {
        "unique_id": DBT_DPONE_PUBLISH_HELPER_RECORD[_UNIQUE_ID],
        "resource_type": "macro",
        "package_name": DBT_DPONE_PUBLISH_HELPER_RECORD[_PACKAGE_NAME],
        "name": DBT_DPONE_PUBLISH_HELPER_RECORD[_NAME],
        "macro_sql": HELPER_PATH.read_text(encoding="utf-8").rstrip("\r\n"),
        "depends_on": {"macros": list(DBT_DPONE_PUBLISH_HELPER_RECORD[_DEPENDENCIES])},
    }
