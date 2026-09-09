from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from dpone.adapters.dbt_workflow_selection import ManifestPreviewSelectionResolver
from dpone.app.context import AppContext
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.cli import main as cli_main
from dpone.commands import (
    dbt_dev_evidence_campaign_cmd,
    dbt_dev_evidence_cmd,
    dbt_publish_cmd,
)
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"
MANIFEST = DEMO / "fixtures" / "manifest.v12.json"
PROFILES = DEMO / "dpone" / "dbt-publish-profiles.yml"
SQLSERVER_PROJECT_FLAGS = """\
flags:
  dbt_sqlserver_enable_safe_type_expansion: false
  dbt_sqlserver_use_dbt_transactions: true
  dbt_sqlserver_use_default_schema_concat: true
  dbt_sqlserver_use_native_string_types: true
"""


@pytest.fixture(autouse=True)
def _isolate_release_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep CLI unit tests credential-free while production compile stays strict."""

    real_builder = build_dbt_dpone_compiler
    monkeypatch.setattr(
        AppContext,
        "build_dbt_artifact_writer",
        lambda _self, *, dbt_profiles_dir: DbtArtifactWriter(
            selection_resolver=ManifestPreviewSelectionResolver(),
            bundle_operations=RuntimeDbtProjectBundleOperations(),
            project_policy=DbtSqlserverProjectPolicyValidator(),
            dbt_profiles_dir=dbt_profiles_dir,
        ),
    )
    monkeypatch.setattr(
        AppContext,
        "build_dbt_publish_compiler",
        lambda _self, *, root, require_certified_routes: real_builder(
            root=root,
            require_certified_routes=False,
        ),
    )


def _common() -> list[str]:
    return ["--manifest", str(MANIFEST), "--profiles", str(PROFILES)]


def _empty_manifest(tmp_path: Path) -> Path:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["config"]["meta"]["dpone"]["publish"]["enabled"] = False
    return _write_dbt_project(tmp_path, manifest_payload=payload)


def _execute_pack_args(path: str, *extra: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    dbt_publish_cmd.register_execute_pack_parser(subparsers)
    return parser.parse_args(["execute-pack", path, "--format", "json", *extra])


def _write_dbt_project(
    root: Path,
    *,
    manifest_payload: dict[str, Any] | None = None,
    project_config: str = "name: test\nversion: 1.0\n",
    project_flags: str = SQLSERVER_PROJECT_FLAGS,
) -> Path:
    root.mkdir(exist_ok=True)
    (root / "dbt_project.yml").write_text(
        project_config + project_flags,
        encoding="utf-8",
    )
    (root / "target").mkdir()
    manifest = root / "target" / "manifest.json"
    manifest.write_text(
        json.dumps(manifest_payload) if manifest_payload is not None else MANIFEST.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return manifest


def _workflow_policy_case(
    case: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    current_id = "model.dpone_dbt_demo.competitive_pricing"
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    history = payload["nodes"][history_id]
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"
    profiles = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    if case == "foreign_publish":
        return payload, profiles

    shared_id = "model.dpone_dbt_demo.shared_pricing_stage"
    current = payload["nodes"][current_id]
    shared = copy.deepcopy(current)
    shared.update(
        {
            "unique_id": shared_id,
            "name": "shared_pricing_stage",
            "fqn": ["dpone_dbt_demo", "shared_pricing_stage"],
            "alias": "shared_pricing_stage",
            "relation_name": "[DWH].[pricing].[shared_pricing_stage]",
            "original_file_path": "models/shared_pricing_stage.sql",
        }
    )
    shared["config"]["meta"] = {}
    shared["depends_on"]["nodes"] = []
    payload["nodes"][shared_id] = shared
    current["depends_on"]["nodes"] = [shared_id]
    history["depends_on"]["nodes"] = [shared_id]
    payload["parent_map"][shared_id] = []
    payload["parent_map"][current_id] = [shared_id]
    payload["parent_map"][history_id] = [shared_id]
    payload["child_map"][shared_id] = [current_id, history_id]
    payload["child_map"][current_id] = [child for child in payload["child_map"][current_id] if child != history_id]
    return payload, profiles


def _set_mtime(path: Path, nanoseconds: int) -> None:
    os.utime(path, ns=(nanoseconds, nanoseconds))


def _manifest_with_unknown_profile_model() -> dict[str, Any]:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    source = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    blocked = copy.deepcopy(source)
    blocked["unique_id"] = "model.dpone_dbt_demo.blocked_pricing"
    blocked["name"] = "blocked_pricing"
    blocked["alias"] = "blocked_pricing"
    blocked["original_file_path"] = "models/blocked_pricing.sql"
    blocked["config"]["alias"] = "blocked_pricing"
    blocked["config"]["meta"]["dpone"]["publish"]["profile"] = "unknown_profile"
    payload["nodes"][blocked["unique_id"]] = blocked
    return payload


def test_execute_pack_parser_accepts_only_a_relative_path_and_json() -> None:
    args = _execute_pack_args("runtime/packs/orders.json")

    assert args.relative_pack_path == "runtime/packs/orders.json"
    assert args.format == "json"


@pytest.mark.parametrize("path", ["/runtime/pack.json", "../pack.json", "runtime/../pack.json", r"runtime\\pack.json"])
def test_execute_pack_parser_rejects_unsafe_paths(path: str) -> None:
    with pytest.raises(SystemExit, match="2"):
        _execute_pack_args(path)


def test_execute_pack_parser_rejects_arbitrary_passthrough() -> None:
    with pytest.raises(SystemExit, match="2"):
        _execute_pack_args("runtime/pack.json", "--select", "unexpected")


def test_execute_pack_handler_delegates_one_path_without_shell_argv(
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []
    evidence = {"schema": "dpone.dbt-execution-evidence.v1", "status": "failed"}

    def execute_dbt_pack(path: str):
        observed.append(path)
        return SimpleNamespace(
            exit_code=17,
            evidence=SimpleNamespace(to_dict=lambda: evidence),
        )

    monkeypatch.setitem(
        sys.modules,
        "dpone.runtime.dbt_execution",
        SimpleNamespace(execute_dbt_pack=execute_dbt_pack),
    )

    exit_code = dbt_publish_cmd.cmd_execute_pack(
        _execute_pack_args("runtime/packs/orders.json"),
        ctx=object(),
        logger=logging.getLogger("test.dbt.execute-pack"),
    )

    captured = capsys.readouterr()
    assert exit_code == 17
    assert observed == ["runtime/packs/orders.json"]
    assert json.loads(captured.out) == evidence
    assert captured.err == ""


def test_execute_pack_is_registered_on_the_canonical_cli(
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = {"schema": "dpone.dbt-execution-evidence.v1", "status": "passed"}
    monkeypatch.setitem(
        sys.modules,
        "dpone.runtime.dbt_execution",
        SimpleNamespace(
            execute_dbt_pack=lambda path: SimpleNamespace(
                exit_code=0,
                evidence=SimpleNamespace(to_dict=lambda: {**evidence, "pack": path}),
            )
        ),
    )

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "execute-pack",
                "runtime/packs/orders.json",
                "--format",
                "json",
            ]
        )

    assert json.loads(capsys.readouterr().out) == {
        **evidence,
        "pack": "runtime/packs/orders.json",
    }


def test_execute_pack_handler_emits_stable_runtime_error(
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StableRuntimeError(RuntimeError):
        code = "DPONE_DBT_PACK_INVALID"

    def execute_dbt_pack(_path: str):
        raise StableRuntimeError("execution pack is invalid")

    monkeypatch.setitem(
        sys.modules,
        "dpone.runtime.dbt_execution",
        SimpleNamespace(execute_dbt_pack=execute_dbt_pack),
    )

    exit_code = dbt_publish_cmd.cmd_execute_pack(
        _execute_pack_args("runtime/packs/orders.json"),
        ctx=object(),
        logger=logging.getLogger("test.dbt.execute-pack"),
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 1
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_PACK_INVALID"
    assert payload["fixes"] == [
        {
            "id": "resolve_pack_invalid",
            "safety": "manual",
            "description": (
                "Re-fetch the pinned release and deployment artifacts, verify "
                "their checksums, and never edit the generated execution pack."
            ),
        }
    ]
    assert captured.err == ""
    assert "Traceback" not in captured.out


@pytest.mark.parametrize(
    ("code", "expected"),
    (
        (
            "DPONE_DBT_SELECTION_DRIFT",
            "The build did not start and the target was not mutated.",
        ),
        (
            "DPONE_DBT_TARGET_IDENTITY_MISMATCH",
            "The build did not start and the target was not mutated.",
        ),
        (
            "DPONE_DBT_SCHEMA_DRIFT",
            "publish a new immutable release",
        ),
        (
            "COMMIT_UNKNOWN",
            "Do not retry automatically.",
        ),
    ),
)
def test_execute_pack_errors_explain_safe_recovery(
    capsys,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
    expected: str,
) -> None:
    class StableRuntimeError(RuntimeError):
        def __init__(self, error_code: str) -> None:
            super().__init__("private runtime detail")
            self.code = error_code

    error = StableRuntimeError(code)
    monkeypatch.setitem(
        sys.modules,
        "dpone.runtime.dbt_execution",
        SimpleNamespace(execute_dbt_pack=lambda _path: (_ for _ in ()).throw(error)),
    )

    exit_code = dbt_publish_cmd.cmd_execute_pack(
        _execute_pack_args("runtime/packs/orders.json"),
        ctx=object(),
        logger=logging.getLogger("test.dbt.execute-pack"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["code"] == code
    assert expected in payload["fixes"][0]["description"]
    assert "private runtime detail" not in json.dumps(payload)


def test_execute_pack_handler_redacts_unexpected_runtime_error(
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def execute_dbt_pack(_path: str):
        raise RuntimeError("private runtime detail")

    monkeypatch.setitem(
        sys.modules,
        "dpone.runtime.dbt_execution",
        SimpleNamespace(execute_dbt_pack=execute_dbt_pack),
    )

    exit_code = dbt_publish_cmd.cmd_execute_pack(
        _execute_pack_args("runtime/packs/orders.json"),
        ctx=object(),
        logger=logging.getLogger("test.dbt.execute-pack"),
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 5
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_INTERNAL"
    assert len(payload["trace_id"]) == 32
    assert payload["fixes"][0]["safety"] == "manual"
    assert captured.err == ""
    assert "Traceback" not in captured.out
    assert "private runtime detail" not in captured.out


def test_dbt_check_json(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(["dbt", "check", *_common(), "--format", "json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.dbt-publish-compile.v2"
    assert payload["passed"] is True


def test_dbt_check_rejects_missing_sqlserver_flag_with_actionable_json(
    tmp_path: Path,
    capsys,
) -> None:
    invalid_flags = SQLSERVER_PROJECT_FLAGS.replace(
        "  dbt_sqlserver_use_dbt_transactions: true\n",
        "",
    )
    manifest = _write_dbt_project(
        tmp_path,
        project_flags=invalid_flags,
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "check",
                "--manifest",
                str(manifest),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["code"] == "DPONE_DBT_SQLSERVER_PROJECT_POLICY_INVALID"
    assert "dbt_sqlserver_use_dbt_transactions" in payload["message"]
    assert "Set `flags.dbt_sqlserver_use_dbt_transactions: true`" in payload["fixes"][0]["description"]
    assert captured.err == ""


@pytest.mark.parametrize(
    ("failure_case", "expected_code", "expected_field"),
    [
        (
            "adapter_config",
            "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED",
            "config.as_columnstore",
        ),
        (
            "physical_constraint",
            "DPONE_DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED",
            "columns.product_id.constraints",
        ),
        (
            "unique_key_expression",
            "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED",
            "config.unique_key",
        ),
        (
            "nullable_unique_key",
            "DPONE_DBT_UNIQUE_KEY_NULLABLE",
            "config.unique_key",
        ),
    ],
)
def test_dbt_check_graph_policy_failure_is_actionable_in_text_and_json(
    tmp_path: Path,
    capsys,
    failure_case: str,
    expected_code: str,
    expected_field: str,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    if failure_case == "adapter_config":
        node["config"]["as_columnstore"] = True
    elif failure_case == "physical_constraint":
        node["columns"]["product_id"]["constraints"] = [{"type": "unique"}]
    else:
        node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing_history"]
        if failure_case == "unique_key_expression":
            node["config"]["unique_key"] = "lower(product_id)"
        else:
            node["columns"]["product_id"]["constraints"] = []
    manifest = _write_dbt_project(
        tmp_path,
        manifest_payload=payload,
    )
    base = [
        "dbt",
        "check",
        "--manifest",
        str(manifest),
        "--profiles",
        str(PROFILES),
    ]

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(base)
    text_result = capsys.readouterr()

    with pytest.raises(SystemExit, match="1"):
        cli_main.main([*base, "--format", "json"])
    json_result = capsys.readouterr()
    error = json.loads(json_result.out)

    assert expected_code in text_result.out
    assert expected_field in text_result.out
    assert "Next:" in text_result.out
    assert error["code"] == expected_code
    assert expected_field in error["message"]
    assert error["path"]
    assert error["fixes"][0]["description"]
    assert text_result.err == json_result.err == ""
    assert not (tmp_path / "generated").exists()


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("foreign_publish", "DPONE_DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED"),
        ("shared_non_publish", "DPONE_DBT_WORKFLOW_GRAPH_OVERLAP"),
    ],
)
def test_dbt_check_workflow_ownership_failures_are_actionable(
    tmp_path: Path,
    capsys,
    case: str,
    expected_code: str,
) -> None:
    payload, profiles = _workflow_policy_case(case)
    manifest = _write_dbt_project(tmp_path, manifest_payload=payload)
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(
        yaml.safe_dump(profiles, sort_keys=False),
        encoding="utf-8",
    )
    base = [
        "dbt",
        "check",
        "--manifest",
        str(manifest),
        "--profiles",
        str(profiles_path),
    ]

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(base)
    text_result = capsys.readouterr()
    with pytest.raises(SystemExit, match="1"):
        cli_main.main([*base, "--format", "json"])
    json_result = capsys.readouterr()
    error = json.loads(json_result.out)

    assert expected_code in text_result.out
    assert "Next:" in text_result.out
    assert error["code"] == expected_code
    assert error["path"]
    assert error["fixes"][0]["description"]
    assert text_result.err == json_result.err == ""
    assert not (tmp_path / "generated").exists()


def test_dbt_check_zero_models_is_structured_json_failure(tmp_path: Path, capsys) -> None:
    manifest = _empty_manifest(tmp_path)

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(["dbt", "check", "--manifest", str(manifest), "--profiles", str(PROFILES), "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_NO_PUBLISH_MODELS"
    assert payload["fixes"][0]["safety"] == "manual"
    assert captured.err == ""
    assert "Traceback" not in captured.out


def test_dbt_check_allow_empty_is_report_only(tmp_path: Path, capsys) -> None:
    manifest = _empty_manifest(tmp_path)

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "check",
                "--manifest",
                str(manifest),
                "--allow-empty",
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.dbt-publish-compile.v2"
    assert payload["passed"] is True
    assert payload["models"] == []
    assert payload["artifacts"] == {}
    assert captured.err == ""


def test_dbt_check_invalid_manifest_is_structured_json_failure(tmp_path: Path, capsys) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{invalid", encoding="utf-8")

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(["dbt", "check", "--manifest", str(manifest), "--profiles", str(PROFILES), "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_MANIFEST_INVALID_JSON"
    assert captured.err == ""
    assert "Traceback" not in captured.out


def test_dbt_check_text_and_json_use_the_same_public_error_code(
    tmp_path: Path,
    capsys,
) -> None:
    missing_profiles = tmp_path / "missing-policy.yml"
    base = [
        "dbt",
        "check",
        "--manifest",
        str(MANIFEST),
        "--profiles",
        str(missing_profiles),
    ]

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(base)
    text_result = capsys.readouterr()

    with pytest.raises(SystemExit, match="1"):
        cli_main.main([*base, "--format", "json"])
    json_result = capsys.readouterr()

    assert "- ERROR DPONE_DBT_PROFILES_MISSING:" in text_result.out
    assert json.loads(json_result.out)["code"] == "DPONE_DBT_PROFILES_MISSING"
    assert text_result.err == json_result.err == ""


def test_dbt_explain_json(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(["dbt", "explain", *_common(), "--model", "competitive_pricing", "--format", "json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.dbt-publish-explain.v1"
    assert payload["resolved_strategy"]["mode"] == "partition_replace"
    assert "Compatibility starts with release 0.73.26" in captured.err
    assert "2027-07-29" in captured.err
    assert "dpone dbt explain MODEL" in captured.err


def test_dbt_explain_accepts_beginner_positional_model(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "explain",
                "competitive_pricing",
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )
    assert json.loads(capsys.readouterr().out)["model"].endswith(".competitive_pricing")


@pytest.mark.parametrize("command", ["check", "explain"])
def test_dbt_profiles_dir_is_not_advertised_on_read_only_commands(
    command: str,
) -> None:
    with pytest.raises(SystemExit, match="2"):
        cli_main.main(
            [
                "dbt",
                command,
                "--dbt-profiles-dir",
                "/tmp/profiles",
            ]
        )


def test_dbt_check_discovers_default_manifest_without_running_dbt(tmp_path: Path, capsys) -> None:
    _write_dbt_project(tmp_path)
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "check",
                "--project-dir",
                str(tmp_path),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )
    assert json.loads(capsys.readouterr().out)["passed"] is True


@pytest.mark.parametrize(
    "root_file",
    [
        "dbt_project.yml",
        "packages.yml",
        "dependencies.yml",
        "package-lock.yml",
        "selectors.yml",
    ],
)
def test_dbt_check_rejects_default_manifest_older_than_bundle_root_file(
    tmp_path: Path,
    capsys,
    root_file: str,
) -> None:
    manifest = _write_dbt_project(tmp_path / "project")
    changed = manifest.parents[1] / root_file
    if root_file != "dbt_project.yml":
        changed.write_text("{}\n", encoding="utf-8")
    _set_mtime(manifest, 1_700_000_000_000_000_000)
    _set_mtime(changed, 1_700_000_001_000_000_000)

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "check",
                str(manifest.parents[1]),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_MANIFEST_STALE"
    assert payload["fixes"][0]["description"] == "Run `dbt parse`, then retry."


@pytest.mark.parametrize(
    ("field", "configured_root"),
    [
        ("analysis-paths", "custom_analyses"),
        ("asset-paths", "custom_assets"),
        ("docs-paths", "custom_docs"),
        ("macro-paths", "custom_macros"),
        ("model-paths", "custom_models"),
        ("seed-paths", "custom_seeds"),
        ("snapshot-paths", "custom_snapshots"),
        ("test-paths", "custom_tests"),
        ("packages-install-path", "custom_packages"),
    ],
)
def test_dbt_check_rejects_default_manifest_older_than_configured_bundle_root(
    tmp_path: Path,
    capsys,
    field: str,
    configured_root: str,
) -> None:
    project = tmp_path / "project"
    manifest = _write_dbt_project(
        project,
        project_config=f"name: test\nversion: 1.0\n{field}: [{configured_root}]\n",
    )
    source_root = project / configured_root
    source_root.mkdir()
    changed = source_root / "parse_input.sql"
    changed.write_text("select 1\n", encoding="utf-8")
    _set_mtime(manifest, 1_700_000_000_000_000_000)
    _set_mtime(changed, 1_700_000_001_000_000_000)

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "check",
                str(project),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_MANIFEST_STALE"


def test_dbt_check_accepts_beginner_project_positional(tmp_path: Path, capsys) -> None:
    _write_dbt_project(tmp_path)

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "check",
                str(tmp_path),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    assert json.loads(capsys.readouterr().out)["passed"] is True


def test_dbt_check_missing_project_is_actionable_configuration_error(
    tmp_path: Path,
    capsys,
) -> None:
    missing = tmp_path / "missing-dbt-project"

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(["dbt", "check", str(missing), "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_PROJECT_INVALID"
    assert payload["stage"] == "dbt_check"
    assert payload["fixes"]
    assert "Traceback" not in json.dumps(payload)


def test_dbt_check_implicit_cwd_outside_project_is_actionable_and_read_only(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    before = tuple(tmp_path.iterdir())

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(["dbt", "check", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_PROJECT_INVALID"
    assert "dbt_project.yml" in payload["message"]
    assert payload["fixes"]
    assert tuple(tmp_path.iterdir()) == before


def test_dbt_check_rejects_ambiguous_default_profile_registries_without_absolute_paths(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DPONE_DBT_PUBLISH_PROFILES", raising=False)
    project = tmp_path / "project"
    _write_dbt_project(project)
    for relative in ("dpone/dbt-publish-profiles.yml", ".dpone/dbt-publish-profiles.yml"):
        policy = project / relative
        policy.parent.mkdir()
        policy.write_bytes(PROFILES.read_bytes())

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(["dbt", "check", str(project), "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload)
    assert payload["code"] == "DPONE_DBT_PROFILES_AMBIGUOUS"
    assert "dpone/dbt-publish-profiles.yml" in rendered
    assert ".dpone/dbt-publish-profiles.yml" in rendered
    assert str(tmp_path) not in rendered
    assert payload["fixes"]


def test_dbt_explain_project_model_isolated_from_other_model_blockers(
    tmp_path: Path,
    capsys,
) -> None:
    project = tmp_path / "project"
    _write_dbt_project(project, manifest_payload=_manifest_with_unknown_profile_model())

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "check",
                str(project),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )
    assert json.loads(capsys.readouterr().out)["code"] == "DPONE_DBT_PROFILE_UNKNOWN"

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "explain",
                str(project),
                "competitive_pricing",
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.dbt-publish-explain.v1"
    assert payload["model"] == "model.dpone_dbt_demo.competitive_pricing"


def test_dbt_explain_canonical_model_and_project_dir_form(
    tmp_path: Path,
    capsys,
) -> None:
    project = tmp_path / "project"
    _write_dbt_project(project)

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "explain",
                "competitive_pricing",
                "--project-dir",
                str(project),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.dbt-publish-explain.v1"
    assert payload["model"] == "model.dpone_dbt_demo.competitive_pricing"


def test_dbt_explain_help_names_canonical_and_compatibility_forms(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(["dbt", "explain", "--help"])

    help_text = capsys.readouterr().out
    assert "dpone dbt explain MODEL [--project-dir PROJECT]" in help_text
    assert "dpone dbt explain PROJECT MODEL" in help_text
    assert "dpone dbt explain --model MODEL" in help_text
    assert "(compatibility)" in help_text
    assert "Deprecated compatibility form" in help_text
    assert "[model] [MODEL]" not in help_text


def test_dbt_explain_unknown_profile_returns_model_scoped_remediation(
    tmp_path: Path,
    capsys,
) -> None:
    project = tmp_path / "project"
    _write_dbt_project(project, manifest_payload=_manifest_with_unknown_profile_model())

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "explain",
                str(project),
                "blocked_pricing",
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_PROFILE_UNKNOWN"
    assert payload["path"] == "models/blocked_pricing.sql"
    assert payload["fixes"] == [
        {
            "id": "resolve_profile_unknown",
            "safety": "manual",
            "description": "Select a published profile or ask its platform owner to add one.",
        }
    ]


def test_dbt_explain_missing_model_returns_actionable_fix(capsys) -> None:
    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "explain",
                "missing",
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_MODEL_NOT_FOUND"
    assert payload["fixes"] == [
        {
            "id": "resolve_model_not_found",
            "safety": "manual",
            "description": ("Run dpone dbt check and use an exact unique_id, FQN, name or alias."),
        }
    ]


def test_dbt_explain_accepts_exact_fqn(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "explain",
                "dpone_dbt_demo.competitive_pricing",
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["model"] == "model.dpone_dbt_demo.competitive_pricing"


def test_dbt_check_text_lists_exact_unique_ids(capsys) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(["dbt", "check", *_common()])

    output = capsys.readouterr().out
    assert "models:" in output
    assert "model.dpone_dbt_demo.competitive_pricing" in output


def test_dbt_check_conflicting_project_alias_is_usage_error(
    tmp_path: Path,
    capsys,
) -> None:
    other = tmp_path / "other"
    other.mkdir()

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(
            [
                "dbt",
                "check",
                str(tmp_path),
                "--project-dir",
                str(other),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_PROJECT_ARGUMENT_CONFLICT"
    assert payload["fixes"][0]["safety"] == "manual"


def test_dbt_explain_compatibility_project_conflict_is_usage_error(
    tmp_path: Path,
    capsys,
) -> None:
    project = tmp_path / "project"
    other = tmp_path / "other"
    _write_dbt_project(project)
    _write_dbt_project(other)

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(
            [
                "dbt",
                "explain",
                str(project),
                "competitive_pricing",
                "--project-dir",
                str(other),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_PROJECT_ARGUMENT_CONFLICT"


def test_dbt_explain_missing_model_is_structured_json_failure(capsys) -> None:
    with pytest.raises(SystemExit, match="1"):
        cli_main.main(["dbt", "explain", *_common(), "--model", "missing", "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_MODEL_NOT_FOUND"
    assert "Compatibility starts with release 0.73.26" in captured.err
    assert "2027-07-29" in captured.err


def test_dbt_compile_writes_artifacts(tmp_path: Path, capsys) -> None:
    output = tmp_path / "airflow"
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(["dbt", "compile", *_common(), "--output-dir", str(output), "--format", "json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert (output / "_dags" / "DAG__pricing__competitive_pricing__refresh.dag-spec.json").exists()


def test_dbt_compile_always_requires_certified_routes(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[bool] = []
    real_builder = build_dbt_dpone_compiler

    def strict_builder(_self, *, root, require_certified_routes):
        observed.append(require_certified_routes)
        return real_builder(root=root, require_certified_routes=False)

    monkeypatch.setattr(AppContext, "build_dbt_publish_compiler", strict_builder)

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "compile",
                *_common(),
                "--output-dir",
                str(tmp_path / "compiled"),
                "--format",
                "json",
            ]
        )

    assert observed == [True]
    assert json.loads(capsys.readouterr().out)["passed"] is True


def test_dbt_verify_promotion_proves_source_mirror(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "compiled"
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "compile",
                str(DEMO),
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--output-dir",
                str(output),
                "--format",
                "json",
            ]
        )
    capsys.readouterr()

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "verify-promotion",
                str(DEMO),
                "--compiled-root",
                str(output),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.dbt-promotion-verification.v1"
    assert payload["passed"] is True
    assert payload["release_id"].startswith("sha256:")


def test_dbt_render_ci_report_uses_compiled_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "compiled"
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "compile",
                str(DEMO),
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--output-dir",
                str(output),
                "--format",
                "json",
            ]
        )
    capsys.readouterr()

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "render-ci-report",
                "--compiled-root",
                str(output),
                "--airflow-base-url",
                "https://airflow.dev.example",
            ]
        )

    rendered = capsys.readouterr().out
    assert "dpone dbt self-service preview" in rendered
    assert "DAG__pricing__competitive_pricing__refresh" in rendered
    assert "https://airflow.dev.example/dags/" in rendered


def test_dbt_verify_promotion_fails_closed_on_source_drift(
    tmp_path: Path,
    capsys,
) -> None:
    project = tmp_path / "project"
    shutil.copytree(DEMO, project)
    output = tmp_path / "compiled"
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "compile",
                str(project),
                "--manifest",
                str(MANIFEST),
                "--profiles",
                str(PROFILES),
                "--output-dir",
                str(output),
                "--format",
                "json",
            ]
        )
    capsys.readouterr()
    (project / "models" / "competitive_pricing.sql").write_text(
        "select 0 as changed\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "verify-promotion",
                str(project),
                "--compiled-root",
                str(output),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_PROMOTION_SOURCE_DRIFT"


def test_dbt_materialize_release_installs_exact_downloaded_bytes(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "sha256:" + "a" * 64
    release_dir = tmp_path / "cache" / "releases" / expected.replace(":", "-")

    def materialize(*_args, **kwargs):
        assert kwargs["expected_release_id"] == expected
        return SimpleNamespace(
            release_id=expected,
            release_dir=release_dir,
            no_op=False,
        )

    monkeypatch.setattr(
        "dpone.commands.dbt_promotion_cmd.DbtReleaseMaterializer.materialize",
        materialize,
    )

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "materialize-release",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--cache-root",
                str(tmp_path / "cache"),
                "--expected-release-id",
                expected,
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.dbt-release-materialization.v1"
    assert payload["release_id"] == expected
    assert payload["no_op"] is False


def test_dbt_materialize_release_rejects_wrong_release_identity(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = "sha256:" + "b" * 64
    monkeypatch.setattr(
        "dpone.commands.dbt_promotion_cmd.DbtReleaseMaterializer.materialize",
        lambda *_args, **_kwargs: SimpleNamespace(
            release_id=observed,
            release_dir=tmp_path / "cache",
            no_op=False,
        ),
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "materialize-release",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--cache-root",
                str(tmp_path / "cache"),
                "--expected-release-id",
                "sha256:" + "a" * 64,
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_RELEASE_INTEGRITY_INVALID"


def test_dbt_materialize_release_reports_cache_lock_failure(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.commands import dbt_promotion_cmd

    def fail_materialize(*_args, **_kwargs):
        raise dbt_promotion_cmd.DbtReleaseMaterializationError(
            "private lock detail",
            code="DPONE_DBT_RELEASE_CACHE_LOCK_FAILED",
        )

    monkeypatch.setattr(
        "dpone.commands.dbt_promotion_cmd.DbtReleaseMaterializer.materialize",
        fail_materialize,
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "materialize-release",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--cache-root",
                str(tmp_path / "cache"),
                "--expected-release-id",
                "sha256:" + "a" * 64,
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED"
    assert ".promotion.lock" in json.dumps(payload)
    assert "private lock detail" not in json.dumps(payload)


def test_dbt_compile_conflict_is_structured_and_preserves_output(tmp_path: Path, capsys) -> None:
    output = tmp_path / "airflow"
    command = ["dbt", "compile", *_common(), "--output-dir", str(output), "--format", "json"]
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(command)
    first = json.loads(capsys.readouterr().out)
    evidence = output / first["artifacts"]["evidence"]
    evidence.write_text("concurrent writer\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(command)

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_PUBLISH_OUTPUT_CONFLICT"
    assert evidence.read_text(encoding="utf-8") == "concurrent writer\n"
    assert captured.err == ""
    assert "Traceback" not in captured.out


def test_dbt_compile_unexpected_failure_has_no_traceback(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExplodingWriter:
        def write(self, *_args, **_kwargs):
            raise RuntimeError("private implementation detail")

    monkeypatch.setattr(
        AppContext,
        "build_dbt_artifact_writer",
        lambda _self, *, dbt_profiles_dir: ExplodingWriter(),
    )

    with pytest.raises(SystemExit, match="5"):
        cli_main.main(
            [
                "dbt",
                "compile",
                *_common(),
                "--output-dir",
                str(tmp_path / "airflow"),
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_INTERNAL"
    assert len(payload["trace_id"]) == 32
    assert payload["fixes"][0]["safety"] == "manual"
    assert captured.err == ""
    assert "Traceback" not in captured.out
    assert "RuntimeError" not in captured.out
    assert "private implementation detail" not in captured.out


def test_finalize_dev_evidence_cli_emits_machine_readable_identity(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    payload = {
        "schema": "dpone.dbt-dev-evidence-bundle.v1",
        "file_count": 4,
        "subject_sha256": "sha256:" + "a" * 64,
    }

    class Service:
        def finalize(self, **kwargs):
            observed.update(kwargs)
            return SimpleNamespace(to_dict=lambda: payload)

    monkeypatch.setattr(
        dbt_dev_evidence_cmd,
        "build_dbt_dev_evidence_bundle_service",
        lambda: Service(),
    )
    release_id = "sha256:" + "b" * 64
    deployment_id = "sha256:" + "c" * 64

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "finalize-dev-evidence",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--source-evidence-root",
                str(tmp_path / "source"),
                "--output-root",
                str(tmp_path / "trusted"),
                "--expected-release-id",
                release_id,
                "--expected-deployment-id",
                deployment_id,
                "--producer-repository",
                "PaulKov/airflow-dev",
                "--producer-workflow",
                "dbt-self-service-dev-evidence.yml",
                "--source-commit",
                "d" * 40,
                "--format",
                "json",
            ]
        )

    assert json.loads(capsys.readouterr().out) == payload
    assert observed == {
        "compiled_root": tmp_path / "compiled",
        "source_evidence_root": tmp_path / "source",
        "output_root": tmp_path / "trusted",
        "expected_release_id": release_id,
        "expected_deployment_id": deployment_id,
        "expected_activation_id": None,
        "producer_repository": "PaulKov/airflow-dev",
        "producer_workflow": "dbt-self-service-dev-evidence.yml",
        "source_commit": "d" * 40,
    }


def test_finalize_dev_evidence_rejects_invalid_campaign_request_as_contract_failure(
    tmp_path: Path,
    capsys,
) -> None:
    campaign_request = tmp_path / "campaign-request.json"
    campaign_request.write_text("{invalid", encoding="utf-8")

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "finalize-dev-evidence",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--source-evidence-root",
                str(tmp_path / "source"),
                "--output-root",
                str(tmp_path / "trusted"),
                "--expected-release-id",
                "sha256:" + "b" * 64,
                "--expected-deployment-id",
                "sha256:" + "c" * 64,
                "--campaign-request",
                str(campaign_request),
                "--producer-repository",
                "PaulKov/airflow-dev",
                "--producer-workflow",
                "dbt-self-service-dev-evidence.yml",
                "--source-commit",
                "d" * 40,
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID"
    assert payload["stage"] == "dbt_dev_evidence"


def test_run_dev_evidence_campaign_missing_request_is_local_error(
    tmp_path: Path,
    capsys,
) -> None:
    with pytest.raises(SystemExit, match="2"):
        cli_main.main(
            [
                "dbt",
                "run-dev-evidence-campaign",
                "--request",
                str(tmp_path / "missing-request.json"),
                "--evidence-root",
                str(tmp_path),
                "--airflow-api-url",
                "https://airflow.internal",
                "--airflow-api-version",
                "v2",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID"
    assert payload["stage"] == "dbt_dev_evidence"
    assert "Airflow did not produce" not in payload["message"]


@pytest.mark.parametrize(
    ("arguments", "path"),
    [
        (("--timeout-seconds", "29"), "timeout_seconds"),
        (("--timeout-seconds", "7201"), "timeout_seconds"),
        (("--poll-interval-seconds", "0"), "poll_interval_seconds"),
        (("--poll-interval-seconds", "61"), "poll_interval_seconds"),
        (
            (
                "--timeout-seconds",
                "30",
                "--poll-interval-seconds",
                "31",
            ),
            "poll_interval_seconds",
        ),
        (("--request-timeout-seconds", "0"), "request_timeout_seconds"),
        (("--request-timeout-seconds", "61"), "request_timeout_seconds"),
    ],
)
def test_run_dev_evidence_campaign_rejects_invalid_limits_before_io(
    arguments: tuple[str, ...],
    path: str,
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dbt_dev_evidence_campaign_cmd,
        "read_dev_evidence_request",
        lambda _path: pytest.fail("request must not be read for invalid CLI limits"),
    )

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(
            [
                "dbt",
                "run-dev-evidence-campaign",
                "--request",
                str(tmp_path / "request.json"),
                "--evidence-root",
                str(tmp_path),
                "--airflow-api-url",
                "https://airflow.internal",
                "--airflow-api-version",
                "v2",
                *arguments,
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_DBT_DEV_EVIDENCE_LIMIT_INVALID"
    assert payload["stage"] == "dbt_dev_evidence"
    assert payload["path"] == path
    assert payload["fixes"][0]["safety"] == "manual"


def test_verify_dev_evidence_integrity_cli_redacts_contract_failure(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Service:
        def verify(self, **_kwargs):
            raise dbt_dev_evidence_cmd.DbtDevEvidenceBundleError("private evidence detail")

    monkeypatch.setattr(
        dbt_dev_evidence_cmd,
        "build_dbt_dev_evidence_bundle_service",
        lambda: Service(),
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "verify-dev-evidence-integrity",
                "--compiled-root",
                str(tmp_path / "compiled"),
                "--evidence-root",
                str(tmp_path / "trusted"),
                "--expected-release-id",
                "sha256:" + "b" * 64,
                "--expected-deployment-id",
                "sha256:" + "c" * 64,
                "--format",
                "json",
            ]
        )

    output = capsys.readouterr()
    error = json.loads(output.out)
    assert error["schema"] == "dpone.error.v1"
    assert error["code"] == "DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID"
    assert output.err == ""
    assert "private evidence detail" not in output.out


@pytest.mark.parametrize(
    "error_code",
    [
        "DPONE_DBT_PROJECT_BUNDLE_INVALID",
        "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED",
    ],
)
def test_dbt_compile_materialization_failure_is_not_internal(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
) -> None:
    class RejectingMaterializer:
        def materialize(self, **_kwargs):
            raise dbt_publish_cmd.DbtReleaseMaterializationError(
                "private release detail",
                code=error_code,
            )

    class PassingWriter:
        def __init__(self, **_kwargs):
            pass

        def write(self, report, *_args, **_kwargs):
            return report

    monkeypatch.setattr(
        AppContext,
        "build_dbt_release_materializer",
        lambda _self: RejectingMaterializer(),
    )
    monkeypatch.setattr(
        AppContext,
        "build_dbt_artifact_writer",
        lambda _self, *, dbt_profiles_dir: PassingWriter(),
    )
    monkeypatch.setattr(
        dbt_publish_cmd,
        "_build",
        lambda _args, *, context: SimpleNamespace(
            passed=True,
            release_id="sha256:" + "a" * 64,
        ),
    )

    with pytest.raises(SystemExit, match="1"):
        cli_main.main(
            [
                "dbt",
                "compile",
                *_common(),
                "--output-dir",
                str(tmp_path / "airflow"),
                "--cache-root",
                str(tmp_path / "cache"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == error_code
    assert "private release detail" not in json.dumps(payload)
    if error_code == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED":
        assert ".promotion.lock" in json.dumps(payload)
        assert "certified CI compile" not in json.dumps(payload)
