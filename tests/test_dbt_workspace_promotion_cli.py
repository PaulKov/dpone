"""Workspace CLI uses the real services and preserves complete failure reports."""

import json
from pathlib import Path

import jsonschema
import pytest

from dpone.cli import main as cli_main
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from tests.test_dbt_workspace_mirror_service import _arguments, _files


def _prepare_args(arguments):
    flags = {
        "root": arguments["repository_root"],
        **{
            key.replace("_", "-"): arguments[key]
            for key in (
                "compiled_root",
                "mirror_root",
                "source_snapshot_path",
                "descriptor_path",
                "expected_release_id",
                "dev_deployment_id",
                "dev_evidence_ref",
            )
        },
        **{key.replace("_", "-"): value for key, value in arguments["trust"].to_dict().items()},
    }
    return [
        "dbt",
        "workspace",
        "prepare-prod-mirror",
        *[token for key, value in flags.items() for token in (f"--{key}", str(value))],
        "--format",
        "json",
    ]


def _invoke(arguments, capsys):
    with pytest.raises(SystemExit) as result:
        cli_main.main(arguments)
    return result.value.code, capsys.readouterr()


def test_prepare_and_verify_cli_match_real_service_artifacts(tmp_path: Path, capsys) -> None:
    arguments, _ = _arguments(tmp_path)
    code, output = _invoke(_prepare_args(arguments), capsys)
    assert code == 0
    prepared = json.loads(output.out)
    jsonschema.validate(prepared, dbt_schema_contracts()[prepared["schema"]])
    assert prepared["projects"] == ["dbt/alpha", "dbt/beta"]
    assert prepared["no_op"] is False
    code, output = _invoke(
        [
            "dbt",
            "workspace",
            "verify-promotion",
            "--root",
            str(arguments["repository_root"]),
            "--descriptor",
            arguments["descriptor_path"],
            "--release-set",
            str(arguments["compiled_root"] / "release-set.json"),
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0
    report = json.loads(output.out)
    jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
    assert report["passed"] and len(report["projects"]) == 2


def test_verify_failure_emits_both_project_rows_and_exit_two(tmp_path: Path, capsys) -> None:
    arguments, _ = _arguments(tmp_path)
    assert _invoke(_prepare_args(arguments), capsys)[0] == 0
    (arguments["repository_root"] / arguments["mirror_root"] / "dbt/beta/dbt_project.yml").unlink()
    code, output = _invoke(
        [
            "dbt",
            "workspace",
            "verify-promotion",
            "--root",
            str(arguments["repository_root"]),
            "--descriptor",
            arguments["descriptor_path"],
            "--release-set",
            str(arguments["compiled_root"] / "release-set.json"),
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 2
    report = json.loads(output.out)
    jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
    assert not report["passed"]
    assert report["projects"][0]["passed"]
    assert report["projects"][1]["observed_project_bundle_sha256"] is None


def test_missing_campaign_flag_is_usage_error_without_file_changes(tmp_path: Path, capsys) -> None:
    arguments, _ = _arguments(tmp_path)
    command = _prepare_args(arguments)
    index = command.index("--dev-evidence-set-id")
    del command[index : index + 2]
    before = _files(arguments["repository_root"])
    code, output = _invoke(command, capsys)
    assert code == 2 and "--dev-evidence-set-id" in output.err
    assert _files(arguments["repository_root"]) == before


@pytest.mark.parametrize("command", ["prepare-prod-mirror", "verify-promotion"])
def test_workspace_help_requires_no_application_context_or_database(command: str, capsys) -> None:
    code, output = _invoke(["dbt", "workspace", command, "--help"], capsys)
    assert code == 0 and "--root" in output.out
