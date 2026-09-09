"""Native CLI parity and safe report destination regression tests."""

import json
import shutil

import pytest

from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from tests.dbt_compact_wire_v2_helpers import SIDECAR
from tests.test_dbt_compact_wire_v2 import compiled_workspace as compiled_workspace


def invoke_native_cli(tmp_path, compiled, capsys, *arguments):
    import argparse
    import logging
    from types import SimpleNamespace

    from dpone.adapters.fs_local import LocalFileSystem
    from dpone.commands.gitops.airflow_compact_pack_release_cmd import (
        cmd_gitops_airflow_release_materialize,
        register_release_materialize_parser,
    )

    parser = argparse.ArgumentParser()
    register_release_materialize_parser(parser.add_subparsers())
    args = parser.parse_args(
        [
            "release-materialize",
            "--pack-root",
            str(compiled),
            "--cache-root",
            "cache",
            "--xcom-sidecar-image",
            SIDECAR,
            *arguments,
        ]
    )
    ctx = SimpleNamespace(settings=SimpleNamespace(repo_root=tmp_path), fs=LocalFileSystem())
    code = cmd_gitops_airflow_release_materialize(args, ctx=ctx, logger=logging.getLogger("synthetic-cli"))
    return code, capsys.readouterr()


@pytest.mark.parametrize("output", ["compiled/release-set.json", "cache/report.json", "../SENSITIVE_SENTINEL"])
def test_cli_rejects_managed_or_unsafe_output_before_publication(compiled_workspace, tmp_path, capsys, output):
    compiled = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, compiled)
    before = (compiled / "release-set.json").read_bytes()
    code, captured = invoke_native_cli(tmp_path, compiled, capsys, "--output", output)
    assert code != 0
    assert not (tmp_path / "cache").exists()
    assert (compiled / "release-set.json").read_bytes() == before
    assert "SENSITIVE_SENTINEL" not in captured.out + captured.err


@pytest.mark.parametrize("mode", ["json", "markdown"])
def test_native_cli_output_matches_api(compiled_workspace, tmp_path, capsys, mode):
    code, captured = invoke_native_cli(tmp_path, compiled_workspace, capsys, "--format", mode, "--output", "report.txt")
    assert code == 0 and captured.err == ""
    text = (tmp_path / "report.txt").read_text()
    assert text.strip() == captured.out.strip()
    report = json.loads(text if mode == "json" else text.split("```json\n")[1].split("```", 1)[0])
    api = materialize_compact_pack_release(
        pack_root=compiled_workspace, cache_root=tmp_path / "cache", xcom_sidecar_image=SIDECAR
    )
    assert report["release_id"] == api.release_id and report["passed"]


@pytest.mark.parametrize("link_type", ["symlink", "hardlink"])
def test_cli_report_alias_cannot_modify_input(compiled_workspace, tmp_path, capsys, link_type):
    compiled = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, compiled)
    source = compiled / "release-set.json"
    before = source.read_bytes()
    destination = tmp_path / "report.json"
    if link_type == "symlink":
        destination.symlink_to(source)
    else:
        destination.hardlink_to(source)
    code, captured = invoke_native_cli(tmp_path, compiled, capsys, "--output", "report.json")
    assert code == 2 and "DPONE_COMPACT_PACK_RELEASE_OUTPUT_INVALID" in captured.out
    assert source.read_bytes() == before and not (tmp_path / "cache").exists()
