"""Actionable stale-manifest recovery without executing dbt or reading secrets."""

from __future__ import annotations

import argparse
import json
import os
import shlex
from pathlib import Path

import pytest

from dpone.commands.dbt_publish_cli_support import emit_failure, stale_default_manifest


@pytest.mark.parametrize("profile_mode", ["regular", "missing", "linked_file", "linked_directory"])
@pytest.mark.parametrize("output_format", ["text", "json"])
def test_stale_recovery_preserves_selected_project_and_safe_local_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_mode: str,
    output_format: str,
) -> None:
    root = tmp_path / "project with spaces; $(not-a-command)"
    root.mkdir()
    manifest = root / "target" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text("{}", encoding="utf-8")
    (root / "dbt_project.yml").write_text("name: demo\nversion: '1'\n", encoding="utf-8")
    os.utime(manifest, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    profiles = root / "profiles"
    if profile_mode == "linked_directory":
        elsewhere = tmp_path / "external"
        elsewhere.mkdir()
        (elsewhere / "profiles.yml").write_text("do not read", encoding="utf-8")
        profiles.symlink_to(elsewhere, target_is_directory=True)
    elif profile_mode != "missing":
        profiles.mkdir()
        if profile_mode == "linked_file":
            (profiles / "profiles.yml").symlink_to(root / "dbt_project.yml")
        else:
            (profiles / "profiles.yml").write_text("do not read", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    report = stale_default_manifest(argparse.Namespace(project=str(root)), manifest)
    assert report is not None
    hint = report.blockers[0].remediation
    assert hint is not None
    command = hint.split("`", 2)[1]
    expected = ["dbt", "parse", "--project-dir", str(root), "--no-partial-parse"]
    if profile_mode == "regular":
        expected += ["--profiles-dir", str(profiles)]
    assert shlex.split(command) == expected
    emit_failure(report.blockers, output_format, stage="check")
    output = capsys.readouterr()
    assert output.err == ""
    if output_format == "json":
        payload = json.loads(output.out)
        assert payload["code"] == "DPONE_DBT_MANIFEST_STALE"
        assert payload["fixes"][0]["description"] == hint
    else:
        assert f"Next: {hint}" in output.out
    assert manifest.read_text(encoding="utf-8") == "{}"
