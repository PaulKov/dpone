from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "promote_snapshot_to_argocd_mr.py"
SPEC = importlib.util.spec_from_file_location("promote_snapshot_to_argocd_mr_tool", TOOL_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_promote_tool_dry_run_json(tmp_path: Path, monkeypatch, capsys) -> None:
    snapshot_env = tmp_path / "snapshot.env"
    snapshot_env.write_text(
        "DPONE_PACKAGE_SPEC=dpone==1.2.3.dev45\nDPONE_SNAPSHOT_VERSION=1.2.3.dev45\n",
        encoding="utf-8",
    )
    repo_dir = tmp_path / "argocd-main"
    repo_dir.mkdir()
    exit_code = MODULE.main(
        [
            "--snapshot-env",
            str(snapshot_env),
            "--repo-dir",
            str(repo_dir),
            "--dry-run",
            "--no-open-mr",
            "--format",
            "json",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["package_spec"] == "dpone==1.2.3.dev45"
    assert payload["dry_run"] is True
    assert payload["branch_name"].startswith("auto/dpone-airflow-dev/")
