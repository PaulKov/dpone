from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _gh_pr(path: Path, *, number: int, base: str, head: str) -> Path:
    return _write_json(
        path,
        {
            "number": number,
            "title": f"PR {number}",
            "baseRefName": base,
            "headRefName": head,
            "state": "OPEN",
            "mergeStateStatus": "CLEAN",
            "isDraft": False,
            "statusCheckRollup": [{"name": "docs", "status": "COMPLETED", "conclusion": "SUCCESS"}],
        },
    )


def test_ops_release_rc_collect_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-rc-collect",
                "--release",
                "v0.10.0",
                "--previous-release",
                "v0.9.0",
                "--package-version",
                "0.10.0",
                "--base-branch",
                "codex/a",
                "--head-branch",
                "codex/c",
                "--pull-request-json",
                str(_gh_pr(tmp_path / "pr-1.json", number=1, base="codex/a", head="codex/b")),
                "--pr-json",
                str(_gh_pr(tmp_path / "pr-2.json", number=2, base="codex/b", head="codex/c")),
                "--artifact",
                "route_release_finalizer=test_artifacts/release/v0.10.0/route_release_finalizer.json",
                "--artifact",
                "release_evidence_pack=test_artifacts/release/v0.10.0/release_evidence_pack.json",
                "--output-dir",
                str(tmp_path / "rc-collect"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert payload["merge_train"]["pull_requests"][1]["number"] == 2
    assert Path(payload["merge_train_path"]).exists()
    assert Path(payload["inputs_path"]).exists()


def test_ops_release_rc_collect_cli_returns_nonzero_without_pr_inputs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-rc-collect",
                "--release",
                "v0.10.0",
                "--previous-release",
                "v0.9.0",
                "--package-version",
                "0.10.0",
                "--base-branch",
                "master",
                "--head-branch",
                "codex/release",
                "--output-dir",
                str(tmp_path / "rc-collect"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 1
    assert payload["passed"] is False
    assert "release_rc_collect.pull_requests_missing" in payload["blockers"]
