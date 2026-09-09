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


def _merge_train(path: Path, *, merge_state: str = "CLEAN") -> Path:
    return _write_json(
        path,
        {
            "base_branch": "codex/route-bootstrap-doctor",
            "head_branch": "codex/route-conformance-lab",
            "pull_requests": [
                {
                    "number": 76,
                    "title": "Add route conformance lab",
                    "base_ref": "codex/route-bootstrap-doctor",
                    "head_ref": "codex/route-conformance-lab",
                    "state": "OPEN",
                    "merge_state": merge_state,
                    "is_draft": False,
                    "checks": [{"name": "docs", "status": "COMPLETED", "conclusion": "SUCCESS"}],
                    "url": "https://github.com/PaulKov/dpone/pull/76",
                }
            ],
        },
    )


def _artifact(path: Path, *, passed: bool = True) -> Path:
    return _write_json(
        path,
        {
            "passed": passed,
            "summary": "ok" if passed else "failed",
            "blockers": [] if passed else ["artifact.failed"],
        },
    )


def test_ops_release_rc_finalize_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    route_finalizer = _artifact(tmp_path / "route_release_finalizer.json")
    release_pack = _artifact(tmp_path / "release_evidence_pack.json")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-rc-finalize",
                "--release",
                "v0.10.0",
                "--previous-release",
                "v0.9.0",
                "--package-version",
                "0.10.0",
                "--merge-train-json",
                str(_merge_train(tmp_path / "merge_train.json")),
                "--artifact",
                f"route_release_finalizer={route_finalizer}",
                "--artifact",
                f"release_evidence_pack={release_pack}",
                "--output-dir",
                str(tmp_path / "rc-final"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert payload["level"] == "rc_ready"
    assert payload["merge_train"]["pull_requests"][0]["number"] == 76
    assert Path(payload["json_path"]).exists()


def test_ops_release_rc_finalize_cli_returns_nonzero_for_blocked_train(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-rc-finalize",
                "--release",
                "v0.10.0",
                "--previous-release",
                "v0.9.0",
                "--package-version",
                "0.10.0",
                "--merge-train-json",
                str(_merge_train(tmp_path / "merge_train.json", merge_state="DIRTY")),
                "--output-dir",
                str(tmp_path / "rc-final"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 1
    assert payload["passed"] is False
    assert "pull_request.76.not_clean" in payload["blockers"]
    assert "route_release_finalizer.missing" in payload["blockers"]
