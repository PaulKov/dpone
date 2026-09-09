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


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_strategy_certification_bundle_cli_writes_json_and_markdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    replay = _write_json(tmp_path / "replay.json", {"status": "executed", "state_committed": True})
    matrix = _write_json(tmp_path / "matrix.json", {"passed": True, "case_count": 200})
    native_transfer = _write_json(
        tmp_path / "evidence_index.json",
        {
            "schema_version": "dpone.native_transfer.evidence.v1",
            "contract": {"route": "postgres_to_mssql", "strategy": "cdc_apply"},
            "artifacts": [{"name": "typed_reconciliation.json", "sha256": "a" * 64}],
        },
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "strategy",
                "certification-bundle",
                "--bundle-id",
                "cli_bundle",
                "--output-dir",
                str(tmp_path / "bundle"),
                "--replay-evidence",
                str(replay),
                "--matrix-artifact",
                str(matrix),
                "--native-transfer-evidence",
                str(native_transfer),
                "--docs-link",
                "docs/testing/replay-integration.md",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["json_path"].endswith("strategy_certification_bundle.json")
    assert payload["markdown_path"].endswith("strategy_certification_bundle.md")
    bundle = json.loads(Path(payload["json_path"]).read_text(encoding="utf-8"))
    assert bundle["passed"] is True
    assert [item["kind"] for item in bundle["evidence_items"]] == ["replay", "matrix", "native_transfer"]


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path
