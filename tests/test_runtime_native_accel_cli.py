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


def test_native_accel_doctor_accepts_table_format_alias(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["runtime", "native-accel", "doctor", "--format", "table"])

    assert exc.value.code == 0
    assert "dpone runtime native-accel doctor" in capsys.readouterr().out


def test_native_accel_benchmark_writes_output_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "manifest.yml"
    manifest.write_text("name: smoke\nsource: {type: mssql}\nsink: {type: clickhouse}\n", encoding="utf-8")
    output_dir = tmp_path / "benchmark"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "runtime",
                "native-accel",
                "benchmark",
                "--manifest",
                str(manifest),
                "--rows",
                "10000",
                "--output",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_json"] == str(output_dir / "native_accel_benchmark_plan.json")
    assert payload["artifact_markdown"] == str(output_dir / "native_accel_benchmark_plan.md")
    assert json.loads((output_dir / "native_accel_benchmark_plan.json").read_text(encoding="utf-8"))["rows"] == 10000
    assert "dpone runtime native-accel benchmark" in (output_dir / "native_accel_benchmark_plan.md").read_text(
        encoding="utf-8"
    )
