"""Invalid native stage limits use the CLI's configuration-error boundary."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from dpone.cli.main import main
from dpone.commands.plan_cmd import ExecutionPlanService

SAMPLE = Path(__file__).resolve().parents[1] / "examples/native/clickhouse-to-mssql-native.yaml"
FIELDS = ("encoding_parallelism", "import_parallelism")
FORMATS = ("text", "json", "md")


def _manifest(tmp_path: Path, field: str, value: object, *, omit: bool = False) -> Path:
    raw = yaml.safe_load(SAMPLE.read_text())
    chunks = raw["defaults"]["source"]["options"]["native_transfer"]["execution"]["native_chunks"]
    if not omit:
        chunks[field] = value
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("output_format", FORMATS)
@pytest.mark.parametrize("value", [None, True, "2", 0, 65, 1.5])
def test_invalid_native_stage_limit_is_configuration_error(
    tmp_path, monkeypatch, capsys, caplog, field, output_format, value
):
    path = _manifest(tmp_path, field, value)
    monkeypatch.chdir(tmp_path)
    before = sorted(tmp_path.rglob("*"))
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 2
    assert field in caplog.text
    assert "Traceback" not in caplog.text
    assert capsys.readouterr().out == ""
    assert sorted(tmp_path.rglob("*")) == before


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("output_format", FORMATS)
def test_null_limit_subprocess_has_actionable_stderr(tmp_path, field, output_format):
    path = _manifest(tmp_path, field, None)
    result = subprocess.run(
        [sys.executable, "-m", "dpone.cli.main", "plan", str(path), "--format", output_format],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SAMPLE.parents[2] / "src")},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert f"mssql_native.invalid_limit:{field}" in result.stderr
    assert f"source.options.native_transfer.execution.native_chunks.{field}" in result.stderr
    assert "1..64" in result.stderr
    assert "omit" in result.stderr
    assert "chunking.parallelism" in result.stderr
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("output_format", FORMATS)
@pytest.mark.parametrize("value", [None, 1, 64])
def test_valid_or_omitted_limit_keeps_plan_output(tmp_path, monkeypatch, capsys, field, output_format, value):
    path = _manifest(tmp_path, field, value, omit=value is None)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 0
    output = capsys.readouterr()
    assert "composition_required" in output.out
    assert not output.err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "message",
    [
        "unrelated failure",
        "mssql_native.invalid_limit:max_rows",
        "mssql_native.invalid_limit:encoding_parallelism:extra",
    ],
)
def test_unrelated_value_error_is_not_reclassified(monkeypatch, message):
    def fail(*args, **kwargs):
        raise ValueError(message)

    monkeypatch.setattr(ExecutionPlanService, "plan_manifest", fail)
    with pytest.raises(ValueError, match=message):
        main(["plan", "unused.yaml"])
