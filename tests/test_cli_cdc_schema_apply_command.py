from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.services.ops import command_handlers_cdc


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_ops_cdc_schema_apply_cli_delegates_typed_refresh_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    schema_change = tmp_path / "schema_change.json"
    schema_change.write_text('{"change": {}, "plan": {}}', encoding="utf-8")
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {
                "schema_version": "dpone.cdc_schema_apply.v1",
                "passed": True,
                "target_dataset": "serving.orders_current_typed",
            }

        def to_markdown(self) -> str:
            return "# fake\n"

    class _SchemaApply:
        def apply(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def schema_apply(self) -> _SchemaApply:
            return _SchemaApply()

    class _Release:
        def cdc(self) -> _Cdc:
            return _Cdc()

    monkeypatch.setattr(
        command_handlers_cdc.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-schema-apply",
                "--schema-change-json",
                str(schema_change),
                "--sink",
                "clickhouse",
                "--target-dataset",
                "serving.orders_current_typed",
                "--mode",
                "apply",
                "--sink-connection-id",
                "clickhouse-prod",
                "--credentials-source",
                "env",
                "--typed-refresh",
                "--cdc-dataset",
                "analytics.orders_cdc",
                "--unique-key",
                "order_id",
                "--column",
                "order_id=Int32",
                "--column",
                "status_reason=Nullable(String)",
                "--fail-on-parse-errors",
                "--schema-drift-mode",
                "strict",
                "--quarantine-dataset",
                "serving.orders_parse_quarantine",
                "--require-approval",
                "--output-dir",
                str(tmp_path / "schema_apply"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["schema_change_json"] == str(schema_change)
    assert captured["sink"] == "clickhouse"
    assert captured["target_dataset"] == "serving.orders_current_typed"
    assert captured["mode"] == "apply"
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["typed_refresh"] is True
    assert captured["cdc_dataset"] == "analytics.orders_cdc"
    assert captured["unique_key"] == ("order_id",)
    assert captured["columns"] == ("order_id=Int32", "status_reason=Nullable(String)")
    assert captured["fail_on_parse_errors"] is True
    assert captured["schema_drift_mode"] == "strict"
    assert captured["quarantine_dataset"] == "serving.orders_parse_quarantine"
    assert captured["require_approval"] is True
