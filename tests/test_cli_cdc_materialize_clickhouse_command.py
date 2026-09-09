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


def test_ops_cdc_materialize_clickhouse_cli_delegates_connection_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"passed": True, "target_dataset": "serving.orders_current"}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Materialization:
        def materialize(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def materialization(self) -> _Materialization:
            return _Materialization()

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
                "cdc-materialize-clickhouse",
                "--cdc-dataset",
                "analytics.orders_cdc",
                "--target-dataset",
                "serving.orders_current",
                "--unique-key",
                "order_id",
                "--sink-connection-id",
                "clickhouse-prod",
                "--credentials-source",
                "env",
                "--delete-mode",
                "tombstone",
                "--output-dir",
                str(tmp_path / "materialization"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["cdc_dataset"] == "analytics.orders_cdc"
    assert captured["target_dataset"] == "serving.orders_current"
    assert captured["unique_key"] == ("order_id",)
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["credentials_source"] == "env"
    assert captured["delete_mode"] == "tombstone"
