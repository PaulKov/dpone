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


def test_route_transport_certification_cli_writes_route_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))
    manifest = _manifest(tmp_path)
    artifact_dir = tmp_path / "certification"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-transport-certification",
                "--manifest",
                str(manifest),
                "--profile",
                "static",
                "--artifact-dir",
                str(artifact_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "dpone.native_transfer.route_certification.v1"
    assert payload["status"] == "unverified"
    assert payload["passed"] is False
    assert payload["behavior_passed"] is True
    assert payload["evidence_status"] == "UNVERIFIED"
    assert payload["route"] == {"source": "postgres", "sink": "clickhouse", "strategy": "full_refresh"}
    assert payload["certified_transports"] == ["stream"]
    assert payload["capability_hash"].startswith("sha256:")
    assert payload["decision"]["selected_transport"] == "stream"
    assert (artifact_dir / "native_transfer_route_certification.json").exists()
    assert (artifact_dir / "native_transfer_route_certification.md").exists()


def _manifest(tmp_path: Path) -> Path:
    path = tmp_path / "manifest.yaml"
    path.write_text(
        """
name: orders
source:
  type: postgres
  connection_id: postgres
  table:
    schema: public
    name: orders
  options:
    extract_mode: copy_to_stdout
    native_transfer:
      execution:
        transport:
          mode: auto
        certification:
          mode: certified_only
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path
