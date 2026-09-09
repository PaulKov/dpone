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


def test_connectors_scaffold_cli_generates_sdk_package_with_certification(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "connectors",
                "scaffold",
                "demo_api",
                "--root",
                str(tmp_path),
                "--connector-type",
                "api",
                "--capability",
                "source",
                "--capability",
                "sink",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    connector_root = tmp_path / "dpone-connector-demo-api"

    assert payload["connector"] == "demo_api"
    assert payload["package_name"] == "dpone-connector-demo-api"
    assert payload["sdk_root"] == str(connector_root)
    assert payload["certification_manifest"] == str(connector_root / "certification" / "certification.yaml")
    assert (connector_root / "pyproject.toml").exists()
    assert (connector_root / "certification" / "run_certification.py").exists()


def test_connectors_scaffold_cli_accepts_native_transfer_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "connectors",
                "scaffold",
                "warehouse_db",
                "--root",
                str(tmp_path),
                "--connector-type",
                "database",
                "--capability",
                "source",
                "--native-capability",
                "stream_export",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["native_capabilities"] == ["stream_export"]


def test_connectors_certify_cli_renders_native_transfer_capability_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    certification_dir = tmp_path / "certification"
    certification_dir.mkdir()
    (certification_dir / "certification.yaml").write_text(
        """
contract_version: "1"
connector: warehouse_db
connector_type: database
status: community
native_transfer:
  capabilities:
    stream_export:
      formats: [tabseparated, jsonl]
      bounded: true
      supports_checksum: true
      supports_cleanup: true
    stream_staging_load:
      formats: [tabseparated, jsonl]
      staging_safe: true
      supports_abort: true
      supports_idempotency_key: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    artifact_dir = tmp_path / "artifacts"
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "connectors",
                "certify",
                "--profile",
                "static",
                "--capability",
                "native_transfer.stream",
                "--artifact-dir",
                str(artifact_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "dpone.connector_capability_certification.v1"
    assert payload["connector"] == "warehouse_db"
    assert payload["profile"] == "static"
    assert payload["passed"] is False
    assert payload["capabilities"]["native_transfer.stream"]["status"] == "unverified"
    assert payload["blockers"] == ["static_profile_is_plan_only"]
    assert (artifact_dir / "connector-capability-certification.json").exists()
    assert (artifact_dir / "connector-capability-certification.md").exists()


def test_connectors_certify_cli_does_not_synthesize_vendor_live_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    certification_dir = tmp_path / "certification"
    certification_dir.mkdir()
    (certification_dir / "certification.yaml").write_text(
        """
contract_version: "1"
connector: warehouse_db
connector_type: database
status: community
native_transfer:
  capabilities:
    stream_export:
      formats: [tabseparated]
      bounded: true
      supports_checksum: true
      supports_cleanup: true
    stream_staging_load:
      formats: [tabseparated]
      staging_safe: true
      supports_abort: true
      supports_idempotency_key: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "connectors",
                "certify",
                "--profile",
                "vendor_live",
                "--capability",
                "native_transfer.stream",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["status"] == "unverified"
    assert payload["capabilities"]["native_transfer.stream"]["status"] == "unverified"
    assert payload["blockers"] == ["live_evidence_not_provided:vendor_live"]
