from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_data_product_trust_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    for kind, payload in _complete_evidence().items():
        _write_json(evidence_dir / f"{kind}.json", payload)

    for args in (
        ["data", "product", "trust", "--help"],
        ["data", "product", "trust", "lake", "index", "--help"],
        ["data", "product", "trust", "query", "--help"],
        ["data", "product", "trust", "snapshot", "--help"],
        ["data", "product", "trust", "gate", "--help"],
        ["data", "product", "trust", "report", "--help"],
        ["data", "product", "trust", "export", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()
    for args in (
        ["schema", "migration", "bundle", "build", "--help"],
        ["schema", "migration", "registry", "record", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
        help_text = capsys.readouterr().out
        assert "--data-product-trust-gate" in help_text
        assert "--data-product-trust-report" in help_text
        assert "--data-product-trust-export" in help_text
        if args[-3:] == ["registry", "record", "--help"]:
            assert "trust_gate_passed" in help_text

    index_path = tmp_path / "trust-index.json"
    with pytest.raises(SystemExit) as index_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "lake",
                "index",
                "--manifests",
                str(manifest),
                "--evidence-dir",
                str(evidence_dir),
                "--format",
                "json",
                "--output",
                str(index_path),
            ]
        )
    assert index_exit.value.code == 0
    index = json.loads(capsys.readouterr().out)
    assert index["schema_version"] == "dpone.data_product_evidence_lake_index.v1"
    assert _read_json(index_path)["evidence_lake_index_id"] == index["evidence_lake_index_id"]

    with pytest.raises(SystemExit) as query_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "query",
                "--index",
                str(index_path),
                "--product-id",
                "analytics.orders",
                "--domain",
                "quality",
                "--format",
                "json",
            ]
        )
    assert query_exit.value.code == 0
    query = json.loads(capsys.readouterr().out)
    assert query["summary"]["matched"] == 1

    snapshot_path = tmp_path / "trust-snapshot.json"
    with pytest.raises(SystemExit) as snapshot_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "snapshot",
                "--index",
                str(index_path),
                "--product-id",
                "analytics.orders",
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(snapshot_path),
            ]
        )
    assert snapshot_exit.value.code == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["status"] == "allowed"

    gate_path = tmp_path / "trust-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "gate",
                "--snapshot",
                str(snapshot_path),
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "allowed"

    report_path = tmp_path / "trust.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "report",
                "--snapshot",
                str(snapshot_path),
                "--gate",
                str(gate_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    report = capsys.readouterr().out
    assert "# Data Product Trust Center" in report
    assert report_path.read_text(encoding="utf-8") == report

    export_path = tmp_path / "trust-export.json"
    with pytest.raises(SystemExit) as export_exit:
        cli_main.main(
            [
                "data",
                "product",
                "trust",
                "export",
                "--snapshot",
                str(snapshot_path),
                "--target",
                "opa",
                "--format",
                "json",
                "--output",
                str(export_path),
            ]
        )
    assert export_exit.value.code == 0
    export = json.loads(capsys.readouterr().out)
    assert export["target"] == "opa"
    assert export_path.read_text(encoding="utf-8") == json.dumps(export, ensure_ascii=False, indent=2) + "\n"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest() -> dict:
    from tests.test_data_product_trust_contracts import _manifest

    return _manifest()


def _complete_evidence() -> dict[str, dict]:
    from tests.test_data_product_trust_contracts import _gate

    return {
        "schema_contract_gate": _gate(
            "dpone.schema_contract_gate.v1", "schema_contract_gate", "schema_contract_gate_id"
        ),
        "data_product_assertion_gate": _gate(
            "dpone.data_product_assertion_gate.v1", "data_product_assertion_gate", "assertion_gate_id"
        ),
        "data_product_policy_gate": _gate(
            "dpone.data_product_policy_gate.v1", "data_product_policy_gate", "policy_gate_id"
        ),
        "data_product_compliance_gate": _gate(
            "dpone.data_product_compliance_gate.v1", "data_product_compliance_gate", "compliance_gate_id"
        ),
        "data_product_access_gate": _gate(
            "dpone.data_product_access_gate.v1", "data_product_access_gate", "access_gate_id"
        ),
        "data_product_connection_rotation_gate": _gate(
            "dpone.data_product_connection_rotation_gate.v1",
            "data_product_connection_rotation_gate",
            "connection_rotation_gate_id",
        ),
        "data_product_cost_gate": _gate("dpone.data_product_cost_gate.v1", "data_product_cost_gate", "cost_gate_id"),
        "data_product_ring_gate": _gate("dpone.data_product_ring_gate.v1", "data_product_ring_gate", "ring_gate_id"),
    }
