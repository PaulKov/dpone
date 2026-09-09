from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_consumer_matrix import SchemaConsumerMatrixBuilder
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_contract_registry_store import LocalJsonSchemaContractRegistryStore
from dpone.services.schema_migration import MigrationControlFacade


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_contract_views_cli_plan_gate_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head_manifest = _manifest(version="2.0.0", drop_amount=True, serving=True)
    matrix = SchemaConsumerMatrixBuilder().build(
        base=base,
        head=SchemaContractVersionBuilder().build(manifest=head_manifest),
        inventory=_inventory(),
        unknown_consumer="warn",
    )
    base_path = _write_json(tmp_path / "base-contract.json", base)
    manifest_path = _write_manifest(tmp_path / "head.yaml", head_manifest)
    matrix_path = _write_json(tmp_path / "matrix.json", matrix)
    plan_output = tmp_path / "compatibility-views.json"

    for args in (
        ["schema", "contract", "views", "--help"],
        ["schema", "contract", "views", "plan", "--help"],
        ["schema", "contract", "views", "gate", "--help"],
        ["schema", "contract", "views", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "views",
                "plan",
                "--manifest",
                str(manifest_path),
                "--against",
                str(base_path),
                "--consumer-matrix",
                str(matrix_path),
                "--format",
                "json",
                "--output",
                str(plan_output),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(plan_output.read_text(encoding="utf-8"))
    assert plan["schema_version"] == "dpone.schema_contract_compatibility_view_plan.v1"
    assert plan["status"] == "planned"
    assert json.loads(capsys.readouterr().out)["compatibility_view_plan_id"] == plan["compatibility_view_plan_id"]

    consumer_gate = _write_json(
        tmp_path / "consumer-gate.json",
        {
            "schema_version": "dpone.schema_contract_consumer_gate.v1",
            "status": "blocked",
            "consumer_matrix_id": matrix["consumer_matrix_id"],
            "blockers": matrix["blockers"],
            "warnings": matrix["warnings"],
        },
    )
    gate_output = tmp_path / "compatibility-view-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "views",
                "gate",
                "--plan",
                str(plan_output),
                "--consumer-gate",
                str(consumer_gate),
                "--format",
                "json",
                "--output",
                str(gate_output),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(gate_output.read_text(encoding="utf-8"))
    assert gate["schema_version"] == "dpone.schema_contract_compatibility_view_gate.v1"
    assert gate["status"] == "allowed"

    report_output = tmp_path / "compatibility-view-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "views",
                "report",
                "--plan",
                str(plan_output),
                "--format",
                "md",
                "--output",
                str(report_output),
            ]
        )
    assert report_exit.value.code == 0
    assert "# Schema Contract Compatibility Views" in report_output.read_text(encoding="utf-8")

    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "column_removed", "path": "amount"},),
        strategy="expand_contract",
    )
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    bundle_dir = tmp_path / "bundle"
    with pytest.raises(SystemExit) as bundle_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--compatibility-view-gate",
                str(gate_output),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert bundle_exit.value.code == 0
    bundle = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["summary"]["compatibility_view_gate_id"] == gate["compatibility_view_gate_id"]

    registry_output = tmp_path / "registry-record.json"
    with pytest.raises(SystemExit) as registry_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_dir / "bundle.json"),
                "--compatibility-view-gate",
                str(gate_output),
                "--environment",
                "prod",
                "--stage",
                "compatibility_view_planned",
                "--store-uri",
                str(tmp_path / "registry.json"),
                "--format",
                "json",
                "--output",
                str(registry_output),
            ]
        )
    assert registry_exit.value.code == 0
    registry_record = json.loads(registry_output.read_text(encoding="utf-8"))
    assert registry_record["stage"] == "compatibility_view_planned"
    assert "compatibility_view_gate" in {item["kind"] for item in registry_record["artifact_refs"]}


def test_migration_plan_embeds_compatibility_view_summary(tmp_path: Path) -> None:
    store = tmp_path / "contracts.json"
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0", store_uri=store))
    LocalJsonSchemaContractRegistryStore(store).append(base)
    manifest_path = _write_manifest(
        tmp_path / "manifest.yaml",
        _manifest(version="2.0.0", drop_amount=True, serving=True, store_uri=store),
    )

    payload = MigrationControlFacade().plan(manifest_path=str(manifest_path))

    summary = payload["compatibility_view_summary"]
    assert summary["enabled"] is True
    assert summary["status"] == "planned"
    assert summary["views_count"] == 1
    assert payload["phases"][0]["name"] == "compatibility_views_expand"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest(
    *,
    version: str,
    drop_amount: bool = False,
    serving: bool = False,
    store_uri: Path | str = ".dpone/schema-contracts/registry.json",
) -> dict[str, object]:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
    }
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": True}
    contract: dict[str, object] = {
        "id": "analytics.orders",
        "version": version,
        "owner": "data-platform",
        "compatibility": "backward",
        "registry": {"enabled": True, "mode": "gate", "store_backend": "local_json", "store_uri": str(store_uri)},
        "versioning": {"semver": "strict", "unknown_consumer": "warn"},
        "consumers": {
            "manual": [
                {
                    "id": "finance.daily_margin",
                    "type": "dashboard",
                    "owner": "finance-analytics",
                    "version_constraint": "1.x",
                    "reads": {"columns": ["amount", "customer_id"]},
                }
            ]
        },
        "columns": columns,
    }
    if serving:
        contract["serving"] = {
            "enabled": True,
            "mode": "gate",
            "view_naming": "{schema}.{table}__contract_v{major}",
            "versions": [
                {
                    "constraint": "1.x",
                    "source_contract": "analytics.orders@1.5.0",
                    "view": "analytics.orders__contract_v1",
                    "remove_after": "2099-09-01",
                    "owner": "data-platform",
                }
            ],
        }
    return {
        "source": {"options": {"columns": [{"name": key, "type": "string"} for key in columns]}},
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": contract,
                "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
            },
        },
    }


def _inventory() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_consumer_inventory.v1",
        "contract_id": "analytics.orders",
        "status": "discovered",
        "blockers": [],
        "warnings": [],
        "consumers": [
            {
                "id": "finance.daily_margin",
                "type": "dashboard",
                "owner": "finance-analytics",
                "source": "manual",
                "version_constraint": "1.x",
                "reads": {"columns": ["amount", "customer_id"]},
            }
        ],
    }


def _write_manifest(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
