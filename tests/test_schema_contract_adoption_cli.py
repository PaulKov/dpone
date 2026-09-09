from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_compatibility_views import CompatibilityViewPlanner
from dpone.readiness.schema_contract_consumer_matrix import SchemaConsumerMatrixBuilder
from dpone.readiness.schema_contract_consumer_test_kit import SchemaConsumerCertificationEvaluator
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_contract_registry_store import LocalJsonSchemaContractRegistryStore
from dpone.services.schema_migration import MigrationControlFacade


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_contract_adoption_cli_plan_status_gate_retire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    base, head, matrix, view_plan = _evidence()
    manifest_path = _write_manifest(tmp_path / "manifest.yaml", _manifest(version="2.0.0", adoption=True))
    matrix_path = _write_json(tmp_path / "matrix.json", matrix)
    view_path = _write_json(tmp_path / "views.json", view_plan)

    for args in (
        ["schema", "contract", "adoption", "--help"],
        ["schema", "contract", "adoption", "plan", "--help"],
        ["schema", "contract", "adoption", "status", "--help"],
        ["schema", "contract", "adoption", "gate", "--help"],
        ["schema", "contract", "adoption", "retire", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "adoption-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "adoption",
                "plan",
                "--manifest",
                str(manifest_path),
                "--consumer-matrix",
                str(matrix_path),
                "--compatibility-view-plan",
                str(view_path),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["schema_version"] == "dpone.schema_contract_adoption_plan.v1"
    assert json.loads(capsys.readouterr().out)["adoption_plan_id"] == plan["adoption_plan_id"]

    certification = SchemaConsumerCertificationEvaluator().evaluate(
        test_kit={
            "schema_version": "dpone.schema_contract_consumer_test_kit.v1",
            "test_kit_id": "sha256:" + "1" * 64,
            "contract_id": "analytics.orders",
            "status": "ready",
            "test_cases": [
                {"test_case_id": "sha256:" + "2" * 64, "consumer_id": "finance.daily_margin", "required": True}
            ],
            "blockers": [],
            "warnings": [],
        },
        result={"status": "passed"},
    )
    cert_path = _write_json(tmp_path / "consumer-certification.json", certification)
    registry_path = _write_json(
        tmp_path / "registry.json",
        {
            "schema_version": "dpone.schema_migration_evidence_registry.v1",
            "record_count": 1,
            "records": [
                {
                    "schema_version": "dpone.schema_migration_evidence_registry_record.v1",
                    "stage": "consumer_migrated",
                    "status": "ready",
                    "artifact_refs": [{"kind": "consumer", "path": "finance.daily_margin"}],
                }
            ],
        },
    )
    status_path = tmp_path / "adoption-status.json"
    with pytest.raises(SystemExit) as status_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "adoption",
                "status",
                "--plan",
                str(plan_path),
                "--registry",
                str(registry_path),
                "--consumer-certification",
                str(cert_path),
                "--format",
                "json",
                "--output",
                str(status_path),
            ]
        )
    assert status_exit.value.code == 0
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["status"] == "migrated"

    gate_path = tmp_path / "retirement-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "adoption",
                "gate",
                "--status",
                str(status_path),
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert gate["status"] == "allowed"

    retire_path = tmp_path / "retirement-plan.json"
    with pytest.raises(SystemExit) as retire_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "adoption",
                "retire",
                "--gate",
                str(gate_path),
                "--compatibility-view-plan",
                str(view_path),
                "--format",
                "json",
                "--output",
                str(retire_path),
            ]
        )
    assert retire_exit.value.code == 0
    retirement = json.loads(retire_path.read_text(encoding="utf-8"))
    assert retirement["status"] == "ready"

    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "compatibility_view_retired", "path": "analytics.orders__contract_v1"},),
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
                "--contract-retirement-gate",
                str(gate_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert bundle_exit.value.code == 0
    bundle = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["summary"]["contract_retirement_gate_id"] == gate["retirement_gate_id"]

    record_path = tmp_path / "registry-record.json"
    with pytest.raises(SystemExit) as registry_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_dir / "bundle.json"),
                "--contract-retirement-gate",
                str(gate_path),
                "--environment",
                "prod",
                "--stage",
                "contract_retirement_ready",
                "--store-uri",
                str(tmp_path / "evidence-registry.json"),
                "--format",
                "json",
                "--output",
                str(record_path),
            ]
        )
    assert registry_exit.value.code == 0
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["stage"] == "contract_retirement_ready"
    assert "contract_retirement_gate" in {item["kind"] for item in record["artifact_refs"]}

    del base, head


def test_migration_plan_embeds_adoption_summary(tmp_path: Path) -> None:
    store = tmp_path / "contracts.json"
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0", store_uri=store))
    LocalJsonSchemaContractRegistryStore(store).append(base)
    manifest_path = _write_manifest(
        tmp_path / "manifest.yaml",
        _manifest(version="2.0.0", drop_amount=True, serving=True, adoption=True, store_uri=store),
    )

    payload = MigrationControlFacade().plan(manifest_path=str(manifest_path))

    summary = payload["adoption_summary"]
    assert summary["enabled"] is True
    assert summary["status"] in {"planned", "blocked"}
    assert summary["contract_id"] == "analytics.orders"
    assert "adoption_plan_id" in summary


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _evidence() -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head_manifest = _manifest(version="2.0.0", drop_amount=True, serving=True, adoption=True)
    head = SchemaContractVersionBuilder().build(manifest=head_manifest)
    matrix = SchemaConsumerMatrixBuilder().build(
        base=base,
        head=head,
        inventory={
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
        },
        unknown_consumer="warn",
    )
    view_plan = CompatibilityViewPlanner().plan(
        manifest=head_manifest,
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
    )
    return base, head, matrix, view_plan


def _manifest(
    *,
    version: str,
    drop_amount: bool = False,
    serving: bool = False,
    adoption: bool = False,
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
        "registry": {"enabled": True, "mode": "gate", "store_backend": "local_json", "store_uri": str(store_uri)},
        "columns": columns,
    }
    if adoption:
        contract["adoption"] = {
            "enabled": True,
            "mode": "gate",
            "profile": "prod_strict",
            "default_migration_window_days": 90,
            "expired_window_policy": "block",
            "require_consumer_certification": True,
            "unknown_consumer": "warn",
        }
    if serving:
        contract["serving"] = {
            "enabled": True,
            "mode": "gate",
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
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {"schema_contract": contract},
        }
    }


def _write_manifest(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
