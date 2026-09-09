from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_impact import SchemaImpactFacade


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_impact_group_help_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as group_exit:
        cli_main.main(["schema", "impact", "--help"])
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(["schema", "impact", "plan", "--help"])

    assert group_exit.value.code == 0
    assert plan_exit.value.code == 0


def test_schema_impact_plan_outputs_json_md_text_and_file(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path, mode="gate")
    pack = _write_pack(tmp_path, risk="destructive")
    output = tmp_path / "impact.json"

    with pytest.raises(SystemExit) as json_exit:
        cli_main.main(
            [
                "schema",
                "impact",
                "plan",
                "--pack",
                str(pack),
                "--manifest",
                str(manifest),
                "--output",
                str(output),
                "--format",
                "json",
            ]
        )

    assert json_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_impact_plan.v1"
    assert payload["summary"]["max_severity"] == "critical"
    assert json.loads(output.read_text(encoding="utf-8"))["required_approvals"] == [
        "compatibility_breaking",
        "data_destructive",
    ]

    with pytest.raises(SystemExit) as md_exit:
        cli_main.main(["schema", "impact", "plan", "--pack", str(pack), "--manifest", str(manifest), "--format", "md"])
    assert md_exit.value.code == 0
    assert "Schema Impact Plan" in capsys.readouterr().out


def test_schema_impact_gate_blocks_and_allows_with_approval(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path, mode="gate")
    pack = _write_pack(tmp_path, risk="unsafe_direct_rename")
    impact = tmp_path / "impact.json"
    approval = tmp_path / "approval.yaml"

    with pytest.raises(SystemExit):
        cli_main.main(
            [
                "schema",
                "impact",
                "plan",
                "--pack",
                str(pack),
                "--manifest",
                str(manifest),
                "--output",
                str(impact),
                "--format",
                "json",
            ]
        )
    plan_payload = json.loads(capsys.readouterr().out)

    with pytest.raises(SystemExit) as blocked:
        cli_main.main(["schema", "impact", "gate", "--pack", str(pack), "--impact", str(impact), "--format", "json"])

    assert blocked.value.code == 2
    blocked_payload = json.loads(capsys.readouterr().out)
    assert "schema_impact.approval_required:direct_rename" in blocked_payload["blockers"]

    approval.write_text(
        yaml.safe_dump(
            {
                "pack_id": plan_payload["pack_id"],
                "impact_plan_id": plan_payload["impact_plan_id"],
                "approved_by": "finance-data-owner",
                "approved_risks": ["compatibility_breaking", "direct_rename"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as allowed:
        cli_main.main(
            [
                "schema",
                "impact",
                "gate",
                "--pack",
                str(pack),
                "--impact",
                str(impact),
                "--approval",
                str(approval),
                "--format",
                "json",
            ]
        )

    assert allowed.value.code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "allowed"


def test_schema_migration_plan_embeds_impact_summary_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path, mode="gate")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["schema", "migration", "plan", "--manifest", str(manifest), "--format", "json"])

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_migration_pack.v1"
    assert payload["impact_summary"]["schema_version"] == "dpone.schema_impact_plan.v1"
    assert payload["impact_summary"]["pack_id"] == payload["pack_id"]


def test_schema_migration_apply_runs_impact_gate_when_pack_contains_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path, mode="gate")
    plan_path = tmp_path / "pack.json"
    ledger = tmp_path / "ledger.json"
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "drop_column", "path": "columns.amount", "risk": "destructive"},),
    )
    payload = pack.to_dict()
    payload["impact_summary"] = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))
    plan_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            ["schema", "migration", "apply", "--plan", str(plan_path), "--ledger", str(ledger), "--format", "json"]
        )

    assert apply_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert any(item.startswith("schema_impact.approval_required") for item in payload["blockers"])
    assert not ledger.exists()


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_pack(tmp_path: Path, *, risk: str) -> Path:
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=(
            {
                "change_type": "direct_rename" if risk == "unsafe_direct_rename" else "drop_column",
                "path": "columns.amount",
                "risk": risk,
            },
        ),
    )
    path = tmp_path / "pack.json"
    path.write_text(json.dumps(pack.to_dict(), ensure_ascii=False), encoding="utf-8")
    return path


def _write_manifest(tmp_path: Path, *, mode: str) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"options": {"columns": [{"name": "id", "type": "bigint"}]}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "analytics", "name": "orders"},
                    "options": {
                        "schema_impact": {
                            "enabled": True,
                            "mode": mode,
                            "approval": {
                                "required_for": [
                                    "compatibility_breaking",
                                    "data_destructive",
                                    "direct_rename",
                                    "shadow_cutover",
                                ]
                            },
                            "sources": {
                                "manual": {
                                    "consumers": [
                                        {
                                            "id": "finance.daily_margin",
                                            "type": "dashboard",
                                            "owner": "finance-analytics",
                                            "reads": [
                                                {"dataset": "clickhouse.analytics.orders", "columns": ["amount"]}
                                            ],
                                        }
                                    ]
                                }
                            },
                        }
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest
