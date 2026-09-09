from __future__ import annotations

import json
from pathlib import Path

import yaml

from dpone.services.schema_migration import MigrationControlFacade


def test_schema_migration_pack_embeds_identity_decisions_and_expand_contract_phases(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, strategy="expand_contract")
    source = _write_columns(tmp_path / "source.json", [{"name": "client_id", "dtype": "Int64"}])
    actual = tmp_path / "actual.json"
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "engine": "MergeTree",
                "order_by": ["client_id"],
                "columns": {"client_id": {"type": "Int64"}},
            }
        ),
        encoding="utf-8",
    )

    payload = MigrationControlFacade().plan(
        manifest_path=str(manifest),
        source_path=str(source),
        actual_path=str(actual),
    )

    assert payload["identity_decisions"][0]["action"] == "rename_alias"
    assert payload["identity_migration"]["strategy"] == "expand_contract"
    assert [phase["name"] for phase in payload["phases"][:4]] == ["expand", "backfill", "validate", "contract"]
    assert any("ADD COLUMN" in op["sql"] for op in payload["phases"][0]["operations"])
    assert payload["phases"][2]["validations"][0]["sql"].startswith("SELECT 1")
    assert payload["blockers"] == []


def test_schema_migration_pack_marks_direct_rename_as_unsafe_and_approval_required(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, strategy="direct_rename")
    source = _write_columns(tmp_path / "source.json", [{"name": "client_id", "dtype": "Int64"}])
    actual = tmp_path / "actual.json"
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "engine": "MergeTree",
                "order_by": ["client_id"],
                "columns": {"client_id": {"type": "Int64"}},
            }
        ),
        encoding="utf-8",
    )

    payload = MigrationControlFacade().plan(
        manifest_path=str(manifest),
        source_path=str(source),
        actual_path=str(actual),
    )

    assert any("direct_rename" in warning for warning in payload["warnings"])
    assert any(change.get("risk") == "unsafe_direct_rename" for change in payload["changes"])

    pack_path = tmp_path / "direct-rename-pack.json"
    pack_path.write_text(json.dumps(payload), encoding="utf-8")
    code, result = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(tmp_path / "ledger.json"),
        phase="direct_rename",
    )

    assert code == 2
    assert result["blockers"] == ["migration.approval_required"]


def _write_manifest(tmp_path: Path, *, strategy: str) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"type": "mssql", "table": {"schema": "dbo", "name": "orders"}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {
                        "physical_design": {
                            "storage": {"clickhouse": {"engine": "MergeTree", "order_by": ["client_id"]}}
                        },
                        "schema_identity": {
                            "enabled": True,
                            "rename": {"strategy": strategy},
                            "columns": {
                                "customer_id": {
                                    "id": "orders.customer_id",
                                    "aliases": [{"name": "client_id"}],
                                }
                            },
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_columns(path: Path, columns: list[dict[str, object]]) -> Path:
    path.write_text(json.dumps(columns), encoding="utf-8")
    return path
