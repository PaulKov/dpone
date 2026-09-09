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


def test_schema_contract_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    for args in (
        ["schema", "contract", "--help"],
        ["schema", "contract", "publish", "--help"],
        ["schema", "contract", "check", "--help"],
        ["schema", "contract", "gate", "--help"],
        ["schema", "contract", "history", "--help"],
        ["schema", "contract", "latest", "--help"],
        ["schema", "contract", "consumers", "--help"],
        ["schema", "contract", "consumers", "lineage", "--help"],
        ["schema", "contract", "consumers", "discover", "--help"],
        ["schema", "contract", "consumers", "matrix", "--help"],
        ["schema", "contract", "consumers", "gate", "--help"],
        ["schema", "contract", "deprecate", "--help"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main(args)
        assert exc.value.code == 0
    assert "contract" in capsys.readouterr().out


def test_schema_contract_cli_publish_check_gate_and_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    store_uri = tmp_path / "contracts.json"
    base_manifest = _write_manifest(tmp_path, "base.yaml", version="1.0.0", store_uri=store_uri)
    head_manifest = _write_manifest(
        tmp_path,
        "head.yaml",
        version="1.1.0",
        store_uri=store_uri,
        include_status=True,
    )

    publish_output = tmp_path / "publish.json"
    with pytest.raises(SystemExit) as publish_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "publish",
                "--manifest",
                str(base_manifest),
                "--format",
                "json",
                "--output",
                str(publish_output),
            ]
        )
    assert publish_exit.value.code == 0
    published = json.loads(publish_output.read_text(encoding="utf-8"))
    assert published["schema_version"] == "dpone.schema_contract_version.v1"
    assert published["contract_id"] == "analytics.orders"

    check_output = tmp_path / "check.md"
    with pytest.raises(SystemExit) as check_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "check",
                "--manifest",
                str(head_manifest),
                "--against",
                "analytics.orders@1.0.0",
                "--compatibility",
                "backward",
                "--format",
                "md",
                "--output",
                str(check_output),
            ]
        )
    assert check_exit.value.code == 0
    assert "Schema Contract Compatibility" in check_output.read_text(encoding="utf-8")

    pack_path = tmp_path / "pack.json"
    pack_path.write_text(
        json.dumps({"schema_version": "dpone.schema_migration_pack.v1", "pack_id": "sha256:" + "1" * 64}),
        encoding="utf-8",
    )
    gate_output = tmp_path / "gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "gate",
                "--manifest",
                str(head_manifest),
                "--pack",
                str(pack_path),
                "--format",
                "json",
                "--output",
                str(gate_output),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(gate_output.read_text(encoding="utf-8"))
    assert gate["schema_version"] == "dpone.schema_contract_gate.v1"
    assert gate["status"] in {"allowed", "warning"}
    assert gate["pack_id"] == "sha256:" + "1" * 64

    for args in (
        ["schema", "contract", "latest", "--contract", "analytics.orders", "--format", "json"],
        ["schema", "contract", "history", "--contract", "analytics.orders", "--format", "table"],
        ["schema", "contract", "consumers", "--contract", "analytics.orders", "--version", "1.x", "--format", "table"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main([*args, "--store-uri", str(store_uri)])
        assert exc.value.code == 0

    deprecate_output = tmp_path / "deprecate.json"
    with pytest.raises(SystemExit) as deprecate_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "deprecate",
                "--contract",
                "analytics.orders",
                "--column",
                "customer_id",
                "--remove-after",
                "2026-09-01",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
                "--output",
                str(deprecate_output),
            ]
        )
    assert deprecate_exit.value.code == 0
    assert json.loads(deprecate_output.read_text(encoding="utf-8"))["schema_version"] == "dpone.schema_contract_gate.v1"
    assert "dpone.schema_contract" in capsys.readouterr().out


def test_schema_contract_consumers_discover_matrix_gate_cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    store_uri = tmp_path / "contracts.json"
    base_manifest = _write_manifest(tmp_path, "base.yaml", version="1.5.0", store_uri=store_uri)
    head_manifest = _write_manifest(
        tmp_path,
        "head.yaml",
        version="1.6.0",
        store_uri=store_uri,
        include_status=True,
        discovery=True,
    )
    with pytest.raises(SystemExit) as publish_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "publish",
                "--manifest",
                str(base_manifest),
                "--format",
                "json",
            ]
        )
    assert publish_exit.value.code == 0

    inventory_output = tmp_path / "consumers.json"
    with pytest.raises(SystemExit) as discover_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "discover",
                "--manifest",
                str(head_manifest),
                "--format",
                "json",
                "--output",
                str(inventory_output),
            ]
        )
    assert discover_exit.value.code == 0
    inventory = json.loads(inventory_output.read_text(encoding="utf-8"))
    assert inventory["schema_version"] == "dpone.schema_contract_consumer_inventory.v1"
    assert inventory["summary"]["consumers_count"] == 1

    matrix_output = tmp_path / "matrix.json"
    with pytest.raises(SystemExit) as matrix_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "matrix",
                "--manifest",
                str(head_manifest),
                "--against",
                "analytics.orders@1.5.0",
                "--consumers",
                str(inventory_output),
                "--format",
                "json",
                "--output",
                str(matrix_output),
            ]
        )
    assert matrix_exit.value.code == 0
    matrix = json.loads(matrix_output.read_text(encoding="utf-8"))
    assert matrix["schema_version"] == "dpone.schema_contract_consumer_matrix.v1"
    assert matrix["status"] in {"compatible", "warning"}

    pack_path = tmp_path / "pack.json"
    pack_path.write_text(
        json.dumps({"schema_version": "dpone.schema_migration_pack.v1", "pack_id": "sha256:" + "3" * 64}),
        encoding="utf-8",
    )
    gate_output = tmp_path / "consumer-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "gate",
                "--manifest",
                str(head_manifest),
                "--pack",
                str(pack_path),
                "--matrix",
                str(matrix_output),
                "--format",
                "json",
                "--output",
                str(gate_output),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(gate_output.read_text(encoding="utf-8"))
    assert gate["schema_version"] == "dpone.schema_contract_consumer_gate.v1"
    assert gate["status"] == "allowed"


def test_schema_contract_consumers_lineage_cli_and_matrix_integration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    store_uri = tmp_path / "contracts.json"
    compiled_dir = tmp_path / "target" / "compiled"
    (compiled_dir / "models").mkdir(parents=True)
    (compiled_dir / "models" / "daily_margin.sql").write_text(
        "select amount, customer_id from analytics.orders",
        encoding="utf-8",
    )
    dbt_manifest = tmp_path / "target" / "manifest.json"
    dbt_manifest.write_text(
        json.dumps(
            {
                "sources": {
                    "source.project.orders": {
                        "unique_id": "source.project.orders",
                        "schema": "analytics",
                        "name": "orders",
                    }
                },
                "nodes": {
                    "model.project.daily_margin": {
                        "unique_id": "model.project.daily_margin",
                        "resource_type": "model",
                        "name": "daily_margin",
                        "depends_on": {"nodes": ["source.project.orders"]},
                        "compiled_path": "models/daily_margin.sql",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    base_manifest = _write_manifest(tmp_path, "base.yaml", version="1.5.0", store_uri=store_uri)
    head_manifest = _write_manifest(
        tmp_path,
        "head.yaml",
        version="1.6.0",
        store_uri=store_uri,
        include_status=True,
        discovery=True,
        dbt_manifest=dbt_manifest,
        dbt_compiled=compiled_dir,
    )
    with pytest.raises(SystemExit) as publish_exit:
        cli_main.main(["schema", "contract", "publish", "--manifest", str(base_manifest), "--format", "json"])
    assert publish_exit.value.code == 0

    lineage_output = tmp_path / "lineage.json"
    with pytest.raises(SystemExit) as lineage_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "lineage",
                "--manifest",
                str(head_manifest),
                "--format",
                "json",
                "--output",
                str(lineage_output),
            ]
        )
    assert lineage_exit.value.code == 0
    lineage = json.loads(lineage_output.read_text(encoding="utf-8"))
    assert lineage["schema_version"] == "dpone.schema_contract_consumer_lineage.v1"
    assert lineage["summary"]["by_confidence"]["parsed"] == 2

    inventory_output = tmp_path / "consumers.json"
    with pytest.raises(SystemExit) as discover_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "discover",
                "--manifest",
                str(head_manifest),
                "--lineage",
                str(lineage_output),
                "--format",
                "json",
                "--output",
                str(inventory_output),
            ]
        )
    assert discover_exit.value.code == 0
    assert json.loads(inventory_output.read_text(encoding="utf-8"))["summary"]["consumers_count"] == 2

    matrix_output = tmp_path / "matrix.json"
    with pytest.raises(SystemExit) as matrix_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "matrix",
                "--manifest",
                str(head_manifest),
                "--against",
                "analytics.orders@1.5.0",
                "--consumers",
                str(inventory_output),
                "--lineage",
                str(lineage_output),
                "--format",
                "json",
                "--output",
                str(matrix_output),
            ]
        )
    assert matrix_exit.value.code == 0
    matrix = json.loads(matrix_output.read_text(encoding="utf-8"))
    assert matrix["summary"]["lineage_confidence"]["parsed"] == 2


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_manifest(
    tmp_path: Path,
    name: str,
    *,
    version: str,
    store_uri: Path,
    include_status: bool = False,
    discovery: bool = False,
    dbt_manifest: Path | None = None,
    dbt_compiled: Path | None = None,
) -> Path:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
        "amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": True},
    }
    if include_status:
        columns["status"] = {"type": "string", "nullable": True}
    path = tmp_path / name
    path.write_text(
        yaml.safe_dump(
            {
                "source": {"options": {"columns": [{"name": key, "type": "string"} for key in columns]}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "analytics", "name": "orders"},
                    "options": {
                        "schema_contract": {
                            "id": "analytics.orders",
                            "version": version,
                            "owner": "data-platform",
                            "enforcement": "strict",
                            "compatibility": "backward",
                            "registry": {
                                "enabled": True,
                                "mode": "gate",
                                "store_backend": "local_json",
                                "store_uri": str(store_uri),
                            },
                            "versioning": {"semver": "strict", "unknown_consumer": "warn"},
                            "consumers": {
                                **({"discovery": _discovery_sources(dbt_manifest, dbt_compiled)} if discovery else {}),
                                "manual": [
                                    {
                                        "id": "finance.daily_margin",
                                        "type": "dashboard",
                                        "owner": "finance-analytics",
                                        "version_constraint": ">=1.0,<2.0",
                                        "reads": {"columns": ["amount", "customer_id"]},
                                    }
                                ],
                            },
                            "columns": columns,
                        },
                        "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _discovery_sources(dbt_manifest: Path | None, dbt_compiled: Path | None) -> dict[str, object]:
    sources: dict[str, object] = {"manual": True}
    if dbt_manifest is not None:
        sources["dbt_manifest"] = str(dbt_manifest)
    if dbt_compiled is not None:
        sources["dbt_compiled_sql"] = str(dbt_compiled)
    return {
        "enabled": True,
        "mode": "gate",
        "unknown_consumer": "warn",
        "low_confidence_major_change": "block",
        "required_sources": ["manual"],
        "sources": sources,
    }
