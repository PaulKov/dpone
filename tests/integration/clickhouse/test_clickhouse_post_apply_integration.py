from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_post_apply import PostApplyVerificationFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def test_clickhouse_post_apply_verifies_wide_target_and_blocks_drift_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_post_apply_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(f"CREATE TABLE {quoted} ({_wide_columns_ddl()}) ENGINE = MergeTree ORDER BY id")
    try:
        columns = _wide_column_names()
        clickhouse_connector.connection.execute(
            f"INSERT INTO {quoted} ({', '.join(f'`{column}`' for column in columns)}) VALUES",
            [_wide_row(index) for index in range(10_000)],
        )
        facade = PostApplyVerificationFacade()
        pack = _pack(database=database, table=table, order_by=["id"])
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
        ledger_path = _write_json(tmp_path / "ledger.json", _ledger(pack, environment="prod"))
        manifest_path = _write_json(tmp_path / "manifest.json", _manifest(database=database, table=table, ok=True))
        connection_path = _write_json(tmp_path / "clickhouse-prod.json", _connection(clickhouse_settings, database))

        plan = facade.plan(
            pack_path=str(pack_path),
            ledger_path=str(ledger_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        plan_path = _write_json(tmp_path / "post-apply-plan.json", plan)
        run = facade.run(plan_path=str(plan_path), execute=True)
        run_path = _write_json(tmp_path / "post-apply-run.json", run)
        certificate = facade.certify(run_path=str(run_path), profile="prod_strict")

        assert plan["status"] == "planned"
        assert run["status"] == "passed"
        assert run["metrics"]["row_count"] == 10_000
        assert run["metrics"]["canaries_executed"] == 1
        assert certificate["status"] == "verified"

        failing_manifest = _write_json(
            tmp_path / "manifest-failing-canary.json", _manifest(database=database, table=table, ok=False)
        )
        failing_plan = facade.plan(
            pack_path=str(pack_path),
            ledger_path=str(ledger_path),
            manifest_path=str(failing_manifest),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        failing_run = facade.run(plan_path=str(_write_json(tmp_path / "failing-plan.json", failing_plan)), execute=True)
        assert failing_run["status"] == "blocked"
        assert any("schema_migration_post_apply.canary_failed" in blocker for blocker in failing_run["blockers"])

        drift_pack = _pack(database=database, table=table, order_by=["parent_id"])
        drift_pack_path = _write_json(tmp_path / "drift-pack.json", drift_pack.to_dict(command="plan"))
        drift_ledger_path = _write_json(tmp_path / "drift-ledger.json", _ledger(drift_pack, environment="prod"))
        drift_plan = facade.plan(
            pack_path=str(drift_pack_path),
            ledger_path=str(drift_ledger_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        drift_run = facade.run(plan_path=str(_write_json(tmp_path / "drift-plan.json", drift_plan)), execute=True)
        assert "schema_migration_post_apply.physical_drift" in drift_run["blockers"]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")


def _pack(*, database: str, table: str, order_by: list[str]) -> MigrationPack:
    dataset = f"{database}.{table}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={
            "sink_type": "clickhouse",
            "table": dataset,
            "engine": "MergeTree",
            "order_by": order_by,
            "columns": _desired_columns(),
        },
        actual={
            "sink_type": "clickhouse",
            "table": dataset,
            "engine": "MergeTree",
            "order_by": order_by,
            "columns": _desired_columns(),
        },
        strategy="online_safe",
        rollback={"supported": True, "supported_until_phase": "apply", "ddl": []},
    )


def _manifest(*, database: str, table: str, ok: bool) -> dict[str, object]:
    query = f"SELECT {'count() >= 10000' if ok else '0'} AS ok FROM `{database}`.`{table}`"
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "post_apply": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "verification": {
                                "ledger_state": True,
                                "physical_design": True,
                                "row_count": True,
                                "typed_hash": True,
                                "null_distribution": True,
                                "duplicate_key": True,
                                "null_key": True,
                                "nested_parent_child": True,
                                "canary_queries": True,
                                "rollback_window": True,
                            },
                            "canaries": [
                                {
                                    "id": "orders_count_positive",
                                    "type": "sql",
                                    "owner": "data-platform",
                                    "severity": "critical",
                                    "query": query,
                                    "expect": {"column": "ok", "equals": 1},
                                }
                            ],
                        }
                    }
                }
            }
        }
    }


def _ledger(pack: MigrationPack, *, environment: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_ledger.v1",
        "records": [
            {
                "schema_version": "dpone.schema_migration_ledger_record.v1",
                "pack_id": pack.pack_id,
                "status": "applied",
                "target": pack.target.to_dict(),
                "desired_fingerprint": pack.desired_fingerprint,
                "actual_fingerprint": pack.actual_fingerprint,
                "environment": environment,
                "blockers": [],
                "warnings": [],
            }
        ],
    }


def _connection(clickhouse_settings, database: str) -> dict[str, object]:
    return {
        "type": "clickhouse",
        "environment": "prod",
        "host": clickhouse_settings.host,
        "port": clickhouse_settings.port,
        "database": database,
        "user": clickhouse_settings.user,
        "password": clickhouse_settings.password,
        "secure": clickhouse_settings.secure,
    }


def _desired_columns() -> dict[str, dict[str, object]]:
    columns = {
        "id": {"type": "Int64", "nullable": False, "position": 1},
        "parent_id": {"type": "Nullable(Int64)", "nullable": True, "position": 2},
    }
    columns.update(
        {f"col_{index:03d}": {"type": "String", "nullable": False, "position": index + 3} for index in range(198)}
    )
    return columns


def _wide_column_names() -> list[str]:
    return ["id", "parent_id", *(f"col_{index:03d}" for index in range(198))]


def _wide_row(index: int) -> tuple[object, ...]:
    return (index, None if index % 13 == 0 else index // 10, *(f"value_{index}_{column}" for column in range(198)))


def _wide_columns_ddl() -> str:
    columns = ["id Int64", "parent_id Nullable(Int64)"]
    columns.extend(f"`col_{index:03d}` String" for index in range(198))
    return ", ".join(columns)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
