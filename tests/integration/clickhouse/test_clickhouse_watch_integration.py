from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_watch import MigrationWatchFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def test_clickhouse_watch_certifies_wide_target_and_recommends_rollback_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_watch_{uuid.uuid4().hex[:10]}"
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
        facade = MigrationWatchFacade()
        pack = _pack(database=database, table=table, order_by=["id"])
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
        certificate_path = _write_json(tmp_path / "post-apply-certificate.json", _post_apply_certificate(pack))
        connection_path = _write_json(tmp_path / "clickhouse-prod.json", _connection(clickhouse_settings, database))
        manifest_path = _write_json(tmp_path / "manifest.json", _manifest(database=database, table=table, ok=True))

        plan = facade.plan(
            pack_path=str(pack_path),
            post_apply_certificate_path=str(certificate_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        run = facade.run(plan_path=str(_write_json(tmp_path / "watch-plan.json", plan)), execute=True)
        certificate = facade.certify(run_path=str(_write_json(tmp_path / "watch-run.json", run)), profile="prod_strict")

        assert plan["status"] == "planned"
        assert run["status"] == "passed"
        assert run["samples"] == {"planned": 2, "executed": 2, "passed": 2, "failed": 0}
        assert run["remediation"]["decision"] == "continue"
        assert certificate["status"] == "stable"

        failing_manifest_path = _write_json(
            tmp_path / "manifest-failing-canary.json", _manifest(database=database, table=table, ok=False)
        )
        failing_plan = facade.plan(
            pack_path=str(pack_path),
            post_apply_certificate_path=str(certificate_path),
            manifest_path=str(failing_manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        failing_run = facade.run(
            plan_path=str(_write_json(tmp_path / "failing-watch-plan.json", failing_plan)), execute=True
        )
        assert failing_run["status"] == "blocked"
        assert failing_run["remediation"]["decision"] == "rollback_required"
        assert any("schema_migration_watch.canary_failed" in blocker for blocker in failing_run["blockers"])

        drift_pack = _pack(database=database, table=table, order_by=["parent_id"])
        drift_pack_path = _write_json(tmp_path / "drift-pack.json", drift_pack.to_dict(command="plan"))
        drift_certificate_path = _write_json(
            tmp_path / "drift-post-apply-certificate.json", _post_apply_certificate(drift_pack)
        )
        drift_plan = facade.plan(
            pack_path=str(drift_pack_path),
            post_apply_certificate_path=str(drift_certificate_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        drift_run = facade.run(plan_path=str(_write_json(tmp_path / "drift-watch-plan.json", drift_plan)), execute=True)
        assert "schema_migration_watch.physical_drift" in drift_run["blockers"]
        assert drift_run["remediation"]["decision"] == "rollback_required"
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")


def test_clickhouse_watch_query_health_budget_blocks_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_watch_health_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(f"CREATE TABLE {quoted} (id Int64, value String) ENGINE = MergeTree ORDER BY id")
    try:
        clickhouse_connector.connection.execute(
            f"INSERT INTO {quoted} (id, value) VALUES", [(index, f"value_{index}") for index in range(1_000)]
        )
        _require_query_log_for_table(clickhouse_connector, quoted)
        facade = MigrationWatchFacade()
        pack = _small_pack(database=database, table=table)
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
        certificate_path = _write_json(tmp_path / "post-apply-certificate.json", _post_apply_certificate(pack))
        connection_path = _write_json(tmp_path / "clickhouse-prod.json", _connection(clickhouse_settings, database))
        manifest_path = _write_json(
            tmp_path / "manifest-query-health.json",
            _manifest(database=database, table=table, ok=True, query_health=True),
        )

        plan = facade.plan(
            pack_path=str(pack_path),
            post_apply_certificate_path=str(certificate_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        run = facade.run(plan_path=str(_write_json(tmp_path / "watch-plan.json", plan)), execute=True)

        assert run["status"] == "blocked"
        assert any("schema_migration_watch.query_health_blocked" in blocker for blocker in run["blockers"])
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")


def _require_query_log_for_table(clickhouse_connector, quoted_table: str) -> None:
    clickhouse_connector.execute_query(f"SELECT count() FROM {quoted_table}")
    clickhouse_connector.execute_query("SYSTEM FLUSH LOGS")
    try:
        rows = clickhouse_connector.get_records(
            "SELECT max(read_rows) AS max_read_rows FROM system.query_log "
            "WHERE event_time >= now() - INTERVAL 15 MINUTE "
            f"AND position(query, {_quote_literal(quoted_table)}) > 0",
            as_dict=True,
        )
    except Exception as exc:  # noqa: BLE001 - server-level query_log configuration is external to dpone.
        pytest.skip(f"ClickHouse system.query_log is unavailable: {exc}")
    max_read_rows = int((rows[0] if rows else {}).get("max_read_rows", 0) or 0)
    if max_read_rows <= 1:
        pytest.skip("ClickHouse system.query_log did not capture table reads for this server configuration")


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
        rollback={"supported": True, "supported_until_phase": "contract", "ddl": []},
    )


def _small_pack(*, database: str, table: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={
            "sink_type": "clickhouse",
            "table": dataset,
            "engine": "MergeTree",
            "order_by": ["id"],
            "columns": {
                "id": {"type": "Int64", "nullable": False, "position": 1},
                "value": {"type": "String", "nullable": False, "position": 2},
            },
        },
        actual={
            "sink_type": "clickhouse",
            "table": dataset,
            "engine": "MergeTree",
            "order_by": ["id"],
            "columns": {
                "id": {"type": "Int64", "nullable": False, "position": 1},
                "value": {"type": "String", "nullable": False, "position": 2},
            },
        },
        strategy="online_safe",
        rollback={"supported": True, "supported_until_phase": "contract", "ddl": []},
    )


def _manifest(*, database: str, table: str, ok: bool, query_health: bool = False) -> dict[str, object]:
    query = f"SELECT {'count() >= 10000' if ok else '0'} AS ok FROM `{database}`.`{table}`"
    if query_health:
        query = f"SELECT count() >= 1000 AS ok FROM `{database}`.`{table}`"
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "watch": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "window": {
                                "duration": "0s",
                                "interval": "0s",
                                "min_successful_samples": 2,
                                "max_failed_samples": 0,
                            },
                            "verification": {
                                "post_apply_recheck": True,
                                "physical_design": True,
                                "row_count": True,
                                "typed_hash": True,
                                "null_distribution": True,
                                "duplicate_key": True,
                                "null_key": True,
                                "canary_queries": True,
                                "query_health": query_health,
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
                            "query_health": {
                                "lookback": "15m",
                                "max_error_count": 0,
                                "max_p95_ms": 1,
                                "max_read_rows": 1,
                            },
                            "remediation": {
                                "mode": "recommend",
                                "rollback_on": [
                                    "critical_canary_failure",
                                    "physical_drift",
                                    "query_health_blocked",
                                    "rollback_window_closing",
                                ],
                            },
                        }
                    }
                }
            }
        }
    }


def _post_apply_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_post_apply_certificate.v1",
        "certificate_id": "sha256:" + "8" * 64,
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "verified",
        "profile": "prod_strict",
        "checks": [],
        "rollback_window": {"status": "open", "supported_until_phase": "contract"},
        "blockers": [],
        "warnings": [],
        "metrics": {"row_count": 10_000},
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


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
