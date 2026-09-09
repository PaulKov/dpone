from __future__ import annotations

from dpone.services.safe_sample_policy import TemporaryTargetPlan


def _plan() -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_dev",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
        ttl_seconds=86400,
        cleanup_required=True,
        pii_policy="masked",
    )


def test_clickhouse_temporary_target_adapter_creates_and_drops_only_temp_table() -> None:
    from dpone.services.safe_sample_clickhouse_target import ClickHouseTemporaryTargetAdapter

    statements: list[str] = []
    adapter = ClickHouseTemporaryTargetAdapter(execute=statements.append)

    create_metadata = adapter.create(_plan())
    drop_metadata = adapter.drop(_plan())

    assert statements == [
        "CREATE DATABASE IF NOT EXISTS `dpone_tmp_development`",
        "CREATE TABLE `dpone_tmp_development`.`orders_daily_abc123` AS `analytics`.`orders`",
        "ALTER TABLE `dpone_tmp_development`.`orders_daily_abc123` ADD COLUMN "
        "`_dpone_sample_expires_at` DateTime MATERIALIZED now() + toIntervalSecond(86400)",
        "ALTER TABLE `dpone_tmp_development`.`orders_daily_abc123` MODIFY TTL `_dpone_sample_expires_at` DELETE",
        "DROP TABLE IF EXISTS `dpone_tmp_development`.`orders_daily_abc123`",
    ]
    assert create_metadata == {
        "backend": "clickhouse",
        "operation": "create",
        "statements": 4,
        "server_side_expiry": True,
        "ttl_seconds": 86400,
    }
    assert drop_metadata == {"backend": "clickhouse", "operation": "drop", "statements": 1}
    assert "DROP TABLE IF EXISTS `analytics`.`orders`" not in statements


def test_clickhouse_temporary_target_adapter_escapes_identifiers() -> None:
    from dpone.services.safe_sample_clickhouse_target import ClickHouseTemporaryTargetAdapter

    plan = _plan()
    unsafe = TemporaryTargetPlan(
        mode=plan.mode,
        pipeline_id=plan.pipeline_id,
        process=plan.process,
        sink_type=plan.sink_type,
        connection_ref=plan.connection_ref,
        original_table={"schema": "ana`lytics", "name": "or`ders"},
        temporary_table={"schema": "tmp`schema", "name": "tmp`orders"},
        ttl_seconds=plan.ttl_seconds,
        cleanup_required=plan.cleanup_required,
        pii_policy=plan.pii_policy,
    )
    statements: list[str] = []

    ClickHouseTemporaryTargetAdapter(execute=statements.append).create(unsafe)

    assert statements == [
        "CREATE DATABASE IF NOT EXISTS `tmp``schema`",
        "CREATE TABLE `tmp``schema`.`tmp``orders` AS `ana``lytics`.`or``ders`",
        "ALTER TABLE `tmp``schema`.`tmp``orders` ADD COLUMN `_dpone_sample_expires_at` "
        "DateTime MATERIALIZED now() + toIntervalSecond(86400)",
        "ALTER TABLE `tmp``schema`.`tmp``orders` MODIFY TTL `_dpone_sample_expires_at` DELETE",
    ]


def test_clickhouse_temporary_target_registry_builds_lifecycle_executor() -> None:
    from dpone.services.safe_sample_clickhouse_target import clickhouse_temporary_target_registry

    statements: list[str] = []
    executor = clickhouse_temporary_target_registry(execute=statements.append).executor_for(_plan())

    result = executor.prepare(_plan())

    assert result.status == "prepared"
    assert result.adapter_metadata == {
        "backend": "clickhouse",
        "operation": "create",
        "statements": 4,
        "server_side_expiry": True,
        "ttl_seconds": 86400,
    }
    assert statements == [
        "CREATE DATABASE IF NOT EXISTS `dpone_tmp_development`",
        "CREATE TABLE `dpone_tmp_development`.`orders_daily_abc123` AS `analytics`.`orders`",
        "ALTER TABLE `dpone_tmp_development`.`orders_daily_abc123` ADD COLUMN "
        "`_dpone_sample_expires_at` DateTime MATERIALIZED now() + toIntervalSecond(86400)",
        "ALTER TABLE `dpone_tmp_development`.`orders_daily_abc123` MODIFY TTL `_dpone_sample_expires_at` DELETE",
    ]


def test_clickhouse_temporary_target_adapter_rejects_invalid_ttl_before_ddl() -> None:
    from dataclasses import replace

    from dpone.services.safe_sample_clickhouse_target import ClickHouseTemporaryTargetAdapter

    statements: list[str] = []

    for invalid_ttl in (0, -1):
        try:
            ClickHouseTemporaryTargetAdapter(execute=statements.append).create(
                replace(_plan(), ttl_seconds=invalid_ttl)
            )
        except ValueError as exc:
            assert "ttl_seconds" in str(exc)
        else:
            raise AssertionError("invalid TTL must fail closed")

    assert statements == []


def test_clickhouse_temporary_target_adapter_compensates_partial_create_failure() -> None:
    from dpone.services.safe_sample_clickhouse_target import ClickHouseTemporaryTargetAdapter

    statements: list[str] = []

    def execute(statement: str) -> None:
        statements.append(statement)
        if "ADD COLUMN" in statement:
            raise RuntimeError("TTL DDL rejected")

    try:
        ClickHouseTemporaryTargetAdapter(execute=execute).create(_plan())
    except RuntimeError as exc:
        assert str(exc) == "TTL DDL rejected"
    else:
        raise AssertionError("partial create must preserve the original error")

    assert statements[-1] == "DROP TABLE IF EXISTS `dpone_tmp_development`.`orders_daily_abc123`"
