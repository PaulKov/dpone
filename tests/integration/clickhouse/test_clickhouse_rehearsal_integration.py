from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_fixture import MigrationRehearsalDataFacade
from dpone.services.schema_migration_rehearsal import MigrationRehearsalFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def test_clickhouse_rehearsal_executes_setting_migration_and_certifies_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_rehearsal_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{database}`.`{table}` (id Int64, name String) ENGINE = MergeTree ORDER BY id"
    )
    try:
        pack_path = _write_json(tmp_path / "pack.json", _setting_pack(database=database, table=table).to_dict())
        connection_path = _write_json(
            tmp_path / "clickhouse-stage.json",
            {
                "type": "clickhouse",
                "environment": "stage",
                "host": clickhouse_settings.host,
                "port": clickhouse_settings.port,
                "database": database,
                "user": clickhouse_settings.user,
                "password": clickhouse_settings.password,
                "secure": clickhouse_settings.secure,
            },
        )
        facade = MigrationRehearsalFacade()

        plan = facade.plan(
            pack_path=str(pack_path),
            environment="stage",
            target_connection_path=str(connection_path),
            output_dir=str(tmp_path / "rehearsal"),
        )
        run = facade.run(plan_path=str(tmp_path / "rehearsal" / "rehearsal-plan.json"), execute=True)
        run_path = _write_json(tmp_path / "rehearsal-run.json", run)
        certificate = facade.certify(run_path=str(run_path), profile="prod_strict")

        assert plan["status"] == "planned"
        assert run["status"] == "passed"
        assert run["metrics"]["operations_executed"] == 2
        assert certificate["status"] == "certified"
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")


def test_clickhouse_rehearsal_fixture_profiles_wide_synthetic_data_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_rehearsal_fixture_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(f"CREATE TABLE {quoted} ({_wide_columns_ddl()}) ENGINE = MergeTree ORDER BY id")
    try:
        pack = _setting_pack(database=database, table=table)
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict())
        manifest_path = _write_json(tmp_path / "manifest.json", _wide_manifest())
        connection_path = _write_json(
            tmp_path / "clickhouse-stage.json",
            {
                "type": "clickhouse",
                "environment": "stage",
                "host": clickhouse_settings.host,
                "port": clickhouse_settings.port,
                "database": database,
                "user": clickhouse_settings.user,
                "password": clickhouse_settings.password,
                "secure": clickhouse_settings.secure,
            },
        )
        fixture_facade = MigrationRehearsalDataFacade()
        rehearsal_facade = MigrationRehearsalFacade()

        fixture_plan = fixture_facade.plan(pack_path=str(pack_path), manifest_path=str(manifest_path))
        fixture_plan_path = _write_json(tmp_path / "fixture-plan.json", fixture_plan)
        fixture_build = fixture_facade.build(
            plan_path=str(fixture_plan_path),
            target_connection_path=str(connection_path),
            execute=True,
        )
        fixture_build_path = _write_json(tmp_path / "fixture-build.json", fixture_build)
        before_profile = fixture_facade.profile(
            fixture_build_path=str(fixture_build_path),
            target_connection_path=str(connection_path),
            stage="before",
        )
        before_profile_path = _write_json(tmp_path / "profile-before.json", before_profile)

        rehearsal_facade.plan(
            pack_path=str(pack_path),
            environment="stage",
            target_connection_path=str(connection_path),
            output_dir=str(tmp_path / "rehearsal"),
        )
        run = rehearsal_facade.run(plan_path=str(tmp_path / "rehearsal" / "rehearsal-plan.json"), execute=True)
        run_path = _write_json(tmp_path / "rehearsal-run.json", run)
        after_profile = fixture_facade.profile(
            fixture_build_path=str(fixture_build_path),
            target_connection_path=str(connection_path),
            stage="after",
        )
        after_profile_path = _write_json(tmp_path / "profile-after.json", after_profile)
        certificate = rehearsal_facade.certify(
            run_path=str(run_path),
            profile="prod_strict",
            fixture_build_path=str(fixture_build_path),
            before_profile_path=str(before_profile_path),
            after_profile_path=str(after_profile_path),
        )

        assert fixture_plan["requirements"]["min_rows"] == 10_000
        assert len(fixture_plan["columns"]) == 200
        assert fixture_build["status"] == "built"
        assert fixture_build["metrics"]["row_count"] == 10_000
        assert before_profile["status"] == "profiled"
        assert before_profile["metrics"]["row_count"] == 10_000
        assert after_profile["metrics"]["row_count"] == 10_000
        assert before_profile["metrics"]["typed_hash"] == after_profile["metrics"]["typed_hash"]
        assert run["status"] == "passed"
        assert certificate["status"] == "certified"
        assert certificate["fixture_build_id"] == fixture_build["fixture_build_id"]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")


def _setting_pack(*, database: str, table: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    quoted = f"`{database}`.`{table}`"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={"sink_type": "clickhouse", "table": dataset, "table_settings": {"min_rows_for_wide_part": 1}},
        actual={"sink_type": "clickhouse", "table": dataset, "table_settings": {}},
        changes=({"change_type": "table_setting", "path": "table_settings.min_rows_for_wide_part"},),
        ddl=(f"ALTER TABLE {quoted} MODIFY SETTING min_rows_for_wide_part = 1",),
        strategy="online_safe",
        rollback={"supported": True, "ddl": [f"ALTER TABLE {quoted} RESET SETTING min_rows_for_wide_part"]},
    )


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _wide_manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "rehearsal": {
                            "data_fixture": {
                                "mode": "synthetic",
                                "min_rows": 10_000,
                                "max_rows": 10_000,
                                "min_columns": 200,
                                "include_edge_cases": True,
                                "preserve_hierarchy": True,
                                "seed": "clickhouse-live-fixture",
                            },
                            "quality_profile": {
                                "row_count": True,
                                "typed_hash": True,
                                "duplicate_key": True,
                                "null_key": True,
                                "nested_parent_child": True,
                            },
                        }
                    }
                }
            }
        },
        "schema": {"columns": _wide_manifest_columns()},
    }


def _wide_manifest_columns() -> list[dict[str, object]]:
    return [
        {"name": "id", "type": "Int64", "key": True},
        {"name": "parent_id", "type": "Nullable(Int64)", "parent": "id"},
        *({"name": f"col_{index:03d}", "type": "String"} for index in range(198)),
    ]


def _wide_columns_ddl() -> str:
    columns = ["id Int64", "parent_id Nullable(Int64)"]
    columns.extend(f"`col_{index:03d}` String" for index in range(198))
    return ", ".join(columns)
