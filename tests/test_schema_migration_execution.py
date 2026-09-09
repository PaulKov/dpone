from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration import MigrationControlFacade
from dpone.services.schema_migration_execution import load_migration_operation_executor


class _FakeExecutor:
    def __init__(self, results: dict[str, list[tuple[Any, ...]]] | None = None) -> None:
        self.executed: list[str] = []
        self._results = results or {}

    def execute(self, operation: dict[str, Any]) -> dict[str, Any]:
        sql = str(operation["sql"])
        self.executed.append(sql)
        return {
            "name": operation.get("name"),
            "operation_type": operation.get("operation_type", "sql"),
            "status": "executed",
            "sql": sql,
            "result_rows": self._results.get(sql, []),
        }


def test_apply_non_phased_pack_executes_target_ddl_before_writing_ledger(tmp_path: Path) -> None:
    pack_path = _write_pack(
        tmp_path,
        MigrationPack.build(
            target=MigrationTarget(sink_type="clickhouse", table="landing.orders"),
            desired={"table": "landing.orders"},
            ddl=("ALTER TABLE `landing`.`orders` MODIFY SETTING index_granularity = 8192",),
            strategy="online_safe",
        ),
    )
    ledger = tmp_path / "ledger.json"
    executor = _FakeExecutor()

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(ledger),
        execute=True,
        executor=executor,
    )

    assert code == 0
    assert executor.executed == ["ALTER TABLE `landing`.`orders` MODIFY SETTING index_granularity = 8192"]
    assert payload["operations"][0]["status"] == "executed"
    assert json.loads(ledger.read_text(encoding="utf-8"))["records"][0]["operations"][0]["status"] == "executed"


def test_apply_blocks_mismatched_bundle_gate_pack_id(tmp_path: Path) -> None:
    pack_path = _write_pack(
        tmp_path,
        MigrationPack.build(
            target=MigrationTarget(sink_type="clickhouse", table="landing.orders"),
            desired={"table": "landing.orders"},
            ddl=("ALTER TABLE `landing`.`orders` MODIFY SETTING index_granularity = 8192",),
            strategy="online_safe",
        ),
    )
    gate_path = tmp_path / "bundle-gate.json"
    gate_path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_bundle_gate.v1",
                "status": "allowed",
                "pack_id": "sha256:" + "0" * 64,
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(tmp_path / "ledger.json"),
        bundle_gate_path=str(gate_path),
    )

    assert code == 2
    assert payload["status"] == "blocked"
    assert "migration_bundle_gate.pack_id_mismatch" in payload["blockers"]


def test_apply_blocks_rejected_bundle_gate(tmp_path: Path) -> None:
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="landing.orders"),
        desired={"table": "landing.orders"},
        ddl=("ALTER TABLE `landing`.`orders` MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )
    pack_path = _write_pack(tmp_path, pack)
    gate_path = tmp_path / "bundle-gate.json"
    gate_path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_bundle_gate.v1",
                "status": "blocked",
                "pack_id": pack.pack_id,
                "blockers": ["migration_bundle_gate.required_artifact_missing:approval"],
            }
        ),
        encoding="utf-8",
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(tmp_path / "ledger.json"),
        bundle_gate_path=str(gate_path),
    )

    assert code == 2
    assert payload["status"] == "blocked"
    assert "migration_bundle_gate.not_allowed" in payload["blockers"]
    assert "migration_bundle_gate.required_artifact_missing:approval" in payload["blockers"]


def test_apply_accepts_allowed_bundle_gate(tmp_path: Path) -> None:
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="landing.orders"),
        desired={"table": "landing.orders"},
        ddl=("ALTER TABLE `landing`.`orders` MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )
    pack_path = _write_pack(tmp_path, pack)
    gate_path = tmp_path / "bundle-gate.json"
    gate_path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_bundle_gate.v1",
                "status": "warning",
                "pack_id": pack.pack_id,
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(tmp_path / "ledger.json"),
        bundle_gate_path=str(gate_path),
    )

    assert code == 0
    assert payload["status"] == "applied"


def test_apply_phase_executes_phase_operations_and_records_results(tmp_path: Path) -> None:
    pack_path = _write_pack(tmp_path, _shadow_pack())
    ledger = tmp_path / "ledger.json"
    executor = _FakeExecutor()

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(ledger),
        phase="create_shadow",
        execute=True,
        executor=executor,
    )

    assert code == 0
    assert executor.executed == ["CREATE TABLE `landing`.`__shadow` (`id` Int64) ENGINE = MergeTree ORDER BY `id`"]
    assert payload["status"] == "phase_applied"
    assert payload["phase"] == "create_shadow"
    assert payload["operations"][0]["status"] == "executed"


def test_apply_validate_phase_blocks_when_validation_pairs_differ(tmp_path: Path) -> None:
    pack = _shadow_pack()
    pack_path = _write_pack(tmp_path, pack)
    ledger = tmp_path / "ledger.json"
    ledger.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_ledger.v1",
                "records": [
                    {
                        "schema_version": "dpone.schema_migration_ledger_record.v1",
                        "pack_id": pack.pack_id,
                        "status": "phase_applied",
                        "phase": "create_shadow",
                        "target": {"sink_type": "clickhouse", "table": "landing.orders"},
                        "blockers": [],
                        "warnings": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    before = json.loads(ledger.read_text(encoding="utf-8"))
    executor = _FakeExecutor(
        {
            "SELECT count() FROM `landing`.`orders`": [(2,)],
            "SELECT count() FROM `landing`.`__shadow`": [(1,)],
            "SELECT duplicate FROM `landing`.`__shadow`": [],
        }
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(ledger),
        phase="validate",
        execute=True,
        executor=executor,
    )

    assert code == 2
    assert payload["status"] == "blocked"
    assert "migration.validation_mismatch:validate_1:validate_2" in payload["blockers"]
    assert json.loads(ledger.read_text(encoding="utf-8")) == before


def test_apply_validate_phase_records_fingerprint_instead_of_large_result_rows(tmp_path: Path) -> None:
    pack = _shadow_pack_with_matching_hash_validation()
    pack_path = _write_pack(tmp_path, pack)
    ledger = tmp_path / "ledger.json"
    _write_applied_phase(ledger, pack.pack_id, "create_shadow")
    rows = [(index,) for index in range(25)]
    executor = _FakeExecutor(
        {
            "SELECT hash FROM `landing`.`orders`": rows,
            "SELECT hash FROM `landing`.`__shadow`": rows,
        }
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(ledger),
        phase="validate",
        execute=True,
        executor=executor,
    )

    assert code == 0
    operation = payload["operations"][0]
    assert operation["row_count"] == 25
    assert operation["result_truncated"] is True
    assert "result_rows" not in operation
    assert "_comparison_rows" not in operation


def test_apply_validate_phase_accepts_empty_final_validation_result(tmp_path: Path) -> None:
    pack = _shadow_pack()
    pack_path = _write_pack(tmp_path, pack)
    ledger = tmp_path / "ledger.json"
    _write_applied_phase(ledger, pack.pack_id, "create_shadow")
    executor = _FakeExecutor(
        {
            "SELECT count() FROM `landing`.`orders`": [(1,)],
            "SELECT count() FROM `landing`.`__shadow`": [(1,)],
            "SELECT duplicate FROM `landing`.`__shadow`": [],
        }
    )

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(ledger),
        phase="validate",
        execute=True,
        executor=executor,
    )

    assert code == 0
    assert payload["status"] == "phase_applied"
    assert payload["phase"] == "validate"


def test_apply_execute_requires_target_executor_when_phase_has_sql(tmp_path: Path) -> None:
    pack_path = _write_pack(tmp_path, _shadow_pack())

    code, payload = MigrationControlFacade().apply(
        plan_path=str(pack_path),
        ledger_path=str(tmp_path / "ledger.json"),
        phase="create_shadow",
        execute=True,
    )

    assert code == 2
    assert payload["status"] == "blocked"
    assert "migration.target_executor_required" in payload["blockers"]


def test_clickhouse_executor_factory_uses_target_connection_json(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    import dpone.runtime.connectors.clickhouse as clickhouse_module

    created: list[dict[str, Any]] = []

    class FakeClickHouseConnector:
        def __init__(self, **kwargs: Any) -> None:
            created.append(kwargs)

        def get_records(self, query: str) -> list[tuple[int]]:
            assert query == "SELECT 1"
            return [(1,)]

    monkeypatch.setattr(clickhouse_module, "ClickHouseConnector", FakeClickHouseConnector)
    connection = tmp_path / "clickhouse.json"
    connection.write_text(
        json.dumps(
            {
                "type": "clickhouse",
                "host": "127.0.0.1",
                "port": 59000,
                "database": "dpone_it",
                "user": "default",
                "password": "secret",
            }
        ),
        encoding="utf-8",
    )

    executor = load_migration_operation_executor(
        target=MigrationTarget(sink_type="clickhouse", table="dpone_it.orders"),
        target_connection_path=str(connection),
    )
    assert executor is not None
    result = executor.execute({"name": "validate_1", "operation_type": "validation", "sql": "SELECT 1"})

    assert created == [
        {
            "host": "127.0.0.1",
            "port": 59000,
            "database": "dpone_it",
            "user": "default",
            "password": "secret",
            "secure": False,
            "compression": True,
        }
    ]
    assert result["status"] == "executed"
    assert result["row_count"] == 1


def _write_pack(tmp_path: Path, pack: MigrationPack) -> Path:
    path = tmp_path / "pack.json"
    path.write_text(json.dumps(pack.to_dict(), indent=2), encoding="utf-8")
    return path


def _write_applied_phase(ledger: Path, pack_id: str, phase: str) -> None:
    ledger.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_ledger.v1",
                "records": [
                    {
                        "schema_version": "dpone.schema_migration_ledger_record.v1",
                        "pack_id": pack_id,
                        "status": "phase_applied",
                        "phase": phase,
                        "target": {"sink_type": "clickhouse", "table": "landing.orders"},
                        "blockers": [],
                        "warnings": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _shadow_pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="landing.orders"),
        desired={"table": "landing.orders"},
        actual={"table": "landing.orders"},
        strategy="shadow",
        phases=(
            {
                "name": "create_shadow",
                "operations": [
                    {
                        "name": "create_shadow_table",
                        "operation_type": "sql",
                        "sql": "CREATE TABLE `landing`.`__shadow` (`id` Int64) ENGINE = MergeTree ORDER BY `id`",
                    }
                ],
                "validations": [],
                "preconditions": [],
            },
            {
                "name": "validate",
                "operations": [],
                "validations": [
                    {
                        "name": "validate_1",
                        "operation_type": "validation",
                        "sql": "SELECT count() FROM `landing`.`orders`",
                    },
                    {
                        "name": "validate_2",
                        "operation_type": "validation",
                        "sql": "SELECT count() FROM `landing`.`__shadow`",
                    },
                    {
                        "name": "validate_3",
                        "operation_type": "validation",
                        "sql": "SELECT duplicate FROM `landing`.`__shadow`",
                    },
                ],
                "preconditions": [],
            },
        ),
        rollback={"supported": True, "ddl": []},
    )


def _shadow_pack_with_matching_hash_validation() -> MigrationPack:
    pack = _shadow_pack()
    phases = list(pack.phases)
    phases[1] = {
        "name": "validate",
        "operations": [],
        "validations": [
            {
                "name": "validate_1",
                "operation_type": "validation",
                "sql": "SELECT hash FROM `landing`.`orders`",
            },
            {
                "name": "validate_2",
                "operation_type": "validation",
                "sql": "SELECT hash FROM `landing`.`__shadow`",
            },
        ],
        "preconditions": [],
    }
    return MigrationPack.build(
        target=pack.target,
        desired=pack.desired,
        actual=pack.actual,
        strategy=pack.strategy,
        phases=tuple(phases),
        rollback=pack.rollback,
    )
