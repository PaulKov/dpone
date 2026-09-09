from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.data_contract_evidence import DataContractEvidenceBundleWriter
from dpone.ops.quarantine import QuarantineService
from dpone.readiness.physical_apply import DdlExecutionRequest, PhysicalDdlApplyService
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.schema_contracts import SchemaContract
from dpone.readiness.type_compatibility import TypeCompatibilityGate
from dpone.strategy_intelligence.advisor import StrategyAdvisor, StrategyContext
from dpone.type_system import ContractEnforcementService


class _RecordingExecutor:
    def __init__(self) -> None:
        self.requests: list[DdlExecutionRequest] = []

    def execute(self, request: DdlExecutionRequest) -> None:
        self.requests.append(request)


def test_strict_contract_rejects_bad_rows_before_target_load() -> None:
    contract = SchemaContract.from_config(
        {
            "enforcement": "strict",
            "columns": {
                "amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False},
                "updated_at": {"type": "timestamp", "timezone": True, "nullable": False},
            },
        }
    )

    result = ContractEnforcementService().enforce(
        rows=[
            {"amount": "12.30", "updated_at": "2026-06-06T10:00:00+00:00"},
            {"amount": "not-a-decimal", "updated_at": "2026-06-06T10:00:00+00:00"},
        ],
        contract=contract,
        run_id="01JSTRICTCONTRACT00000000",
        load_id="01JSTRICTLOAD00000000000",
    )

    assert result.passed is False
    assert result.target_rows == [{"amount": "12.30", "updated_at": "2026-06-06T10:00:00+00:00"}]
    assert result.rejected_rows == 1
    assert result.diagnostics[0].column == "amount"
    assert result.state_commit_allowed is False


def test_coerce_contract_converts_safe_values_without_collapsing_empty_string() -> None:
    contract = SchemaContract.from_config(
        {
            "enforcement": "coerce",
            "columns": {
                "amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False},
                "comment": {"type": "string", "nullable": True},
            },
        }
    )

    result = ContractEnforcementService().enforce(
        rows=[{"amount": "12.30", "comment": ""}],
        contract=contract,
        run_id="01JCOERCECONTRACT000000000",
        load_id="01JCOERCELOAD00000000000",
    )

    assert result.passed is True
    assert result.target_rows == [{"amount": "12.30", "comment": ""}]
    assert result.diagnostics == ()
    assert result.state_commit_allowed is True


def test_quarantine_contract_keeps_target_clean_and_persists_bad_rows(tmp_path: Path) -> None:
    contract = SchemaContract.from_config(
        {
            "enforcement": "quarantine",
            "columns": {
                "amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False},
            },
        }
    )
    quarantine = QuarantineService(tmp_path / "quarantine")

    result = ContractEnforcementService(quarantine=quarantine).enforce(
        rows=[{"amount": "10.00"}, {"amount": "bad"}],
        contract=contract,
        run_id="01JQUARANTINECONTRACT00000",
        load_id="01JQUARANTINELOAD0000000",
    )

    exported = quarantine.export(run_id="01JQUARANTINECONTRACT00000")

    assert result.passed is True
    assert result.target_rows == [{"amount": "10.00"}]
    assert result.quarantined_rows == 1
    assert result.state_commit_allowed is True
    assert exported.total_rows == 1
    assert exported.entries[0].row == {"amount": "bad"}
    assert exported.entries[0].diagnostics["column"] == "amount"


def test_variant_column_policy_routes_incompatible_values_to_dpone_namespace() -> None:
    contract = SchemaContract.from_config(
        {
            "enforcement": "warn",
            "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 2}},
        }
    )

    result = ContractEnforcementService().enforce(
        rows=[{"amount": "bad", "status": "new"}],
        contract=contract,
        conflict_policy="variant_column",
        run_id="01JVARIANTCONTRACT0000000",
        load_id="01JVARIANTLOAD000000000",
    )

    assert result.passed is True
    assert result.target_rows == [{"amount": None, "__dpone__nc__amount": "bad", "status": "new"}]
    assert result.diagnostics[0].action == "variant_column"


def test_online_physical_ddl_apply_rejects_blocking_changes_but_executes_safe_new_table() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="landing.orders",
        source_schema=[("id", "bigint"), ("amount", "numeric(18,2)")],
        options=PhysicalDesignOptions.from_config(
            {
                "apply": "online",
                "storage": {"mssql": {"compression": "none", "clustered_columnstore": True}},
            }
        ),
    )
    executor = _RecordingExecutor()

    report = PhysicalDdlApplyService(executor=executor).apply(plan, table_exists=True)

    assert report.applied is False
    assert "physical_design.blocking_ddl_requires_safe_window_or_manual_approval" in report.blockers
    assert executor.requests == []

    new_table_report = PhysicalDdlApplyService(executor=executor).apply(plan, table_exists=False)

    assert new_table_report.applied is True
    assert len(executor.requests) == len(plan.ddl)
    assert all(request.sink_type == "mssql" for request in executor.requests)


def test_type_compatibility_gate_blocks_unsafe_narrowing_and_can_plan_variant_column() -> None:
    gate = TypeCompatibilityGate()
    strict = gate.evaluate(
        sink_type="mssql",
        source_columns={"amount": "decimal(18,4)"},
        target_columns={"amount": "int"},
        conflict_policy="fail",
    )
    variant = gate.evaluate(
        sink_type="mssql",
        source_columns={"amount": "decimal(18,4)"},
        target_columns={"amount": "int"},
        conflict_policy="variant_column",
    )

    assert strict.passed is False
    assert strict.decisions[0].action == "fail"
    assert variant.passed is True
    assert variant.decisions[0].action == "variant_column"
    assert variant.decisions[0].target_column == "__dpone__nc__amount"


def test_data_contract_evidence_bundle_writes_audit_and_openlineage_facets(tmp_path: Path) -> None:
    contract = SchemaContract.from_config(
        {"enforcement": "strict", "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 2}}}
    )
    enforcement = ContractEnforcementService().enforce(
        rows=[{"amount": "12.30"}],
        contract=contract,
        run_id="01JEVIDENCERUN00000000000",
        load_id="01JEVIDENCELOAD000000000",
    )

    artifact = DataContractEvidenceBundleWriter(tmp_path).write(
        run_id="01JEVIDENCERUN00000000000",
        pipeline="orders",
        enforcement=enforcement,
        ddl_apply={"applied": True, "blockers": []},
        compatibility={"passed": True, "decisions": []},
    )

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.data_contract_evidence.v1"
    assert payload["passed"] is True
    assert payload["openlineage_facets"]["dpone_data_contract"]["passed"] is True
    assert "# dpone data contract evidence" in artifact.markdown_path.read_text(encoding="utf-8")


def test_strategy_advisor_v2_warns_for_dirty_sources_and_recommends_partition_replace() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="api",
            sink_type="mssql",
            requested_mode="auto",
            unique_key=("order_id",),
            estimated_rows=5_000_000,
            changed_percent=0.25,
            delete_percent=0.05,
            partition_column="business_date",
        )
    )

    assert decision.strategy_mode == "partition_replace"
    assert any(signal.code == "delete_semantics" for signal in decision.safety_gates)
    assert any(signal.code == "dirty_source_contract_recommended" for signal in decision.warnings)
