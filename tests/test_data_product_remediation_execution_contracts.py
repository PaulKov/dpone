from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_remediation import (
    DataProductRemediationGate,
    DataProductRemediationPlanner,
)
from dpone.readiness.data_product_remediation_execution import (
    RemediationExecutionCertifier,
    RemediationExecutionPlanner,
    RemediationExecutionRunner,
)


class _FakeExecutor:
    def __init__(self, *, status: str = "succeeded") -> None:
        self.status = status
        self.calls: list[tuple[str, ...]] = []

    def execute(self, argv: tuple[str, ...], *, timeout_seconds: int) -> dict:
        self.calls.append(argv)
        return {
            "status": self.status,
            "exit_code": 0 if self.status == "succeeded" else 1,
            "duration_ms": 7,
            "stdout": "ok",
            "stderr": "" if self.status == "succeeded" else "failed",
            "timeout_seconds": timeout_seconds,
        }


def test_disabled_execution_emits_noop_plan_run_and_certificate() -> None:
    execution_plan = RemediationExecutionPlanner().plan(
        manifest=_manifest(execution_enabled=False),
        remediation_plan=_remediation_plan(),
        remediation_gate={},
        authority_gate={},
        parameters={},
    )
    run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=_FakeExecutor(),
        execute=False,
        idempotency_key=None,
        lock={},
    )
    certificate = RemediationExecutionCertifier().certify(run=run, evidence_payloads=(), profile="prod_strict")

    assert execution_plan["schema_version"] == "dpone.data_product_remediation_execution_plan.v1"
    assert execution_plan["status"] == "disabled"
    assert execution_plan["steps"] == []
    assert run["status"] == "disabled"
    assert certificate["status"] == "certified"


def test_execution_plan_resolves_allowlisted_commands_and_blocks_unresolved_placeholders() -> None:
    remediation_plan = _remediation_plan()
    unresolved = RemediationExecutionPlanner().plan(
        manifest=_manifest(),
        remediation_plan=remediation_plan,
        remediation_gate=_remediation_gate(remediation_plan),
        authority_gate=_authority_gate(),
        parameters={},
    )
    resolved = RemediationExecutionPlanner().plan(
        manifest=_manifest(),
        remediation_plan=remediation_plan,
        remediation_gate=_remediation_gate(remediation_plan),
        authority_gate=_authority_gate(),
        parameters={"assertion-plan": "orders.assertion-plan.json"},
    )

    assert unresolved["status"] == "blocked"
    assert any(
        blocker.startswith("data_product_remediation_execution.unresolved_placeholder:")
        for blocker in unresolved["blockers"]
    )
    assert resolved["status"] == "ready"
    assert resolved["steps"][0]["argv"] == [
        "dpone",
        "data",
        "product",
        "assertions",
        "evaluate",
        "--plan",
        "orders.assertion-plan.json",
        "--format",
        "json",
    ]


def test_execution_plan_blocks_non_allowlisted_commands() -> None:
    remediation_plan = _remediation_plan()
    remediation_plan["actions"][0]["commands"] = ["python -c 'print(1)'"]

    execution_plan = RemediationExecutionPlanner().plan(
        manifest=_manifest(),
        remediation_plan=remediation_plan,
        remediation_gate=_remediation_gate(remediation_plan),
        authority_gate=_authority_gate(),
        parameters={},
    )

    assert execution_plan["status"] == "blocked"
    assert "data_product_remediation_execution.command_not_allowed:python" in execution_plan["blockers"]


def test_runner_dry_run_executes_nothing_and_execute_requires_lock_and_idempotency() -> None:
    execution_plan = _execution_plan()
    executor = _FakeExecutor()

    dry_run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=executor,
        execute=False,
        idempotency_key=None,
        lock={},
    )
    missing_preconditions = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=executor,
        execute=True,
        idempotency_key=None,
        lock={},
    )

    assert dry_run["status"] == "dry_run"
    assert dry_run["executed"] is False
    assert executor.calls == []
    assert missing_preconditions["status"] == "blocked"
    assert "data_product_remediation_execution.idempotency_key_required" in missing_preconditions["blockers"]
    assert "data_product_remediation_execution.lock_required" in missing_preconditions["blockers"]


def test_runner_executes_ready_steps_with_injected_executor_and_certifies_fresh_evidence() -> None:
    execution_plan = _execution_plan()
    executor = _FakeExecutor()
    run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=executor,
        execute=True,
        idempotency_key="orders-remediation-1",
        lock=_lock(execution_plan),
    )
    certificate = RemediationExecutionCertifier().certify(
        run=run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="prod_strict",
    )

    assert run["status"] == "executed"
    assert run["executed"] is True
    assert executor.calls == [tuple(execution_plan["steps"][0]["argv"])]
    assert run["step_receipts"][0]["stdout_sha256"]
    assert certificate["status"] == "certified"
    assert certificate["summary"]["certified_steps"] == 1


def test_certificate_blocks_dry_run_and_failed_execution_in_strict_profile() -> None:
    execution_plan = _execution_plan()
    dry_run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=_FakeExecutor(),
        execute=False,
        idempotency_key=None,
        lock={},
    )
    failed_run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=_FakeExecutor(status="failed"),
        execute=True,
        idempotency_key="orders-remediation-1",
        lock=_lock(execution_plan),
    )

    dry_certificate = RemediationExecutionCertifier().certify(
        run=dry_run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="prod_strict",
    )
    failed_certificate = RemediationExecutionCertifier().certify(
        run=failed_run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="prod_strict",
    )
    advisory_certificate = RemediationExecutionCertifier().certify(
        run=dry_run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="advisory",
    )

    assert dry_certificate["status"] == "blocked"
    assert "data_product_remediation_execution.execution_required" in dry_certificate["blockers"]
    assert failed_certificate["status"] == "blocked"
    assert "data_product_remediation_execution.step_failed" in failed_certificate["blockers"]
    assert advisory_certificate["status"] == "warning"


def test_execution_artifacts_are_deterministic_and_schema_valid() -> None:
    execution_plan = _execution_plan()
    run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=_FakeExecutor(),
        execute=True,
        idempotency_key="orders-remediation-1",
        lock=_lock(execution_plan),
    )
    certificate = RemediationExecutionCertifier().certify(
        run=run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="prod_strict",
    )
    repeat = _execution_plan()

    assert execution_plan["remediation_execution_plan_id"] == repeat["remediation_execution_plan_id"]
    for name, payload in (
        ("data-product-remediation-execution-plan.schema.json", execution_plan),
        ("data-product-remediation-execution-run.schema.json", run),
        ("data-product-remediation-execution-certificate.schema.json", certificate),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _execution_plan() -> dict:
    remediation_plan = _remediation_plan()
    return RemediationExecutionPlanner().plan(
        manifest=_manifest(),
        remediation_plan=remediation_plan,
        remediation_gate=_remediation_gate(remediation_plan),
        authority_gate=_authority_gate(),
        parameters={"assertion-plan": "orders.assertion-plan.json"},
    )


def _remediation_plan() -> dict:
    plan = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(status="blocked"),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
    )
    assert plan["status"] == "ready"
    return plan


def _remediation_gate(plan: dict) -> dict:
    return DataProductRemediationGate().evaluate(plan=plan, profile="prod_strict")


def _manifest(*, execution_enabled: bool = True) -> dict:
    from tests.test_data_product_remediation_contracts import _manifest

    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["remediation"]["execution"] = {
        "enabled": execution_enabled,
        "mode": "gate",
        "profile": "prod_strict",
        "require_remediation_gate": True,
        "require_authority_gate": True,
        "require_lock": True,
        "require_idempotency_key": True,
        "unresolved_command_policy": "block",
        "dry_run_status": "warning",
        "command_timeout_seconds": 30,
        "allowed_command_prefixes": [["dpone", "data", "product", "assertions"]],
    }
    return manifest


def _trust_gate(*, status: str) -> dict:
    from tests.test_data_product_remediation_contracts import _trust_gate

    return _trust_gate(status=status)


def _trust_snapshot() -> dict:
    from tests.test_data_product_remediation_contracts import _trust_snapshot

    return _trust_snapshot()


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    from tests.test_data_product_remediation_contracts import _assertion_gate

    return _assertion_gate(status=status, evidence_id=evidence_id)


def _authority_gate() -> dict:
    return {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "status": "allowed",
        "authority_gate_id": "sha256:authority",
        "product_id": "analytics.orders",
        "blockers": [],
        "warnings": [],
    }


def _lock(execution_plan: dict) -> dict:
    return {
        "schema_version": "dpone.local_lock.v1",
        "status": "acquired",
        "lock_id": "sha256:lock",
        "remediation_execution_plan_id": execution_plan["remediation_execution_plan_id"],
    }
