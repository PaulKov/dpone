"""Provider-neutral remediation execution planning and certification."""

from __future__ import annotations

import hashlib
import re
import shlex
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from dpone.readiness import data_product_remediation_execution_support as support

_PLACEHOLDER = re.compile(r"<([^<>]+)>")


class RemediationCommandExecutor(Protocol):
    """Minimal port for controlled command execution."""

    def execute(self, argv: tuple[str, ...], *, timeout_seconds: int) -> Mapping[str, Any]: ...


class RemediationExecutionPlanner:
    """Resolve remediation runbook commands into controlled execution steps."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        remediation_plan: Mapping[str, Any],
        remediation_gate: Mapping[str, Any] | None = None,
        authority_gate: Mapping[str, Any] | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = support.RemediationExecutionOptions.from_manifest(manifest)
        if not options.enabled:
            return _plan_payload(options, remediation_plan, (), (), (), None)
        blockers = _input_blockers(options, remediation_gate, authority_gate)
        steps, step_blockers, warnings = _steps(remediation_plan, options, parameters or {})
        blockers.extend(step_blockers)
        blockers, warnings = support.apply_profile(blockers=blockers, warnings=warnings, profile=options.profile)
        return _plan_payload(
            options,
            remediation_plan,
            steps,
            blockers,
            warnings,
            _mapping(authority_gate).get("authority_gate_id"),
        )


class RemediationExecutionRunner:
    """Execute or dry-run a remediation execution plan through an injected executor."""

    def run(
        self,
        *,
        execution_plan: Mapping[str, Any],
        executor: RemediationCommandExecutor | None,
        execute: bool,
        idempotency_key: str | None,
        lock: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if execution_plan.get("status") == "disabled":
            return _run_payload(execution_plan, "disabled", execute, idempotency_key, (), (), ())
        blockers = list(support.strings(execution_plan.get("blockers")))
        warnings = list(support.strings(execution_plan.get("warnings")))
        options = _execution_policy(execution_plan)
        if blockers:
            return _run_payload(execution_plan, "blocked", execute, idempotency_key, (), blockers, warnings)
        if not execute:
            warnings.append("data_product_remediation_execution.not_executed")
            return _run_payload(
                execution_plan,
                "dry_run",
                False,
                idempotency_key,
                _dry_receipts(execution_plan),
                blockers,
                warnings,
            )
        blockers.extend(_execution_blockers(options, idempotency_key, lock, executor))
        if blockers:
            return _run_payload(execution_plan, "blocked", execute, idempotency_key, (), blockers, warnings)
        receipts, step_blockers = _execute_steps(execution_plan, executor)
        blockers.extend(step_blockers)
        status = "failed" if blockers else "executed"
        return _run_payload(execution_plan, status, True, idempotency_key, receipts, blockers, warnings)


class RemediationExecutionCertifier:
    """Certify that execution ran and produced fresh expected evidence."""

    def certify(
        self,
        *,
        run: Mapping[str, Any],
        evidence_payloads: Sequence[Mapping[str, Any]],
        profile: str = "prod_strict",
    ) -> dict[str, Any]:
        if run.get("status") == "disabled":
            return _certificate_payload(run, "certified", (), (), (), profile)
        refs = _evidence_refs(evidence_payloads, _product(run))
        blockers = list(support.strings(run.get("blockers")))
        warnings = list(support.strings(run.get("warnings")))
        blockers.extend(_run_status_blockers(run))
        blockers.extend(_fresh_evidence_blockers(run, refs))
        blockers, warnings = support.apply_profile(blockers=blockers, warnings=warnings, profile=profile)
        status = support.status(blockers, warnings, "certified")
        return _certificate_payload(run, status, refs, blockers, warnings, profile)


def _steps(
    plan: Mapping[str, Any],
    options: support.RemediationExecutionOptions,
    parameters: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], list[str], list[str]]:
    steps: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    for action in support.mappings(plan.get("actions")):
        for command in support.strings(action.get("commands")):
            step, step_blockers, step_warnings = _step(action, command, options, parameters)
            steps.append(step)
            blockers.extend(step_blockers)
            warnings.extend(step_warnings)
    return tuple(steps), blockers, warnings


def _step(
    action: Mapping[str, Any],
    command: str,
    options: support.RemediationExecutionOptions,
    parameters: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    resolved, blockers, warnings = _resolve_command(command, options, parameters)
    argv = tuple(shlex.split(resolved)) if resolved and not blockers else ()
    if argv and not _allowed(argv, options.allowed_command_prefixes):
        blockers.append(f"data_product_remediation_execution.command_not_allowed:{argv[0]}")
    payload = {
        "action_id": action.get("action_id"),
        "domain": action.get("domain"),
        "owner": action.get("owner"),
        "repair_class": action.get("repair_class"),
        "command_template": command,
        "command": resolved,
        "argv": list(argv),
        "expected_evidence": [dict(item) for item in support.mappings(action.get("expected_evidence"))],
        "status": "blocked" if blockers else "ready",
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
    }
    return support.payload_id(payload, "step_id"), blockers, warnings


def _resolve_command(
    command: str,
    options: support.RemediationExecutionOptions,
    parameters: Mapping[str, Any],
) -> tuple[str, list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in parameters and parameters[name] is not None:
            return shlex.quote(str(parameters[name]))
        message = f"data_product_remediation_execution.unresolved_placeholder:{name}"
        if options.unresolved_command_policy == "warn":
            warnings.append(message)
        else:
            blockers.append(message)
        return match.group(0)

    return _PLACEHOLDER.sub(replace, command), blockers, warnings


def _allowed(argv: Sequence[str], prefixes: Sequence[Sequence[str]]) -> bool:
    return any(tuple(argv[: len(prefix)]) == tuple(prefix) for prefix in prefixes)


def _input_blockers(
    options: support.RemediationExecutionOptions,
    remediation_gate: Mapping[str, Any] | None,
    authority_gate: Mapping[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    if options.require_remediation_gate and _mapping(remediation_gate).get("status") not in {"allowed", "warning"}:
        blockers.append("data_product_remediation_execution.remediation_gate_required")
    if options.require_authority_gate and _mapping(authority_gate).get("status") not in {"allowed", "warning"}:
        blockers.append("data_product_remediation_execution.authority_gate_required")
    return blockers


def _execution_policy(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(plan.get("execution_policy"))


def _execution_blockers(
    policy: Mapping[str, Any],
    idempotency_key: str | None,
    lock: Mapping[str, Any] | None,
    executor: RemediationCommandExecutor | None,
) -> list[str]:
    blockers: list[str] = []
    if policy.get("require_idempotency_key", True) and not idempotency_key:
        blockers.append("data_product_remediation_execution.idempotency_key_required")
    if policy.get("require_lock", True) and _mapping(lock).get("status") != "acquired":
        blockers.append("data_product_remediation_execution.lock_required")
    if executor is None:
        blockers.append("data_product_remediation_execution.executor_required")
    return blockers


def _execute_steps(
    plan: Mapping[str, Any], executor: RemediationCommandExecutor | None
) -> tuple[tuple[dict[str, Any], ...], list[str]]:
    receipts: list[dict[str, Any]] = []
    blockers: list[str] = []
    timeout = int(_execution_policy(plan).get("command_timeout_seconds") or 300)
    for step in support.mappings(plan.get("steps")):
        result = executor.execute(tuple(support.strings(step.get("argv"))), timeout_seconds=timeout) if executor else {}
        receipt = _receipt(step, result)
        receipts.append(receipt)
        if receipt.get("status") != "succeeded":
            blockers.append("data_product_remediation_execution.step_failed")
    return tuple(receipts), support.dedupe(blockers)


def _dry_receipts(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return tuple(
        _receipt(step, {"status": "dry_run", "exit_code": None}) for step in support.mappings(plan.get("steps"))
    )


def _receipt(step: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    status = str(result.get("status") or "failed")
    payload = {
        "step_id": step.get("step_id"),
        "action_id": step.get("action_id"),
        "argv": list(support.strings(step.get("argv"))),
        "status": status,
        "exit_code": result.get("exit_code"),
        "duration_ms": result.get("duration_ms"),
        "timeout_seconds": result.get("timeout_seconds"),
        "stdout_sha256": _digest(result.get("stdout")),
        "stderr_sha256": _digest(result.get("stderr")),
        "stdout_snippet": _snippet(result.get("stdout")),
        "stderr_snippet": _snippet(result.get("stderr")),
    }
    return support.payload_id(payload, "step_receipt_id")


def _run_status_blockers(run: Mapping[str, Any]) -> list[str]:
    status = str(run.get("status") or "")
    if status == "dry_run":
        return ["data_product_remediation_execution.execution_required"]
    if status in {"blocked", "failed"}:
        return ["data_product_remediation_execution.step_failed"]
    return []


def _fresh_evidence_blockers(run: Mapping[str, Any], refs: Sequence[Mapping[str, Any]]) -> list[str]:
    by_kind = {str(ref.get("artifact_kind")): ref for ref in refs}
    blockers: list[str] = []
    for step in support.mappings(run.get("steps")):
        for expected in support.mappings(step.get("expected_evidence")):
            kind = str(expected.get("kind") or "")
            ref = by_kind.get(kind)
            if not ref:
                blockers.append(f"data_product_remediation_execution.evidence_missing:{kind}")
            elif ref.get("evidence_id") == expected.get("previous_evidence_id"):
                blockers.append(f"data_product_remediation_execution.evidence_stale:{kind}")
            elif str(ref.get("status")) not in set(support.strings(expected.get("allowed_statuses"))):
                blockers.append(f"data_product_remediation_execution.evidence_blocked:{kind}")
    return support.dedupe(blockers)


def _plan_payload(
    options: support.RemediationExecutionOptions,
    remediation_plan: Mapping[str, Any],
    steps: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
    authority_gate_id: Any,
) -> dict[str, Any]:
    status = "disabled" if not options.enabled else support.status(blockers, warnings, "ready")
    payload = {
        "schema_version": support.PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": dict(options.product),
        "product_id": options.product.get("id"),
        "pack_id": remediation_plan.get("pack_id"),
        "bundle_id": remediation_plan.get("bundle_id"),
        "remediation_plan_id": remediation_plan.get("remediation_plan_id"),
        "authority_gate_id": authority_gate_id,
        "steps": [dict(step) for step in steps],
        "execution_policy": {
            "require_lock": options.require_lock,
            "require_idempotency_key": options.require_idempotency_key,
            "command_timeout_seconds": options.command_timeout_seconds,
            "allowed_command_prefixes": [list(prefix) for prefix in options.allowed_command_prefixes],
        },
        "summary": {"steps": len(steps), "ready_steps": sum(1 for step in steps if step.get("status") == "ready")},
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
    }
    return support.payload_id(payload, "remediation_execution_plan_id")


def _run_payload(
    plan: Mapping[str, Any],
    status: str,
    executed: bool,
    idempotency_key: str | None,
    receipts: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.RUN_SCHEMA,
        "status": status,
        "executed": bool(executed and status == "executed"),
        "product": _product(plan),
        "product_id": plan.get("product_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "remediation_plan_id": plan.get("remediation_plan_id"),
        "remediation_execution_plan_id": plan.get("remediation_execution_plan_id"),
        "idempotency_key": idempotency_key,
        "steps": [dict(step) for step in support.mappings(plan.get("steps"))],
        "step_receipts": [dict(receipt) for receipt in receipts],
        "summary": {"executed_steps": sum(1 for receipt in receipts if receipt.get("status") == "succeeded")},
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
    }
    return support.payload_id(payload, "remediation_execution_run_id")


def _certificate_payload(
    run: Mapping[str, Any],
    status: str,
    refs: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
    profile: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": support.CERTIFICATE_SCHEMA,
        "status": status,
        "profile": profile,
        "product": _product(run),
        "product_id": run.get("product_id"),
        "pack_id": run.get("pack_id"),
        "bundle_id": run.get("bundle_id"),
        "remediation_plan_id": run.get("remediation_plan_id"),
        "remediation_execution_plan_id": run.get("remediation_execution_plan_id"),
        "remediation_execution_run_id": run.get("remediation_execution_run_id"),
        "summary": {
            "certified_steps": sum(
                1 for receipt in support.mappings(run.get("step_receipts")) if receipt.get("status") == "succeeded"
            )
        },
        "evidence_refs": [dict(ref) for ref in refs],
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
    }
    return support.payload_id(payload, "remediation_execution_certificate_id")


def _evidence_refs(payloads: Sequence[Mapping[str, Any]], product: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return tuple(support.evidence_ref(payload, product) for payload in payloads if isinstance(payload, Mapping))


def _digest(raw: Any) -> str:
    data = str(raw or "").encode("utf-8")
    return hashlib.sha256(data).hexdigest() if data else ""


def _snippet(raw: Any) -> str:
    text = str(raw or "").strip()
    return text[:200]


def _product(payload: Mapping[str, Any]) -> dict[str, Any]:
    return dict(_mapping(payload.get("product")))


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = (
    "RemediationCommandExecutor",
    "RemediationExecutionCertifier",
    "RemediationExecutionPlanner",
    "RemediationExecutionRunner",
)
