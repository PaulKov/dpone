from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.airflow_compat import python_operator_class
from dpone_airflow_pack.launch_pin import (
    launch_pin_required,
    raise_for_launch_pin_resolution,
    resolve_launch_pin,
)
from dpone_airflow_pack.launch_pin_cleanup import (
    AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
    AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
    build_cleanup_handle_from_pin,
)
from dpone_airflow_pack.launch_pin_locator import LaunchPinStoreLocator
from dpone_airflow_pack.outcome_gate import GitOpsAirflowOutcomeGateEvaluator
from dpone_airflow_pack.strict_json import loads_strict_json_object


def build_pack_outcome_task(
    *,
    pack: Mapping[str, Any],
    dag: Any,
    upstream_task_id: str,
    node: Any = None,
    task_group: Any = None,
    launch_pin_store: Mapping[str, str | None] | None = None,
) -> Any | None:
    """Build the optional non-mapped outcome gate from static pack metadata."""

    outcome = pack.get("outcome_gate")
    if not isinstance(outcome, Mapping) or not outcome:
        return None
    frozen_store = dict(launch_pin_store) if isinstance(launch_pin_store, Mapping) else None
    return python_operator_class()(
        dag=dag,
        task_id=(
            node.task_id("outcome_gate")
            if node is not None
            else str(outcome.get("task_id") or f"{upstream_task_id}__outcome_gate")
        ),
        **({"task_group": task_group} if task_group is not None else {}),
        python_callable=evaluate_pack_outcome,
        op_kwargs={
            "upstream_task_id": upstream_task_id,
            "required_status": str(outcome.get("required_status") or "passed"),
            "expected_run_identity": _expected_run_identity(pack),
            "expected_deployment_identity": _expected_deployment_identity(pack),
            "launch_pin_required": launch_pin_required(pack),
            "launch_pin_store": frozen_store,
        },
    )


def evaluate_pack_outcome(
    *,
    upstream_task_id: str,
    required_status: str = "passed",
    expected_run_identity: Mapping[str, Any] | None = None,
    expected_deployment_identity: Mapping[str, Any] | None = None,
    expected_runtime_evidence_sha256: str | None = None,
    launch_pin_required: bool = False,
    launch_pin_store: Mapping[str, Any] | None = None,
    ti: Any = None,
    **_: Any,
) -> dict[str, Any]:
    """Evaluate the XCom summary produced by a dpone runtime KPO.

    Launch-time pin written after concrete pod selection supplies run identity,
    deployment identity, and optional evidence digest expectations. Parse-time
    ``op_kwargs`` alone are unsafe after an Exact-cache tip flip re-materializes
    a deferred ``outcome_gate``.

    On a PRESENT pin the gate publishes an occurrence-bound cleanup handle and a
    structured result XCom so ``launch_pin_cleanup`` can delete exactly that
    occurrence and rematerialize gate failure for DAG status.
    """

    published = False
    try:
        summary = _pull_summary(ti=ti, upstream_task_id=upstream_task_id)
        resolution = resolve_launch_pin(
            ti=ti,
            upstream_task_id=upstream_task_id,
            required=bool(launch_pin_required),
            launch_pin_store=launch_pin_store,
            runtime_summary=summary,
        )
        raise_for_launch_pin_resolution(resolution)
        effective_run_identity: Mapping[str, Any] | None
        effective_deployment_identity: Mapping[str, Any] | None
        effective_evidence: str | None
        if resolution.status == "PRESENT":
            # Pin fields are authoritative, including explicit nulls — never fall back
            # to parse-time tip values for a PRESENT full-envelope pin.
            assert resolution.pin is not None
            effective_run_identity = resolution.run_identity
            effective_deployment_identity = resolution.deployment_identity
            effective_evidence = resolution.expected_runtime_evidence_sha256
            _publish_cleanup_handle(ti=ti, pin=resolution.pin, store_locator=resolution.store_locator)
        elif resolution.status == "PRESENT_LEGACY_PARTIAL":
            effective_run_identity = expected_run_identity
            effective_deployment_identity = resolution.deployment_identity or expected_deployment_identity
            effective_evidence = expected_runtime_evidence_sha256
        else:
            effective_run_identity = expected_run_identity
            effective_deployment_identity = expected_deployment_identity
            effective_evidence = expected_runtime_evidence_sha256
        report = GitOpsAirflowOutcomeGateEvaluator().evaluate(
            xcom_summary_path=f"xcom://{upstream_task_id}",
            xcom_summary=summary,
            required_status=required_status,
            expected_run_identity=effective_run_identity,
            expected_deployment_identity=effective_deployment_identity,
            expected_runtime_evidence_sha256=effective_evidence,
        )
        payload = report.to_jsonable()
        _publish_gate_result(ti=ti, passed=bool(report.passed), payload=payload)
        published = True
        if not report.passed:
            raise RuntimeError(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return payload
    except Exception as exc:
        if not published:
            _publish_gate_result(
                ti=ti,
                passed=False,
                payload=_redacted_early_failure_payload(exc),
            )
        raise


def _publish_cleanup_handle(
    *,
    ti: Any,
    pin: Mapping[str, Any],
    store_locator: LaunchPinStoreLocator | None = None,
) -> None:
    if ti is None or not hasattr(ti, "xcom_push"):
        return
    if store_locator is None or not store_locator.namespace or not str(store_locator.namespace).strip():
        return
    try:
        from dpone_airflow_pack.launch_pin_locator import LaunchPinStoreLocator

        locator = store_locator if isinstance(store_locator, LaunchPinStoreLocator) else None
        if locator is None:
            return
        ti.xcom_push(
            key=AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
            value=build_cleanup_handle_from_pin(
                pin,
                store_backend=locator.backend,
                store_kubernetes_conn_id=locator.kubernetes_conn_id,
                store_namespace=locator.namespace,
            ),
        )
    except Exception:  # noqa: BLE001 - cleanup handle is best-effort relative to gate verdict
        pass


def _publish_gate_result(*, ti: Any, passed: bool, payload: Mapping[str, Any]) -> None:
    if ti is None or not hasattr(ti, "xcom_push"):
        return
    try:
        ti.xcom_push(
            key=AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
            value={"passed": passed, "payload": dict(payload)},
        )
    except Exception:  # noqa: BLE001
        pass


def _redacted_early_failure_payload(exc: BaseException) -> dict[str, Any]:
    detail = str(exc)
    if len(detail) > 2000:
        detail = detail[:2000] + "…"
    code = "DPONE_AIRFLOW_OUTCOME_GATE_EARLY_FAILURE"
    for marker in (
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_INVALID",
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_UNAVAILABLE",
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_MISSING",
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_CONFLICT",
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_STALE_WRITER",
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_RECOVERY_REQUIRED",
    ):
        if marker in detail:
            code = marker
            break
    return {"code": code, "passed": False, "detail": detail}


def evaluate_inline_pack_outcome(
    summary: object,
    *,
    task_id: str,
    required_status: str = "passed",
    expected_run_identity: Mapping[str, Any] | None = None,
    expected_deployment_identity: Mapping[str, Any] | None = None,
    expected_runtime_evidence_sha256: str | None = None,
) -> object:
    """Fail one runtime operator from the XCom value it returned directly."""

    normalized = _summary_mapping(summary, task_id=task_id)
    report = GitOpsAirflowOutcomeGateEvaluator().evaluate(
        xcom_summary_path=f"xcom://{task_id}",
        xcom_summary=normalized,
        required_status=required_status,
        expected_run_identity=expected_run_identity,
        expected_deployment_identity=expected_deployment_identity,
        expected_runtime_evidence_sha256=expected_runtime_evidence_sha256,
    )
    if not report.passed:
        payload = report.to_jsonable()
        payload["code"] = "DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"
        raise RuntimeError(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return summary


def evaluate_operator_inline_outcome(
    operator: Any,
    result: Any,
) -> Any:
    """Apply inline outcome expectations configured on a runtime operator."""

    required_status = getattr(operator, "inline_outcome_required_status", None)
    if required_status is None:
        return result
    task_id = str(getattr(operator, "task_id", "") or getattr(operator, "kwargs", {}).get("task_id") or "dpone_runtime")
    return evaluate_inline_pack_outcome(
        result,
        task_id=task_id,
        required_status=required_status,
        expected_run_identity=getattr(operator, "expected_run_identity", None),
        expected_deployment_identity=getattr(operator, "expected_deployment_identity", None),
        expected_runtime_evidence_sha256=getattr(operator, "expected_runtime_evidence_sha256", None),
    )


def pull_deferrable_xcom(*, context: Any, task_id: str) -> Any:
    """Read the value that KPO versions returning ``None`` already pushed."""

    task_instance = context.get("ti") if isinstance(context, Mapping) else None
    if task_instance is None or not hasattr(task_instance, "xcom_pull"):
        raise RuntimeError(
            "DPONE_AIRFLOW_INLINE_OUTCOME_FAILED: deferrable KPO did not return an XCom summary "
            "and the task context cannot read the pushed value"
        )
    return task_instance.xcom_pull(task_ids=task_id)


def _pull_summary(*, ti: Any, upstream_task_id: str) -> Mapping[str, Any]:
    if ti is None or not hasattr(ti, "xcom_pull"):
        raise RuntimeError("Airflow task instance with xcom_pull is required to evaluate dpone outcome")
    try:
        return _coerce_strict_summary(
            ti.xcom_pull(task_ids=upstream_task_id),
            task_id=upstream_task_id,
            code_prefix="",
        )
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001 - keep Airflow task failure machine-readable
        raise RuntimeError(
            f"dpone runtime task `{upstream_task_id}` did not return a strict JSON object XCom summary"
        ) from exc


def _summary_mapping(summary: object, *, task_id: str) -> Mapping[str, Any]:
    try:
        return _coerce_strict_summary(
            summary,
            task_id=task_id,
            code_prefix="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED: ",
        )
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001 - keep Airflow task failure machine-readable
        raise RuntimeError(
            f"DPONE_AIRFLOW_INLINE_OUTCOME_FAILED: dpone runtime task `{task_id}` "
            "did not return a strict JSON object XCom summary"
        ) from exc


def _coerce_strict_summary(
    summary: object,
    *,
    task_id: str,
    code_prefix: str,
) -> dict[str, Any]:
    """Normalize only the result supplied by the current Airflow task attempt."""

    if isinstance(summary, str):
        try:
            payload = loads_strict_json_object(summary)
        except (ValueError, RecursionError) as exc:
            raise RuntimeError(
                f"{code_prefix}dpone runtime task `{task_id}` did not return a strict JSON object XCom summary"
            ) from exc
        return _reject_contradictory_success(payload, task_id=task_id, code_prefix=code_prefix)
    if isinstance(summary, Mapping):
        try:
            payload = loads_strict_json_object(
                json.dumps(summary, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
            )
        except (TypeError, ValueError, RecursionError) as exc:
            raise RuntimeError(
                f"{code_prefix}dpone runtime task `{task_id}` did not return a strict JSON object XCom summary"
            ) from exc
        return _reject_contradictory_success(payload, task_id=task_id, code_prefix=code_prefix)
    raise RuntimeError(f"{code_prefix}dpone runtime task `{task_id}` did not return a JSON object XCom summary")


def _reject_contradictory_success(
    payload: Mapping[str, Any],
    *,
    task_id: str,
    code_prefix: str,
) -> dict[str, Any]:
    status = str(payload.get("status") or "").strip().lower()
    recovery = payload.get("recovery")
    if status == "passed" and isinstance(recovery, Mapping) and str(recovery.get("code") or "") == "COMMIT_UNKNOWN":
        raise RuntimeError(
            f"{code_prefix}dpone runtime task `{task_id}` reported passed status with COMMIT_UNKNOWN recovery"
        )
    return dict(payload)


def _expected_run_identity(pack: Mapping[str, Any]) -> dict[str, Any] | None:
    identity = pack.get("_dpone_run_identity")
    return dict(identity) if isinstance(identity, Mapping) else None


def _expected_deployment_identity(pack: Mapping[str, Any]) -> dict[str, Any] | None:
    identity = pack.get("_dpone_deployment_identity")
    return dict(identity) if isinstance(identity, Mapping) else None


def _operator_outcome_expectations(
    params: object,
    *,
    explicit_evidence_digest: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    values = params if isinstance(params, Mapping) else {}
    raw_identity = values.get("dpone_run_identity")
    identity = (
        dict(raw_identity) if isinstance(raw_identity, Mapping) else ({} if "dpone_run_identity" in values else None)
    )
    raw_deployment_identity = values.get("dpone_deployment_identity")
    deployment_identity = (
        dict(raw_deployment_identity)
        if isinstance(raw_deployment_identity, Mapping)
        else ({} if "dpone_deployment_identity" in values else None)
    )
    raw_digest = values.get("dpone_runtime_evidence_sha256")
    digest = explicit_evidence_digest
    if digest is None and "dpone_runtime_evidence_sha256" in values:
        digest = raw_digest if isinstance(raw_digest, str) else ""
    return identity, deployment_identity, digest


__all__ = [
    "build_pack_outcome_task",
    "evaluate_inline_pack_outcome",
    "evaluate_operator_inline_outcome",
    "evaluate_pack_outcome",
    "pull_deferrable_xcom",
]
