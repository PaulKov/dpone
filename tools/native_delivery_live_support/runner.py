"""Real-row experiment orchestration, retaining every trial and honest absence."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from .artifacts import ArtifactStore, digest
from .correctness import RECOVERY_CHECKS, SAMPLE_CHECKS, aggregate, check, failure_recovery, receipt, snapshot_checks
from .execution import DeliveryClock, ExecutionAdapter, environment_record, git_identity, measured, unavailable
from .maintenance import record_owner
from .profiles import Dataset
from .resources import ProcessTreeRss


def configuration(limits: dict[str, Any]) -> dict[str, Any]:
    """Reuse canonical limit validation; missing or surplus fields are errors."""
    from dpone.contracts.mssql_native_chunks import NativeChunkLimits

    if set(limits) != set(NativeChunkLimits.__dataclass_fields__):
        raise ValueError("exact_limits_required")
    resolved = asdict(NativeChunkLimits(**limits))
    return {"limits": resolved, "sha256": digest(resolved)}


def route_record(strategy: str, mode: str) -> dict[str, str]:
    if strategy not in {"full_refresh", "partition_replace"} or mode not in {"bounded_native", "isolated_switch"}:
        raise ValueError("invalid_route")
    if mode == "isolated_switch" and strategy != "partition_replace":
        raise ValueError("switch_requires_partition_replace")
    return {"source": "clickhouse", "sink": "mssql", "strategy": strategy, "mode": mode}


def _envelope(
    dataset: Dataset, config: dict[str, Any], route: dict[str, str], subject: dict[str, Any]
) -> dict[str, Any]:
    producer = git_identity(Path(__file__).resolve().parents[2])
    return {
        "schema_version": 1,
        "kind": "native-delivery-run",
        "producer": {"name": "dpone-native-delivery-live", "version": "1", **producer},
        "subject": subject,
        "route": route,
        "workload": dataset.envelope(),
        "configuration": config,
        "environment": {},
        "samples": [],
        "fidelity_receipt": None,
        "recovery_receipt": None,
        "status": "UNVERIFIED",
        "limitations": [],
    }


def absent_run(
    store: ArtifactStore,
    dataset: Dataset,
    config: dict[str, Any],
    route: dict[str, str],
    subject: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    """Write structurally complete absence without importing any route factory."""
    envelope = _envelope(dataset, config, route, subject)
    environment = {"versions": {}, "target_layout_sha256": digest({"unavailable": True}), "resource_profile": {}}
    envelope["environment"] = {**environment, "sha256": digest(environment)}
    for scope, names in (
        ("type_fidelity", SAMPLE_CHECKS),
        ("failure_recovery", RECOVERY_CHECKS),
        ("sample", SAMPLE_CHECKS),
    ):
        checks = [check(name, None, None, status="UNVERIFIED", reason=reason) for name in names]
        if route["strategy"] == "full_refresh" and scope != "failure_recovery":
            checks[-1] = check(
                "outside_window_unchanged",
                None,
                None,
                status="N/A",
                reason="full_refresh_has_no_outside_window",
                method="not_applicable",
            )
        ref = receipt(
            store,
            envelope,
            sample_id=scope.replace("_", "-"),
            scope=scope,
            fixture=dataset,
            checks=checks,
            execution="hermetic",
        )
        if scope == "sample":
            envelope["samples"].append(
                {
                    "id": "sample",
                    "is_warmup": False,
                    "status": "SKIP",
                    "reason": reason,
                    "visibility_seconds": unavailable("seconds", reason),
                    "pipeline_seconds": unavailable("seconds", reason),
                    "correctness": ref,
                    "observations": None,
                    "metrics": {},
                }
            )
        else:
            envelope["fidelity_receipt" if scope == "type_fidelity" else "recovery_receipt"] = ref
    envelope["status"] = "SKIP"
    envelope["limitations"] = [reason, "No live execution; no performance or certification claim."]
    store.publish(envelope)
    return envelope


def _trial(
    adapter: ExecutionAdapter,
    envelope: dict[str, Any],
    dataset: Dataset,
    store: ArtifactStore,
    sample_id: str,
    *,
    scope: str,
    warmup: bool,
) -> dict[str, Any]:
    clock = DeliveryClock()
    checks = [check(name, None, None, status="UNVERIFIED", reason="trial_not_completed") for name in SAMPLE_CHECKS]
    if envelope["route"]["strategy"] == "full_refresh":
        checks[-1] = check(
            "outside_window_unchanged",
            None,
            None,
            status="N/A",
            reason="full_refresh_has_no_outside_window",
            method="not_applicable",
        )
    visibility, pipeline = unavailable("seconds", "trial_not_completed"), unavailable("seconds", "trial_not_completed")
    metrics: dict[str, Any] = {}
    status, reason, known = "FAIL", "trial_failed", False
    session = None
    try:
        if adapter.subject() != envelope["subject"] or environment_record(adapter.factory) != envelope["environment"]:
            raise ValueError("trial_identity_drift")
        session = adapter.factory.open(dataset, case=sample_id, clock=clock)
        record_owner(store, session, sample_id)
        before_outside = tuple(session.snapshot().outside_rows)
        with ProcessTreeRss() as rss:
            session.run()
            visibility, pipeline = clock.finish()
        metrics["process_set_rss"] = rss.metric()
        after = session.snapshot()
        known = after.commit_known
        checks = snapshot_checks(dataset, before_outside, after, envelope["route"]["strategy"])
        status = aggregate(item["status"] for item in checks)
        reason = None if status == "PASS" else "correctness_not_passed"
        if status == "PASS" and visibility["availability"] != "measured":
            status, reason = "UNVERIFIED", "missing_clock_boundary"
        metrics["rows"] = measured(checks[0]["observed"]["rows"], "rows", "exact_target_multiset")
        metrics["encoded_bytes"] = (
            measured(after.encoded_bytes, "bytes", "retained_native_file_receipts")
            if after.encoded_bytes is not None
            else unavailable("bytes", "native_byte_observation_unavailable")
        )
        for name in ("sql_allocated_bytes", "sql_log_bytes", "sql_wait_seconds"):
            value = after.sql_metrics.get(name)
            unit = "seconds" if name.endswith("seconds") else "bytes"
            metrics[name] = (
                unavailable(unit, "sql_observation_unavailable")
                if value is None
                else measured(value, unit, "target_session_observation")
            )
        if adapter.subject() != envelope["subject"] or environment_record(adapter.factory) != envelope["environment"]:
            status, reason = "FAIL", "trial_identity_drift"
    except Exception:
        status, reason = "FAIL", "trial_execution_failed"
    finally:
        if session is not None:
            try:
                if known:
                    session.cleanup()
            except Exception:
                status, reason = "FAIL", "owned_cleanup_failed"
            try:
                session.close()
            except Exception:
                status, reason = "FAIL", "session_close_failed"
    reference = receipt(
        store,
        envelope,
        sample_id=sample_id,
        scope=scope,
        fixture=dataset,
        checks=checks,
        execution=adapter.factory.execution,
    )
    return {
        "id": sample_id,
        "is_warmup": warmup,
        "status": status,
        "reason": reason,
        "visibility_seconds": visibility,
        "pipeline_seconds": pipeline,
        "correctness": reference,
        "observations": None,
        "metrics": metrics,
    }


def run_benchmark(
    *,
    adapter: ExecutionAdapter,
    dataset: Dataset,
    config: dict[str, Any],
    route: dict[str, str],
    store: ArtifactStore,
    trials: int = 3,
) -> dict[str, Any]:
    """Run exact profile proofs, one warmup and declared trials in recorded order.

    No percentiles or speed ratios are calculated here. DDA-01 consumes the
    immutable envelopes; hermetic receipts never supply live comparison authority.
    """
    if type(trials) is not int or not 3 <= trials <= 100:
        raise ValueError("at_least_three_trials_required")
    store.preflight()
    envelope = _envelope(
        dataset, configuration(config["limits"]), route_record(route["strategy"], route["mode"]), adapter.subject()
    )
    envelope["environment"] = environment_record(adapter.factory)
    fixture = Dataset(dataset.profile, rows=32, seed=dataset.seed)
    fidelity = _trial(adapter, envelope, fixture, store, "type-fidelity", scope="type_fidelity", warmup=False)
    envelope["fidelity_receipt"] = fidelity["correctness"]
    try:
        recovery = failure_recovery(adapter.factory, fixture, route["strategy"], store)
    except Exception:
        recovery = [
            check(name, None, None, status="FAIL", reason="recovery_fixture_failed") for name in RECOVERY_CHECKS
        ]
    envelope["recovery_receipt"] = receipt(
        store,
        envelope,
        sample_id="failure-recovery",
        scope="failure_recovery",
        fixture=fixture,
        checks=recovery,
        execution=adapter.factory.execution,
    )
    # Fidelity failures prevent larger throughput experiments; no false timing.
    if fidelity["status"] == "PASS" and envelope["recovery_receipt"]["status"] == "PASS":
        for index in range(trials + 1):
            envelope["samples"].append(
                _trial(adapter, envelope, dataset, store, f"trial-{index:03d}", scope="sample", warmup=index == 0)
            )
    statuses = [fidelity["status"], envelope["recovery_receipt"]["status"], *(s["status"] for s in envelope["samples"])]
    envelope["status"] = aggregate(statuses)
    envelope["limitations"] = [
        "Single declared workload/configuration; no campaign or p95 claim.",
        "Exact target readback is outside timing and uses memory proportional to unique rows.",
        "RSS is sampled coordinator/descendants; external SQL servers and between-sample peaks are excluded.",
        "Observations sidecar unavailable until the optional observer is supplied by integration.",
    ]
    if adapter.factory.execution != "live" or envelope["subject"]["dirty"] or envelope["producer"]["dirty"]:
        if envelope["status"] == "PASS":
            envelope["status"] = "UNVERIFIED"
        envelope["limitations"].append("Hermetic execution or dirty code cannot authorize performance certification.")
    if route["mode"] == "isolated_switch":
        envelope["limitations"].append("Isolated SWITCH component only; public native SWITCH remains rejected.")
    store.publish(envelope)
    return envelope
