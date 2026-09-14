#!/usr/bin/env python3
"""Offline preparation and explicit native runtime composition for a synthetic window.

Run from a development checkout with ``uv run python
examples/native/raw-window-runtime.py --work-dir /tmp/dpone-raw-example``.
Without --live, the command checks local SQLite authority only. Explicit live
scenarios compose the existing owned disposable ClickHouse/SQL Server fixture.
Deployment code imports ``compose_runtime`` and supplies all real authority ports.
See docs/delivery-acceleration/raw-window-composition.md before executing a route.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.bounded_window import WindowContractError, WindowLease, WindowLeaseLost
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.manifest.mssql_native_policy import native_source_read_mode, native_window, validate_native_config
from dpone.ports.bounded_window import WindowStore
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings


def synthetic_config() -> LoadConfig:
    """Freeze one authored UTC day; connection IDs are synthetic unresolved names."""
    return LoadConfig(
        source_conn_id="synthetic_source",
        target_conn_id="synthetic_target",
        source_schema="synthetic",
        source_table="events",
        target_database="synthetic",
        target_schema="dbo",
        target_table="events",
        staging_database="synthetic",
        staging_schema="dbo",
        load_strategy=LoadStrategy("partition_replace"),
        log_sample_rows=0,
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "mssql_native_window": {
                "column": "observed_at",
                "anchor": "data_interval_end",
                "lookback": "P1D",
            },
            "interval": {"interval_end": "2026-01-02T00:00:00+00:00"},
            "native_transfer": {
                "source_read": {"mode": "raw_single_query"},
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 1},
                    "native_chunks": {
                        "max_rows": 16,
                        "max_bytes": 1048576,
                        "max_row_bytes": 65536,
                        "max_pending": 1,
                        "max_staging_tables": 512,
                        "max_total_encoded_bytes": 104857600,
                        "stage_allocated_bytes_stop_threshold": 268435456,
                    },
                },
            },
        },
    )


def bind_plan(config: LoadConfig, **identity: str) -> NativeChunkPlan:
    """Bind policy to actual persisted identity supplied by target-only admission.

    Required identity fields: run_id, target_id, source_query_id, window_fingerprint,
    schema_fingerprint and wire_fingerprint. Never invent new identity during recovery.
    """
    validate_native_config(config)
    return NativeChunkPlan(**identity, source_read_mode=native_source_read_mode(config))


def compose_runtime(
    config: LoadConfig,
    *,
    store: WindowStore,
    target_id: str,
    bindings: Callable[..., NativeRuntimeBindings],
    source: Callable[[Any, NativeRuntimeBindings], AbstractContextManager[Any]],
    quality: Callable[[Any, Any, WindowLease], None],
    evidence: Callable[[Any, Any, Any, WindowLease], None],
    advance_state: Callable[[Any, Any, WindowLease], None],
) -> NativeMssqlRuntime:
    """Construct the real runtime; no authority callback has a permissive default.

    ``bindings`` must restore the existing plan using ``bind_plan``, target receipt
    authority, stage ownership and schema exclusion. ``source`` owns one physical
    ClickHouse session and real source DDL exclusion. The remaining callbacks
    verify quality and durably fence evidence/checkpoint writes, respectively.
    ``store`` alone does not exclude external SQL or ClickHouse writers.
    """
    validate_native_config(config)
    return NativeMssqlRuntime(
        store=store,
        target_id=target_id,
        bindings=bindings,
        source=source,
        preflight=validate_native_config,
        quality=quality,
        evidence=evidence,
        advance_state=advance_state,
    )


def run_window(config: LoadConfig, *, owner: str, **adapters: Any) -> Any:
    """Execute or recover the same persisted invocation through supplied authorities.

    Recovery uses the same API and identity; the runtime probes target authority
    before opening ``source``. Call only in an explicitly authorized environment.
    """
    return compose_runtime(config, **adapters).run(config, owner=owner)


def prepare_offline(work_dir: Path) -> dict[str, Any]:
    """Exercise actual local lease exclusion/CAS without manufacturing route evidence."""
    config = synthetic_config()
    validate_native_config(config)
    store = SQLiteWindowStore(work_dir / "example-authority.sqlite3", clock=time.time)
    lease = store.acquire("synthetic-offline-target", "example", 60)
    try:
        try:
            store.acquire(lease.target_id, "competing-example", 60)
        except WindowLeaseLost:
            pass
        else:
            raise RuntimeError("example.local_lease_exclusion_failed")
        key = "example/offline-preparation"
        previous = store.load(key)
        record = store.save(key, previous.revision if previous else None, "offline-only", lease)
        try:
            store.save(key, None, "must-not-replace", lease)
        except WindowContractError:
            pass
        else:
            raise RuntimeError("example.local_cas_failed")
        if store.load(key) != record:
            raise RuntimeError("example.local_record_changed")
    finally:
        store.release(lease)
    window = native_window(config)
    return {
        "status": "composition_required",
        "live_preflight": "not_run",
        "certification_status": "unverified",
        "source_read_mode": native_source_read_mode(config),
        "window": window.to_dict(),
        "local_lease_exclusion": "PASS",
        "local_cas": "PASS",
        "source_queries": 0,
        "target_publications": 0,
    }


def require_raw_session(session: Any) -> None:
    """Reject mismatched configuration/plan before run or same-invocation attach."""
    if (
        native_source_read_mode(session.config) != "raw_single_query"
        or session.plan.source_read_mode != "raw_single_query"
    ):
        raise ValueError("example.raw_binding_required")


def expect_fault(session: Any, fault: str) -> None:
    """Accept only the requested controlled failure, never an arbitrary driver error."""
    session.arm_fault(fault)
    try:
        session.run()
    except RuntimeError as error:
        if str(error) != "local_fixture.injected:" + fault:
            raise
    else:
        raise RuntimeError("example.expected_fault_missing")


def recover_completed(factory: Any, session: Any) -> Any:
    """Reconstruct a fresh real Session after durable EOF and poison source access."""
    expect_fault(session, "after_eof")
    journal = session.journal_data()
    if not journal or journal.get("phase") != "stage_complete" or not journal.get("completion_metadata"):
        raise RuntimeError("example.stage_complete_required")
    invocation = session.invocation_id
    session.close()
    attached = factory.attach(invocation)
    require_raw_session(attached)
    attached.recover(source_allowed=False)
    return attached


def replace_incomplete(factory: Any, session: Any, dataset: Any) -> Any:
    """Verify partial failure, clean proven ownership, and admit a new full extraction."""
    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.profiles import exact_multiset

    before = session.snapshot()
    expect_fault(session, "during_source")
    journal = session.journal_data()
    after = session.snapshot()
    if (journal and journal.get("complete")) or after.publications or not after.commit_known:
        raise RuntimeError("example.incomplete_state_required")
    if exact_multiset(before.rows) != exact_multiset(after.rows):
        raise RuntimeError("example.partial_failure_changed_target")
    if exact_multiset(before.outside_rows) != exact_multiset(after.outside_rows):
        raise RuntimeError("example.partial_failure_changed_outside_window")
    old_invocation = session.invocation_id
    session.cleanup()
    session.close()
    replacement = factory.open(dataset, case="new-after-incomplete", clock=DeliveryClock())
    if replacement.invocation_id == old_invocation:
        raise RuntimeError("example.new_invocation_required")
    require_raw_session(replacement)
    return replacement


def run_local_example(work_dir: Path, *, scenario: str) -> dict[str, Any]:
    """Execute actual disposable fixture adapters; emitted checks are not certification.

    The live environment and its credentials must already be explicitly approved
    and provisioned as described in the local Docker runbook. Exceptions retain
    unresolved owned resources and write only a sanitized error class.
    """
    approvals = ("DPONE_RUN_INTEGRATION", "DPONE_RUN_INTEGRATION_LIVE", "DPONE_DDA_DISPOSABLE_APPROVED")
    if any(os.environ.get(key) != "1" for key in approvals):
        raise ValueError("example.local_approval_required")
    if scenario not in {"first-load", "completed-stage", "incomplete-stage"}:
        raise ValueError("example.invalid_scenario")
    # Repository tools are deliberately local fixture dependencies, not installed
    # runtime plugin discovery. Lazy loading keeps offline help network-free.
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.append(str(root))
    from tools.native_delivery_live_support.correctness import snapshot_checks
    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.profiles import Dataset
    from tools.native_delivery_live_support.runner import configuration, route_record
    from tools.native_delivery_local.environment import Environment
    from tools.native_delivery_local.factory import LocalRouteFactory

    work_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    report_path = work_dir / "scenario.json"
    report = {"scenario": scenario, "status": "FAIL", "certification_status": "unverified", "invocations": []}
    # Refuse overwrite before any environment access or database provisioning.
    with report_path.open("x", encoding="utf-8") as output:
        session = None
        try:
            config = synthetic_config()
            execution = config.options["native_transfer"]["execution"]
            limits = {**execution["native_chunks"], "parallelism": execution["chunking"]["parallelism"]}
            factory = LocalRouteFactory(
                configuration=configuration(limits),
                route=route_record("partition_replace", "bounded_native"),
                environment=Environment(root=work_dir / "invocations"),
                source_read_mode="raw_single_query",
            )
            dataset = Dataset("unicode", 32)
            session = factory.open(dataset, case=scenario, clock=DeliveryClock())
            require_raw_session(session)
            report["invocations"].append(session.invocation_id)
            outside = tuple(session.snapshot().outside_rows)
            if scenario == "completed-stage":
                session = recover_completed(factory, session)
            else:
                if scenario == "incomplete-stage":
                    session = replace_incomplete(factory, session, dataset)
                    report["invocations"].append(session.invocation_id)
                    outside = tuple(session.snapshot().outside_rows)
                session.run()
            observed = session.snapshot()
            checks = snapshot_checks(dataset, outside, observed, "partition_replace")
            report.update(checks=checks, source_queries=observed.source_queries, publications=observed.publications)
            if any(item["status"] != "PASS" for item in checks):
                raise RuntimeError("example.live_observation_failed")
            session.cleanup()
            report.update(status="PASS", owned_fixture_cleanup="PASS")
        except BaseException as error:
            report["error_type"] = type(error).__name__
            report["owned_fixture_cleanup"] = "UNVERIFIED"
            raise
        finally:
            if session is not None:
                session.close()
            output.write(json.dumps(report, sort_keys=True) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True, help="Durable private local artifact directory")
    parser.add_argument("--live", action="store_true", help="Run real adapters in the approved disposable environment")
    parser.add_argument(
        "--scenario", choices=("first-load", "completed-stage", "incomplete-stage"), default="first-load"
    )
    args = parser.parse_args()
    try:
        report = (
            run_local_example(args.work_dir, scenario=args.scenario) if args.live else prepare_offline(args.work_dir)
        )
    except Exception as error:
        print(json.dumps({"status": "FAIL", "error_type": type(error).__name__, "certification_status": "unverified"}))
        raise SystemExit(1) from None
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
