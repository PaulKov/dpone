"""Exact fixture assertions and receipt construction, outside the delivery clock."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from .artifacts import ArtifactStore, canonical_json, digest
from .execution import SHA, DeliveryClock, RouteFactory, Snapshot
from .maintenance import record_owner
from .profiles import Dataset, capture_rows, exact_multiset, multiset_summary

SAMPLE_CHECKS = (
    "typed_content",
    "duplicate_multiplicity",
    "metadata_parity",
    "commit_receipt_binding",
    "outside_window_unchanged",
)
RECOVERY_CHECKS = ("empty_input", "rollback", "receipt_first_recovery", "source_free_resume")


def aggregate(statuses: Iterable[str]) -> str:
    values = set(statuses)
    if "FAIL" in values:
        return "FAIL"
    if values <= {"PASS", "N/A"} and values:
        return "PASS"
    return "UNVERIFIED"


def check(
    name: str,
    expected: Any,
    observed: Any,
    *,
    method: str = "transaction_fixture",
    status: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    outcome = status or ("PASS" if canonical_json(expected) == canonical_json(observed) else "FAIL")
    return {
        "id": name,
        "status": outcome,
        "method": method,
        "reason": reason if outcome == "PASS" else reason or "assertion_mismatch",
        "expected": expected,
        "observed": observed,
        "evidence": None,
    }


def _hash_check(name: str, expected: str | None, observed: str | None) -> dict[str, Any]:
    if not all(isinstance(value, str) and SHA.fullmatch(value) for value in (expected, observed)):
        return check(name, None, None, status="UNVERIFIED", reason="authoritative_binding_unavailable")
    return check(
        name,
        expected,
        observed,
        method="live_observation" if name == "commit_receipt_binding" else "transaction_fixture",
    )


def snapshot_checks(
    dataset: Dataset,
    before_outside: Iterable[Mapping[str, object]],
    after: Snapshot,
    strategy: str,
    *,
    require_pipeline_complete: bool = True,
) -> list[dict[str, Any]]:
    expected = exact_multiset(dataset.generate())
    observed = exact_multiset(after.rows)
    checks = [
        check(
            name,
            multiset_summary(expected),
            multiset_summary(observed),
            method="exact_typed_multiset",
            status="PASS" if expected == observed else "FAIL",
        )
        for name in ("typed_content", "duplicate_multiplicity")
    ]
    checks.extend(
        [
            _hash_check("metadata_parity", after.metadata_expected, after.metadata_observed),
            _hash_check("commit_receipt_binding", after.receipt_expected, after.receipt_observed),
        ]
    )
    expected_state = {"source_queries": 1, "publications": 1, "commit_known": True}
    observed_state = {key: getattr(after, key) for key in expected_state}
    if require_pipeline_complete:
        expected_state["pipeline_complete"] = True
        observed_state["pipeline_complete"] = after.pipeline_complete
    if canonical_json(expected_state) != canonical_json(observed_state):
        checks[3] = check(
            "commit_receipt_binding",
            expected_state,
            observed_state,
            method="live_observation",
            reason="invocation_or_pipeline_unconfirmed",
        )
    if strategy == "partition_replace":
        prior, current = exact_multiset(before_outside), exact_multiset(after.outside_rows)
        checks.append(
            check(
                "outside_window_unchanged",
                multiset_summary(prior),
                multiset_summary(current),
                method="exact_typed_multiset",
                status="PASS" if prior == current else "FAIL",
            )
        )
    else:
        checks.append(
            check(
                "outside_window_unchanged",
                None,
                None,
                method="not_applicable",
                status="N/A",
                reason="full_refresh_has_no_outside_window",
            )
        )
    return checks


def receipt(
    store: ArtifactStore,
    envelope: dict[str, Any],
    *,
    sample_id: str,
    scope: str,
    fixture: Dataset,
    checks: list[dict[str, Any]],
    execution: str,
) -> dict[str, str]:
    """Persist independent observation bytes before the bound correctness receipt."""
    status = aggregate(item["status"] for item in checks)
    identity = {
        "schema_version": 1,
        "subject_commit": envelope["subject"]["commit"],
        "workload_sha256": envelope["workload"]["sha256"],
        "configuration_sha256": envelope["configuration"]["sha256"],
        "environment_sha256": envelope["environment"]["sha256"],
        "sample_id": sample_id,
        "route": envelope["route"],
        "execution": execution,
        "scope": scope,
        "fixture": {
            "id": f"{fixture.profile}:{sample_id}",
            "rows": fixture.rows,
            "sha256": digest(fixture.description()),
        },
    }
    evidence = store.write(
        f"{sample_id}-observation",
        {
            **identity,
            "kind": "native-delivery-live-observation",
            "status": status,
            "checks": checks,
        },
    )
    return store.write(
        f"{sample_id}-correctness",
        {
            **identity,
            "kind": "native-delivery-correctness",
            "checks": [{**item, "evidence": evidence} for item in checks],
            "status": status,
        },
    )


def _capture(snapshot: Snapshot) -> Snapshot:
    """Materialize driver iterators once before checking the same boundary twice."""
    return replace(snapshot, rows=capture_rows(snapshot.rows), outside_rows=capture_rows(snapshot.outside_rows))


def _bindings_unchanged(before: Snapshot, after: Snapshot) -> bool:
    return all(
        getattr(before, name) == getattr(after, name)
        for name in ("metadata_expected", "metadata_observed", "receipt_expected", "receipt_observed")
    )


def failure_recovery(
    factory: RouteFactory, dataset: Dataset, strategy: str, store: ArtifactStore | None = None
) -> list[dict[str, Any]]:
    """Exercise real injected failure boundaries; never perform target mutations here.

    Unknown outcome deliberately retains objects. The factory's recover(False)
    must poison source acquisition, not merely accept a Boolean hint.
    """
    outcomes: dict[str, dict[str, Any]] = {}
    for case in ("empty_input", "rollback", "source_free_resume", "receipt_first_recovery", "unknown_commit"):
        fixture = Dataset(dataset.profile, rows=0 if case == "empty_input" else dataset.rows, seed=dataset.seed)
        session = factory.open(fixture, case=case, clock=DeliveryClock())
        known = False
        try:
            if store is not None:
                record_owner(store, session, case)
            before = _capture(session.snapshot())
            before_rows = exact_multiset(before.rows)
            before_outside = capture_rows(before.outside_rows)
            fault = {
                "rollback": "before_commit",
                "source_free_resume": "after_eof",
                "receipt_first_recovery": "lost_ack",
                "unknown_commit": "unknown_commit",
            }.get(case)
            if fault:
                session.arm_fault(fault)
            failed = False
            try:
                session.run()
            except Exception:
                failed = True  # Never persist exception text from connectors.
            initial = _capture(session.snapshot())
            initial_rows = exact_multiset(initial.rows)
            initial_outside = capture_rows(initial.outside_rows)
            checks: list[dict[str, Any]] = []
            if case in {"source_free_resume", "receipt_first_recovery", "unknown_commit"}:
                recover_failed = False
                try:
                    session.recover(source_allowed=False)
                except Exception:
                    recover_failed = True
                after = _capture(session.snapshot())
                if case == "unknown_commit":
                    old_state = initial_rows == before_rows and initial.publications == 0
                    new_state = initial_rows == exact_multiset(fixture.generate()) and initial.publications == 1
                    checks.append(
                        _hash_check(
                            "metadata_parity",
                            before.metadata_observed if old_state else initial.metadata_expected,
                            initial.metadata_observed,
                        )
                    )
                    # This negative fixture must permit deliberately unavailable
                    # receipt authority, while rejecting a present wrong receipt.
                    if initial.receipt_observed is not None:
                        checks.append(
                            _hash_check("commit_receipt_binding", initial.receipt_expected, initial.receipt_observed)
                        )
                    ok = (
                        failed
                        and recover_failed
                        and (old_state or new_state)
                        and not initial.commit_known
                        and not after.commit_known
                        and not initial.pipeline_complete
                        and not after.pipeline_complete
                        and initial.publications == after.publications
                        and initial.source_queries == after.source_queries
                        and initial_rows == exact_multiset(after.rows)
                        and exact_multiset(initial_outside) == exact_multiset(after.outside_rows)
                        and (
                            (new_state and strategy != "partition_replace")
                            or exact_multiset(initial_outside) == exact_multiset(before_outside)
                        )
                        and initial.stage_reads == after.stage_reads
                        and _bindings_unchanged(initial, after)
                    )
                else:
                    checks = snapshot_checks(fixture, before_outside, after, strategy)
                    ok = not recover_failed and initial.source_queries == after.source_queries
                    if case == "source_free_resume":
                        checks.append(
                            _hash_check("metadata_parity", before.metadata_observed, initial.metadata_observed)
                        )
                        ok &= (
                            failed
                            and initial.commit_known
                            and not initial.pipeline_complete
                            and initial_rows == before_rows
                            and exact_multiset(initial_outside) == exact_multiset(before_outside)
                            and initial.receipt_observed == before.receipt_observed
                            and initial.publications == 0
                            and after.publications == 1
                        )
                    else:
                        checks.extend(
                            snapshot_checks(fixture, before_outside, initial, strategy, require_pipeline_complete=False)
                        )
                        ok &= (
                            initial.commit_known
                            and initial.publications == after.publications == 1
                            and initial_rows == exact_multiset(after.rows)
                            and exact_multiset(initial_outside) == exact_multiset(after.outside_rows)
                            and initial.stage_reads == after.stage_reads
                            and _bindings_unchanged(initial, after)
                        )
            elif case == "rollback":
                after = initial
                checks.append(_hash_check("metadata_parity", before.metadata_observed, initial.metadata_observed))
                ok = (
                    failed
                    and initial.commit_known
                    and not initial.pipeline_complete
                    and initial_rows == before_rows
                    and exact_multiset(initial_outside) == exact_multiset(before_outside)
                    and initial.receipt_observed == before.receipt_observed
                    and initial.publications == before.publications
                )
            else:
                after = initial
                checks = snapshot_checks(fixture, before_outside, initial, strategy)
                ok = not failed
            known = after.commit_known
            ok &= before.source_queries == before.publications == 0 and not before.pipeline_complete
            ok &= initial.source_queries == 1
            if fault is not None:
                ok &= fault not in before.fault_events and fault in initial.fault_events
            if case == "receipt_first_recovery":
                ok &= initial.receipt_probes > before.receipt_probes
            status = aggregate(["PASS" if ok else "FAIL", *(item["status"] for item in checks)])
            outcomes[case] = check(
                case,
                True,
                bool(ok),
                status=status,
                reason="authoritative_binding_unavailable" if status == "UNVERIFIED" else None,
            )
        except Exception:
            outcomes[case] = check(case, True, False, reason="recovery_fixture_failed")
        finally:
            try:
                if known:
                    session.cleanup()
            except Exception:
                outcomes[case] = check(case, True, False, reason="owned_cleanup_failed")
            finally:
                session.close()
    known_check, unknown_check = outcomes["receipt_first_recovery"], outcomes["unknown_commit"]
    recovery_status = aggregate([known_check["status"], unknown_check["status"]])
    outcomes["receipt_first_recovery"] = check(
        "receipt_first_recovery",
        {"known_commit": True, "unknown_blocks_replay": True},
        {"known_commit": known_check["status"] == "PASS", "unknown_blocks_replay": unknown_check["status"] == "PASS"},
        status=recovery_status,
        reason="authoritative_binding_unavailable" if recovery_status == "UNVERIFIED" else None,
    )
    return [outcomes[name] for name in RECOVERY_CHECKS]
