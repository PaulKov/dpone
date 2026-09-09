"""Strict real-vendor PostgreSQL→MSSQL artifact-integrity certification.

Each parametrized node is one exact reviewed ``artifact_integrity_faults``
case.  PostgreSQL produces the payload, SQL Server performs staging and target
work, and the generic governance catalog records (or rejects) the attempt.
Only the requested filesystem boundary is faulted by the test harness.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import (
    ObservedImageDigest,
    RouteLiveObservationRecorder,
)
from tools.route_live_certification.reviewed_case import ReviewedCase
from tools.route_live_certification.reviewed_cases_runtime import artifact_integrity_suite

from tests.integration.postgres.postgres_live_support import (
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_artifact_integrity_live_support import (
    BASE_ROWS,
    CHANGED_ROWS,
    ArtifactFaultProbe,
    ArtifactGovernedRunner,
    QuietArtifactLogger,
    drop_target,
    ensure_source,
    load_config,
    mutate_after_real_bcp,
    owned_files,
    receipt_count,
    require_artifact_released,
    require_integrity_code,
    rows_sha256,
    set_source_rows,
    staging_count,
    target_rows,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedMssqlRoute,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_CASES = artifact_integrity_suite().cases
_EXPECTED_INTEGRITY_CODES = {
    "append_after_verification": "artifact_integrity.byte_count_mismatch",
    "digest_mismatch": "artifact_integrity.sha256_mismatch",
    "inode_replacement": "artifact_integrity.file_identity_mismatch",
    "missing_payload": "artifact_integrity.file_unavailable",
    "scope_escape": "artifact_integrity.owned_scope_mismatch",
    "size_mismatch": "artifact_integrity.byte_count_mismatch",
    "symlink_alias": "artifact_integrity.regular_file_required",
    "truncate_after_verification": "artifact_integrity.byte_count_mismatch",
}


@dataclass(frozen=True, slots=True)
class _CaseObservation:
    before: list[dict[str, Any]]
    after: list[dict[str, Any]]
    details: dict[str, Any]


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
@pytest.mark.parametrize("reviewed_case", _CASES, ids=lambda value: value.case_id)
def test_postgres_mssql_artifact_integrity_faults_live(
    reviewed_case: ReviewedCase,
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute and record one exact reviewed artifact-integrity boundary."""

    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_source(postgres)
    config = load_config(
        reviewed_case.case_id,
        target_database=governed_mssql_live_campaign.target_database,
        owned_root=tmp_path / "owned",
    )
    route = governed_mssql_live_campaign.route(
        target_schema=config.target_schema,
        target_table=config.target_table,
    )
    probes: list[ArtifactFaultProbe] = []
    drop_target(route.target, config)
    try:
        observation = _execute_case(
            reviewed_case.case_id,
            postgres=postgres,
            route=route,
            config=config,
            owned_root=tmp_path / "owned",
            probes=probes,
        )
        route_live_recorder.observe_case_digests(
            "artifact_integrity_faults",
            reviewed_case.case_id,
            before_image=ObservedImageDigest(rows_sha256(observation.before)),
            after_image=ObservedImageDigest(rows_sha256(observation.after)),
            observations=observation.details,
        )
    finally:
        for probe in probes:
            probe.cleanup_injected_paths()
        drop_target(route.target, config)
        postgres.close()


def _execute_case(
    case_id: str,
    *,
    postgres: Any,
    route: GovernedMssqlRoute,
    config: Any,
    owned_root: Path,
    probes: list[ArtifactFaultProbe],
) -> _CaseObservation:
    if case_id in {"valid_exact_payload", "post_consume_cleanup"}:
        return _run_success(case_id, postgres, route, config, owned_root, probes)
    if case_id == "retry_identical_bytes":
        return _run_identical_replay(postgres, route, config, owned_root, probes)
    if case_id == "retry_changed_bytes":
        return _run_changed_concurrent_replay(postgres, route, config, owned_root, probes)
    expected_code = _EXPECTED_INTEGRITY_CODES.get(case_id)
    if expected_code is None:
        raise AssertionError(f"unimplemented reviewed artifact case: {case_id}")
    return _run_integrity_reject(
        case_id,
        expected_code,
        postgres=postgres,
        route=route,
        config=config,
        owned_root=owned_root,
        probes=probes,
    )


def _probe(
    probes: list[ArtifactFaultProbe],
    *,
    fault: str | None,
    owned_root: Path,
    pause_after_extract: bool = False,
) -> ArtifactFaultProbe:
    value = ArtifactFaultProbe(
        fault=fault,
        owned_root=owned_root,
        pause_after_extract=pause_after_extract,
    )
    probes.append(value)
    return value


def _runner(route: GovernedMssqlRoute, postgres: Any, probe: ArtifactFaultProbe) -> ArtifactGovernedRunner:
    return ArtifactGovernedRunner(
        route,
        postgres,
        probe,
        logger=QuietArtifactLogger(),
    )


def _run_success(
    case_id: str,
    postgres: Any,
    route: GovernedMssqlRoute,
    config: Any,
    owned_root: Path,
    probes: list[ArtifactFaultProbe],
) -> _CaseObservation:
    set_source_rows(postgres, BASE_ROWS)
    before = target_rows(route.target, config)
    before_receipts = receipt_count(route)
    probe = _probe(probes, fault=None, owned_root=owned_root)
    result = _runner(route, postgres, probe).run(config, label=case_id)
    after = target_rows(route.target, config)
    assert before == []
    assert after == [{"id": 1, "value": "stable-one"}, {"id": 2, "value": "stable-two"}]
    assert probe.extraction_count == 1
    require_artifact_released(probe)
    assert owned_files(owned_root) == ()
    assert staging_count(route.target, config) == 0
    assert receipt_count(route) == before_receipts + 1
    assert result["artifact_terminal"]["cleanup_succeeded"] is True
    return _CaseObservation(
        before,
        after,
        {
            "boundary": "target_commit_then_source_terminal",
            "source_extracts": probe.extraction_count,
            "staging_objects_after": 0,
            "owned_files_after": [],
            "receipt_delta": 1,
        },
    )


def _run_integrity_reject(
    case_id: str,
    expected_code: str,
    *,
    postgres: Any,
    route: GovernedMssqlRoute,
    config: Any,
    owned_root: Path,
    probes: list[ArtifactFaultProbe],
) -> _CaseObservation:
    set_source_rows(postgres, BASE_ROWS)
    baseline_probe = _probe(probes, fault=None, owned_root=owned_root)
    _runner(route, postgres, baseline_probe).run(config, label=f"{case_id}_baseline")
    require_artifact_released(baseline_probe)
    before = target_rows(route.target, config)
    before_receipts = receipt_count(route)

    set_source_rows(postgres, CHANGED_ROWS)
    fault_probe = _probe(probes, fault=case_id, owned_root=owned_root)
    fault_runner = _runner(route, postgres, fault_probe)
    caught: Exception | None = None
    try:
        with mutate_after_real_bcp(route, fault_probe):
            fault_runner.run(config, label=f"{case_id}_fault")
    except Exception as exc:  # noqa: BLE001 - exact typed failure asserted below.
        caught = exc
    if caught is None:
        raise AssertionError(f"{case_id} reached a committed target outcome")
    require_integrity_code(caught, expected_code)

    after = target_rows(route.target, config)
    assert after == before
    assert staging_count(route.target, config) == 0
    assert receipt_count(route) == before_receipts
    assert fault_probe.extraction_count == 1
    require_artifact_released(fault_probe)
    return _CaseObservation(
        before,
        after,
        {
            "boundary": "after_bcp" if case_id.endswith("after_verification") else "artifact_preflight",
            "diagnostic": expected_code,
            "source_extracts": fault_probe.extraction_count,
            "staging_objects_after": 0,
            "receipt_delta": 0,
            "runtime_owned_file_released": True,
        },
    )


def _run_identical_replay(
    postgres: Any,
    route: GovernedMssqlRoute,
    config: Any,
    owned_root: Path,
    probes: list[ArtifactFaultProbe],
) -> _CaseObservation:
    set_source_rows(postgres, BASE_ROWS)
    before = target_rows(route.target, config)
    before_receipts = receipt_count(route)
    probe = _probe(probes, fault=None, owned_root=owned_root)
    runner = _runner(route, postgres, probe)
    context = route.run_context("artifact_retry_identical")

    runner.run(config, label="retry_identical", run_context=context)
    committed = target_rows(route.target, config)
    runner.run(config, label="retry_identical", run_context=context)
    after = target_rows(route.target, config)

    assert before == []
    assert committed == after
    assert probe.extraction_count == 1
    assert receipt_count(route) == before_receipts + 1
    assert staging_count(route.target, config) == 0
    require_artifact_released(probe)
    assert owned_files(owned_root) == ()
    return _CaseObservation(
        before,
        after,
        {
            "boundary": "durable_receipt_pre_source_replay",
            "invocations": 2,
            "source_extracts": 1,
            "second_invocation_source_extracts": 0,
            "staging_objects_after": 0,
            "receipt_delta": 1,
        },
    )


def _run_changed_concurrent_replay(
    postgres: Any,
    route: GovernedMssqlRoute,
    config: Any,
    owned_root: Path,
    probes: list[ArtifactFaultProbe],
) -> _CaseObservation:
    concurrent_postgres = postgres_connector()
    wait_until_ready("postgres", lambda: concurrent_postgres.get_records("SELECT 1"))
    ensure_source(concurrent_postgres)
    context = route.run_context("artifact_retry_changed")
    changed_probe = _probe(
        probes,
        fault=None,
        owned_root=owned_root,
        pause_after_extract=True,
    )
    first_probe = _probe(probes, fault=None, owned_root=owned_root)
    changed_runner = _runner(route, concurrent_postgres, changed_probe)
    first_runner = _runner(route, postgres, first_probe)
    set_source_rows(postgres, CHANGED_ROWS)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                changed_runner.run,
                config,
                label="retry_changed",
                run_context=context,
            )
            changed_probe.wait_until_extracted(future)
            try:
                set_source_rows(postgres, BASE_ROWS)
                first_runner.run(config, label="retry_changed", run_context=context)
                before = target_rows(route.target, config)
                receipts_after_commit = receipt_count(route)
            finally:
                changed_probe.release()

            caught: Exception | None = None
            try:
                future.result(timeout=120)
            except Exception as exc:  # noqa: BLE001 - exact production diagnostic asserted below.
                caught = exc
        if caught is None:
            raise AssertionError("changed pre-admitted payload replay reached a committed outcome")
        assert str(caught) == "mssql_transaction.concurrent_receipt_payload_mismatch"
        after = target_rows(route.target, config)
        assert after == before
        assert receipt_count(route) == receipts_after_commit == 1
        assert staging_count(route.target, config) == 0
        assert first_probe.extraction_count == 1
        assert changed_probe.extraction_count == 1
        require_artifact_released(first_probe)
        require_artifact_released(changed_probe)
        assert owned_files(owned_root) == ()
        return _CaseObservation(
            before,
            after,
            {
                "boundary": "pre_admitted_payload_finalization_after_concurrent_receipt",
                "diagnostic": "mssql_transaction.concurrent_receipt_payload_mismatch",
                "source_extracts": 2,
                "staging_objects_after": 0,
                "receipt_count": 1,
            },
        )
    finally:
        changed_probe.release()
        concurrent_postgres.close()


__all__ = ["test_postgres_mssql_artifact_integrity_faults_live"]
