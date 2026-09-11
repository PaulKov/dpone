"""Classify native outcomes from protected originals and fresh SQL observations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import (
    ARTIFACT_ROLES,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtNativeOutcome,
    DbtOutcomeExpectation,
)
from dpone.contracts.dbt_graph_contract import dbt_graph_contract_sha256, expected_dbt_run_result_ids
from dpone.contracts.strict_json import strict_json_object
from dpone.services.dbt_dev_evidence_contracts import validate_dbt_execution_evidence_contract


class CompositionDbtOutcomeObserver:
    """No caller result/path/status is accepted by observe(attempt).

    The app injects the canonical runtime ``parse_dbt_run_results`` because
    services do not import runtime. Store reads are protected originals; the DB
    observer independently opens fresh control/catalog authority on every call.
    Its original must bind attempt/intent/invocation and the exact expected
    materializations (unique_id, kind, schema digest). Semantic fixture parity
    remains separate campaign evidence, never inferred from successful dbt.
    """

    def __init__(
        self, store: Any, *, result_parser: Callable[..., Any], materialization_observer: Callable[..., bytes]
    ):
        self._store = store
        self._parse_results = result_parser
        self._observe_materializations = materialization_observer
        self._manifest_validator = OfficialDbtManifestValidator()
        self._results_validator = OfficialDbtRunResultsValidator()

    def observe(self, attempt: CompositionAttemptIdentity) -> DbtNativeOutcome:
        intent = self._store.load_intent(attempt)
        if intent.attempt != attempt:
            raise DbtCaptureError("capture_attempt")
        try:
            record = self._store.read_capture(attempt)
            if type(record) is not DbtCaptureRecord or record.intent != intent:
                raise DbtCaptureError("capture_missing")
            if record.phase == "UNDISPATCHED":
                if record.exit_record is not None or record.originals or not record.undispatched_closure_original:
                    raise DbtCaptureError("capture_undispatched_conflict")
                closure = strict_json_object(record.undispatched_closure_original)
                if closure.get("build_dispatched") is not False or closure.get("closed") is not True:
                    raise DbtCaptureError("capture_undispatched_closure")
                if closure != {
                    "attempt_sha256": attempt.attempt_sha256,
                    "intent_sha256": intent.intent_sha256,
                    "build_dispatched": False,
                    "closed": True,
                }:
                    raise DbtCaptureError("capture_undispatched_closure")
                return DbtNativeOutcome("FAILED", "protected_undispatched_closure", intent.intent_sha256)
            expectation = self._store.load_expectation(attempt)
            if type(expectation) is not DbtOutcomeExpectation:
                raise DbtCaptureError("capture_expectation")
            invocation = self._require_successful_capture(record, expectation)
            raw = self._observe_materializations(attempt, intent, expectation, invocation)
            if type(raw) is not bytes or not 0 < len(raw) <= 1024 * 1024:
                raise DbtCaptureError("materialization_original")
            observation = strict_json_object(raw)
            if (
                set(observation)
                != {
                    "schema",
                    "attempt_sha256",
                    "intent_sha256",
                    "invocation_id",
                    "materializations",
                    "catalog",
                }
                or observation["schema"] != "dpone.composition-dbt-materialization-observation.v1"
                or observation["attempt_sha256"] != attempt.attempt_sha256
                or observation["intent_sha256"] != intent.intent_sha256
                or observation["invocation_id"] != invocation
                or observation["materializations"] != [list(row) for row in expectation.materializations]
                or not expectation.materializations
                or not self._matches_catalog(observation["catalog"], expectation)
            ):
                raise DbtCaptureError("materialization_mismatch")
            return DbtNativeOutcome("SUCCEEDED", "native_invocation_verified", intent.intent_sha256, raw)
        except Exception:
            # Build dispatch may already have committed arbitrary model SQL.
            # Missing, conflicting and failure artifacts are not rollback proof.
            return DbtNativeOutcome("COMMIT_UNKNOWN", "native_outcome_unverified", intent.intent_sha256)

    @staticmethod
    def _matches_catalog(catalog: object, expectation: DbtOutcomeExpectation) -> bool:
        if not isinstance(catalog, list) or len(catalog) != len(expectation.materializations):
            return False
        expected = {unique_id: schema_sha256 for unique_id, _kind, schema_sha256 in expectation.materializations}
        return all(
            isinstance(row, dict)
            and row.get("unique_id") in expected
            and row.get("schema_sha256") == expected[row["unique_id"]]
            for row in catalog
        ) and len({row["unique_id"] for row in catalog}) == len(catalog)

    def _require_successful_capture(self, record: DbtCaptureRecord, expected: DbtOutcomeExpectation) -> str:
        exited = record.exit_record
        if (
            record.phase != "CAPTURED"
            or exited is None
            or exited.intent != record.intent
            or exited.child.exit_code != 0
            or not exited.quiescence_original
            or tuple(item.role for item in record.originals) != ARTIFACT_ROLES
        ):
            raise DbtCaptureError("capture_incomplete")
        if tuple((row.role, row.relative_path) for row in record.originals) != record.intent.artifact_paths:
            raise DbtCaptureError("capture_paths")
        if (
            record.originals[0] != exited.preflight_original
            or record.originals[0].sha256 != record.intent.preflight_manifest_sha256
        ):
            raise DbtCaptureError("capture_preflight_changed")
        closure = strict_json_object(exited.quiescence_original)
        if (
            closure.get("schema") != "dpone.composition-dbt-quiescence.v1"
            or closure.get("intent_sha256") != record.intent.intent_sha256
            or closure.get("pid") != exited.child.pid
            or closure.get("start_ticks") != exited.child.start_ticks
            or closure.get("child_uid") != record.intent.child_uid
            or not closure.get("boot_id")
            or not closure.get("pid_namespace")
        ):
            raise DbtCaptureError("capture_quiescence_subject")
        payloads = {item.role: strict_json_object(item.content) for item in record.originals}
        for role in ("preflight_manifest", "build_manifest"):
            manifest = payloads[role]
            if manifest.get("metadata", {}).get("dbt_version") != expected.dbt_core_version:
                raise DbtCaptureError("capture_manifest_toolchain")
            violations = self._manifest_validator.validate(
                manifest, version=int(expected.manifest_schema_version.removeprefix("v"))
            )
            if any(row.severity == "error" for row in violations):
                raise DbtCaptureError("capture_manifest_schema")
            if (
                dbt_graph_contract_sha256(manifest, expected.selected_graph_unique_ids)
                != expected.graph_contract_sha256
                or expected_dbt_run_result_ids(manifest, expected.selected_graph_unique_ids)
                != expected.expected_run_result_unique_ids
            ):
                raise DbtCaptureError("capture_manifest_selection")
        expected_materialized = {
            unique_id
            for unique_id in expected.expected_run_result_unique_ids
            if unique_id.startswith(("model.", "seed.", "snapshot."))
        }
        if (
            len(expected.materializations) != len(expected_materialized)
            or {row[0] for row in expected.materializations} != expected_materialized
        ):
            raise DbtCaptureError("materialization_selection")
        results = payloads["run_results"]
        if self._results_validator.validate(
            results, version=int(expected.run_results_schema_version.removeprefix("v"))
        ):
            raise DbtCaptureError("capture_results_schema")
        parsed = self._parse_results(
            results,
            expected_run_result_unique_ids=expected.expected_run_result_unique_ids,
            expected_dbt_version=expected.dbt_core_version,
            expected_schema_version=expected.run_results_schema_version,
        )
        if payloads["build_manifest"]["metadata"].get("invocation_id") != parsed.invocation_id:
            raise DbtCaptureError("capture_manifest_invocation")
        if not parsed.passes(expected.warning_policy):
            raise DbtCaptureError("capture_results_failed")
        evidence = validate_dbt_execution_evidence_contract(payloads["execution_evidence"])
        if (
            evidence.status != "passed"
            or evidence.invocation_id != parsed.invocation_id
            or evidence.toolchain_sha256 != record.intent.toolchain_sha256
            or evidence.dbt_version != parsed.dbt_version
            or evidence.dbt_schema_version != parsed.schema_version
            or evidence.dbt_warning_policy != expected.warning_policy
        ):
            raise DbtCaptureError("capture_evidence_mismatch")
        if not expected.evidence_subject or any(
            payloads["execution_evidence"].get(key) != value for key, value in expected.evidence_subject
        ):
            raise DbtCaptureError("capture_evidence_subject")
        attempt = record.intent.attempt
        airflow = evidence.airflow
        if (airflow.get("run_id"), airflow.get("task_id"), airflow.get("try_number"), airflow.get("map_index")) != (
            attempt.dag_run_id,
            attempt.task_id,
            attempt.try_number,
            attempt.map_index,
        ):
            raise DbtCaptureError("capture_evidence_attempt")
        if tuple(sorted((node.unique_id, node.status, node.execution_time) for node in evidence.nodes)) != tuple(
            sorted((node.unique_id, node.status, node.execution_time) for node in parsed.nodes)
        ):
            raise DbtCaptureError("capture_evidence_nodes")
        return parsed.invocation_id
