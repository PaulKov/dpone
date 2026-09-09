from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator, ValidationError
from yaml.constructor import ConstructorError

from tests.test_ci_shadow_spec_contracts import ADR_0048, ROOT, SPEC, _read, _squash_whitespace

POLICY = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-reconciliation-mvp.yml"
SCHEMA = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-reconciliation-mvp.schema.json"
TASK = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-closure-pr2-spec.yml"


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConstructorError(
                "while constructing a mapping", node.start_mark, f"duplicate key: {key!r}", key_node.start_mark
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


def _load_policy_text(text: str) -> dict[str, Any]:
    payload = yaml.load(text, Loader=_UniqueKeyLoader)
    assert isinstance(payload, dict)
    return payload


def _policy() -> dict[str, Any]:
    return _load_policy_text(POLICY.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _classify_auditor_producer(
    policy: dict[str, Any],
    *,
    producer_created_at: int,
    scan_from: int,
    safe_scan_through: int,
    observation_started_at: int,
    present_in_producer_query: bool = True,
    identity_resolvable: bool = True,
) -> dict[str, Any]:
    cross_window = policy["cross_window_auditor_policy"]
    if not identity_resolvable:
        return cross_window["missing_foreign_or_unresolvable"]
    if scan_from <= producer_created_at <= safe_scan_through and not present_in_producer_query:
        return cross_window["inside_interval_missing_from_producer_query"]
    if producer_created_at < scan_from:
        return cross_window["before_scan_from"]
    if producer_created_at <= safe_scan_through:
        return cross_window["inside_interval"]
    if producer_created_at <= observation_started_at:
        return cross_window["after_safe_through_not_after_observation_start"]
    return cross_window["after_observation_start"]


def test_design_fixture_is_closed_duplicate_safe_and_not_runtime_input() -> None:
    policy = _policy()
    _validator().validate(policy)

    assert policy["artifact_kind"] == "design_fixture"
    assert policy["status"] == "APPROVED_DESIGN"
    assert policy["runtime_consumable"] is False
    assert policy["runtime_contract"] == {
        "owner": "dpone.contracts.ci_shadow_reconciliation.ReconciliationPolicyV1",
        "child_contract_must_freeze_exact_bytes": True,
        "external_policy_cli_argument": False,
        "producer_workflow_id": 343714753,
        "auditor_workflow_id": 343909056,
    }

    duplicate = POLICY.read_text(encoding="utf-8") + "\nstatus: BROKEN\n"
    with pytest.raises(ConstructorError):
        _load_policy_text(duplicate)

    unknown = copy.deepcopy(policy)
    unknown["unknown"] = True
    with pytest.raises(ValidationError):
        _validator().validate(unknown)

    changed_query = copy.deepcopy(policy)
    changed_query["scan"]["producer_query"] = "/different"
    with pytest.raises(ValidationError):
        _validator().validate(changed_query)

    changed_invariant = copy.deepcopy(policy)
    changed_invariant["decision"]["pass_requires"][0] = "different"
    with pytest.raises(ValidationError):
        _validator().validate(changed_invariant)

    changed_cross_window = copy.deepcopy(policy)
    changed_cross_window["cross_window_auditor_policy"]["before_scan_from"]["outcome"] = "EVALUATE_NORMALLY"
    with pytest.raises(ValidationError):
        _validator().validate(changed_cross_window)

    changed_capacity = copy.deepcopy(policy)
    changed_capacity["capacity_calibration"]["minimum_safety_factor"] = 1
    with pytest.raises(ValidationError):
        _validator().validate(changed_capacity)

    wrong_type = copy.deepcopy(policy)
    wrong_type["limits"]["max_api_requests"] = True
    with pytest.raises(ValidationError):
        _validator().validate(wrong_type)


def test_reconciliation_policy_freezes_interval_and_pagination_seams() -> None:
    policy = _policy()
    scan = policy["scan"]

    assert policy["schedule"] == {
        "utc": "03:15",
        "observation_window_days": 14,
        "grace_minutes": 30,
    }
    assert scan["lower_bound"] == "safe_scan_through - 14 days"
    assert scan["upper_bound"] == "floor_to_utc_second(observation_started_at) - 30 minutes"
    assert scan["interval"] == "closed_utc_seconds"
    assert scan["timestamp_field"] == "workflow_run.created_at"
    assert scan["split_trigger"] == "total_count >= 1000"
    assert scan["split_rule"] == (
        "lo==hi is irreducible; hi==lo+1s splits to [lo,lo] and [hi,hi]; otherwise [lo,mid] and [mid,hi] overlap at mid"
    )
    assert scan["seam_rule"] == "deduplicate identical stable keys; changed bytes are UNVERIFIED"
    assert "two consecutive complete whole-tree observations" in scan["completeness_rule"]
    assert scan["complete_observation_includes"] == [
        "producer and auditor run plus every attempt record",
        "exact producer run lookup for every auditor record",
        "exact-attempt Jobs pages",
        "artifact inventory metadata",
        "bounded downloaded artifact bytes and digests",
    ]
    assert scan["report_observation_boundary"] == "completion of the second matching whole-tree observation"
    assert scan["provider_reads_after_boundary"] is False
    assert set(policy["stateful_features"].values()) == {False}


@pytest.mark.parametrize(
    ("created_at", "expected_outcome", "affects_root"),
    [
        (99, "OUT_OF_SCOPE_OLD_PRODUCER_RERUN", False),
        (100, "EVALUATE_NORMALLY", True),
        (200, "EVALUATE_NORMALLY", True),
        (201, "DEFER_TO_NEXT_REPORT", False),
        (230, "DEFER_TO_NEXT_REPORT", False),
        (231, "UNVERIFIED", True),
    ],
)
def test_cross_window_auditor_boundaries_are_exact(created_at: int, expected_outcome: str, affects_root: bool) -> None:
    result = _classify_auditor_producer(
        _policy(),
        producer_created_at=created_at,
        scan_from=100,
        safe_scan_through=200,
        observation_started_at=230,
    )

    assert result["outcome"] == expected_outcome
    assert result["affects_current_root"] is affects_root


def test_cross_window_auditor_omission_and_foreign_identity_block() -> None:
    policy = _policy()

    omitted = _classify_auditor_producer(
        policy,
        producer_created_at=150,
        scan_from=100,
        safe_scan_through=200,
        observation_started_at=230,
        present_in_producer_query=False,
    )
    foreign = _classify_auditor_producer(
        policy,
        producer_created_at=150,
        scan_from=100,
        safe_scan_through=200,
        observation_started_at=230,
        identity_resolvable=False,
    )

    assert omitted == {
        "outcome": "UNVERIFIED",
        "code": "RECONCILIATION_PRODUCER_QUERY_OMISSION",
        "affects_current_root": True,
        "recovery": "RECONCILIATION_RETRY",
    }
    assert foreign == {
        "outcome": "UNVERIFIED",
        "code": "RECONCILIATION_PRODUCER_IDENTITY_UNRESOLVABLE",
        "affects_current_root": True,
        "recovery": "VERIFY_PRODUCER_IDENTITY_OR_CREATE_NEW_RUN",
    }
    assert policy["cross_window_auditor_policy"]["after_observation_start"] == {
        "outcome": "UNVERIFIED",
        "code": "RECONCILIATION_FUTURE_PRODUCER",
        "affects_current_root": True,
        "recovery": "RECONCILIATION_RETRY",
    }
    assert policy["cross_window_auditor_policy"]["report_every_classification"] is True


def test_reconciliation_policy_closes_decision_and_auditor_reruns() -> None:
    policy = _policy()

    assert policy["decision"]["coverage_status"] == ["PASS", "UNVERIFIED"]
    assert policy["decision"]["product_status"] == ["PASS", "FAIL", "UNVERIFIED"]
    assert policy["decision"]["root_precedence"] == ["UNVERIFIED", "FAIL", "PASS"]
    assert policy["decision"]["in_window_canary_handling"] == "fold_as_ordinary_producer_attempt"
    assert policy["identity"]["audit_rerun_selection"] == "highest_observed_attempt_must_be_terminal"
    assert policy["identity"]["higher_nonterminal_auditor_attempt"] == "UNVERIFIED"
    assert policy["identity"]["independent_auditor_run_ids"] == "UNVERIFIED"
    assert policy["identity"]["conflicting_decisions"] == "UNVERIFIED"


def test_report_write_and_overflow_are_bounded_without_self_hash() -> None:
    policy = _policy()
    artifact = policy["artifact"]

    assert artifact["name"] == "pr-gate-shadow-reconciliation-<observer_run_id>-<observer_run_attempt>.json"
    assert artifact["payload_self_digest"] is False
    assert artifact["post_upload_provider_digest_is_transport_identity"] is True
    assert artifact["writer_port"] == "dpone.ports.evidence.CreateOnlyEvidenceWriterV1"
    assert artifact["writer_adapter"] == ("dpone.adapters.filesystem_evidence.DescriptorPinnedCreateOnlyEvidenceWriter")
    assert artifact["existing_helper_reuse"] == (
        "forbidden until compatibility-preserving adapter extraction is proved"
    )
    assert artifact["write_transaction"] == ("confined stage -> file fsync -> atomic no-replace commit -> parent fsync")
    assert "current file+parent fsync required" in artifact["existing_identical_target"]
    assert artifact["existing_different_target"] == "UNVERIFIED"
    assert policy["overflow"] == {
        "stop_immediately": True,
        "complete": False,
        "observed_count_field": "observed_at_least",
        "last_complete_slice_field": "last_complete_slice",
        "full_set_digest_allowed": False,
        "decision": "UNVERIFIED",
        "code": "RECONCILIATION_RESOURCE_LIMIT",
        "recovery": (
            "transient API or wall-time failure may retry; a deterministic cap waits for a clean window or "
            "approved contract amendment"
        ),
    }
    assert policy["limits"]["max_api_requests"] == 800
    assert policy["limits"]["max_raw_run_records"] == 65536
    assert policy["limits"]["max_response_bytes"] == 134217728
    assert policy["limits"]["response_byte_scope"] == (
        "API response bodies plus downloaded artifact bytes across both observations"
    )
    assert policy["limits"]["max_wall_seconds"] == 900


def test_retention_uses_trusted_upload_contract_not_timestamp_subtraction() -> None:
    policy = _policy()
    retention = policy["retention"]

    assert retention["repository_settings_are_authority"] is False
    assert retention["trusted_workflow_upload_retention_days"] == 90
    assert retention["producer_upload_authority"] == "workflow path/mode/blob identical in authenticated B/H/M"
    assert "default-branch workflow" in retention["auditor_upload_authority"]
    assert "default-branch workflow" in retention["reconciler_upload_authority"]
    assert retention["expires_at_is_availability_deadline_only"] is True
    assert retention["exact_expires_minus_created_required"] is False
    assert retention["require_not_expired"] is True
    assert "never repairs an overlapping daily interval" in retention["current_head_recovery"]
    assert "later complete clean window" in retention["daily_root_recovery"]
    assert "administration:read" in policy["permissions"]["forbidden"]


def test_canary_phases_are_non_excluding_and_acyclic() -> None:
    policy = _policy()
    fork = policy["fork_approval"]
    canaries = policy["acceptance_live_canaries"]

    assert fork["same_run_or_attempt_continuity_required"] is False
    assert fork["auditable_input"] == "independently_authenticated_post_start_completed_attempt"
    assert canaries["are_daily_pass_prerequisites"] is False
    assert canaries["in_window_attempts_affect_daily_root"] is True
    pre = canaries["pre_pr4c_fork_lifecycle"]
    post = canaries["post_pr4c_goal_acceptance"]
    assert pre["timing"] == ("after PR4B safe-fallback merge; before PR4C reconciler child contract and implementation")
    assert post["timing"] == "after PR4C implementation; before goal acceptance"
    assert any("before approval" in item for item in pre["observations"])
    assert any("route canaries" in item for item in post["observations"])


def test_spec_adr_and_task_reference_the_design_fixture_boundary() -> None:
    spec = _squash_whitespace(_read(SPEC))
    adr = _squash_whitespace(_read(ADR_0048))
    task = yaml.safe_load(TASK.read_text(encoding="utf-8"))

    for text in (spec, adr):
        assert "dpone-ci-shadow-reconciliation-mvp.yml" in text
        assert "design fixture" in text
        assert "provider-observable exact interval" in text
        assert "UNVERIFIED > FAIL > PASS" in text
        assert "same run ID or run attempt" in text
        assert "two consecutive complete whole-window observations" in text.lower()
        assert "adjacent" in text and "singleton" in text
        assert "evidence_observed_through" in text
        assert "exact-attempt Jobs" in text
        assert "provider read" in text
        assert "`workflow_run.created_at`" in text
        assert "auditor rerun" in text
        assert "OUT_OF_SCOPE_OLD_PRODUCER_RERUN" in text
        assert "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED" in text
        assert "parent" in text and "hard maxima" in text
        assert "dpone.ci-shadow-reconciliation-capacity.v1" in text
        assert "bundle" in text and "digest" in text
        assert "total_http_requests" in text
        assert "remaining" in text and "wall" in text and "budget" in text
        assert "default-branch workflow ID/path/revision/ref/event/run/attempt" in text
        assert "CreateOnlyEvidenceWriterV1" in text
        assert "DescriptorPinnedCreateOnlyEvidenceWriter" in text
        assert "current" in text and "file" in text and "parent" in text and "fsync" in text
    assert "There is no cursor, predecessor state, unresolved ledger, genesis state" in spec
    assert "there is no “latest artifact wins” rule" in spec
    assert "test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-mvp.yml" in task["owned_paths"]
    assert "test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-mvp.schema.json" in task["owned_paths"]
    assert "test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-capacity.schema.json" in task["owned_paths"]


def test_policy_files_stay_human_reviewable() -> None:
    assert len(POLICY.read_text(encoding="utf-8").splitlines()) <= 300
    assert len(SCHEMA.read_text(encoding="utf-8").splitlines()) <= 370
    assert Path(POLICY).suffix == ".yml"
