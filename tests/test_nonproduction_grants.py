"""Phase/scope comparison contracts, not actual signature verification."""

import hashlib
from dataclasses import replace
from datetime import timedelta

import pytest

from dpone.contracts.nonproduction_grants import (
    NonproductionExecutionGrant,
    NonproductionQualificationGrant,
    parse_nonproduction_grant,
    validate_grant_subject,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.nonproduction_authority_helpers import (
    NOW,
    digest,
    execution,
    identifier,
    limits,
    policy,
    qualification,
    verification_inputs,
)


def test_distinct_phase_types_cannot_be_interchanged():
    assert NonproductionExecutionGrant is not NonproductionQualificationGrant


@pytest.mark.parametrize("make", [qualification, execution])
def test_phase_canonical_roundtrip_and_exact_structural_subject(make):
    value = policy()
    grant = make(value)
    assert parse_nonproduction_grant(grant.to_bytes()) == grant
    assert type(grant).from_bytes(grant.to_bytes()) == grant
    validate_grant_subject(grant, **verification_inputs(grant, value))


@pytest.mark.parametrize("make", [qualification, execution])
@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "schema",
        "phase",
        "missing_scope",
        "missing_plan",
        "unknown_scope",
        "optional_trust",
        "duplicate",
        "noncanonical",
        "wrong_type",
    ],
)
def test_closed_phase_readers_reject_malformed_or_swapped_authority(make, damage):
    grant = make()
    body = grant.to_dict()
    if damage == "unknown":
        body["verified"] = "PASS"
    elif damage == "schema":
        body["schema"] = "dpone.dbt-release-set.v2"
    elif damage == "phase":
        body["phase"] = "execution" if grant.phase == "qualification" else "qualification"
    elif damage == "missing_scope":
        del body["scope"]
    elif damage == "missing_plan":
        del body["qualification_plan_sha256" if grant.phase == "qualification" else "deployment_id"]
    elif damage == "unknown_scope":
        body["scope"]["provenance"] = {"trust_tier": "non_production"}
    elif damage == "optional_trust":
        body["scope"]["trust_tier"] = "production"
    elif damage == "wrong_type":
        body["scope"]["participants"][0]["connector"] = ["mssql"]
    raw = canonical_json_bytes(body)
    if damage == "duplicate":
        raw = raw.replace(b'"revocation_epoch":2', b'"revocation_epoch":2,"revocation_epoch":2')
    elif damage == "noncanonical":
        raw += b"\n"
    with pytest.raises(NonproductionAuthorityError):
        type(grant).from_bytes(raw)
    with pytest.raises(NonproductionAuthorityError):
        parse_nonproduction_grant(raw)


def test_qualification_subject_is_one_exact_run_and_both_plans():
    grant = qualification()
    expected = dict(
        qualification_run_id=grant.qualification_run_id,
        fixture_plan_sha256=grant.fixture_plan_sha256,
        qualification_plan_sha256=grant.qualification_plan_sha256,
    )
    grant.require_qualification_subject(**expected)
    for key in expected:
        changed = identifier(99) if key.endswith("_id") else digest("changed")
        with pytest.raises(NonproductionAuthorityError, match="qualification_subject"):
            grant.require_qualification_subject(**dict(expected, **{key: changed}))
    with pytest.raises(NonproductionAuthorityError):
        NonproductionExecutionGrant.from_bytes(grant.to_bytes())


def test_execution_subject_requires_complete_ancestor_and_pack_membership():
    grant = execution()
    expected = dict(
        qualified_set_sha256=grant.qualified_set_sha256,
        native_release_id=grant.native_release_id,
        parent_release_id=grant.parent_release_id,
        deployment_id=grant.deployment_id,
        activation_id=grant.activation_id,
        workloads=grant.workloads,
    )
    grant.require_execution_subject(**expected)
    for key in expected:
        if key == "workloads":
            changed = (replace(grant.workloads[0], pack_sha256=digest("different pack")), *grant.workloads[1:])
        elif key == "activation_id":
            changed = identifier(99)
        else:
            changed = digest("changed")
        with pytest.raises(NonproductionAuthorityError, match="execution_subject"):
            grant.require_execution_subject(**dict(expected, **{key: changed}))
    with pytest.raises(NonproductionAuthorityError):
        NonproductionQualificationGrant.from_bytes(grant.to_bytes())


@pytest.mark.parametrize("damage", ["missing_generated", "missing_ordinary", "duplicate", "unsorted"])
def test_whole_parent_scope_is_required_at_execution_comparison(damage):
    grant = execution()
    workloads = grant.workloads
    if damage == "missing_generated":
        workloads = (workloads[0], workloads[2])
    elif damage == "missing_ordinary":
        workloads = workloads[:2]
    elif damage == "duplicate":
        workloads = (workloads[0], *workloads)
    else:
        workloads = workloads[::-1]
    with pytest.raises(NonproductionAuthorityError):
        grant.require_execution_subject(
            qualified_set_sha256=grant.qualified_set_sha256,
            native_release_id=grant.native_release_id,
            parent_release_id=grant.parent_release_id,
            deployment_id=grant.deployment_id,
            activation_id=grant.activation_id,
            workloads=workloads,
        )


@pytest.mark.parametrize(
    "field", ["document_sha256", "policy_sha256", "phase", "issuer", "identity", "root", "backend", "caller_pass"]
)
def test_signature_comparison_requires_exact_trusted_verifier_subject(field):
    value = policy()
    grant = execution(value)
    inputs = verification_inputs(grant, value)
    subject = inputs["signature_subject"]
    if field in {"document_sha256", "policy_sha256"}:
        subject = replace(subject, **{field: digest("wrong")})
    elif field == "phase":
        subject = replace(subject, phase="qualification")
    elif field == "caller_pass":
        subject = {"status": "PASS", "subject_sha256": grant.grant_sha256}
    else:
        signer_field = "trust_root_sha256" if field == "root" else field
        changed = (
            digest("untrusted root")
            if field == "root"
            else "github_artifact_attestation_v1"
            if field == "backend"
            else "untrusted"
        )
        subject = replace(subject, signer=replace(subject.signer, **{signer_field: changed}))
    with pytest.raises(NonproductionAuthorityError):
        validate_grant_subject(grant, **dict(inputs, signature_subject=subject))


@pytest.mark.parametrize(
    "field", ["campaign_id", "compilation_intent_sha256", "fixture_sha256", "source_commit", "participants"]
)
def test_expected_scope_cannot_come_from_changed_claims(field):
    value = policy()
    grant = execution(value)
    inputs = verification_inputs(grant, value)
    changed = identifier(99) if field == "campaign_id" else "b" * 40 if field == "source_commit" else digest("changed")
    if field == "participants":
        changed = (
            replace(grant.scope.participants[0], write_relations=(digest("changed effects"),)),
            *grant.scope.participants[1:],
        )
    expected = replace(grant.scope, **{field: changed})
    with pytest.raises(NonproductionAuthorityError, match="scope_subject"):
        validate_grant_subject(grant, **dict(inputs, expected_scope=expected))


@pytest.mark.parametrize("boundary", ["before", "expiry", "later", "naive", "none", "revocation", "epoch_bool"])
def test_expiry_and_external_revocation_block_structural_admission(boundary):
    value = policy()
    grant = execution(value)
    inputs = verification_inputs(grant, value)
    if boundary == "before":
        inputs["now"] = NOW.replace(hour=0)
    elif boundary == "expiry":
        inputs["now"] = NOW.replace(hour=2, minute=0)
    elif boundary == "later":
        inputs["now"] = NOW + timedelta(days=1)
    elif boundary == "naive":
        inputs["now"] = NOW.replace(tzinfo=None)
    elif boundary == "none":
        inputs["now"] = None
    else:
        inputs["current_revocation_epoch"] = True if boundary == "epoch_bool" else 3
    with pytest.raises(NonproductionAuthorityError):
        validate_grant_subject(grant, **inputs)


def test_grant_revocation_and_external_policy_pin_are_independent_inputs():
    value = policy(revoked_grant_ids=(identifier(8),))
    grant = execution(value)
    with pytest.raises(NonproductionAuthorityError, match="revoked"):
        validate_grant_subject(grant, **verification_inputs(grant, value))
    value = policy()
    grant = execution(value)
    with pytest.raises(NonproductionAuthorityError, match="policy_pin"):
        validate_grant_subject(grant, **dict(verification_inputs(grant, value), expected_policy_sha256=digest("wrong")))


@pytest.mark.parametrize("make", [qualification, execution])
def test_grant_changes_cannot_reset_its_one_time_consumption_identity(make):
    grant = make()
    changed = replace(grant, scope=replace(grant.scope, compilation_intent_sha256=digest("other intent")))
    assert changed.grant_sha256 != grant.grant_sha256
    assert changed.consumption_subject_sha256 == grant.consumption_subject_sha256
    other = replace(grant, grant_id=identifier(99))
    assert other.consumption_subject_sha256 != grant.consumption_subject_sha256
    assert execution().consumption_subject_sha256 != qualification().consumption_subject_sha256


def test_execution_grant_changes_bind_new_activation_without_request_hash_cycle():
    grant = execution()
    changed = replace(grant, activation_id=identifier(99))
    assert changed.grant_sha256 != grant.grant_sha256
    assert "activation_request_sha256" not in grant.to_dict()
    assert "runtime_context_sha256" not in grant.to_dict()


def test_workload_effective_budget_is_minimum_of_all_three_signed_ceilings():
    value = policy(limits=limits(max_source_rows=50))
    grant = execution(value, limits=limits(max_source_bytes=500))
    workloads = (replace(grant.workloads[0], limits=limits(max_attempts=2)), *grant.workloads[1:])
    grant = replace(grant, workloads=workloads)
    result = grant.workload_limits("a_native", policy=value)
    assert (result.max_source_rows, result.max_source_bytes, result.max_attempts) == (50, 500, 2)
    with pytest.raises(NonproductionAuthorityError, match="workload_membership"):
        grant.workload_limits("unlisted", policy=value)


def test_policy_workload_limit_cannot_be_overridden_by_a_larger_grant():
    value = policy(limits=limits(max_workloads=2))
    grant = execution(value)
    with pytest.raises(NonproductionAuthorityError, match="closure"):
        validate_grant_subject(grant, **verification_inputs(grant, value))


@pytest.mark.parametrize("make", [qualification, execution])
def test_approved_validity_and_policy_window_are_strict(make):
    with pytest.raises(NonproductionAuthorityError, match="validity"):
        make(expires_at="2026-09-11T01:00:01Z")
    value = policy(not_before="2026-09-10T01:30:00Z", expires_at="2026-09-10T23:00:00Z")
    grant = make(value)
    with pytest.raises(NonproductionAuthorityError, match="policy_validity"):
        validate_grant_subject(grant, **verification_inputs(grant, value))


def test_shorter_workload_validity_cannot_be_widened_by_global_grant_window():
    value = policy()
    grant = execution(value)
    workloads = (replace(grant.workloads[0], limits=limits(max_validity_seconds=1)), *grant.workloads[1:])
    with pytest.raises(NonproductionAuthorityError, match="validity"):
        changed = replace(grant, workloads=workloads)
        validate_grant_subject(changed, **verification_inputs(changed, value))


def test_lower_workload_cardinality_cannot_be_widened_by_global_grant_limit():
    value = policy()
    grant = execution(value)
    workloads = (replace(grant.workloads[0], limits=limits(max_workloads=1)), *grant.workloads[1:])
    with pytest.raises(NonproductionAuthorityError, match="closure"):
        changed = replace(grant, workloads=workloads)
        validate_grant_subject(changed, **verification_inputs(changed, value))


def test_signed_grant_subject_distinguishes_backslash_from_slash_workload_bytes():
    value = policy()
    original = execution(value)
    first = replace(
        original, workloads=(replace(original.workloads[0], workload_id=r"a\native"), *original.workloads[1:])
    )
    second = replace(
        original, workloads=(replace(original.workloads[0], workload_id="a/native"), *original.workloads[1:])
    )
    assert first.to_bytes() != second.to_bytes()
    with pytest.raises(NonproductionAuthorityError, match="signature_subject"):
        validate_grant_subject(
            second, **dict(verification_inputs(second, value), signature_subject=first.signature_subject(value))
        )


@pytest.mark.parametrize("workload_id", ["a/native", r"a\native", "a_native_данные"])
def test_grant_subject_hashes_pin_exact_bytes_and_one_time_key(workload_id):
    grant = execution()
    grant = replace(grant, workloads=(replace(grant.workloads[0], workload_id=workload_id), *grant.workloads[1:]))
    assert grant.grant_sha256 == "sha256:" + hashlib.sha256(grant.to_bytes()).hexdigest()
    key = canonical_json_bytes({"grant_id": grant.grant_id, "phase": grant.phase})
    assert grant.consumption_subject_sha256 == "sha256:" + hashlib.sha256(key).hexdigest()
