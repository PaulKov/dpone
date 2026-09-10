"""Original-byte registration values are documentary, never signature evidence."""

import hashlib
import json
from dataclasses import asdict, replace
from typing import Any

import pytest

from dpone.contracts.nonproduction_registration import (
    NonproductionGrantRegistration,
    NonproductionRegistrationOriginals,
    require_execution_memberships,
    signature_subject_bytes,
    workload_document,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, canonical_document
from tests.nonproduction_authority_helpers import NOW, execution, limits, qualification
from tests.nonproduction_signature_helpers import BUNDLE, github_policy
from tests.test_nonproduction_activation import request


def originals(grant=None, policy=None):
    policy = policy or github_policy()
    grant = grant or execution(policy)
    return NonproductionRegistrationOriginals(
        grant.to_bytes(),
        BUNDLE,
        signature_subject_bytes(grant.signature_subject(policy)),
        request(grant).to_bytes() if grant.phase == "execution" else None,
    )


def test_original_documents_roundtrip_without_a_signature_or_permission_claim():
    value = originals()
    assert value.grant == execution(github_policy())
    assert value.subject == value.grant.signature_subject(github_policy())
    assert value.grant_sha256 == "sha256:" + hashlib.sha256(value.grant_bytes).hexdigest()
    assert value.bundle_sha256 == "sha256:" + hashlib.sha256(BUNDLE).hexdigest()
    assert value.request_sha256 == request(value.grant).request_sha256
    receipt = NonproductionGrantRegistration(value, 1, "2026-09-10T01:30:00Z")
    assert receipt.originals == value and receipt.registered_at.endswith("Z")
    assert "inert bundle" not in repr(value)


@pytest.mark.parametrize("field", ["grant_bytes", "signature_subject_bytes", "request_bytes"])
@pytest.mark.parametrize("change", ["noncanonical", "duplicate", "unknown", "nonobject"])
def test_rejects_altered_canonical_originals(field, change):
    value = originals()
    raw = getattr(value, field)
    body = json.loads(raw)
    if change == "noncanonical":
        raw += b"\n"
    elif change == "duplicate":
        key = next(iter(body))
        raw = raw[:-1] + b"," + json.dumps(key).encode() + b":null}"
    elif change == "unknown":
        raw = canonical_document(dict(body, caller_pass=True))
    else:
        raw = b"[]"
    with pytest.raises(NonproductionAuthorityError):
        replace(value, **{field: raw})


@pytest.mark.parametrize("field", ["grant_bytes", "bundle_bytes", "signature_subject_bytes"])
@pytest.mark.parametrize("raw", [b"", "bytes", bytearray(b"bytes"), None])
def test_strict_original_transport_types(field, raw):
    with pytest.raises(NonproductionAuthorityError):
        replace(originals(), **{field: raw})


def test_subject_and_request_bindings_are_exact_even_with_recomputed_hashes():
    value = originals()
    subject = json.loads(value.signature_subject_bytes)
    subject["phase"] = "qualification"
    with pytest.raises(NonproductionAuthorityError):
        replace(value, signature_subject_bytes=canonical_document(subject))
    with pytest.raises(NonproductionAuthorityError):
        replace(
            value,
            request_bytes=replace(request(value.grant), source_subject_sha256="sha256:" + "a" * 64).to_bytes(),
            grant_bytes=qualification(github_policy()).to_bytes(),
        )


def test_qualification_has_no_execution_request_or_invented_memberships():
    value = originals(qualification(github_policy()))
    assert value.request_sha256 is None and value.request_bytes is None
    with pytest.raises(NonproductionAuthorityError):
        replace(value, request_bytes=originals().request_bytes)
    with pytest.raises(NonproductionAuthorityError):
        replace(originals(), request_bytes=None)


@pytest.mark.parametrize(
    "revision,instant",
    [
        (0, "2026-09-10T01:30:00Z"),
        (True, "2026-09-10T01:30:00Z"),
        (2**63, "2026-09-10T01:30:00Z"),
        (1, "2026-09-10T01:30:00+00:00"),
    ],
)
def test_registration_revision_and_instant_are_strict(revision, instant):
    with pytest.raises(NonproductionAuthorityError):
        NonproductionGrantRegistration(originals(), revision, instant)


def test_complete_membership_union_uses_every_current_lower_ceiling():
    policy = github_policy(limits=limits(max_workloads=4))
    grant = execution(policy)
    assert require_execution_memberships(grant, policy, ("old",)) == ("a_native", "b_generated", "c_ordinary", "old")
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        require_execution_memberships(grant, policy, ("old", "older"))
    lower = replace(grant.workloads[0], limits=limits(max_workloads=3))
    grant = replace(grant, workloads=(lower, *grant.workloads[1:]))
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        require_execution_memberships(grant, policy, ("old",))


def test_membership_bytes_preserve_unicode_and_slashes_exactly():
    assert workload_document("a\\native") != workload_document("a/native")
    assert json.loads(workload_document("данные")) == {"workload_id": "данные"}
    assert signature_subject_bytes(originals().subject) == canonical_document(asdict(originals().subject))


def test_bundle_has_its_own_eight_mib_bound_without_a_combined_json_envelope():
    value = replace(originals(), bundle_bytes=b"b" * (8 * 1024 * 1024))
    assert len(value.bundle_bytes) == 8 * 1024 * 1024
    with pytest.raises(NonproductionAuthorityError):
        replace(value, bundle_bytes=value.bundle_bytes + b"b")


@pytest.mark.parametrize("field", ["grant_bytes", "signature_subject_bytes", "request_bytes"])
def test_document_bounds_reject_before_decoding(field):
    with pytest.raises(NonproductionAuthorityError):
        replace(originals(), **{field: b"x" * (1024 * 1024 + 1)})


@pytest.mark.parametrize("members", [("old", "old"), ("z", "a"), (" old",), (True,), ["old"]])
def test_existing_membership_requires_exact_unique_canonical_ids(members):
    with pytest.raises(NonproductionAuthorityError):
        require_execution_memberships(execution(github_policy()), github_policy(), members)


@pytest.mark.parametrize("instant", ["2026-09-10T00:59:59Z", "2026-09-10T02:00:00Z"])
def test_registration_instant_must_fall_inside_its_original_grant_window(instant):
    with pytest.raises(NonproductionAuthorityError):
        NonproductionGrantRegistration(originals(), 1, instant)


def test_missing_policy_is_a_fixed_structural_error():
    missing: Any = None
    with pytest.raises(NonproductionAuthorityError):
        require_execution_memberships(execution(github_policy()), missing, ())


def test_original_policy_comparison_is_documentary_and_requires_exact_external_bytes():
    policy = github_policy()
    assert originals().require_policy(policy.to_bytes(), policy.policy_sha256, policy.revocation_epoch, NOW) == policy
    with pytest.raises(NonproductionAuthorityError):
        originals().require_policy(policy.to_bytes() + b"\n", policy.policy_sha256, policy.revocation_epoch, NOW)
