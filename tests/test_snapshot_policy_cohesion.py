"""Supplemental scratch checks for extracted evidence budget policy."""

import pytest

from dpone.contracts.nonproduction_authority import require_signature_bundle
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from dpone.contracts.runtime_artifact_attestation import MAX_ATTESTATION_BUNDLE_BYTES
from tests.nonproduction_signature_helpers import (
    SignatureDouble,
    TrustProvider,
    github_policy,
    qualification,
    qualification_inputs,
    trust,
)
from tests.test_nonproduction_authenticator import authenticator


@pytest.mark.parametrize("bundle", [None, "x", bytearray(b"x"), b"", b"x" * (MAX_ATTESTATION_BUNDLE_BYTES + 1)])
def test_invalid_bundle_precedes_trust_and_signature(bundle):
    policy = github_policy()
    grant = qualification(policy)
    provider = TrustProvider(trust(policy))
    verifier = SignatureDouble(grant.signature_subject(policy))
    service = authenticator(grant, policy, provider=provider, verifier=verifier)
    args = dict(qualification_inputs(grant), sigstore_bundle=bundle)
    with pytest.raises(NonproductionAuthorityError, match="signature_bundle_budget"):
        service.authenticate_qualification(**args)
    assert provider.reads == 0
    assert verifier.calls == []


@pytest.mark.parametrize("size", [1, MAX_ATTESTATION_BUNDLE_BYTES])
def test_raw_bundle_boundaries_remain_inclusive(size):
    require_signature_bundle(b"x" * size)


@pytest.mark.parametrize("recovery", [False, True])
def test_snapshot_subject_policy_preserves_wire_bytes(recovery):
    from tests.composition_snapshot_helpers import intent, occurrence

    value = intent()
    before = value.to_bytes()
    value.require_prepared_subject(value.attempt, value.generation.record_sha256)
    value.require_parent_scope(occurrence(), recovery=recovery)
    assert value.to_bytes() == before


@pytest.mark.parametrize("fault", ["attempt", "generation"])
def test_snapshot_subject_mismatch_has_stable_reason(fault):
    from dpone.contracts.composition_activation import CompositionAdmissionError
    from tests.composition_snapshot_helpers import digest, intent

    value = intent()
    other = intent(try_number=2)
    with pytest.raises(CompositionAdmissionError, match="snapshot_prepared_subject"):
        value.require_prepared_subject(
            other.attempt if fault == "attempt" else value.attempt,
            digest("wrong") if fault == "generation" else value.generation.record_sha256,
        )
