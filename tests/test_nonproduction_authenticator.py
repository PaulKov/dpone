"""Authentication orchestration tests; capability doubles are not signatures."""

from dataclasses import replace
from datetime import datetime, timedelta, tzinfo

import pytest

from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from dpone.runtime.nonproduction_authentication import NonproductionGrantAuthenticator
from tests.nonproduction_authority_helpers import identifier
from tests.nonproduction_signature_helpers import (
    BUNDLE,
    NOW,
    SignatureDouble,
    TrustProvider,
    execution,
    execution_inputs,
    github_policy,
    qualification,
    qualification_inputs,
    sha256,
    trust,
)


def authenticator(grant, value, *, provider=None, clock=None, verifier=None):
    return NonproductionGrantAuthenticator(
        trust_provider=provider or TrustProvider(trust(value)),
        clock=clock or (lambda: NOW),
        verifier=verifier or SignatureDouble(grant.signature_subject(value)),
    )


@pytest.mark.parametrize("phase", ["qualification", "execution"])
def test_authentication_passes_original_bytes_and_refreshes_trust(phase):
    value = github_policy()
    grant = qualification(value) if phase == "qualification" else execution(value)
    provider = TrustProvider(trust(value))
    verifier = SignatureDouble(grant.signature_subject(value))
    clocks = iter((NOW, NOW + timedelta(seconds=1)))
    service = authenticator(grant, value, provider=provider, clock=lambda: next(clocks), verifier=verifier)
    arguments = qualification_inputs(grant) if phase == "qualification" else execution_inputs(grant)
    result = getattr(service, "authenticate_" + phase)(**arguments)
    assert result == (grant, grant.signature_subject(value))
    assert provider.reads == 2
    assert [kind for kind, _ in verifier.calls] == ["trust", "verify", "trust"]
    passed = verifier.calls[1][1]
    assert passed["grant_bytes"] == grant.to_bytes() and passed["sigstore_bundle"] == BUNDLE
    assert passed["trust"] == trust(value)


@pytest.mark.parametrize("phase", ["qualification", "execution"])
def test_phase_swap_rejected_before_signature_verifier(phase):
    value = github_policy()
    grant = qualification(value) if phase == "qualification" else execution(value)
    other = execution(value) if phase == "qualification" else qualification(value)
    verifier = SignatureDouble(grant.signature_subject(value))
    service = authenticator(grant, value, verifier=verifier)
    arguments = qualification_inputs(grant) if phase == "qualification" else execution_inputs(grant)
    with pytest.raises(NonproductionAuthorityError):
        getattr(service, "authenticate_" + phase)(**dict(arguments, grant_bytes=other.to_bytes()))
    assert not any(kind == "verify" for kind, _ in verifier.calls)


@pytest.mark.parametrize("field", ["qualification_run_id", "fixture_plan_sha256", "qualification_plan_sha256"])
def test_wrong_qualification_subject_never_authenticates(field):
    value = github_policy()
    grant = qualification(value)
    args = qualification_inputs(grant)
    args[field] = identifier(99) if field.endswith("run_id") else sha256(b"foreign")
    verifier = SignatureDouble(grant.signature_subject(value))
    with pytest.raises(NonproductionAuthorityError, match="qualification_subject"):
        authenticator(grant, value, verifier=verifier).authenticate_qualification(**args)
    assert not any(kind == "verify" for kind, _ in verifier.calls)


@pytest.mark.parametrize(
    "field",
    ["qualified_set_sha256", "native_release_id", "parent_release_id", "deployment_id", "activation_id", "workloads"],
)
def test_wrong_execution_subject_never_authenticates(field):
    value = github_policy()
    grant = execution(value)
    args = execution_inputs(grant)
    args[field] = (
        (replace(grant.workloads[0], pack_sha256=sha256(b"foreign")), *grant.workloads[1:])
        if field == "workloads"
        else identifier(99)
        if field == "activation_id"
        else sha256(b"foreign")
    )
    verifier = SignatureDouble(grant.signature_subject(value))
    with pytest.raises(NonproductionAuthorityError, match="execution_subject"):
        authenticator(grant, value, verifier=verifier).authenticate_execution(**args)
    assert not any(kind == "verify" for kind, _ in verifier.calls)


@pytest.mark.parametrize("when", ["before", "after"])
@pytest.mark.parametrize("kind", ["expired", "missing_clock", "revoked", "scope", "policy_pin", "verifier_pin"])
def test_external_scope_clock_and_pin_fail_closed(when, kind):
    value = github_policy()
    grant = execution(value)
    initial = trust(value)
    changed = initial
    current: datetime | None = NOW
    args = execution_inputs(grant)
    if kind == "expired":
        current = NOW + timedelta(hours=1)
    elif kind == "missing_clock":
        current = None
    elif kind == "revoked":
        changed = replace(initial, current_revocation_epoch=3)
    elif kind == "scope":
        args["expected_scope"] = replace(grant.scope, campaign_id=identifier(99))
    elif kind == "policy_pin":
        changed = replace(initial, policy_sha256=sha256(b"foreign"))
    elif kind == "verifier_pin":
        changed = replace(initial, verifier_policy_sha256=sha256(b"foreign"))
    provider = TrustProvider(*(initial, changed) if when == "after" else (changed,))
    clocks = iter((NOW, current) if when == "after" else (current, current))
    with pytest.raises(NonproductionAuthorityError):
        authenticator(grant, value, provider=provider, clock=lambda: next(clocks)).authenticate_execution(**args)


def test_refreshed_trust_change_cannot_be_hidden_by_refingerprinting():
    value = github_policy()
    grant = execution(value)
    initial = trust(value)
    modified = initial.verifier_policy_bytes.replace(b'"timeout_seconds":5', b'"timeout_seconds":6')
    changed = replace(initial, verifier_policy_bytes=modified, verifier_policy_sha256=sha256(modified))
    with pytest.raises(NonproductionAuthorityError, match="trust_changed"):
        authenticator(grant, value, provider=TrustProvider(initial, changed)).authenticate_execution(
            **execution_inputs(grant)
        )


def test_clock_cannot_move_backwards_during_verification():
    value = github_policy()
    grant = execution(value)
    clocks = iter((NOW, NOW - timedelta(seconds=1)))
    with pytest.raises(NonproductionAuthorityError, match="clock"):
        authenticator(grant, value, clock=lambda: next(clocks)).authenticate_execution(**execution_inputs(grant))


def test_signature_subject_must_come_from_verifier_and_match_exact_grant():
    value = github_policy()
    grant = execution(value)
    verifier = SignatureDouble(replace(grant.signature_subject(value), document_sha256=sha256(b"other")))
    with pytest.raises(NonproductionAuthorityError, match="signature_subject"):
        authenticator(grant, value, verifier=verifier).authenticate_execution(**execution_inputs(grant))
    with pytest.raises(TypeError):
        authenticator(grant, value).authenticate_execution(
            **dict(execution_inputs(grant), signature_subject=grant.signature_subject(value))
        )


@pytest.mark.parametrize("boundary", ["provider", "clock", "require_trust", "verify"])
def test_dependency_failures_never_escape_with_raw_diagnostics(boundary, monkeypatch):
    value = github_policy()
    grant = execution(value)

    def fail(**_):
        raise RuntimeError("DSN=private; password=secret")

    provider = TrustProvider(trust(value))
    verifier = SignatureDouble(grant.signature_subject(value))

    def clock():
        return NOW

    if boundary == "provider":
        monkeypatch.setattr(provider, "read", fail)
    elif boundary == "clock":
        clock = fail
    else:
        setattr(verifier, boundary, fail)
    with pytest.raises(NonproductionAuthorityError) as caught:
        authenticator(grant, value, provider=provider, clock=clock, verifier=verifier).authenticate_execution(
            **execution_inputs(grant)
        )
    assert "secret" not in str(caught.value) and "private" not in str(caught.value)
    assert caught.value.__suppress_context__


def test_malformed_clock_timezone_cannot_leak_dependency_diagnostics():
    class BrokenTimezone(tzinfo):
        def utcoffset(self, _):
            raise RuntimeError("secret clock service diagnostic")

        def dst(self, _):
            return None

        def tzname(self, _):
            return "broken"

    value = github_policy()
    grant = execution(value)
    clock_value = datetime(2026, 9, 10, 1, 30, tzinfo=BrokenTimezone())
    with pytest.raises(NonproductionAuthorityError) as caught:
        authenticator(grant, value, clock=lambda: clock_value).authenticate_execution(**execution_inputs(grant))
    assert "secret" not in str(caught.value) and caught.value.__suppress_context__


def test_reread_verifies_again_and_never_claims_one_time_consumption():
    value = github_policy()
    grant = execution(value)
    provider = TrustProvider(trust(value))
    verifier = SignatureDouble(grant.signature_subject(value))
    service = authenticator(grant, value, provider=provider, verifier=verifier)
    for _ in range(2):
        assert service.authenticate_execution(**execution_inputs(grant)) == (grant, grant.signature_subject(value))
    assert provider.reads == 4 and sum(kind == "verify" for kind, _ in verifier.calls) == 2


def test_revoked_grant_is_rejected_before_verifier():
    original = github_policy()
    grant = execution(original)
    value = replace(original, revoked_grant_ids=(grant.grant_id,))
    grant = execution(value)
    verifier = SignatureDouble(grant.signature_subject(value))
    with pytest.raises(NonproductionAuthorityError, match="revoked"):
        authenticator(grant, value, verifier=verifier).authenticate_execution(**execution_inputs(grant))
    assert verifier.calls == []


@pytest.mark.parametrize("result", [None, {"status": "PASS"}, True])
def test_caller_shaped_verification_result_is_never_authority(result):
    value = github_policy()
    grant = execution(value)
    with pytest.raises(NonproductionAuthorityError, match="verification_subject"):
        authenticator(grant, value, verifier=SignatureDouble(result)).authenticate_execution(**execution_inputs(grant))
