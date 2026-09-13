"""Acyclic policy/bootstrap originals; no deployment or enrollment is certified."""

from dataclasses import FrozenInstanceError
from hashlib import sha256

import pytest

from dpone.app.composition_dispatcher_service_config import decode_dispatcher_service_config
from dpone.app.composition_dispatcher_service_policy import decode_dispatcher_service_policy
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import runtime_connection_authority_subject
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_dispatcher_service_config import DIGEST, document


def digest(raw):
    return "sha256:" + sha256(raw).hexdigest()


def policy_document():
    body = document()
    body.pop("authorities")
    body.pop("supervisor_enrollment_sha256")
    return body | {
        "schema": "dpone.composition-dispatcher-service-policy.v1",
        "bootstrap_file": "/var/lib/dpone-bootstrap/startup/bootstrap.json",
        "startup_timeout_seconds": 120,
    }


def decode_policy(body=None, **overrides):
    raw = canonical_json_bytes(policy_document() if body is None else body)
    kwargs = dict(expected_sha256=digest(raw), bootstrap_uid=1200, bootstrap_gid=1201)
    return decode_dispatcher_service_policy(raw, **(kwargs | overrides))


def decode_bootstrap(body):
    raw = canonical_json_bytes(body)
    return decode_dispatcher_service_config(raw, expected_sha256=digest(raw), bootstrap_uid=1200, bootstrap_gid=1201)


def bootstrap_document():
    policy = decode_policy()
    registry = {
        "binding": {
            "schema": "dpone.composition-dispatcher-binding.v2",
            "dispatcher_id": policy.dispatcher_id,
            "connection_ref": "dispatcher",
            "service_policy_sha256": policy.sha256,
        }
    }
    authority = runtime_connection_authority_subject(
        environment="test",
        release_id=DIGEST,
        deployment_id="deployment",
        release_sha256=DIGEST,
        deployment_sha256=DIGEST,
        binding_set_sha256=DIGEST,
        connection_registry_sha256=digest(canonical_json_bytes(registry)),
        credential_runtime_sha256=DIGEST,
    )
    context = {"runtime_authority_sha256": authority, "policy_sha256": policy.sha256}
    return {
        "schema": "dpone.composition-dispatcher-service.v3",
        "policy": policy_document(),
        "supervisor_enrollment_sha256": digest(b"independently observed enrollment"),
        "authorities": {
            authority: next(iter(document()["authorities"].values()))
            | {"context_sha256": digest(canonical_json_bytes(context))}
        },
    }


def test_real_hash_chain_needs_no_fixed_point():
    body = bootstrap_document()
    result = decode_bootstrap(body)
    policy = decode_policy()
    assert result.service_policy == policy
    assert result.service_policy_sha256 == policy.sha256
    assert result.binding_identity_kind == "policy"
    assert result.binding_identity_sha256 == policy.sha256
    assert result.configuration_sha256 == result.sha256 == digest(canonical_json_bytes(body))
    assert result.sha256 != policy.sha256
    assert result.capture_root_identity == policy.capture_root_identity
    with pytest.raises(FrozenInstanceError):
        policy.startup_timeout_seconds = 5


@pytest.mark.parametrize("field", ["supervisor_enrollment_sha256", "authorities"])
def test_bootstrap_changes_never_redefine_policy(field):
    body = bootstrap_document()
    original = decode_bootstrap(body)
    if field == "authorities":
        key = next(iter(body[field]))
        body[field][key]["context_sha256"] = digest(b"new protected context")
    else:
        body[field] = digest(b"new observed enrollment")
    changed = decode_bootstrap(body)
    assert changed.sha256 != original.sha256
    assert changed.service_policy_sha256 == original.service_policy_sha256


def test_policy_change_rotates_policy_and_bootstrap():
    body = bootstrap_document()
    original = decode_bootstrap(body)
    body["policy"]["execution_timeout_seconds"] = 899
    changed = decode_bootstrap(body)
    assert changed.sha256 != original.sha256
    assert changed.service_policy_sha256 != original.service_policy_sha256


def test_legacy_configuration_hash_meaning_unchanged():
    raw = canonical_json_bytes(document())
    result = decode_dispatcher_service_config(raw, expected_sha256=digest(raw), bootstrap_uid=1200, bootstrap_gid=1201)
    assert result.service_policy is None and result.service_policy_sha256 is None
    assert result.binding_identity_kind == "configuration"
    assert result.binding_identity_sha256 == result.configuration_sha256 == digest(raw)
    assert result.document == raw


@pytest.mark.parametrize("field", sorted(policy_document()))
def test_policy_rejects_missing_fields(field):
    body = policy_document()
    del body[field]
    with pytest.raises(CompositionAdmissionError):
        decode_policy(body)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("startup_timeout_seconds", True),
        ("startup_timeout_seconds", 0),
        ("startup_timeout_seconds", 901),
        ("bootstrap_file", "relative"),
        ("bootstrap_file", "/path/../bootstrap.json"),
        ("bootstrap_file", "/var/lib/dpone/context.json\n"),
        ("authorities", {}),
        ("supervisor_enrollment_sha256", DIGEST),
        ("schema", "dpone.composition-dispatcher-service.v2"),
    ],
)
def test_policy_rejects_invalid_or_cyclic_fields(field, bad):
    with pytest.raises(CompositionAdmissionError):
        decode_policy(policy_document() | {field: bad})


@pytest.mark.parametrize("field", ["schema", "policy", "supervisor_enrollment_sha256", "authorities"])
def test_bootstrap_rejects_missing_fields(field):
    body = bootstrap_document()
    del body[field]
    with pytest.raises(CompositionAdmissionError):
        decode_bootstrap(body)


def test_policy_requires_exact_hash_and_bootstrap_identity():
    for overrides in ({"expected_sha256": DIGEST}, {"bootstrap_uid": 1201}, {"bootstrap_gid": 1200}):
        with pytest.raises(CompositionAdmissionError):
            decode_policy(**overrides)
