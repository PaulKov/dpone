"""Real canonical digests keep policy and legacy configuration identities distinct."""

from hashlib import sha256

import pytest

from dpone.contracts.composition_dispatcher_binding import CompositionDispatcherBinding
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

IDENTIFIER = "10000000-0000-4000-8000-000000000001"
POLICY = canonical_json_bytes({"schema": "test-policy", "listener": "127.0.0.1"})
CONFIGURATION = canonical_json_bytes({"schema": "test-configuration", "policy": strict_json_object(POLICY)})
POLICY_SHA = "sha256:" + sha256(POLICY).hexdigest()
CONFIGURATION_SHA = "sha256:" + sha256(CONFIGURATION).hexdigest()


def test_legacy_three_positional_constructor_preserves_exact_v1_bytes():
    binding = CompositionDispatcherBinding(IDENTIFIER, "dispatcher", CONFIGURATION_SHA)
    original = canonical_json_bytes(
        {
            "schema": "dpone.composition-dispatcher-binding.v1",
            "dispatcher_id": IDENTIFIER,
            "connection_ref": "dispatcher",
            "service_configuration_sha256": CONFIGURATION_SHA,
        }
    )
    assert binding.to_bytes() == original
    assert binding.identity_kind == "configuration" and binding.identity_sha256 == CONFIGURATION_SHA
    assert binding.service_policy_sha256 is None
    assert CompositionDispatcherBinding.from_document(original) == binding


def test_policy_v2_roundtrips_distinct_full_byte_hash_without_relabeling_configuration():
    binding = CompositionDispatcherBinding(IDENTIFIER, "dispatcher", service_policy_sha256=POLICY_SHA)
    original = canonical_json_bytes(
        {
            "schema": "dpone.composition-dispatcher-binding.v2",
            "dispatcher_id": IDENTIFIER,
            "connection_ref": "dispatcher",
            "service_policy_sha256": POLICY_SHA,
        }
    )
    assert binding.to_bytes() == original
    assert binding.identity_kind == "policy" and binding.identity_sha256 == POLICY_SHA
    assert binding.service_configuration_sha256 is None and POLICY_SHA != CONFIGURATION_SHA
    assert CompositionDispatcherBinding.from_document(original) == binding
    assert CompositionDispatcherBinding.from_mapping(strict_json_object(original)) == binding


@pytest.mark.parametrize(
    "configuration,policy",
    [(None, None), (CONFIGURATION_SHA, POLICY_SHA), ("", None), (None, "wrong"), (False, None), (None, 3)],
)
def test_constructor_requires_exactly_one_valid_digest(configuration, policy):
    with pytest.raises(CompositionAdmissionError):
        CompositionDispatcherBinding(IDENTIFIER, "dispatcher", configuration, service_policy_sha256=policy)


@pytest.mark.parametrize(
    "version,field",
    [("v1", "service_policy_sha256"), ("v2", "service_configuration_sha256"), ("v3", "service_policy_sha256")],
)
def test_version_cannot_reinterpret_digest_kind(version, field):
    body = {
        "schema": "dpone.composition-dispatcher-binding." + version,
        "dispatcher_id": IDENTIFIER,
        "connection_ref": "dispatcher",
        field: POLICY_SHA,
    }
    with pytest.raises(CompositionAdmissionError):
        CompositionDispatcherBinding.from_mapping(body)


@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("mutation", ["mixed", "unknown", "missing", "noncanonical", "oversized"])
def test_both_versions_remain_closed_bounded_and_canonical(version, mutation):
    field = "service_configuration_sha256" if version == "v1" else "service_policy_sha256"
    body = {
        "schema": "dpone.composition-dispatcher-binding." + version,
        "dispatcher_id": IDENTIFIER,
        "connection_ref": "dispatcher",
        field: POLICY_SHA,
    }
    if mutation == "mixed":
        body["service_policy_sha256" if version == "v1" else "service_configuration_sha256"] = None
    elif mutation == "unknown":
        body["extra"] = 1
    elif mutation == "missing":
        del body[field]
    document = canonical_json_bytes(body)
    if mutation == "noncanonical":
        document += b"\n"
    elif mutation == "oversized":
        document += b" " * 4096
    with pytest.raises(CompositionAdmissionError):
        CompositionDispatcherBinding.from_document(document)
