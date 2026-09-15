"""Native original provider identity, readback and immutable reconciliation."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.adapters.native_original_store import NativeOriginalStore, NativeOriginalStoreError
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import encode_native_original_storage_authority, encode_native_original_subject
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreUnavailable,
)
from dpone.ports.versioned_artifact_store import VersionedArtifactIoBudget
from tests.test_native_original_storage_authority import authority
from tests.test_native_original_subjects import subjects


class Provider:
    def __init__(self):
        self.events = []
        self.objects = {}
        self.mode = "normal"
        self.head_transform = lambda ref: ref
        self.create_transform = lambda ref: ref
        self.read_transform = lambda payload: payload
        self.after_create = lambda: None
        self.writes = 0

    def create(self, *, key, content, sha256, encryption_scope, retention_until, budget):
        self.events.append(("create", budget))
        if self.mode == "unavailable_before":
            raise ArtifactStoreUnavailable("unknown write outcome")
        if key in self.objects:
            raise ArtifactCreateConflict("immutable key already exists")
        reference = ArtifactObjectRef(
            key, "actual-provider-version", len(content), sha256, encryption_scope, retention_until
        )
        self.objects[key] = (reference, content)
        self.writes += 1
        self.after_create()
        if self.mode == "lost_ack":
            raise ArtifactStoreUnavailable("acknowledgement lost")
        return self.create_transform(reference)

    def head(self, *, key, budget):
        self.events.append(("head", budget))
        entry = self.objects.get(key)
        return self.head_transform(entry[0] if entry else None)

    def read_version(self, *, key, version, budget):
        self.events.append(("read", budget))
        reference, payload = self.objects[key]
        assert reference.version == version
        return self.read_transform(payload)


def original_store(provider, *, budget=None, policy=None, policy_ref=None):
    policy = policy or authority()
    policy_ref = policy_ref or OriginalRef(
        "policy/storage", "sha256:" + sha256(encode_native_original_storage_authority(policy)).hexdigest()
    )
    return NativeOriginalStore(
        provider=provider,
        authority=policy,
        authority_ref=policy_ref,
        budget=budget or VersionedArtifactIoBudget(1000, 100, 500.0),
    )


def test_publish_returns_actual_version_only_after_independent_metadata_and_bytes():
    provider = Provider()
    reference = original_store(provider).publish(
        subject=subjects()[0], kind="generation_stored_file_v1", payload=b'{"a":1}'
    )
    assert reference is not None and reference.version == "actual-provider-version"
    assert [name for name, _ in provider.events] == ["create", "head", "read"]
    assert all(budget.deadline_monotonic == 500.0 for _, budget in provider.events)


PAYLOAD = b'{"a":1}'
KIND = "generation_stored_file_v1"


def publish(store, subject=None, payload=PAYLOAD, kind=KIND):
    return store.publish(subject=subject or subjects()[0], payload=payload, kind=kind)


def test_key_grammar_is_exact_and_binds_subject_kind_and_payload():
    provider = Provider()
    store = original_store(provider)
    subject = subjects()[0]
    reference = publish(store, subject)
    expected = (
        "originals/test/native-originals/v1/"
        + sha256(encode_native_original_subject(subject)).hexdigest()
        + "/generation_stored_file_v1/"
        + sha256(PAYLOAD).hexdigest()
    )
    assert reference.key == expected
    keys = {
        reference.key,
        publish(store, subjects()[1]).key,
        publish(store, kind="generation_storage_root_v1").key,
        publish(store, payload=b'{"a":2}').key,
    }
    assert len(keys) == 4
    assert provider.writes == 4


def test_explicit_exact_replay_retains_same_provider_version_without_overwrite():
    provider = Provider()
    store = original_store(provider)
    first = publish(store)
    provider.events.clear()
    assert publish(store) == first
    assert provider.writes == 1
    assert [name for name, _ in provider.events] == ["create", "head", "head", "read"]


def test_lost_ack_can_only_reconcile_the_same_retained_object():
    provider = Provider()
    provider.mode = "lost_ack"
    reference = publish(original_store(provider))
    assert reference.version == "actual-provider-version" and provider.writes == 1
    assert [name for name, _ in provider.events] == ["create", "head", "head", "read"]


def test_missing_object_after_uncertain_ack_never_triggers_another_create():
    provider = Provider()
    provider.mode = "unavailable_before"
    with pytest.raises(NativeOriginalStoreError, match="valid original"):
        publish(original_store(provider))
    assert [name for name, _ in provider.events] == ["create", "head"]
    assert provider.writes == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("key", "originals/test/wrong"),
        ("version", "another-version"),
        ("size_bytes", 8),
        ("sha256", "sha256:" + "b" * 64),
        ("encryption_scope", "other"),
        ("retention_until", "2026-09-17T00:00:00Z"),
    ],
)
def test_create_reference_coordinate_substitution_cannot_pass(field, value):
    provider = Provider()
    provider.create_transform = lambda ref: replace(ref, **{field: value})
    with pytest.raises(NativeOriginalStoreError):
        publish(original_store(provider))
    assert [name for name, _ in provider.events].count("create") == 1
    assert "read" not in [name for name, _ in provider.events]


@pytest.mark.parametrize(
    "field,value",
    [
        ("key", "originals/test/wrong"),
        ("version", "another-version"),
        ("size_bytes", 8),
        ("sha256", "sha256:" + "b" * 64),
        ("encryption_scope", "other"),
        ("retention_until", "2026-09-17T00:00:00Z"),
    ],
)
def test_independent_head_requires_all_six_coordinates(field, value):
    provider = Provider()
    provider.head_transform = lambda ref: replace(ref, **{field: value})
    with pytest.raises(NativeOriginalStoreError, match="metadata"):
        publish(original_store(provider))
    assert [name for name, _ in provider.events] == ["create", "head"]


@pytest.mark.parametrize("payload", [b'{"a":2}', b'{"a":', b'{"a":1}x', "not bytes", bytearray(PAYLOAD)])
def test_corrupt_or_invalid_readback_denies_success(payload):
    provider = Provider()
    provider.read_transform = lambda original: payload
    with pytest.raises(NativeOriginalStoreError):
        publish(original_store(provider))
    assert provider.writes == 1


@pytest.mark.parametrize("payload", [b'{ "a":1}', b'{"a":1,"a":2}', b"[]", b"NaN", "text", None, b""])
def test_invalid_payload_leaves_provider_untouched(payload):
    provider = Provider()
    with pytest.raises((TypeError, ValueError)):
        publish(original_store(provider), payload=payload)
    assert provider.events == []


@pytest.mark.parametrize("kind", ["wrong", None, 1, True])
def test_invalid_kind_leaves_provider_untouched(kind):
    provider = Provider()
    with pytest.raises((TypeError, ValueError)):
        publish(original_store(provider), kind=kind)
    assert provider.events == []


def test_oversize_rejection_precedes_hashing_or_provider_operations():
    class NotReadableBytes(bytes):
        pass

    provider = Provider()
    store = original_store(provider, budget=VersionedArtifactIoBudget(2, 1, 500.0))
    with pytest.raises(ValueError, match="budget"):
        publish(store)
    with pytest.raises(ValueError):
        publish(store, payload=NotReadableBytes(b"{}"))
    assert provider.events == []


@pytest.mark.parametrize("limit", [0, -1, True, 7.0, None])
def test_read_limit_requires_exact_positive_integer_before_io(limit):
    provider = Provider()
    store = original_store(provider)
    reference = publish(store)
    provider.events.clear()
    with pytest.raises(ValueError):
        store.read(reference, expected_kind=KIND, expected_subject=subjects()[0], max_bytes=limit)
    assert provider.events == []


def test_reader_limit_cannot_expand_policy_or_attempt_and_keeps_deadline():
    provider = Provider()
    store = original_store(
        provider, policy=replace(authority(), max_artifact_bytes=10), budget=VersionedArtifactIoBudget(8, 8, 500.0)
    )
    reference = publish(store)
    provider.events.clear()
    assert store.read(reference, expected_kind=KIND, expected_subject=subjects()[0], max_bytes=1000) == PAYLOAD
    assert all(budget == VersionedArtifactIoBudget(8, 8, 500.0) for _, budget in provider.events)
    provider.events.clear()
    assert store.read(reference, expected_kind=KIND, expected_subject=subjects()[0], max_bytes=7) == PAYLOAD
    assert all(budget == VersionedArtifactIoBudget(7, 7, 500.0) for _, budget in provider.events)
    assert all(name != "create" for name, _ in provider.events)


def test_authority_digest_agreement_is_required_before_io():
    provider = Provider()
    with pytest.raises(ValueError, match="canonical authority"):
        original_store(provider, policy_ref=OriginalRef("policy/storage", "sha256:" + "b" * 64))
    assert provider.events == []


def test_authority_and_subject_are_snapshotted_across_provider_boundaries():
    provider = Provider()
    policy = authority()
    subject = subjects()[0]
    subject_before = encode_native_original_subject(subject)
    store = original_store(provider, policy=policy)
    object.__setattr__(policy, "writer_scope", "changed-after-construction")
    provider.after_create = lambda: object.__setattr__(subject, "generation_id", UUID(int=1))
    reference = publish(store, subject)
    assert reference.encryption_scope == "native/test"
    assert sha256(subject_before).hexdigest() in reference.key


def test_read_rejects_subject_kind_or_prefix_substitution_before_io():
    provider = Provider()
    store = original_store(provider)
    reference = publish(store)
    provider.events.clear()
    for changed_ref, kind, subject in (
        (reference, KIND, subjects()[1]),
        (reference, "generation_storage_root_v1", subjects()[0]),
        (replace(reference, key=reference.key + "/alias"), KIND, subjects()[0]),
    ):
        with pytest.raises(NativeOriginalStoreError, match="subject"):
            store.read(changed_ref, expected_kind=kind, expected_subject=subject, max_bytes=1000)
    assert provider.events == []


def test_read_refuses_noncanonical_bytes_even_if_hash_and_metadata_match():
    provider = Provider()
    store = original_store(provider)
    reference = publish(store)
    wrong = b'{ "a":1}'
    digest = "sha256:" + sha256(wrong).hexdigest()
    reference = replace(
        reference, key=reference.key.rsplit("/", 1)[0] + "/" + digest[7:], sha256=digest, size_bytes=len(wrong)
    )
    provider.objects[reference.key] = (reference, wrong)
    with pytest.raises(NativeOriginalStoreError, match="canonical"):
        store.read(reference, expected_kind=KIND, expected_subject=subjects()[0], max_bytes=1000)


@pytest.mark.parametrize("mode", ["normal", "lost_ack", "replay_conflict"])
@pytest.mark.parametrize("failure", ["head", "read", "corrupt"])
def test_reconciliation_failures_retain_object_and_never_repeat_create(mode, failure):
    provider = Provider()
    store = original_store(provider)
    if mode == "replay_conflict":
        publish(store)
        provider.events.clear()
    else:
        provider.mode = mode

    def unavailable(_value):
        raise ArtifactStoreUnavailable("injected read-only provider failure")

    if failure == "head":
        provider.head_transform = unavailable
    elif failure == "read":
        provider.read_transform = unavailable
    else:
        provider.read_transform = lambda data: b'{"a":9}'
    with pytest.raises(ArtifactStoreUnavailable):
        publish(store)
    assert [name for name, _ in provider.events].count("create") == 1
    assert provider.writes == 1 and len(provider.objects) == 1


def test_constructor_budget_snapshot_cannot_be_expanded_through_caller_alias():
    provider = Provider()
    budget = VersionedArtifactIoBudget(7, 3, 500.0)
    store = original_store(provider, budget=budget)
    object.__setattr__(budget, "max_bytes", 9999)
    object.__setattr__(budget, "deadline_monotonic", 99999.0)
    publish(store)
    assert all(value == VersionedArtifactIoBudget(7, 3, 500.0) for _, value in provider.events)


def test_read_reference_is_snapshotted_before_provider_receives_control():
    provider = Provider()
    store = original_store(provider)
    reference = publish(store)

    def mutate_caller_alias(observed):
        object.__setattr__(reference, "version", "caller-mutated-version")
        return observed

    provider.head_transform = mutate_caller_alias
    assert store.read(reference, expected_subject=subjects()[0], expected_kind=KIND, max_bytes=1000) == PAYLOAD


def test_derived_key_length_is_rejected_before_object_io():
    provider = Provider()
    store = original_store(provider, policy=replace(authority(), artifact_prefix="a" * 4000))
    with pytest.raises(ValueError):
        publish(store)
    assert provider.events == []


def test_bound_runtime_and_native_adapter_use_actual_shared_s3_proof():
    from dataclasses import asdict

    from dpone.adapters.versioned_artifact_s3 import VersionedS3ArtifactStore
    from dpone.contracts.native_originals import NativeOriginalStorageAuthority
    from dpone.services.native_original_publication import publish_bound_native_original
    from tests.test_semantic_refresh_artifact_s3 import _clock, _policy
    from tests.test_versioned_artifact_s3 import Client, RecordingBody

    class ReopenClient(Client):
        def __init__(self):
            super().__init__()
            # Dispose the fixture's placeholder before real GET observations.
            self.body.close()
            self.calls.clear()
            self.issued_bodies = []

        def get_object(self, **kwargs):
            self.body = RecordingBody(PAYLOAD, self.tick)
            self.issued_bodies.append(self.body)
            return super().get_object(**kwargs)

    class Bindings:
        def bind(self, binding):
            self.binding = binding
            return OriginalRef(binding.locator, binding.payload_sha256)

        def resolve(self, reference, *, expected_subject, expected_kind):
            assert self.binding.subject == expected_subject and self.binding.kind == expected_kind
            assert reference == OriginalRef(self.binding.locator, self.binding.payload_sha256)
            return self.binding

    policy = NativeOriginalStorageAuthority(provider="s3", **asdict(_policy(object_lock_mode="COMPLIANCE")))
    policy_ref = OriginalRef(
        "policy/storage", "sha256:" + sha256(encode_native_original_storage_authority(policy)).hexdigest()
    )
    client = ReopenClient()
    client.sha256 = "sha256:" + sha256(PAYLOAD).hexdigest()
    client.lock_mode = "COMPLIANCE"
    provider = VersionedS3ArtifactStore(
        client=client,
        policy=policy,
        bucket=policy.bucket_or_container_authority_id,
        operation_prefix=policy.artifact_prefix,
        clock=_clock,
        monotonic_clock=lambda: client.now,
    )
    originals = original_store(provider, policy=policy, policy_ref=policy_ref)
    binding = Bindings()
    reference = publish_bound_native_original(
        writer=originals,
        reader=originals,
        bindings=binding,
        subject=subjects()[0],
        kind=KIND,
        storage_authority=policy_ref,
        locator="generation/stored-file",
        payload=PAYLOAD,
        max_bytes=1000,
    )
    assert reference == OriginalRef("generation/stored-file", client.sha256)
    assert binding.binding.object_ref.version == "version-1"
    assert client.calls.count("put") == 1
    assert client.calls.count("get") == client.calls.count("close") == 2
    assert all(body.closed_count == 1 for body in client.issued_bodies)
