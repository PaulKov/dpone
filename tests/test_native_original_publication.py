"""Fault-oriented proof that references follow independently verified bytes."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    decode_native_original_binding,
    encode_native_original_binding,
)
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef
from dpone.runtime.native_original_publication import NativeOriginalPublicationError, publish_bound_native_original

D = "sha256:" + "a" * 64
PAYLOAD = b'{"schema":"synthetic.v1"}'


class Providers:
    def __init__(self):
        self.events = []
        self.saved = None
        self.payload = None
        self.failure = None
        self.transform = lambda value: value
        self.reply_transform = lambda value: value
        self.read_result = None
        self.creates = 0

    def event(self, name):
        self.events.append(name)
        if self.failure == name:
            raise RuntimeError("unknown " + name + " acknowledgement")

    def publish(self, *, kind, subject, payload):
        self.event("publish")
        if self.payload is not None and self.payload != payload:
            raise RuntimeError("conflicting immutable original")
        if self.payload is None:
            self.creates += 1
        self.payload = payload
        return ArtifactObjectRef(
            "objects/control",
            "provider-version",
            len(payload),
            "sha256:" + sha256(payload).hexdigest(),
            "kms",
            "2027-01-01T00:00:00Z",
        )

    def bind(self, binding):
        self.event("bind")
        encoded = encode_native_original_binding(binding)
        if self.saved is not None and self.saved != encoded:
            raise RuntimeError("conflicting immutable binding")
        self.saved = encoded
        return self.reply_transform(OriginalRef(binding.locator, binding.payload_sha256))

    def resolve(self, reference, *, expected_subject, expected_kind):
        self.event("resolve")
        assert self.saved is not None
        return self.transform(decode_native_original_binding(self.saved))

    def read(self, exactref, *, expected_subject, expected_kind, max_bytes):
        self.event("read")
        assert exactref.version == "provider-version"
        assert max_bytes >= len(PAYLOAD)
        return self.payload if self.read_result is None else self.read_result


def inputs(provider):
    authority = DbtWorkspaceRuntimeAuthority.build(
        environment="dev",
        release_id=D,
        deployment_id=D,
        release_sha256=D,
        deployment_sha256=D,
        binding_set_sha256=D,
        connection_registry_sha256=D,
        credential_runtime_sha256=D,
    )
    return dict(
        writer=provider,
        reader=provider,
        bindings=provider,
        subject=NativeGenerationOriginalSubject(authority, UUID("12345678-1234-5678-1234-567812345678")),
        kind="generation_stored_file_v1",
        storage_authority=OriginalRef("store/policy", D),
        locator="control/original",
        payload=PAYLOAD,
        max_bytes=1024,
    )


def test_success_requires_ordered_publication_independent_binding_and_readback():
    p = Providers()
    result = publish_bound_native_original(**inputs(p))
    assert result == OriginalRef("control/original", "sha256:" + sha256(PAYLOAD).hexdigest())
    assert p.events == ["publish", "bind", "resolve", "read"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("payload", b" " + PAYLOAD),
        ("payload", bytearray(PAYLOAD)),
        ("payload", b'{"x":1,"x":2}'),
        ("max_bytes", True),
        ("max_bytes", 0),
        ("max_bytes", 1),
        ("kind", "unknown"),
        ("locator", "../unsafe"),
        ("subject", None),
        ("storage_authority", None),
    ],
)
def test_invalid_request_has_no_provider_effect(field, value):
    p = Providers()
    args = {**inputs(p), field: value}
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        publish_bound_native_original(**args)
    assert p.events == []


@pytest.mark.parametrize("stage", ["publish", "bind", "resolve", "read"])
def test_uncertainty_never_retries_a_write_or_returns_success(stage):
    p = Providers()
    p.failure = stage
    with pytest.raises(RuntimeError, match="unknown"):
        publish_bound_native_original(**inputs(p))
    order = ["publish", "bind", "resolve", "read"]
    assert p.events == order[: order.index(stage) + 1]


@pytest.mark.parametrize(
    "field,new",
    [
        ("locator", "other"),
        ("payload_sha256", D),
        ("storage_authority", OriginalRef("store/other", D)),
        ("kind", "generation_storage_root_v1"),
    ],
)
def test_top_level_binding_substitution_fails_before_read(field, new):
    p = Providers()
    p.transform = lambda value: replace(value, **{field: new})
    with pytest.raises(NativeOriginalPublicationError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind", "resolve"]


@pytest.mark.parametrize(
    "field,new",
    [
        ("key", "objects/other"),
        ("version", "other"),
        ("size_bytes", 0),
        ("sha256", D),
        ("encryption_scope", "other"),
        ("retention_until", "2028-01-01T00:00:00Z"),
    ],
)
def test_any_provider_coordinate_substitution_fails_before_read(field, new):
    p = Providers()
    p.transform = lambda value: replace(value, object_ref=replace(value.object_ref, **{field: new}))
    with pytest.raises(NativeOriginalPublicationError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind", "resolve"]


@pytest.mark.parametrize("result", [b'{"wrong":true}', bytearray(PAYLOAD), b"x" * 1025])
def test_readback_requires_exact_bounded_bytes(result):
    p = Providers()
    p.read_result = result
    with pytest.raises(NativeOriginalPublicationError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind", "resolve", "read"]


def test_bind_reply_cannot_substitute_reference():
    p = Providers()
    p.reply_transform = lambda _: OriginalRef("wrong/ref", D)
    with pytest.raises(NativeOriginalPublicationError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind"]


def test_exact_explicit_replay_preserves_existing_version_without_recreation():
    p = Providers()
    args = inputs(p)
    first = publish_bound_native_original(**args)
    assert publish_bound_native_original(**args) == first
    assert p.creates == 1
    assert p.events == ["publish", "bind", "resolve", "read"] * 2


def test_conflicting_replay_leaves_retained_bytes_unchanged():
    p = Providers()
    args = inputs(p)
    publish_bound_native_original(**args)
    with pytest.raises(RuntimeError, match="conflicting"):
        publish_bound_native_original(**{**args, "payload": b'{"different":true}'})
    assert p.payload == PAYLOAD
    assert p.creates == 1


def test_binding_mutation_cannot_replace_expected_snapshot():
    class MutatingBinder(Providers):
        def bind(self, binding):
            object.__setattr__(binding.object_ref, "encryption_scope", "substituted")
            return super().bind(binding)

    p = MutatingBinder()
    with pytest.raises(NativeOriginalPublicationError, match="complete published identity"):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind", "resolve"]


def test_subject_substitution_in_resolved_binding_is_rejected():
    p = Providers()
    p.transform = lambda value: replace(
        value, subject=replace(value.subject, generation_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
    )
    with pytest.raises(NativeOriginalPublicationError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish", "bind", "resolve"]


def test_invalid_writer_result_cannot_reach_binding():
    class BadWriter(Providers):
        def publish(self, **kwargs):
            self.event("publish")
            return None

    p = BadWriter()
    with pytest.raises(ValueError):
        publish_bound_native_original(**inputs(p))
    assert p.events == ["publish"]


def test_exact_limit_is_accepted():
    p = Providers()
    assert publish_bound_native_original(**{**inputs(p), "max_bytes": len(PAYLOAD)}).locator == "control/original"
