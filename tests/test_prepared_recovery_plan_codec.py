"""A saved plan confirms exact scope; decoding never supplies a dispatch permit."""

import json
from dataclasses import replace
from importlib import import_module

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    QUALITY_SCHEMA_VERSION,
    AuthorityPhase,
    AuthorityRecord,
    canonical_json,
)
from dpone.contracts.publication_authority_binding import (
    PublicationAuthorityBinding,
    publication_binding_digest,
)
from tests.test_native_prepared_recovery import plan, rig

_BINDING = PublicationAuthorityBinding("mssql", "metadata", "Example_System", "dbo", "example", "test")
_ENDPOINT = "e" * 64


def api():
    return import_module("dpone.contracts.prepared_recovery_plan")


def operator_plan():
    binding = publication_binding_digest(_BINDING, endpoint_identity=_ENDPOINT)
    r = rig(binding=binding)
    nested = plan(r)
    return api().PreparedRecoveryOperatorPlan(_BINDING, _ENDPOINT, "b" * 64, "analytics", "c" * 64, nested)


def decode(value):
    return api().decode_recovery_operator_plan(canonical_json(value).encode())


def test_canonical_roundtrip_preserves_exact_native_origin_and_separates_confirmations():
    value = operator_plan()
    restored = api().decode_recovery_operator_plan(value.payload.encode())
    assert restored == value
    assert restored.recovery.preparation.prepared.version == 1
    assert restored.recovery.preparation.prepared_at.isoformat() == "1970-01-01T00:01:40+00:00"
    assert restored.confirms(value.digest)
    assert not restored.confirms(value.recovery.digest)
    assert "permit" not in json.loads(value.payload)


@pytest.mark.parametrize("confirmation", [None, 7, "", "é" * 64, "A" * 64, "f" * 64])
def test_malformed_or_different_confirmation_is_not_accepted(confirmation):
    assert not operator_plan().confirms(confirmation)


@pytest.mark.parametrize(
    "field,value",
    [("context_subject", "d" * 64), ("sink_endpoint_identity", "d" * 64), ("sink_connection_ref", "other")],
)
def test_operator_confirmation_binds_context_and_sink(field, value):
    original = operator_plan()
    changed = replace(original, **{field: value})
    assert changed.digest != original.digest
    assert not changed.confirms(original.digest)


@pytest.mark.parametrize(
    "raw", [b"", b"{}", b"[]", b"null", b"\xff", b" " * (1024 * 1024 + 1), b'{"x":NaN}', b'{"x":Infinity}']
)
def test_invalid_or_oversized_input_fails_with_fixed_redacted_reason(raw):
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        api().decode_recovery_operator_plan(raw)


@pytest.mark.parametrize(
    "transform",
    [
        lambda raw: b" " + raw,
        lambda raw: raw + b"\n",
        lambda raw: b'{"contract":"duplicate",' + raw[1:],
        lambda raw: raw.replace(b'"current":', b'"current":null,"current":', 1),
    ],
)
def test_noncanonical_or_duplicate_keys_are_rejected(transform):
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        api().decode_recovery_operator_plan(transform(operator_plan().payload.encode()))


@pytest.mark.parametrize(
    "path,value",
    [
        (("contract",), "future"),
        (("surprise",), "synthetic-sensitive-detail"),
        (("binding", "environment"), "other"),
        (("endpoint_identity",), "d" * 64),
        (("context_subject",), "bad"),
        (("sink_connection_ref",), "\nprivate"),
        (("sink_endpoint_identity",), True),
        (("recovery", "contract"), "future"),
        (("recovery", "plan", "preparation", "prepared_at"), "1970-01-01T00:01:40"),
        (("recovery", "plan", "preparation", "prepared_at"), "1970-01-01T01:01:40+01:00"),
        (("recovery", "plan", "preparation", "prepared", "version"), True),
        (("recovery", "plan", "preparation", "prepared", "version"), 0),
        (("recovery", "plan", "preparation", "prepared", "record", "phase"), "DISPATCHING"),
        (("recovery", "plan", "preparation", "current", "version"), 2),
        (("recovery", "plan", "safety", "preparation_payload_sha256"), "d" * 64),
        (("recovery", "plan", "safety", "histories"), []),
        (("recovery", "plan", "safety", "freeze", "excluded_writers"), []),
        (("recovery", "plan", "dispatch_query_digest"), "bad"),
        (("recovery", "plan", "dispatch_token"), ""),
    ],
)
def test_changed_or_malformed_nested_identity_is_rejected_without_echo(path, value):
    document = json.loads(operator_plan().payload)
    current = document
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = value
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        decode(document)


def test_expired_but_valid_historical_plan_decodes_without_claiming_fresh_safety():
    # Observation is at epoch130, freeze expires200. Decoding must not consult
    # current wall time or silently replace the original evidence.
    value = operator_plan()
    assert api().decode_recovery_operator_plan(value.payload.encode()).recovery.safety.freeze.expires_at == 200


def test_deeply_nested_input_is_bounded_failure_not_recursion_escape():
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        api().decode_recovery_operator_plan(b"[" * 2000 + b"]" * 2000)


def test_changed_dispatch_token_is_rejected_even_with_recanonicalized_bytes():
    document = json.loads(operator_plan().payload)
    document["recovery"]["plan"]["dispatch_token"] = "different-nonempty-intent"
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        decode(document)


@pytest.mark.parametrize("value", [True, -1, 0, 2**63, "1", 1.0])
@pytest.mark.parametrize("field", ["version", "staged_rows", "native_port"])
def test_integer_coercion_and_out_of_range_counts_rejected(field, value):
    document = json.loads(operator_plan().payload)
    nested = document["recovery"]["plan"]
    if field == "version":
        nested["preparation"]["prepared"][field] = value
        nested["preparation"]["current"][field] = value
    elif field == "staged_rows":
        nested["preparation"]["prepared"]["record"][field] = value
        nested["preparation"]["current"]["record"][field] = value
    else:
        nested["safety"]["inventory"]["replicas"][0][field] = value
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        decode(document)


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("binding",),
        ("recovery", "plan"),
        ("recovery", "plan", "preparation"),
        ("recovery", "plan", "preparation", "prepared", "record"),
        ("recovery", "plan", "safety", "freeze"),
    ],
)
@pytest.mark.parametrize("change", ["unknown", "missing"])
def test_every_nested_object_is_closed(path, change):
    document = json.loads(operator_plan().payload)
    current = document
    for key in path:
        current = current[key]
    if change == "unknown":
        current["synthetic_unknown"] = 1
    else:
        del current[next(iter(current))]
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        decode(document)


@pytest.mark.parametrize("change", ["duplicate", "missing", "gaps", "pending", "short", "writers"])
def test_incomplete_saved_observation_rejected(change):
    document = json.loads(operator_plan().payload)
    safety = document["recovery"]["plan"]["safety"]
    if change == "duplicate":
        safety["histories"].append(safety["histories"][0])
    elif change == "missing":
        safety["histories"].pop()
    elif change == "gaps":
        safety["histories"][0]["gaps"] = [[110, 120]]
    elif change == "pending":
        safety["histories"][0]["pending_requests"] = ["unknown"]
    elif change == "short":
        safety["histories"][0]["history_from"] = 101
    else:
        safety["freeze"]["excluded_writers"] = ["different"]
    with pytest.raises(ValueError, match="^invalid recovery operator plan$"):
        decode(document)


def test_decoded_plan_executes_shared_service_and_completed_resume_never_redispatches():
    binding = publication_binding_digest(_BINDING, endpoint_identity=_ENDPOINT)
    r = rig(binding=binding)
    wrapped = api().PreparedRecoveryOperatorPlan(_BINDING, _ENDPOINT, "b" * 64, "analytics", "c" * 64, plan(r))
    decoded = api().decode_recovery_operator_plan(wrapped.payload.encode()).recovery
    receipt = r.service.execute(decoded, confirmation_digest=decoded.digest)
    assert receipt.authority.phase.value == "COMPLETED"
    assert len(r.catalog.calls) == 2
    assert r.service.execute(decoded, confirmation_digest=decoded.digest) == receipt
    assert len(r.catalog.calls) == 2


def test_decoded_plan_cannot_supply_a_permit_after_unknown_cas():
    from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationStatus, ClusterPublicationError

    binding = publication_binding_digest(_BINDING, endpoint_identity=_ENDPOINT)
    r = rig(binding=binding)
    wrapped = api().PreparedRecoveryOperatorPlan(_BINDING, _ENDPOINT, "b" * 64, "analytics", "c" * 64, plan(r))
    decoded = api().decode_recovery_operator_plan(wrapped.payload.encode()).recovery
    r.authority.outcome = AuthorityMutationStatus.OUTCOME_UNKNOWN
    with pytest.raises(ClusterPublicationError):
        r.service.execute(decoded, confirmation_digest=decoded.digest)
    assert r.catalog.calls == []
    assert r.authority.mutations == 1


def test_later_preparation_revision_and_microsecond_origin_are_not_normalized():
    binding = publication_binding_digest(_BINDING, endpoint_identity=_ENDPOINT)
    r = rig(binding=binding)
    record = replace(r.authority.prepared.record, dispatch_epoch=8)
    r.authority.prepared = r.authority.current = replace(r.authority.prepared, record=record, version=14)
    nested = r.service.plan(target="target", operation_id=record.operation_id, expected_version=14)
    nested = replace(
        nested,
        preparation=replace(nested.preparation, prepared_at=nested.preparation.prepared_at.replace(microsecond=123456)),
    )
    wrapped = api().PreparedRecoveryOperatorPlan(_BINDING, _ENDPOINT, "b" * 64, "analytics", "c" * 64, nested)
    decoded = api().decode_recovery_operator_plan(wrapped.payload.encode()).recovery
    assert decoded.preparation.prepared.version == 14
    assert decoded.preparation.prepared.record.dispatch_epoch == 8
    assert decoded.preparation.prepared_at.isoformat() == "1970-01-01T00:01:40.123456+00:00"
    assert decoded.digest == nested.digest


def test_first_publication_has_no_synthetic_predecessor():
    binding = publication_binding_digest(_BINDING, endpoint_identity=_ENDPOINT)
    r = rig(binding=binding)
    r.catalog.old = None  # Physical pre-publication generation, not post-dispatch loss.
    r.authority.prepared = r.authority.current = replace(
        r.authority.prepared, record=replace(r.authority.prepared.record, predecessor=None)
    )
    wrapped = api().PreparedRecoveryOperatorPlan(_BINDING, _ENDPOINT, "b" * 64, "analytics", "c" * 64, plan(r))
    decoded = api().decode_recovery_operator_plan(wrapped.payload.encode())
    assert decoded.recovery.preparation.prepared.record.predecessor is None


def test_alias_rotation_changes_operator_scope_without_changing_native_binding():
    value = operator_plan()
    changed = replace(value, binding=replace(value.binding, connection_ref="rotated_metadata"))
    assert changed.recovery.digest == value.recovery.digest
    assert not changed.confirms(value.digest)


@pytest.mark.parametrize("kind", ["native", "quality", "retired"])
def test_record_owned_codec_preserves_legacy_runtime_decode_contract(kind):
    from dpone.runtime.state.mssql_publication_envelope import decode_envelope

    record = operator_plan().recovery.preparation.prepared.record
    if kind == "quality":
        record = replace(record, schema_version=QUALITY_SCHEMA_VERSION, quality_evidence="opaque-not-authenticated")
    elif kind == "retired":
        record = replace(record, phase=AuthorityPhase.RETIRED_UNPUBLISHED)
    restored = AuthorityRecord.from_payload(record.payload.encode())
    assert restored == record == decode_envelope(record.payload.encode())
    assert restored.payload_sha256 == record.payload_sha256


@pytest.mark.parametrize(
    "transform",
    [
        lambda raw: b" " + raw,
        lambda raw: raw + b"\n",
        lambda raw: raw.replace(b'"dispatch_epoch":0', b'"dispatch_epoch":true'),
        lambda raw: raw.replace(
            b'"schema_version":"dpone.clickhouse.cluster-full-refresh.v1"', b'"schema_version":"future"'
        ),
    ],
)
def test_record_codec_and_runtime_wrapper_reject_the_same_malformed_payload(transform):
    from dpone.runtime.state.mssql_publication_envelope import decode_envelope

    raw = transform(operator_plan().recovery.preparation.prepared.record.payload.encode())
    with pytest.raises(ValueError):
        AuthorityRecord.from_payload(raw)
    with pytest.raises(ValueError):
        decode_envelope(raw)
