"""Closed evidence dispatch hashes exact canonical bytes only after validation."""

import json
from dataclasses import FrozenInstanceError, fields, replace
from enum import StrEnum
from functools import partial
from hashlib import sha256
from typing import Any
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_evidence_types import evidence_limit
from dpone.contracts.mssql_sqlclient_departure_execution_evidence import (
    decode_exclusion,
    decode_local_exit,
    decode_result_evidence,
    encode_exclusion,
    encode_local_exit,
    encode_result_evidence,
)
from dpone.contracts.mssql_sqlclient_departure_registration import (
    decode_credential_intent,
    decode_launch_intent,
    decode_registration,
    encode_credential_intent,
    encode_launch_intent,
    encode_registration,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_departure_execution_evidence import execution_values
from tests.test_mssql_sqlclient_departure_ipc import GOLDEN_REQUEST, request
from tests.test_mssql_sqlclient_departure_registration import ERROR, maximal_request, setup_values


def cases(r=None, departure=None):
    r = request() if r is None else r
    return list(
        zip(
            list(Kind),
            (*setup_values(r), *execution_values(r, departure)),
            (
                encode_launch_intent,
                encode_registration,
                encode_credential_intent,
                partial(encode_result_evidence, request=r),
                encode_local_exit,
                encode_exclusion,
            ),
            (
                decode_launch_intent,
                decode_registration,
                decode_credential_intent,
                partial(decode_result_evidence, request=r),
                decode_local_exit,
                decode_exclusion,
            ),
            strict=True,
        )
    )


def record(kind, payload, r=None):
    r = request() if r is None else r
    return SqlClientDepartureEvidenceRecord(
        r.plan.helper_id,
        attempt_identity_digest(r.plan.attempt),
        kind,
        payload,
        r if kind is Kind.RESULT else None,
    )


def test_record_hashes_complete_wrapper():
    r = request()
    payload = encode_launch_intent(setup_values(r)[0])
    value = SqlClientDepartureEvidenceRecord(
        r.plan.helper_id, attempt_identity_digest(r.plan.attempt), Kind.LAUNCH_INTENT, payload
    )
    assert value.receipt.payload_sha256 == sha256(payload).hexdigest()


@pytest.mark.parametrize("kind", list(Kind))
def test_closed_grant_context_validates_all_six_without_changing_legacy_dispatch(kind):
    from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
    from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
        PermissionGrantDepartureEvidenceContext,
    )
    from dpone.contracts.mssql_tds_result import attempt_identity_digest
    from tests.test_mssql_sqlclient_permission_grant_departure import grant_evidence_payloads

    request, _, payloads = grant_evidence_payloads()
    value = SqlClientDepartureEvidenceRecord(
        request.plan.helper_id,
        attempt_identity_digest(request.plan.attempt),
        kind,
        payloads[kind],
        permission_grant_context=PermissionGrantDepartureEvidenceContext(request.plan),
    )
    assert value.receipt.kind is kind


@pytest.mark.parametrize("index", range(6))
def test_independent_exact_schema_and_nested_object_fixture(index):
    from tests.test_mssql_sqlclient_create_departure_codec import GOLDEN

    r = request()
    original = json.loads(GOLDEN_REQUEST)
    subject = dict(helper_id=str(r.plan.helper_id), attempt_sha256=attempt_identity_digest(r.plan.attempt))
    expected = [
        dict(schema="dpone.sqlclient.departure-launch-intent-evidence.v1", plan=original["plan"]),
        dict(
            schema="dpone.sqlclient.departure-registration-evidence.v1",
            **subject,
            launch_intent_sha256="1" * 64,
            startup=original["startup"],
            admission_sha256="f" * 64,
        ),
        dict(
            schema="dpone.sqlclient.departure-credential-intent-evidence.v1",
            **subject,
            registration_sha256="2" * 64,
            request=original,
        ),
        dict(
            schema="dpone.sqlclient.departure-result-evidence.v1",
            **subject,
            credential_intent_sha256="3" * 64,
            result=dict(
                schema="dpone.sqlclient.departure-result.v1",
                departure=json.loads(GOLDEN),
                request_sha256=sha256(GOLDEN_REQUEST).hexdigest(),
            ),
        ),
        dict(
            schema="dpone.sqlclient.departure-local-exit-evidence.v1",
            **subject,
            registration_sha256="2" * 64,
            result_sha256="4" * 64,
            exit=dict(identity=original["startup"]["process"], exit_code=0, reaped=True),
        ),
        dict(
            schema="dpone.sqlclient.departure-exclusion-evidence.v1",
            **subject,
            create_operation_sha256="0" * 64,
            create_result_sha256="1" * 64,
            create_local_exit_sha256="2" * 64,
            result_sha256="3" * 64,
            local_exit_sha256="4" * 64,
        ),
    ][index]
    kind, value, encode, decode = cases(r)[index]
    payload = json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert encode(value) == payload
    assert decode(payload) == value
    received = record(kind, payload, r).receipt
    assert received.payload_sha256 == sha256(payload).hexdigest()
    assert received.byte_count == len(payload)
    assert received.relative_name == (
        f"tds-sqlclient-departure-{subject['attempt_sha256']}-{r.plan.helper_id}-{kind.value}-"
        f"{sha256(payload).hexdigest()}.json"
    )


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_all_six_actual_maximal_nested_codecs_fit_caps(character):
    r, departure = maximal_request(character)
    sizes = {}
    for kind, value, encode, decode in cases(r, departure):
        payload = encode(value)
        sizes[kind.value] = len(payload)
        assert 0 < len(payload) <= evidence_limit(kind)
        assert decode(payload) == value
        receipt = record(kind, payload, r).receipt
        assert receipt.byte_count == len(payload)
        assert len(receipt.relative_name.encode()) <= 255
    print(repr(character), sizes)


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize(
    "mutation",
    [
        "whitespace",
        "key_order",
        "escape",
        "foreign_schema",
        "nan",
        "infinity",
        "trailing",
        "empty",
        "oversize",
        "bytearray",
        "bytes_alias",
        "list",
        "uuid_alias",
    ],
)
def test_every_wrapper_requires_canonical_bounded_bytes(index, mutation):
    kind, value, encode, decode = cases()[index]
    raw = encode(value)
    data = json.loads(raw)
    if mutation == "whitespace":
        payload = raw + b" "
    elif mutation == "key_order":
        payload = json.dumps(dict(reversed(list(data.items()))), ensure_ascii=False, separators=(",", ":")).encode()
    elif mutation == "escape":
        payload = raw.replace(b"dpone", b"\\u0064pone", 1)
    elif mutation == "foreign_schema":
        data["schema"] = "dpone.sqlclient.departure-foreign-evidence.v1"
        payload = canonical_json_bytes(data)
    elif mutation in ("nan", "infinity"):
        payload = raw[:-1] + b',"extra":' + (b"NaN" if mutation == "nan" else b"Infinity") + b"}"
    elif mutation == "trailing":
        payload = raw + b"{}"
    elif mutation == "empty":
        payload = b""
    elif mutation == "oversize":
        payload = b"x" * (evidence_limit(kind) + 1)
    elif mutation == "bytearray":
        payload = bytearray(raw)
    elif mutation == "bytes_alias":

        class BytesAlias(bytes):
            pass

        payload = BytesAlias(raw)
    elif mutation == "list":
        payload = b"[]"
    else:
        target = data["plan"] if kind is Kind.LAUNCH_INTENT else data
        target["helper_id"] = target["helper_id"].replace("-", "")
        payload = canonical_json_bytes(data)
    with pytest.raises(ValueError, match=ERROR):
        decode(payload)
    with pytest.raises(ValueError, match=ERROR):
        record(kind, payload)


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize(
    "field,alias",
    [
        ("helper_id", "44444444-4444-4444-8444-444444444444"),
        ("helper_id", UUID(int=0)),
        ("attempt_sha256", "A" * 64),
        ("attempt_sha256", b"a" * 64),
    ],
)
def test_record_checks_outer_subject_before_payload(index, field, alias, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_evidence as module

    kind, value, encode, _ = cases()[index]
    original = record(kind, encode(value))
    object.__setattr__(original, field, alias)
    calls = []
    monkeypatch.setattr(module, "require_payload", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match=ERROR):
        original.receipt
    assert not calls


@pytest.mark.parametrize("kind_alias", ["result", True, 1, None])
def test_record_kind_checked_before_payload(kind_alias, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_evidence as module

    calls = []
    monkeypatch.setattr(module, "require_payload", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match=ERROR):
        record(kind_alias, b"private-canary")
    assert not calls


def test_record_rejects_equal_foreign_enum():
    class Alias(StrEnum):
        RESULT = "result"

    with pytest.raises(ValueError, match=ERROR):
        record(Alias.RESULT, b"{}")


@pytest.mark.parametrize("index", range(6))
def test_result_context_is_required_exactly_for_result(index):
    kind, value, encode, _ = cases()[index]
    original = record(kind, encode(value))
    contexts: tuple[Any, ...] = (None, {}, True) if kind is Kind.RESULT else (request(), {}, True)
    for context in contexts:
        with pytest.raises(ValueError, match=ERROR):
            replace(original, result_context=context)


@pytest.mark.parametrize("index", range(6))
def test_frozen_original_must_validate_before_receipt_hash(index, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_evidence as module

    kind, value, encode, _ = cases()[index]
    original = record(kind, encode(value))
    object.__setattr__(original, "payload", original.payload + b" ")
    calls = []
    monkeypatch.setattr(module, "sha256", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match=ERROR):
        original.receipt
    assert not calls


@pytest.mark.parametrize("index", range(6))
def test_all_dtos_exact_frozen_slotted_and_wrong_type_encoders(index):
    kind, value, encode, _ = cases()[index]
    original = record(kind, encode(value))
    for dto in (value, original):
        assert not hasattr(dto, "__dict__")
        name = fields(dto)[0].name
        with pytest.raises(FrozenInstanceError):
            setattr(dto, name, getattr(dto, name))

    Alias = type("Alias", (type(value),), {})
    alias = Alias(**{f.name: getattr(value, f.name) for f in fields(value)})
    invalids: tuple[Any, ...] = (None, {}, alias)
    for invalid in invalids:
        with pytest.raises(ValueError, match=ERROR):
            encode(invalid)
    assert "payload=" not in repr(original) and "result_context=" not in repr(original)
    assert "relative_name" not in {f.name for f in fields(original)}


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize("source", range(3))
@pytest.mark.parametrize("target", range(3))
def test_three_helpers_and_attempts_cannot_crossbind(index, source, target):
    requests = []
    for i in range(3):
        r = request()
        attempt = replace(r.plan.attempt, ordinal=i)
        plan = replace(
            r.plan,
            helper_id=UUID(int=i + 1),
            attempt=attempt,
            create_operation=replace(r.plan.create_operation, parent=attempt),
        )
        requests.append(replace(r, plan=plan))
    kind, value, encode, _ = cases(requests[source])[index]
    payload = encode(value)
    if source == target:
        assert record(kind, payload, requests[target]).receipt.byte_count == len(payload)
    else:
        with pytest.raises(ValueError, match=ERROR):
            record(kind, payload, requests[target])


def test_content_links_are_not_parent_ack_authority():
    r = request()
    # These deliberately unrelated references are structurally valid. Only the
    # future parent chain can compare independently retained actual artifact ACKs.
    values = setup_values(r)
    changed = replace(values[1], launch_intent_sha256="9" * 64)
    assert record(Kind.REGISTRATION, encode_registration(changed), r).receipt
    exclusion = replace(execution_values(r)[2], create_result_sha256="8" * 64)
    assert record(Kind.EXCLUSION, encode_exclusion(exclusion), r).receipt


@pytest.mark.parametrize("index", range(6))
def test_all_reference_scalars_revalidated_before_encoding(index):
    class TextAlias(str):
        pass

    kind, _, _, _ = cases()[index]
    for field in fields(cases()[index][1]):
        if not field.name.endswith("sha256") and field.name != "helper_id":
            continue
        aliases = (
            (str(request().plan.helper_id), UUID(int=0), True)
            if field.name == "helper_id"
            else (True, 1, b"a" * 64, "a" * 63, "a" * 65, "A" * 64, "g" * 64, TextAlias("a" * 64))
        )
        for alias in aliases:
            _, value, encode, _ = cases()[index]
            object.__setattr__(value, field.name, alias)
            with pytest.raises(ValueError, match=ERROR):
                encode(value)
    if kind is Kind.LAUNCH_INTENT:
        _, value, encode, _ = cases()[index]
        object.__setattr__(value.plan, "helper_id", str(value.plan.helper_id))
        with pytest.raises(ValueError, match=ERROR):
            encode(value)


@pytest.mark.parametrize("index", range(6))
def test_cap_precedes_any_json_parse(index, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_execution_evidence as execution
    from dpone.contracts import mssql_sqlclient_departure_registration as setup

    kind, _, _, decode = cases()[index]
    calls = []
    for module in (execution, setup):
        monkeypatch.setattr(module, "strict_json_object", lambda *args: calls.append(args))
    for payload in (b"", b"x" * (evidence_limit(kind) + 1), bytearray(b"{}")):
        with pytest.raises(ValueError, match=ERROR):
            decode(payload)
    assert not calls


def object_paths(value, prefix=()):
    """Discover every object in a literal wire fixture, including the root."""
    if type(value) is dict:
        yield prefix
        for key, item in value.items():
            yield from object_paths(item, (*prefix, key))


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate", "shape"])
def test_nested_object_fields_and_shapes_remain_closed(index, mutation):
    from copy import deepcopy

    _, value, encode, decode = cases()[index]
    original = json.loads(encode(value))
    for path in object_paths(original):
        if not path:
            continue  # Every root field is covered by the two codec test modules.
        nested = original
        for key in path:
            nested = nested[key]
        for field in nested:
            data = deepcopy(original)
            target = data
            for key in path:
                target = target[key]
            if mutation == "missing":
                del target[field]
                payload = canonical_json_bytes(data)
            elif mutation == "extra":
                target[field + "_extra"] = "private-canary"
                payload = canonical_json_bytes(data)
            elif mutation == "shape":
                parent = data
                for key in path[:-1]:
                    parent = parent[key]
                parent[path[-1]] = []
                payload = canonical_json_bytes(data)
            else:
                # Duplicate the field in this nested object's exact serialized
                # representation rather than constructing an already deduplicated dict.
                encoded = canonical_json_bytes(target)
                duplicated = encoded[:-1] + b"," + canonical_json_bytes({field: target[field]})[1:-1] + b"}"
                payload = canonical_json_bytes(data).replace(encoded, duplicated, 1)
            with pytest.raises(ValueError, match=ERROR):
                decode(payload)
            if mutation == "shape":
                break


@pytest.mark.parametrize("index", range(6))
def test_valid_alternate_kind_cannot_select_wrong_codec(index):
    kind, value, encode, _ = cases()[index]
    payload = encode(value)
    for other in Kind:
        if other is not kind:
            with pytest.raises(ValueError, match=ERROR):
                record(other, payload)


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize("subject_field", ["helper_id", "attempt_sha256"])
def test_single_subject_component_mismatch_is_not_hidden_by_other_match(index, subject_field):
    kind, value, encode, _ = cases()[index]
    original = record(kind, encode(value))
    changed = UUID(int=7) if subject_field == "helper_id" else "9" * 64
    with pytest.raises(ValueError, match=ERROR):
        replace(original, **{subject_field: changed})


def v2_cases(r=None, departure=None):
    from dpone.contracts.mssql_sqlclient_departure_ipc_v2_codec import make_departure_result_v2
    from dpone.contracts.mssql_sqlclient_departure_versioned_payloads import (
        SqlClientDepartureCredentialIntentV2,
        SqlClientDepartureLaunchIntentV2,
        SqlClientDepartureResultEvidenceV2,
        decode_credential_intent_v2,
        decode_launch_intent_v2,
        decode_result_evidence_v2,
        encode_credential_intent_v2,
        encode_launch_intent_v2,
        encode_result_evidence_v2,
    )
    from tests.mssql_sqlclient_departure_v2_fixtures import sample
    from tests.test_mssql_sqlclient_departure_ipc_v2 import request as request_v2

    r = request_v2() if r is None else r
    departure = sample() if departure is None else departure
    subject = r.plan.helper_id, attempt_identity_digest(r.plan.attempt)
    return (
        (
            Kind.LAUNCH_INTENT,
            SqlClientDepartureLaunchIntentV2(r.plan),
            encode_launch_intent_v2,
            decode_launch_intent_v2,
        ),
        (
            Kind.CREDENTIAL_INTENT,
            SqlClientDepartureCredentialIntentV2(*subject, "2" * 64, r),
            encode_credential_intent_v2,
            decode_credential_intent_v2,
        ),
        (
            Kind.RESULT,
            SqlClientDepartureResultEvidenceV2(*subject, "3" * 64, make_departure_result_v2(r, departure)),
            partial(encode_result_evidence_v2, request=r),
            partial(decode_result_evidence_v2, request=r),
        ),
    )


@pytest.mark.parametrize("index", range(3))
def test_v2_nested_payload_roundtrip_and_v1_rejection(index):
    from tests.test_mssql_sqlclient_departure_ipc_v2 import request as request_v2

    r = request_v2()
    kind, value, encode, decode = v2_cases(r)[index]
    body = encode(value)
    assert decode(body) == value
    assert record(kind, body, r).receipt.payload_sha256 == sha256(body).hexdigest()
    old_kind, old, old_encode, old_decode = cases()[[0, 2, 3][index]]
    assert old_kind is kind
    for action in (
        lambda: old_encode(value),
        lambda: encode(old),
        lambda: old_decode(body),
        lambda: decode(old_encode(old)),
    ):
        with pytest.raises(ValueError):
            action()
    if kind is Kind.RESULT:
        with pytest.raises(ValueError):
            record(kind, body, request())
        with pytest.raises(ValueError):
            record(kind, old_encode(old), r)


@pytest.mark.parametrize("index", range(3))
@pytest.mark.parametrize("mutation", ["unknown", "schema", "missing", "duplicate", "whitespace"])
def test_v2_closed_evidence_shapes(index, mutation):
    _, value, encode, decode = v2_cases()[index]
    raw = encode(value)
    data = json.loads(raw)
    if mutation == "unknown":
        data["unexpected"] = 1
    elif mutation == "schema":
        data["schema"] = data["schema"].replace("v2", "v1")
    elif mutation == "missing":
        data.pop(next(k for k in data if k != "schema"))
    elif mutation == "duplicate":
        raw = raw[:-1] + b',"schema":' + json.dumps(data["schema"]).encode() + b"}"
    else:
        raw += b" "
    if mutation in ("unknown", "schema", "missing"):
        raw = canonical_json_bytes(data)
    with pytest.raises(ValueError):
        decode(raw)


@pytest.mark.parametrize("index", range(3))
def test_v2_evidence_limits_before_parser(index, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_versioned_payloads as module

    kind, _, _, decode = v2_cases()[index]
    calls = []
    monkeypatch.setattr(module, "strict_json_object", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        decode(b"x" * (evidence_limit(kind) + 1))
    assert calls == []


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
@pytest.mark.parametrize("escaped_path", [False, True])
def test_actual_v2_evidence_and_private_maxima(character, escaped_path, record_property):
    from dpone.adapters.mssql_tds_coordinator_connection import TdsConnectionMaterial
    from dpone.app.mssql_sqlclient_departure_request import (
        SqlClientDepartureCredentialsV2,
        encode_departure_credentials_v2,
    )
    from tests.test_mssql_sqlclient_departure_ipc_v2 import maximum_request
    from tests.test_mssql_sqlclient_departure_request import decode_v2

    r, d = maximum_request(character)
    if escaped_path:
        root = "/" + '"' * 4095
        # Codepoint-bounded attempt fields have a different maximum from SQL
        # UTF-16 identifiers and UTF-8-bounded paths: combine all three factors.
        attempt = replace(
            r.plan.attempt,
            target_key="\U0010ffff" * 256,
            run_id="\U0010ffff" * 256,
            schema="\U0010ffff" * 128,
            table="\U0010ffff" * 128,
        )
        owner = replace(r.plan.ownership, owner="\U0010ffff" * 256)
        operation = replace(r.plan.create_operation, parent=attempt)
        r = replace(
            r,
            plan=replace(r.plan, package_root=root, attempt=attempt, ownership=owner, create_operation=operation),
            startup=replace(r.startup, package_root=root),
        )
    lengths = {}
    for kind, value, encode, decode in v2_cases(r, d):
        raw = encode(value)
        assert len(raw) <= evidence_limit(kind)
        assert decode(raw) == value
        assert record(kind, raw, r).receipt.byte_count == len(raw)
        lengths[kind.value] = len(raw)
    material = TdsConnectionMaterial(
        "x" * 255, 65535, r.plan.database.name, r.plan.observer_admission.login.name, "\x01" * 16384
    )
    value = SqlClientDepartureCredentialsV2(request=r, connection_material=material)
    private = encode_departure_credentials_v2(value)
    assert len(private) <= 196608
    assert private.count(b"\\u0001") == 16384
    assert decode_v2(private, r) == value
    lengths["private_credentials"] = len(private)
    # Only lengths are retained; the synthetic private envelope is never logged.
    record_property("actual_v2_lengths", json.dumps(lengths, sort_keys=True))


def v2_original_leaves():
    from tests.mssql_sqlclient_departure_v2_fixtures import leaves

    for index, (_, value, _, _) in enumerate(v2_cases()):
        for path, scalar in leaves(value):
            yield index, path, scalar


@pytest.mark.parametrize("index,path,scalar", list(v2_original_leaves()))
def test_v2_evidence_original_aliases_precede_wrapper_serialization(index, path, scalar, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_versioned_payloads as module
    from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt

    _, value, encode, _ = v2_cases()[index]
    invalid = corrupt(value, path, alias(scalar))
    calls = []
    monkeypatch.setattr(module, "canonical_json_bytes", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        encode(invalid)
    assert calls == []


def observe_record(index=0, r=None):
    """Independently supplied context, including plan-only prelaunch intent."""
    from tests.test_mssql_sqlclient_observe_departure_evidence import chain

    r, _, payloads = chain(r)
    kind = tuple(Kind)[index]
    context = {"observe_plan": r.plan} if kind is Kind.LAUNCH_INTENT else {"observe_request": r}
    return SqlClientDepartureEvidenceRecord(
        r.plan.helper_id,
        attempt_identity_digest(r.plan.attempt),
        kind,
        payloads[index],
        observer_admission=r.plan.observer_admission,
        **context,
    )


@pytest.mark.parametrize("index", range(6))
def test_observe_record_all_six_exact_context(index):
    from tests.test_mssql_sqlclient_observe_departure_evidence import chain

    r, _, payloads = chain()
    value = observe_record(index, r)
    assert value.receipt.payload_sha256 == sha256(payloads[index]).hexdigest()
    assert value.receipt.byte_count == len(payloads[index])
    assert all(
        name + "=" not in repr(value) for name in ("payload", "observe_plan", "observe_request", "observer_admission")
    )


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize(
    "mutation", ["missing", "partial", "conflict", "create", "admission", "subject", "payload", "cap"]
)
def test_observe_record_closed_context(index, mutation):
    from tests.test_mssql_sqlclient_observe_departure_contract import request as observe_request

    value = observe_record(index)
    r = observe_request()
    if mutation == "missing":
        changes = dict(observe_plan=None, observe_request=None, observer_admission=None)
        # Shared registration/exit schemas remain valid CREATE records without
        # supplied OBSERVE context; provenance belongs to the composition.
        if index in (1, 4):
            assert replace(value, **changes).receipt == value.receipt
            return
    elif mutation == "partial":
        changes = dict(observer_admission=None)
    elif mutation == "conflict":
        changes = dict(observe_request=r) if index == 0 else dict(observe_plan=r.plan)
    elif mutation == "create":
        changes = dict(result_context=request())
    elif mutation == "admission":
        changes = dict(
            observer_admission=replace(
                r.plan.observer_admission,
                login=replace(r.plan.observer_admission.login, name="other", original_name="other"),
            )
        )
    elif mutation == "subject":
        changes = dict(helper_id=UUID(int=99))
    elif mutation == "payload":
        changes = dict(payload=value.payload + b" ")
    else:
        changes = dict(payload=b"x" * (evidence_limit(value.kind) + 1))
    with pytest.raises(ValueError):
        replace(value, **changes)


@pytest.mark.parametrize("index", range(6))
def test_observe_context_raw_nested_aliases_before_hash(index, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_evidence as module
    from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves

    original = observe_record(index)
    context_name = "observe_plan" if index == 0 else "observe_request"
    calls = []
    monkeypatch.setattr(module, "sha256", lambda *args: calls.append(args))
    for name in (context_name, "observer_admission"):
        context = getattr(original, name)
        for path, scalar in leaves(context):
            value = observe_record(index)
            object.__setattr__(value, name, corrupt(context, path, alias(scalar)))
            with pytest.raises(ValueError):
                value.receipt
    assert calls == []


@pytest.mark.parametrize("index", [1, 4])
def test_observe_shared_schema_checks_actual_registered_process(index):
    from tests.test_mssql_sqlclient_observe_departure_contract import request as observe_request

    r = observe_request()
    value = observe_record(index, r)
    changed = replace(r, startup=replace(r.startup, process=replace(r.startup.process, pid=r.startup.process.pid + 1)))
    with pytest.raises(ValueError):
        replace(value, observe_request=changed)


def test_create_signatures_remain_five_positional_with_keyword_only_observe():
    from inspect import Parameter, signature

    parameters = list(signature(SqlClientDepartureEvidenceRecord).parameters.values())
    assert [p.name for p in parameters[:5]] == ["helper_id", "attempt_sha256", "kind", "payload", "result_context"]
    assert all(p.kind is Parameter.POSITIONAL_OR_KEYWORD for p in parameters[:5])
    assert all(p.kind is Parameter.KEYWORD_ONLY and p.default is None for p in parameters[5:])
    for index in range(6):
        kind, value, encode, _ = cases()[index]
        original = record(kind, encode(value))
        with pytest.raises(ValueError):
            replace(original, observer_admission=observe_record().observer_admission)


def test_observe_launch_needs_no_startup_or_request(monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_evidence as module
    from dpone.contracts.mssql_sqlclient_observe_departure_evidence import encode_observe_departure_launch_intent
    from tests.test_mssql_sqlclient_observe_departure_contract import request as observe_request

    plan = observe_request().plan
    raw = encode_observe_departure_launch_intent(plan, observer_admission=plan.observer_admission)
    monkeypatch.setattr(
        module,
        "decode_observe_departure_evidence",
        lambda *args, **kwargs: pytest.fail("request path used before startup"),
    )
    value = SqlClientDepartureEvidenceRecord(
        plan.helper_id,
        attempt_identity_digest(plan.attempt),
        Kind.LAUNCH_INTENT,
        raw,
        observe_plan=plan,
        observer_admission=plan.observer_admission,
    )
    assert value.observe_request is None and value.receipt.payload_sha256 == sha256(raw).hexdigest()
