"""Pure transcript fixtures confer no SQL, process or delivery authority."""

from dataclasses import asdict, replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_permission_grant import (
    PERMISSIONS,
    SqlClientDirectPermission,
    SqlClientPermissionGrantEvidence,
    encode_permission_grant_evidence,
    encode_permission_grant_request,
    permission_grant_digest,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireBinding,
    PermissionWireState,
    checked_physical_total,
    decode_permission_message,
    encode_permission_message,
    frame_permission_payload,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionWireKind as K,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorGrant, coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import (
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsSchemaObservation,
    authority_digest,
    encode_authority,
)
from dpone.contracts.mssql_tds_coordinator_codec import coordinator_identity_body
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, encode_session_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_permission_grant import request
from tests.test_mssql_tds_coordinator import PROCESS, identity
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_session import CONNECTION, NONCE, STAMP


def fixture():
    req = request()
    op = replace(
        identity(), parent=req.parent, command=TdsCoordinatorCommand.GRANT, command_sha256=permission_grant_digest(req)
    )
    startup = TdsCoordinatorStartup(PROCESS, op.implementation_sha256, "/admitted", NONCE)
    binding = PermissionWireBinding(req, op, startup, OWNER, 123456789)
    session = TdsRemoteSessionIdentity(CONNECTION, 72, STAMP, STAMP, NONCE, b"a" * 32)
    authority = TdsCoordinatorAuthority(
        coordinator_identity_digest(op),
        OWNER,
        PROCESS,
        op.implementation_sha256,
        session,
        TdsDatabaseObservation("db", 5, UUID(int=5)),
        TdsSchemaObservation(1, "schema"),
        TdsLockObservation(0),
    )
    grant = TdsCoordinatorGrant(
        coordinator_identity_digest(op), OWNER, PROCESS, session, authority_digest(authority), UUID(int=99)
    )
    rows = tuple(
        SqlClientDirectPermission(
            1,
            req.stage.object_id,
            0,
            None,
            "G",
            p,
            req.writer.principal_id,
            req.writer.name,
            req.writer.sid,
            "SQL_USER",
            req.management.principal_id,
            req.management.name,
            req.management.sid,
            "SQL_USER",
        )
        for p in PERMISSIONS
    )
    evidence = SqlClientPermissionGrantEvidence(
        request=req, operation=op, grant=grant, authority=authority, direct_permissions=rows
    )
    return binding, authority, grant, evidence


def grant_body(grant):
    return dict(
        asdict(grant), grant_id=str(grant.grant_id), session=strict_json_object(encode_session_identity(grant.session))
    )


def prefix(state, binding, authority, grant, evidence):
    frames = [
        (K.STARTUP, 0, {"startup": strict_json_object(encode_startup(binding.startup))}, "CHILD_TO_PARENT"),
        (
            K.REQUEST,
            1,
            {
                "operation": strict_json_object(canonical_json_bytes(coordinator_identity_body(binding.operation))),
                "execution_owner": asdict(OWNER),
                "request": strict_json_object(encode_permission_grant_request(binding.request)),
            },
            "PARENT_TO_CHILD",
        ),
    ]
    request_payload = None
    for kind, ordinal, body, direction in frames:
        payload = encode_permission_message(binding, kind, ordinal, body)
        state.accept(payload, direction=direction)
        request_payload = payload
    state.accept(
        encode_permission_message(
            binding, K.REQUEST_ACCEPTED, 1, {"request_payload_sha256": sha256(request_payload).hexdigest()}
        ),
        direction="CHILD_TO_PARENT",
    )
    state.consume_credentials(payload_size=10)
    for kind, body, direction in [
        (K.AUTHORITY, {"authority": strict_json_object(encode_authority(authority))}, "CHILD_TO_PARENT"),
        (K.EXECUTE, {"grant": grant_body(grant)}, "PARENT_TO_CHILD"),
        (
            K.PERMISSION_HELD,
            {"evidence": strict_json_object(encode_permission_grant_evidence(evidence))},
            "CHILD_TO_PARENT",
        ),
    ]:
        state.accept(
            encode_permission_message(binding, kind, 2 if kind is K.AUTHORITY else 3, body), direction=direction
        )


@pytest.mark.parametrize("checks", range(7))
def test_complete_and_early_release_transcripts(checks):
    binding, authority, grant, evidence = fixture()
    state = PermissionWireState(binding)
    prefix(state, binding, authority, grant, evidence)
    digest = sha256(encode_permission_grant_evidence(evidence)).hexdigest()
    for ordinal, boundary in enumerate(list(PermissionBoundary)[:checks], 4):
        body = {"boundary": boundary.value, "evidence_sha256": digest}
        state.accept(encode_permission_message(binding, K.CHECK_HELD, ordinal, body), direction="PARENT_TO_CHILD")
        state.accept(
            encode_permission_message(
                binding, K.HELD, ordinal, dict(body, authority=strict_json_object(encode_authority(authority)))
            ),
            direction="CHILD_TO_PARENT",
        )
    for kind, direction in [(K.RELEASE, "PARENT_TO_CHILD"), (K.RELEASED, "CHILD_TO_PARENT")]:
        state.accept(
            encode_permission_message(binding, kind, checks + 4, {"evidence_sha256": digest}), direction=direction
        )
    state.observe_eof()
    assert state.phase == "CLOSED"
    with pytest.raises(ValueError):
        state.observe_eof()


@pytest.mark.parametrize("change", ["direction", "ordinal", "duplicate", "secret", "whitespace", "eof"])
def test_invalid_first_exchange_is_sticky(change):
    binding, *_ = fixture()
    state = PermissionWireState(binding)
    payload = encode_permission_message(
        binding, K.STARTUP, 0, {"startup": strict_json_object(encode_startup(binding.startup))}
    )
    bad = strict_json_object(payload)
    if change == "ordinal":
        bad["ordinal"] = True
    if change == "secret":
        bad["password"] = "PRIVATE_CANARY"
    malformed = canonical_json_bytes(bad)
    if change == "whitespace":
        malformed += b" "
    if change == "duplicate":
        malformed = malformed[:-1] + b',"ordinal":0}'
    with pytest.raises(ValueError):
        if change == "eof":
            state.observe_eof()
        else:
            state.accept(malformed, direction="PARENT_TO_CHILD" if change == "direction" else "CHILD_TO_PARENT")
    with pytest.raises(ValueError):
        state.accept(payload, direction="CHILD_TO_PARENT")


def test_counter_and_single_framing():
    binding, *_ = fixture()
    payload = encode_permission_message(
        binding, K.STARTUP, 0, {"startup": strict_json_object(encode_startup(binding.startup))}
    )
    framed = frame_permission_payload(payload)
    assert framed == len(payload).to_bytes(4, "big") + payload
    assert decode_permission_message(payload, binding=binding, kind=K.STARTUP, ordinal=0).kind is K.STARTUP
    assert checked_physical_total(2097152 - 5, 1) == 2097152
    with pytest.raises(ValueError):
        checked_physical_total(2097152 - 4, 1)
    with pytest.raises(ValueError):
        frame_permission_payload(framed)


def transcript():
    binding, authority, grant, evidence = fixture()
    events = []

    class Recorder:
        def accept(self, payload, *, direction):
            events.append((payload, direction))

        def consume_credentials(self, *, payload_size):
            events.append((payload_size, None))

    prefix(Recorder(), binding, authority, grant, evidence)
    digest = sha256(encode_permission_grant_evidence(evidence)).hexdigest()
    for ordinal, boundary in enumerate(PermissionBoundary, 4):
        body = {"boundary": boundary.value, "evidence_sha256": digest}
        events.append((encode_permission_message(binding, K.CHECK_HELD, ordinal, body), "PARENT_TO_CHILD"))
        events.append(
            (
                encode_permission_message(
                    binding, K.HELD, ordinal, dict(body, authority=strict_json_object(encode_authority(authority)))
                ),
                "CHILD_TO_PARENT",
            )
        )
    for kind, direction in [(K.RELEASE, "PARENT_TO_CHILD"), (K.RELEASED, "CHILD_TO_PARENT")]:
        events.append((encode_permission_message(binding, kind, 10, {"evidence_sha256": digest}), direction))
    return binding, events


def deliver(state, event):
    payload, direction = event
    if direction is None:
        state.consume_credentials(payload_size=payload)
    else:
        return state.accept(payload, direction=direction)


def test_two_endpoints_account_every_physical_prefix_once():
    binding, events = transcript()
    left, right = PermissionWireState(binding), PermissionWireState(binding)
    for event in events:
        assert deliver(left, event) == deliver(right, event)
    assert len(events) == 21
    assert left.total == right.total == sum((p if d is None else len(p)) + 4 for p, d in events)
    left.observe_eof()
    right.observe_eof()


@pytest.mark.parametrize("index", [i for i in range(21) if i != 3])
@pytest.mark.parametrize("fault", ["outer", "body", "direction", "ordinal", "eof"])
def test_each_phase_rejects_malformed_attempt_permanently(index, fault):
    binding, events = transcript()
    state = PermissionWireState(binding)
    for event in events[:index]:
        deliver(state, event)
    payload, direction = events[index]
    body = strict_json_object(payload)
    if fault == "outer":
        body["launch_nonce"] = "ab" * 32
    elif fault == "body":
        body["body"]["password"] = "PRIVATE_CANARY"
    elif fault == "ordinal":
        body["ordinal"] = False
    if fault == "direction":
        direction = "PARENT_TO_CHILD" if direction == "CHILD_TO_PARENT" else "CHILD_TO_PARENT"
    with pytest.raises(ValueError):
        if fault == "eof":
            state.observe_eof()
        else:
            state.accept(canonical_json_bytes(body), direction=direction)
    assert state.failed is True
    with pytest.raises(ValueError):
        deliver(state, events[index])


@pytest.mark.parametrize("index", range(21))
def test_replay_of_each_accepted_phase_fails(index):
    binding, events = transcript()
    state = PermissionWireState(binding)
    for event in events[: index + 1]:
        deliver(state, event)
    with pytest.raises(ValueError):
        deliver(state, events[index])
    assert state.failed


@pytest.mark.parametrize("size", [0, -1, True, 196609, "1", None])
def test_invalid_private_size_poison_without_retaining_credentials(size):
    binding, events = transcript()
    state = PermissionWireState(binding)
    for event in events[:3]:
        deliver(state, event)
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_permission_wire_invalid$"):
        state.consume_credentials(payload_size=size)
    with pytest.raises(ValueError):
        state.consume_credentials(payload_size=1)


@pytest.mark.parametrize("kind", [K.EXECUTE, K.PERMISSION_HELD, K.HELD])
def test_state_rejects_valid_structural_message_with_different_original(kind):
    binding, events = transcript()
    state = PermissionWireState(binding)
    for payload, direction in events:
        if direction is None:
            state.consume_credentials(payload_size=payload)
            continue
        value = strict_json_object(payload)
        if value["kind"] != kind.value:
            state.accept(payload, direction=direction)
            continue
        body = value["body"]
        if kind is K.EXECUTE:
            body["grant"]["authority_sha256"] = "f" * 64
        elif kind is K.PERMISSION_HELD:
            body["evidence"]["grant"]["grant_id"] = str(UUID(int=100))
        else:
            body["authority"]["session"]["session_id"] += 1
        changed = encode_permission_message(binding, kind, value["ordinal"], body)
        # Stateless codec admits this structure; original state must not.
        decode_permission_message(changed, binding=binding, kind=kind, ordinal=value["ordinal"])
        with pytest.raises(ValueError):
            state.accept(changed, direction=direction)
        assert state.failed
        return
    pytest.fail("missing tested phase")


def test_original_binding_mutation_is_not_refreshed():
    binding, events = transcript()
    state = PermissionWireState(binding)
    object.__setattr__(binding.startup, "package_root", "/replacement")
    with pytest.raises(ValueError):
        deliver(state, events[0])
    assert state.failed


@pytest.mark.parametrize("field", ["request_payload_sha256", "evidence_sha256"])
def test_digest_is_exact_payload_not_semantic_digest(field):
    binding, events = transcript()
    state = PermissionWireState(binding)
    for payload, direction in events:
        if direction is None:
            state.consume_credentials(payload_size=payload)
            continue
        value = strict_json_object(payload)
        if field not in value["body"]:
            state.accept(payload, direction=direction)
            continue
        value["body"][field] = permission_grant_digest(binding.request)
        with pytest.raises(ValueError):
            state.accept(canonical_json_bytes(value), direction=direction)
        return
    pytest.fail("missing tested digest")


@pytest.mark.parametrize(
    "current,size,accepted",
    [(2097146, 1, True), (2097147, 1, True), (2097148, 1, False), (True, 1, False), (0, True, False), (-1, 1, False)],
)
def test_aggregate_counter_boundaries(current, size, accepted):
    if accepted:
        assert checked_physical_total(current, size) == current + size + 4
    else:
        with pytest.raises(ValueError):
            checked_physical_total(current, size)


@pytest.mark.parametrize("kind,limit", [(K.STARTUP, 16384), (K.REQUEST, 131072), (K.PERMISSION_HELD, 1048572)])
@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_framing_cap_includes_outer_payload(kind, limit, delta):
    binding, events = transcript()
    payload = next(p for p, d in events if d is not None and strict_json_object(p)["kind"] == kind.value)
    value = strict_json_object(payload)
    value["body"] = {"padding": ""}
    overhead = len(canonical_json_bytes(value))
    value["body"]["padding"] = "x" * (limit + delta - overhead)
    bounded = canonical_json_bytes(value)
    assert len(bounded) == limit + delta
    if delta <= 0:
        assert len(frame_permission_payload(bounded)) == len(bounded) + 4
    else:
        with pytest.raises(ValueError):
            frame_permission_payload(bounded)
    # Framing success deliberately does not validate this invalid body.
    with pytest.raises(ValueError):
        decode_permission_message(bounded, binding=binding, kind=kind, ordinal=value["ordinal"])


def test_public_credentials_and_explicit_failure_are_closed():
    binding, *_ = fixture()
    state = PermissionWireState(binding)
    with pytest.raises(ValueError):
        encode_permission_message(binding, K.CREDENTIALS, 2, {"password": "PRIVATE_CANARY"})
    state.fail()
    with pytest.raises(ValueError):
        state.consume_credentials(payload_size=1)


def test_interrupt_during_decode_leaves_transcript_poisoned(monkeypatch):
    import dpone.contracts.mssql_sqlclient_permission_grant_wire as wire

    binding, events = transcript()
    state = PermissionWireState(binding)
    monkeypatch.setattr(wire, "decode_permission_message", lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        deliver(state, events[0])
    assert state.failed


@pytest.mark.parametrize(
    "index,path",
    [
        (0, ("startup", "process", "pid")),
        (1, ("execution_owner", "fence")),
        (1, ("operation", "slot_index")),
        (1, ("request", "stage", "object_id")),
        (4, ("authority", "transaction_count")),
        (5, ("grant", "ownership", "fence")),
        (6, ("evidence", "direct_permissions", 0, "minor_id")),
    ],
)
def test_nested_bool_aliases_cannot_be_normalized_to_valid_ids(index, path):
    binding, events = transcript()
    payload, _ = events[index]
    value = strict_json_object(payload)
    leaf = value["body"]
    for key in path[:-1]:
        leaf = leaf[key]
    leaf[path[-1]] = False
    with pytest.raises(ValueError):
        encode_permission_message(binding, K(value["kind"]), value["ordinal"], value["body"])
    with pytest.raises(ValueError):
        decode_permission_message(
            canonical_json_bytes(value), binding=binding, kind=K(value["kind"]), ordinal=value["ordinal"]
        )


def test_python_scalar_alias_rejected_before_normalization():
    class TextAlias(str):
        pass

    binding, events = transcript()
    value = strict_json_object(events[0][0])
    value["body"]["startup"]["package_root"] = TextAlias("/admitted")
    with pytest.raises(ValueError):
        encode_permission_message(binding, K.STARTUP, 0, value["body"])


def test_original_nested_uuid_alias_rejects_before_any_hash(monkeypatch):
    import dpone.contracts.mssql_sqlclient_permission_grant_wire as wire

    binding, events = transcript()
    state = PermissionWireState(binding)
    object.__setattr__(binding.operation.operation_id, "int", True)
    called = []
    monkeypatch.setattr(wire, "coordinator_identity_digest", lambda *a: called.append(True))
    with pytest.raises(ValueError):
        deliver(state, events[0])
    assert called == [] and state.failed


@pytest.mark.parametrize("index", [0, 1, 4, 5, 6, 8])
def test_unknown_nested_schema_and_secret_are_not_ignored(index):
    binding, events = transcript()
    payload, direction = events[index]
    value = strict_json_object(payload)
    nested = next(v for v in value["body"].values() if type(v) is dict)
    nested["secret"] = "PRIVATE_CANARY"
    state = PermissionWireState(binding)
    for event in events[:index]:
        deliver(state, event)
    with pytest.raises(ValueError) as failure:
        state.accept(canonical_json_bytes(value), direction=direction)
    assert "PRIVATE_CANARY" not in str(failure.value)


def test_no_check_after_six_and_no_early_release_with_pending_reply():
    binding, events = transcript()
    state = PermissionWireState(binding)
    for event in events[:8]:
        deliver(state, event)
    value = strict_json_object(events[-2][0])
    value["ordinal"] = 4
    with pytest.raises(ValueError):
        state.accept(canonical_json_bytes(value), direction="PARENT_TO_CHILD")
    state = PermissionWireState(binding)
    for event in events[:-2]:
        deliver(state, event)
    value = strict_json_object(events[-4][0])
    value["ordinal"] = 10
    with pytest.raises(ValueError):
        state.accept(canonical_json_bytes(value), direction="PARENT_TO_CHILD")


@pytest.mark.parametrize("entry", ["accept", "consume_credentials", "observe_eof"])
def test_retained_startup_error_is_normalized_at_every_state_entry(entry):
    binding, events = transcript()
    state = PermissionWireState(binding)
    count = 0 if entry == "accept" else 3 if entry == "consume_credentials" else len(events)
    for event in events[:count]:
        deliver(state, event)
    object.__setattr__(binding.startup, "package_root", "invalid")

    def enter():
        if entry == "accept":
            deliver(state, events[0])
        elif entry == "consume_credentials":
            state.consume_credentials(payload_size=10)
        else:
            state.observe_eof()

    with pytest.raises(ValueError) as error:
        enter()
    assert str(error.value) == "mssql_native.sqlclient_permission_wire_invalid"
    assert state.failed is True
    object.__setattr__(binding.startup, "package_root", "/admitted")
    with pytest.raises(ValueError) as error:
        enter()
    assert str(error.value) == "mssql_native.sqlclient_permission_wire_invalid"
    assert state.failed is True
