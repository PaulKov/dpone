"""P10f helper IPC stays closed, bounded, request-bound, and credential-safe."""

import json
from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_writer_settlement_ipc import (
    MAX_FRAME_BYTES,
    SqlClientWriterSettlementCredentials,
    SqlClientWriterSettlementPlan,
    SqlClientWriterSettlementRequest,
)
from dpone.contracts.mssql_sqlclient_writer_settlement_ipc_codec import (
    decode_writer_settlement_credentials,
    decode_writer_settlement_plan,
    decode_writer_settlement_request,
    decode_writer_settlement_result,
    encode_writer_settlement_credentials,
    encode_writer_settlement_plan,
    encode_writer_settlement_request,
    encode_writer_settlement_result,
    make_writer_settlement_result,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_launch_contract import launch
from tests.test_mssql_sqlclient_writer_settlement_adapter import _inputs
from tests.test_mssql_tds_lifecycle import identity
from tests.test_mssql_tds_writer_settlement import _observation


def request():
    writer, writer_admission, management_admission, _, stage, descriptor, expectation = _inputs()
    attempt = identity(
        implementation_sha256="3" * 64,
        file_sha256=expectation.file_sha256,
        database=stage.database_name,
        schema=stage.schema_name,
        table=stage.table_name,
        owner_binding=stage.owner_binding,
    )
    writer = replace(
        writer,
        binding=replace(
            writer.binding,
            identity=attempt,
            attempt_sha256=attempt_identity_digest(attempt),
            input_binding_sha256=input_descriptor_digest(descriptor),
        ),
        announcement=replace(writer.announcement, attempt_sha256=attempt_identity_digest(attempt)),
    )
    plan = SqlClientWriterSettlementPlan(
        helper_id=UUID("44444444-4444-4444-8444-444444444444"),
        attempt=attempt,
        writer_observation=writer,
        writer_admission=writer_admission,
        management_admission=management_admission,
        stage=stage,
        input_descriptor=descriptor,
        expectation=expectation,
        implementation_sha256="e" * 64,
        package_root="/synthetic",
        admission_sha256="f" * 64,
        startup_deadline=10.0,
        operation_deadline=20.0,
        max_address_space_bytes=8 << 30,
    )
    process = replace(launch().process, pid=124)
    startup = TdsCoordinatorStartup(process, plan.implementation_sha256, plan.package_root, bytes(range(32)))
    return SqlClientWriterSettlementRequest(plan=plan, startup=startup)


def credentials(value=None):
    current = request() if value is None else value
    return SqlClientWriterSettlementCredentials(
        request=current,
        request_sha256=sha256(encode_writer_settlement_request(current)).hexdigest(),
        material=TdsConnectionMaterial(
            "db.example",
            1433,
            current.plan.management_admission.database.database_name,
            current.plan.management_admission.login.name,
            "private",
        ),
    )


def observation(value=None):
    current = request() if value is None else value
    view = type("OwnerView", (), {})()
    view.writer_admission = current.plan.writer_admission
    view.observation = current.plan.writer_observation
    view.stage = current.plan.stage
    view.content_expectation = current.plan.expectation
    observed = _observation(view)
    from dpone.contracts.mssql_sqlclient_observation import SqlClientPrincipalResolution, SqlClientSessionAuthority
    from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest

    admission = current.plan.management_admission
    old = observed.departure.observer
    principal = old.authority.principal_resolution.principal
    authority = SqlClientSessionAuthority(
        admission.server,
        admission.database,
        admission.login,
        admission.transport,
        SqlClientPrincipalResolution("mapped_user", principal.principal_id, principal.name, admission.login.sid),
    )
    observer = replace(old, authority=authority)
    guard = observer_incarnation_digest(observer)
    departure = replace(
        observed.departure,
        observer=observer,
        samples=tuple(
            replace(sample, before_sha256=guard, after_sha256=guard) for sample in observed.departure.samples
        ),
    )
    return replace(observed, departure=departure, observer_before=observer, observer_after=observer)


def test_all_four_frames_roundtrip_and_result_binds_exact_request():
    current = request()
    assert decode_writer_settlement_plan(encode_writer_settlement_plan(current.plan)) == current.plan
    assert decode_writer_settlement_request(encode_writer_settlement_request(current)) == current
    secret = credentials(current)
    assert decode_writer_settlement_credentials(encode_writer_settlement_credentials(secret), request=current) == secret
    result = make_writer_settlement_result(current, observation(current))
    assert result.request_sha256 == sha256(encode_writer_settlement_request(current)).hexdigest()
    payload = encode_writer_settlement_result(result, request=current)
    assert decode_writer_settlement_result(payload, request=current) == result


def test_credentials_reject_self_selected_request_hash_before_encoding():
    with pytest.raises(ValueError):
        encode_writer_settlement_credentials(replace(credentials(), request_sha256="0" * 64))


def test_plan_rejects_descriptor_substitution_after_writer_binding():
    current = request()
    altered = replace(
        current.plan.input_descriptor,
        max_row_bytes=current.plan.input_descriptor.max_row_bytes + 1,
    )
    with pytest.raises(ValueError):
        replace(current.plan, input_descriptor=altered)


@pytest.mark.parametrize(
    "field,changed",
    [
        ("helper_id", UUID("55555555-5555-4555-8555-555555555555")),
        ("admission_sha256", "0" * 64),
        ("implementation_sha256", "1" * 64),
        ("package_root", "/other"),
        ("operation_deadline", 19.0),
        ("max_address_space_bytes", 9 << 30),
    ],
)
def test_result_and_credentials_reject_plan_substitution(field, changed):
    original = request()
    result = make_writer_settlement_result(original, observation(original))
    altered_plan = replace(original.plan, **{field: changed})
    altered = replace(
        original,
        plan=altered_plan,
        startup=replace(
            original.startup,
            implementation_sha256=altered_plan.implementation_sha256,
            package_root=altered_plan.package_root,
        ),
    )
    with pytest.raises(ValueError):
        encode_writer_settlement_result(result, request=altered)
    with pytest.raises(ValueError):
        decode_writer_settlement_credentials(
            encode_writer_settlement_credentials(credentials(original)), request=altered
        )


def _wire(kind):
    current = request()
    if kind == "plan":
        return encode_writer_settlement_plan(current.plan), decode_writer_settlement_plan
    if kind == "request":
        return encode_writer_settlement_request(current), decode_writer_settlement_request
    if kind == "credentials":
        return encode_writer_settlement_credentials(
            credentials(current)
        ), lambda payload: decode_writer_settlement_credentials(payload, request=current)
    result = make_writer_settlement_result(current, observation(current))
    return encode_writer_settlement_result(result, request=current), lambda payload: decode_writer_settlement_result(
        payload, request=current
    )


@pytest.mark.parametrize("kind", ["plan", "request", "credentials", "result"])
@pytest.mark.parametrize("mutation", ["unknown", "missing", "trailing"])
def test_frames_are_closed_and_canonical(kind, mutation):
    payload, decode = _wire(kind)
    body = json.loads(payload)
    if mutation == "unknown":
        body["unknown"] = True
        payload = canonical_json_bytes(body)
    elif mutation == "missing":
        del body["schema"]
        payload = canonical_json_bytes(body)
    else:
        payload += b" "
    with pytest.raises(ValueError):
        decode(payload)


@pytest.mark.parametrize("kind", ["plan", "request", "credentials", "result"])
def test_frames_reject_oversize_before_parsing(monkeypatch, kind):
    _, decode = _wire(kind)
    calls = []
    monkeypatch.setattr(
        "dpone.contracts.mssql_sqlclient_writer_settlement_ipc_codec.strict_json_object",
        lambda *args: calls.append(args),
    )
    with pytest.raises(ValueError):
        decode(b"x" * (MAX_FRAME_BYTES + 1))
    assert calls == []


@pytest.mark.parametrize("kind", ["plan", "request", "credentials", "result"])
def test_exact_frame_limit_reaches_parser_but_still_rejects_invalid_json(monkeypatch, kind):
    _, decode = _wire(kind)
    calls = []

    def reject(payload):
        calls.append(len(payload))
        raise ValueError("synthetic invalid JSON")

    monkeypatch.setattr(
        "dpone.contracts.mssql_sqlclient_writer_settlement_ipc_codec.strict_json_object",
        reject,
    )
    with pytest.raises(ValueError):
        decode(b"x" * MAX_FRAME_BYTES)
    assert calls == [MAX_FRAME_BYTES]


def test_public_frames_contain_no_connection_material_or_password():
    current = request()
    result = make_writer_settlement_result(current, observation(current))
    for payload in (
        encode_writer_settlement_plan(current.plan),
        encode_writer_settlement_request(current),
        encode_writer_settlement_result(result, request=current),
    ):
        assert b"private" not in payload
        assert b"db.example" not in payload
        assert b'"material"' not in payload
