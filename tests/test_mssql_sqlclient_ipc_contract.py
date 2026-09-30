from __future__ import annotations

import struct
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.contracts.mssql_native_stage_writer import NativeStageColumnMapping, NativeStageWriteRequest
from dpone.contracts.mssql_native_writer import SQLCLIENT_SESSION_PROOF
from dpone.contracts.mssql_sqlclient_ipc import (
    CREDENTIAL_MAX_BYTES,
    REQUEST_MAX_BYTES,
    RESULT_MAX_BYTES,
    MssqlSqlClientCredentials,
    applock_resource,
    decode_credentials_frame,
    decode_observation_frame,
    decode_request_frame,
    encode_credentials_frame,
    encode_request_frame,
    encode_result_frame,
    sqlclient_application_name,
)
from dpone.contracts.strict_json import canonical_json_bytes


def _request(tmp_path: Path) -> NativeStageWriteRequest:
    path = tmp_path / "sealed.bcp"
    path.write_bytes(b"payload")
    return NativeStageWriteRequest(
        attempt_id="attempt-1",
        qualified_stage="[db].[stage].[raw_1]",
        stage_id_sha256="1" * 64,
        owner_binding_sha256="2" * 64,
        object_id=42,
        schema_sha256="3" * 64,
        file_path=path,
        expected_rows=7,
        encoded_bytes=7,
        max_row_bytes=7,
        file_sha256=sha256(b"payload").hexdigest(),
        grant_token_sha256="5" * 64,
        proof_capability=SQLCLIENT_SESSION_PROOF,
        wire_layout_sha256="6" * 64,
        columns=(NativeStageColumnMapping(0, "event_id", "bigint", False),),
    )


def _result(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "protocol": "dpone.mssql-sqlclient.ipc.v1",
        "attempt_id": "attempt-1",
        "classification": "success",
        "input_rows_consumed": 7,
        "writer_identity_sha256": "7" * 64,
        "runtime_identity_sha256": "8" * 64,
        "metrics": {"launch_seconds": 0.1, "write_seconds": 1.0, "dispose_seconds": 0.2},
    }
    value.update(changes)
    return value


def test_request_frame_is_canonical_length_prefixed_and_closed(tmp_path: Path) -> None:
    frame = encode_request_frame(_request(tmp_path), deadline_budget_ms=12_345)
    document = decode_request_frame(frame)

    assert struct.unpack(">I", frame[:4])[0] == len(frame) - 4
    assert document["deadline_budget_ms"] == 12_345
    assert document["columns"] == [
        {"nullable": False, "ordinal": 0, "target_name": "event_id", "target_type": "bigint"}
    ]
    assert set(document) == {
        "schema_version",
        "protocol",
        "attempt_id",
        "qualified_stage",
        "stage_id_sha256",
        "owner_binding_sha256",
        "object_id",
        "schema_sha256",
        "file_path",
        "expected_rows",
        "encoded_bytes",
        "max_row_bytes",
        "file_sha256",
        "grant_token_sha256",
        "proof_capability",
        "wire_layout_sha256",
        "deadline_budget_ms",
        "columns",
    }


def test_v2_request_selects_persisted_hash_protocol(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request = NativeStageWriteRequest(
        **{name: getattr(request, name) for name in request.__dataclass_fields__ if name != "layout_version"},
        layout_version=2,
    )

    document = decode_request_frame(encode_request_frame(request, deadline_budget_ms=12_345))

    assert document["schema_version"] == 2
    assert document["protocol"] == "dpone.mssql-sqlclient.ipc.v2"
    assert document["layout_version"] == 2


def test_request_decoder_revalidates_nested_contract(tmp_path: Path) -> None:
    frame = encode_request_frame(_request(tmp_path), deadline_budget_ms=12_345)
    document = decode_request_frame(frame)
    document["object_id"] = 0
    payload = canonical_json_bytes(document)
    invalid = struct.pack(">I", len(payload)) + payload

    with pytest.raises(ValueError, match="mssql_sqlclient.invalid_request_frame"):
        decode_request_frame(invalid)


@pytest.mark.parametrize(
    "frame",
    [
        b"",
        b"\x00\x00\x00\x00",
        struct.pack(">I", REQUEST_MAX_BYTES + 1),
        struct.pack(">I", 2) + b"{}" + b"x",
        struct.pack(">I", 3) + b"{}",
        struct.pack(">I", 2) + b"\xff\xff",
        struct.pack(">I", 7) + b'{"a":1}',
        struct.pack(">I", 13) + b'{"a":1,"a":2}',
    ],
)
def test_request_frame_rejects_invalid_ambiguous_or_trailing_bytes(frame: bytes) -> None:
    with pytest.raises(ValueError, match="mssql_sqlclient.invalid_request_frame"):
        decode_request_frame(frame)


def test_credentials_are_one_bounded_secret_frame_and_repr_hides_password() -> None:
    credentials = MssqlSqlClientCredentials(
        host="sql.internal",
        port=1433,
        database="warehouse",
        username="loader",
        password="private-password",
        encrypt=True,
        trust_server_certificate=False,
    )

    frame = encode_credentials_frame(credentials)

    assert isinstance(frame, bytearray)
    assert len(frame) <= CREDENTIAL_MAX_BYTES + 4
    assert b"private-password" in frame
    assert "private-password" not in repr(credentials)
    assert decode_credentials_frame(bytes(frame))["password"] == "private-password"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host", ""),
        ("port", 0),
        ("port", 65_536),
        ("database", "x" * 129),
        ("username", "x\x00y"),
        ("password", ""),
        ("encrypt", False),
    ],
)
def test_credentials_reject_unsupported_or_unbounded_values(field: str, value: object) -> None:
    values: dict[str, object] = {
        "host": "sql.internal",
        "port": 1433,
        "database": "warehouse",
        "username": "loader",
        "password": "private-password",
        "encrypt": True,
        "trust_server_certificate": False,
    }
    values[field] = value
    with pytest.raises(ValueError, match="mssql_sqlclient.invalid_credentials"):
        MssqlSqlClientCredentials(**values)  # type: ignore[arg-type]


def test_result_frame_decodes_to_non_authoritative_observation(tmp_path: Path) -> None:
    request = _request(tmp_path)
    frame = encode_result_frame(_result())

    observation = decode_observation_frame(
        frame,
        request=request,
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    assert len(frame) <= RESULT_MAX_BYTES + 4
    assert observation.positive_terminal is True
    assert observation.input_rows_consumed == request.expected_rows


def test_v2_result_frame_decodes_with_matching_schema_and_protocol(tmp_path: Path) -> None:
    base = _request(tmp_path)
    request = NativeStageWriteRequest(
        **{name: getattr(base, name) for name in base.__dataclass_fields__ if name != "layout_version"},
        layout_version=2,
    )
    frame = encode_result_frame(_result(schema_version=2, protocol="dpone.mssql-sqlclient.ipc.v2"))

    observation = decode_observation_frame(
        frame,
        request=request,
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    assert observation.positive_terminal is True
    assert observation.protocol == "dpone.mssql-sqlclient.ipc.v2"


@pytest.mark.parametrize(
    "changes",
    [
        {"extra": True},
        {"attempt_id": "other"},
        {"input_rows_consumed": 6},
        {"writer_identity_sha256": "9" * 64},
        {"runtime_identity_sha256": "9" * 64},
        {"classification": "timeout", "input_rows_consumed": 7},
        {"metrics": {"launch_seconds": 0.1, "write_seconds": float("inf"), "dispose_seconds": 0.2}},
    ],
)
def test_result_frame_rejects_identity_shape_count_and_metric_drift(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    request = _request(tmp_path)
    with pytest.raises(ValueError, match="mssql_sqlclient.invalid_result_frame"):
        frame = encode_result_frame(_result(**changes))
        decode_observation_frame(
            frame,
            request=request,
            writer_identity_sha256="7" * 64,
            runtime_identity_sha256="8" * 64,
        )


def test_applock_resource_is_domain_separated_and_bounded() -> None:
    token_digest = "5" * 64
    expected = sha256(("dpone.mssql-sqlclient.applock.v1\0" + token_digest).encode()).hexdigest()

    assert applock_resource(token_digest) == "dpone:mssql-native:" + expected
    assert len(applock_resource(token_digest)) <= 255


def test_application_name_binds_exact_writer_request_and_is_bounded() -> None:
    attempt = "attempt-1"
    grant = "5" * 64
    stage = "6" * 64
    expected = sha256(
        "\0".join(("dpone.mssql-sqlclient.application.v2", attempt, grant, "781", stage)).encode()
    ).hexdigest()

    identity = sqlclient_application_name(attempt, grant, 781, stage)
    assert identity == "dpone-mssql-sqlclient:" + expected
    assert len(identity) <= 128
    assert sqlclient_application_name(attempt, grant, 782, stage) != identity
