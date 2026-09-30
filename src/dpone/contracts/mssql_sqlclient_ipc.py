"""Closed framing and payload contracts for the optional SqlClient companion."""

from __future__ import annotations

import math
import re
import struct
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.mssql_native_stage_writer import (
    NativeStageColumnMapping,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
)
from dpone.contracts.mssql_native_writer import SQLCLIENT_SESSION_PROOF
from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object

PROTOCOL = "dpone.mssql-sqlclient.ipc.v1"
PROTOCOL_V2 = "dpone.mssql-sqlclient.ipc.v2"
REQUEST_MAX_BYTES = 1024 * 1024
RESULT_MAX_BYTES = 64 * 1024
CREDENTIAL_MAX_BYTES = 64 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TEXT = re.compile(r"[^\x00-\x1f\x7f]+\Z")
_REQUEST_FIELDS_V1 = frozenset(
    {
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
)
_REQUEST_FIELDS_V2 = _REQUEST_FIELDS_V1 | {"layout_version"}
_COLUMN_FIELDS = frozenset({"ordinal", "target_name", "target_type", "nullable"})
_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "protocol",
        "attempt_id",
        "classification",
        "input_rows_consumed",
        "writer_identity_sha256",
        "runtime_identity_sha256",
        "metrics",
    }
)
_METRIC_FIELDS = frozenset({"launch_seconds", "write_seconds", "dispose_seconds"})


@dataclass(frozen=True, slots=True)
class MssqlSqlClientCredentials:
    """Ephemeral SQL-password connection authority for one companion launch."""

    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)
    encrypt: bool = True
    trust_server_certificate: bool = False

    def __post_init__(self) -> None:
        if (
            not _bounded_text(self.host, 255)
            or type(self.port) is not int
            or not 1 <= self.port <= 65_535
            or not _bounded_text(self.database, 128)
            or not _bounded_text(self.username, 128)
            or not _bounded_text(self.password, 2_048)
            or self.encrypt is not True
            or type(self.trust_server_certificate) is not bool
        ):
            raise ValueError("mssql_sqlclient.invalid_credentials")


def _bounded_text(value: object, limit: int) -> bool:
    return type(value) is str and 0 < len(value) <= limit and _TEXT.fullmatch(value) is not None


def _frame(document: object, *, limit: int, diagnostic: str) -> bytes:
    try:
        payload = canonical_json_bytes(document)
    except (TypeError, ValueError) as error:
        raise ValueError(diagnostic) from error
    if not payload or len(payload) > limit:
        raise ValueError(diagnostic)
    return struct.pack(">I", len(payload)) + payload


def _unframe(frame: bytes, *, limit: int, diagnostic: str) -> dict[str, Any]:
    try:
        if type(frame) is not bytes or len(frame) < 4:
            raise ValueError
        size = struct.unpack(">I", frame[:4])[0]
        payload = frame[4:]
        if size < 1 or size > limit or len(payload) != size:
            raise ValueError
        document = strict_json_object(payload)
        if canonical_json_bytes(document) != payload:
            raise ValueError
        return document
    except (StrictJsonError, TypeError, ValueError, struct.error) as error:
        raise ValueError(diagnostic) from error


def encode_request_frame(request: NativeStageWriteRequest, *, deadline_budget_ms: int) -> bytes:
    """Encode one validated, non-secret companion request."""
    if (
        not isinstance(request, NativeStageWriteRequest)
        or request.proof_capability != SQLCLIENT_SESSION_PROOF
        or type(deadline_budget_ms) is not int
        or not 1 <= deadline_budget_ms <= 2_147_483_647
    ):
        raise ValueError("mssql_sqlclient.invalid_request_frame")
    document = {
        "schema_version": request.layout_version,
        "protocol": PROTOCOL if request.layout_version == 1 else PROTOCOL_V2,
        "attempt_id": request.attempt_id,
        "qualified_stage": request.qualified_stage,
        "stage_id_sha256": request.stage_id_sha256,
        "owner_binding_sha256": request.owner_binding_sha256,
        "object_id": request.object_id,
        "schema_sha256": request.schema_sha256,
        "file_path": str(request.file_path),
        "expected_rows": request.expected_rows,
        "encoded_bytes": request.encoded_bytes,
        "max_row_bytes": request.max_row_bytes,
        "file_sha256": request.file_sha256,
        "grant_token_sha256": request.grant_token_sha256,
        "proof_capability": request.proof_capability,
        "wire_layout_sha256": request.wire_layout_sha256,
        "deadline_budget_ms": deadline_budget_ms,
        "columns": [asdict(column) for column in request.columns],
    }
    if request.layout_version == 2:
        document["layout_version"] = 2
    return _frame(document, limit=REQUEST_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_request_frame")


def decode_request_frame(frame: bytes) -> dict[str, Any]:
    """Strict decoder used by parity tests and reference companion implementations."""
    document = _unframe(frame, limit=REQUEST_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_request_frame")
    try:
        columns = document["columns"]
        if (
            set(document) != (_REQUEST_FIELDS_V1 if document.get("schema_version") == 1 else _REQUEST_FIELDS_V2)
            or document["schema_version"] not in {1, 2}
            or type(document["schema_version"]) is not int
            or document["protocol"] != (PROTOCOL if document["schema_version"] == 1 else PROTOCOL_V2)
            or (document["schema_version"] == 2 and document["layout_version"] != 2)
            or document["proof_capability"] != SQLCLIENT_SESSION_PROOF
            or type(document["deadline_budget_ms"]) is not int
            or not 1 <= document["deadline_budget_ms"] <= 2_147_483_647
            or not isinstance(columns, list)
            or not columns
            or any(not isinstance(column, dict) or set(column) != _COLUMN_FIELDS for column in columns)
        ):
            raise ValueError
        mappings = tuple(NativeStageColumnMapping(**column) for column in columns)
        NativeStageWriteRequest(
            attempt_id=document["attempt_id"],
            qualified_stage=document["qualified_stage"],
            stage_id_sha256=document["stage_id_sha256"],
            owner_binding_sha256=document["owner_binding_sha256"],
            object_id=document["object_id"],
            schema_sha256=document["schema_sha256"],
            file_path=Path(document["file_path"]),
            expected_rows=document["expected_rows"],
            encoded_bytes=document["encoded_bytes"],
            max_row_bytes=document["max_row_bytes"],
            file_sha256=document["file_sha256"],
            grant_token_sha256=document["grant_token_sha256"],
            proof_capability=document["proof_capability"],
            wire_layout_sha256=document["wire_layout_sha256"],
            columns=mappings,
            layout_version=document.get("layout_version", 1),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("mssql_sqlclient.invalid_request_frame") from error
    return document


def encode_credentials_frame(credentials: MssqlSqlClientCredentials) -> bytearray:
    """Encode one mutable frame so the supervisor can clear it after projection."""
    if not isinstance(credentials, MssqlSqlClientCredentials):
        raise ValueError("mssql_sqlclient.invalid_credentials")
    document = {
        "schema_version": "dpone.mssql-sqlclient.credentials.v1",
        "authentication": "sql_password",
        **asdict(credentials),
    }
    return bytearray(_frame(document, limit=CREDENTIAL_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_credentials"))


def decode_credentials_frame(frame: bytes) -> dict[str, Any]:
    """Strict reference decoder for parity tests; callers must clear returned secrets."""
    document = _unframe(frame, limit=CREDENTIAL_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_credentials")
    expected = {
        "schema_version",
        "authentication",
        "host",
        "port",
        "database",
        "username",
        "password",
        "encrypt",
        "trust_server_certificate",
    }
    try:
        if (
            set(document) != expected
            or document["schema_version"] != "dpone.mssql-sqlclient.credentials.v1"
            or document["authentication"] != "sql_password"
        ):
            raise ValueError
        MssqlSqlClientCredentials(
            host=document["host"],
            port=document["port"],
            database=document["database"],
            username=document["username"],
            password=document["password"],
            encrypt=document["encrypt"],
            trust_server_certificate=document["trust_server_certificate"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("mssql_sqlclient.invalid_credentials") from error
    return document


def encode_result_frame(document: dict[str, object]) -> bytes:
    """Encode a result for companion/reference tests through the same size bound."""
    return _frame(document, limit=RESULT_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_result_frame")


def decode_observation_frame(
    frame: bytes,
    *,
    request: NativeStageWriteRequest,
    writer_identity_sha256: str,
    runtime_identity_sha256: str,
) -> NativeStageWriteObservation:
    """Decode a result only when it matches the launched request and runtime."""
    document = _unframe(frame, limit=RESULT_MAX_BYTES, diagnostic="mssql_sqlclient.invalid_result_frame")
    try:
        metrics = document["metrics"]
        if (
            set(document) != _RESULT_FIELDS
            or document["schema_version"] != request.layout_version
            or type(document["schema_version"]) is not int
            or document["protocol"] != (PROTOCOL if request.layout_version == 1 else PROTOCOL_V2)
            or document["attempt_id"] != request.attempt_id
            or document["writer_identity_sha256"] != writer_identity_sha256
            or document["runtime_identity_sha256"] != runtime_identity_sha256
            or not isinstance(metrics, dict)
            or set(metrics) != _METRIC_FIELDS
        ):
            raise ValueError
        observation = NativeStageWriteObservation(
            attempt_id=document["attempt_id"],
            input_rows_consumed=document["input_rows_consumed"],
            classification=document["classification"],
            writer_identity_sha256=document["writer_identity_sha256"],
            runtime_identity_sha256=document["runtime_identity_sha256"],
            protocol=document["protocol"],
            metrics=NativeStageWriteMetrics(
                _seconds(metrics["launch_seconds"]),
                _seconds(metrics["write_seconds"]),
                _seconds(metrics["dispose_seconds"]),
            ),
        )
        if observation.positive_terminal and observation.input_rows_consumed != request.expected_rows:
            raise ValueError
        return observation
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("mssql_sqlclient.invalid_result_frame") from error


def _seconds(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError
    return result


def applock_resource(grant_token_sha256: str) -> str:
    """Derive the same bounded, domain-separated session resource in both runtimes."""
    if type(grant_token_sha256) is not str or _SHA256.fullmatch(grant_token_sha256) is None:
        raise ValueError("mssql_sqlclient.invalid_grant_token_digest")
    digest = sha256(("dpone.mssql-sqlclient.applock.v1\0" + grant_token_sha256).encode()).hexdigest()
    return "dpone:mssql-native:" + digest


def sqlclient_application_name(
    attempt_id: str,
    grant_token_sha256: str,
    object_id: int,
    stage_id_sha256: str,
) -> str:
    """Derive the exact writer-session identity observed during certification."""
    if type(attempt_id) is not str or not attempt_id or "\x00" in attempt_id:
        raise ValueError("mssql_sqlclient.invalid_attempt_id")
    if (
        _SHA256.fullmatch(grant_token_sha256) is None
        or type(object_id) is not int
        or object_id < 1
        or _SHA256.fullmatch(stage_id_sha256) is None
    ):
        raise ValueError("mssql_sqlclient.invalid_application_identity")
    identity = "\0".join(
        (
            "dpone.mssql-sqlclient.application.v2",
            attempt_id,
            grant_token_sha256,
            str(object_id),
            stage_id_sha256,
        )
    )
    digest = sha256(identity.encode()).hexdigest()
    return "dpone-mssql-sqlclient:" + digest


__all__ = [
    "CREDENTIAL_MAX_BYTES",
    "MssqlSqlClientCredentials",
    "PROTOCOL",
    "PROTOCOL_V2",
    "REQUEST_MAX_BYTES",
    "RESULT_MAX_BYTES",
    "applock_resource",
    "sqlclient_application_name",
    "decode_observation_frame",
    "decode_credentials_frame",
    "decode_request_frame",
    "encode_credentials_frame",
    "encode_request_frame",
    "encode_result_frame",
]
