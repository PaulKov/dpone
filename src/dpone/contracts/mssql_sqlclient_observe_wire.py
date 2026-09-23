"""Finite command envelopes and independently accounted complete responses.

All limits count physical prefixes. END validates the entire buffered command;
EOF is never completion. Sequence advances only in the owning transport.
"""

from collections.abc import Iterator
from typing import Any, cast

from dpone.contracts.mssql_sqlclient_observation import _name
from dpone.contracts.mssql_sqlclient_observe import ERROR, SqlClientObserveRequest
from dpone.contracts.mssql_sqlclient_observe_rows import OPCODE_LIMITS
from dpone.contracts.mssql_tds_coordinator_authority import identifier
from dpone.contracts.mssql_tds_session import require_session_nonce
from dpone.contracts.mssql_tds_validation import _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_PHYSICAL_MESSAGE_BYTES = 1048576
MAX_PAYLOAD_BYTES = MAX_PHYSICAL_MESSAGE_BYTES - 4
CONTROL_PAYLOAD_BYTES = 16384
MAX_RESPONSE_BYTES = 8388608
MAX_CREDENTIAL_PAYLOAD_BYTES = 196608
_ARGUMENTS = {
    **dict.fromkeys(OPCODE_LIMITS, ()),
    "GUARD": (),
    "OWN_INCARNATION": (),
    "WRITER_ADMISSION": ("database_id", "login"),
    "PRINCIPALS": ("name", "sid"),
    "PERMISSIONS": ("writer_id", "public_id", "limit"),
    "MEMBER": ("object_id",),
    "OBJECT_PROPERTIES": ("schema_id", "table"),
    "FEATURES": ("object_id",),
    "COLUMNS": ("object_id",),
    "BATCH_MEMBERS": ("object_ids",),
    "BATCH_OBJECTS": ("object_ids",),
    "BATCH_FEATURES": ("object_ids",),
    "BATCH_COLUMNS": ("object_ids",),
    "OBSERVE_SELECTED": (),
}
_LIMITS = {
    **OPCODE_LIMITS,
    "GUARD": 1,
    "OWN_INCARNATION": 1,
    "WRITER_ADMISSION": 1,
    "PRINCIPALS": 2,
    "PERMISSIONS": 4096,
    "MEMBER": 1,
    "OBJECT_PROPERTIES": 1,
    "FEATURES": 1,
    "COLUMNS": 100,
    "BATCH_MEMBERS": 1024,
    "BATCH_OBJECTS": 1024,
    "BATCH_FEATURES": 1024,
    "BATCH_COLUMNS": 8181,
    "OBSERVE_SELECTED": 1,
}


def canonical(body: dict, limit: int = CONTROL_PAYLOAD_BYTES) -> bytes:
    encoded = canonical_json_bytes(body)
    if not 0 < len(encoded) <= limit:
        raise ValueError(ERROR)
    return encoded


def decode(payload: bytes, limit: int = CONTROL_PAYLOAD_BYTES) -> dict:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)
    body = strict_json_object(payload)
    if canonical(body, limit) != payload:
        raise ValueError(ERROR)
    return body


def command(request: SqlClientObserveRequest, nonce: bytes, sequence: int, opcode: str, arguments: dict) -> dict:
    require_session_nonce(nonce)
    _integer(sequence, 1, 2**63 - 1)
    if type(opcode) is not str or opcode not in _ARGUMENTS or type(arguments) is not dict:
        raise ValueError(ERROR)
    if set(arguments) != set(_ARGUMENTS[opcode]):
        raise ValueError(ERROR)
    for key, value in arguments.items():
        if key in ("database_id", "object_id", "schema_id"):
            _integer(value, 1, 2**31 - 1)
        elif key in ("writer_id", "public_id"):
            _integer(value, 0, 2**31 - 1)
        elif key == "limit":
            _integer(value, 1, request.limits.permission_rows)
        elif key == "object_ids":
            maximum = 81 if opcode == "BATCH_COLUMNS" else 1024
            if (
                type(value) is not list
                or not 1 <= len(value) <= maximum
                or any(type(item) is not int or not 1 <= item <= 2**31 - 1 for item in value)
                or sorted(set(value)) != value
            ):
                raise ValueError(ERROR)
        elif key in ("name", "login"):
            _name(value)
        elif key == "table":
            identifier(value)
        elif key == "sid":
            if type(value) is not str or not 2 <= len(value) <= 170 or bytes.fromhex(value).hex() != value:
                raise ValueError(ERROR)
    if opcode == "WRITER_ADMISSION" and arguments != {
        "database_id": request.writer_admission.database.database_id,
        "login": request.writer_admission.login.name,
    }:
        raise ValueError(ERROR)
    if opcode == "PRINCIPALS" and arguments != {
        "name": request.writer_principal.name,
        "sid": bytes.fromhex(request.writer_admission.login.sid).hex(),
    }:
        raise ValueError(ERROR)
    body = dict(
        schema="dpone.sqlclient.observe.command.v1",
        command_sha256=request.command_sha256,
        launch_nonce=nonce.hex(),
        sequence=sequence,
        opcode=opcode,
        arguments=dict(arguments),
    )
    canonical(body)
    return body


def decode_command(payload: bytes, request: SqlClientObserveRequest, nonce: bytes, sequence: int) -> dict:
    body = decode(payload)
    expected = command(request, nonce, sequence, cast(str, body.get("opcode")), cast(dict, body.get("arguments")))
    if body != expected:
        raise ValueError(ERROR)
    return expected


def _envelope(cmd: dict, kind: str, **values: Any) -> dict:
    return dict(
        schema="dpone.sqlclient.observe.response.v1",
        kind=kind,
        **{k: cmd[k] for k in ("command_sha256", "launch_nonce", "sequence", "opcode")},
        **values,
    )


def response_frames(cmd: dict, items: list[Any]) -> Iterator[bytes]:
    """Pack complete items without pagination or repeated SQL acquisition."""
    limit = cmd["arguments"].get("limit", _LIMITS[cmd["opcode"]])
    if type(items) is not list or len(items) > limit:
        raise ValueError(ERROR)
    count = physical = item_bytes = total = 0
    pending: list[Any] = []
    for item in items:
        candidate = canonical_json_bytes(_envelope(cmd, "BATCH", batch_index=count, items=[*pending, item]))
        if len(candidate) > MAX_PAYLOAD_BYTES:
            if not pending:
                raise ValueError(ERROR)
            payload = canonical(_envelope(cmd, "BATCH", batch_index=count, items=pending), MAX_PAYLOAD_BYTES)
            physical += len(payload) + 4
            item_bytes += len(canonical_json_bytes(pending))
            count += 1
            total = physical
            if total > MAX_RESPONSE_BYTES or count > 4096:
                raise ValueError(ERROR)
            yield payload
            pending = []
        pending.append(item)
    if pending:
        payload = canonical(_envelope(cmd, "BATCH", batch_index=count, items=pending), MAX_PAYLOAD_BYTES)
        physical += len(payload) + 4
        item_bytes += len(canonical_json_bytes(pending))
        count += 1
        if physical > MAX_RESPONSE_BYTES or count > 4096:
            raise ValueError(ERROR)
        yield payload
    end = canonical(
        _envelope(
            cmd,
            "END",
            batch_count=count,
            row_count=len(items),
            batch_physical_bytes=physical,
            items_json_bytes=item_bytes,
        )
    )
    if physical + len(end) + 4 > MAX_RESPONSE_BYTES:
        raise ValueError(ERROR)
    yield end


class ResponseReader:
    """Incremental budgets precede appends; only exact END returns any rows."""

    def __init__(self, cmd: dict) -> None:
        self.command = cmd
        self.items: list[Any] = []
        self.total = self.batches = self.physical = self.item_bytes = 0
        self.finished = self.failed = False

    def accept(self, payload: bytes) -> list[Any] | None:
        if self.finished or self.failed:
            raise ValueError(ERROR)
        self.failed = True
        self.total += len(payload) + 4
        if self.total > MAX_RESPONSE_BYTES:
            raise ValueError(ERROR)
        body = decode(payload, MAX_PAYLOAD_BYTES)
        kind = body.get("kind")
        base = _envelope(self.command, cast(str, kind))
        if any(type(body.get(k)) is not type(v) or body.get(k) != v for k, v in base.items()):
            raise ValueError(ERROR)
        if kind == "BATCH":
            if set(body) != set(base) | {"batch_index", "items"}:
                raise ValueError(ERROR)
            batch, items = body["batch_index"], body["items"]
            if type(batch) is not int or batch != self.batches or type(items) is not list or not items:
                raise ValueError(ERROR)
            limit = self.command["arguments"].get("limit", _LIMITS[self.command["opcode"]])
            if self.batches >= 4096 or len(self.items) + len(items) > limit:
                raise ValueError(ERROR)
            self.batches += 1
            self.physical += len(payload) + 4
            self.item_bytes += len(canonical_json_bytes(items))
            self.items.extend(items)
            self.failed = False
            return None
        if len(payload) > CONTROL_PAYLOAD_BYTES:
            raise ValueError(ERROR)
        if kind == "END":
            expected = dict(
                base,
                batch_count=self.batches,
                row_count=len(self.items),
                batch_physical_bytes=self.physical,
                items_json_bytes=self.item_bytes,
            )
            if body != expected or any(
                type(body[k]) is not int
                for k in ("batch_count", "row_count", "batch_physical_bytes", "items_json_bytes")
            ):
                raise ValueError(ERROR)
            self.finished = True
            self.failed = False
            return self.items
        # Every ERROR and every malformed/unrecognized message poisons this reader.
        raise ValueError(ERROR)
