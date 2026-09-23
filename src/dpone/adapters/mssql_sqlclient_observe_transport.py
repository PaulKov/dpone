"""Sequential private OBSERVE frames, bounded before allocation and append.

Unlike the legacy one-shot pipes, a live socket remains open after END. Every
operation shares its original absolute deadline; queued bytes poison success.
"""

import socket
import struct
from typing import Any

from dpone.adapters import _mssql_tds_socket_readiness as readiness
from dpone.adapters.mssql_sqlclient_observer_incarnation import require_own_incarnation_statement
from dpone.contracts.mssql_sqlclient_observe_wire import (
    CONTROL_PAYLOAD_BYTES,
    ERROR,
    MAX_PAYLOAD_BYTES,
    MAX_RESPONSE_BYTES,
    ResponseReader,
    canonical,
    command,
)
from dpone.contracts.mssql_tds_api import (
    OPCODE_LIMITS,
    SqlClientObserveRequest,
    decode_authority,
    decode_rows,
    observation_from_body,
    validate_rows,
)


def _wait(channel: socket.socket, deadline: float, *, write: bool = False) -> None:
    if not readiness._socket_ready(channel, deadline, writing=write):
        raise TimeoutError(ERROR)


def read_frame(channel: socket.socket, *, deadline: float, limit: int, remaining: int | None = None) -> bytes:
    def exact(length: int) -> bytes:
        body = bytearray()
        while len(body) < length:
            _wait(channel, deadline)
            try:
                part = channel.recv(length - len(body))
            except BlockingIOError:
                continue
            if not part:
                raise ValueError(ERROR)
            body.extend(part)
        return bytes(body)

    length = struct.unpack("!I", exact(4))[0]
    if not 0 < length <= limit or remaining is not None and length + 4 > remaining:
        raise ValueError(ERROR)
    return exact(length)


def write_frame(channel: socket.socket, payload: bytes, *, deadline: float, limit: int) -> None:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)
    data = memoryview(struct.pack("!I", len(payload)) + payload)
    while data:
        _wait(channel, deadline, write=True)
        try:
            written = channel.send(data)
        except BlockingIOError:
            continue
        if written <= 0:
            raise ValueError(ERROR)
        data = data[written:]


def require_quiet(channel: socket.socket) -> None:
    if readiness._has_queued_input(channel):
        raise ValueError(ERROR)


class SqlClientObserveCatalog:
    """Finite adapter over one authenticated retained private channel."""

    def __init__(self, process: Any, request: SqlClientObserveRequest, credentials: Any, authority: Any) -> None:
        self._process, self.request = process, request
        self.identity, self.execution_owner, self.process = (
            credentials.identity,
            credentials.execution_owner,
            credentials.process,
        )
        self.authority = authority
        self._sequence = 1
        self._failed = self._busy = False

    def _call(self, opcode: str, arguments: dict | None = None) -> Any:
        if self._failed or self._busy:
            self._failed = True
            raise ValueError(ERROR)
        self._busy = True
        try:
            self._process.assert_current()
            channel, deadline = self._process.channel, self._process.operation_deadline
            require_quiet(channel)
            cmd = command(
                self.request,
                self._process.startup_receipt.launch_nonce,
                self._sequence,
                opcode,
                {} if arguments is None else arguments,
            )
            write_frame(channel, canonical(cmd), deadline=deadline, limit=CONTROL_PAYLOAD_BYTES)
            reader = ResponseReader(cmd)
            while True:
                payload = read_frame(
                    channel, deadline=deadline, limit=MAX_PAYLOAD_BYTES, remaining=MAX_RESPONSE_BYTES - reader.total
                )
                result = reader.accept(payload)
                if result is not None:
                    break
            require_quiet(channel)
            self._process.assert_current()
            value: Any
            if opcode in OPCODE_LIMITS:
                value = validate_rows(opcode, result, decode=True)
            elif opcode == "GUARD":
                if len(result) != 1:
                    raise ValueError(ERROR)
                value = decode_authority(canonical(result[0]))
                if value != self.authority:
                    raise ValueError(ERROR)
            elif opcode == "OBSERVE_SELECTED":
                if len(result) != 1:
                    raise ValueError(ERROR)
                value = observation_from_body(result[0], self.request)
            else:
                value = decode_rows(opcode, result)
            self._sequence += 1
            return value
        except BaseException:
            self._failed = True
            raise
        finally:
            self._busy = False

    def preparation(self, opcode: str) -> list[Any]:
        """Execute a fixed bound-request profile read; no SQL or caller subjects."""
        retained = self._process.attempt._preparation
        if (
            opcode not in OPCODE_LIMITS
            or retained is None
            or retained.handle is not self._process
            or retained.failed
            or retained.attempted
            or not self._process.attempt._busy
        ):
            raise ValueError(ERROR)
        return self._call(opcode)

    def guard(self) -> Any:
        return self._call("GUARD")

    def own_incarnation(self, statement: str) -> list[Any]:
        try:
            require_own_incarnation_statement(statement)
        except ValueError:
            self._failed = True
            raise ValueError(ERROR) from None
        return self.read_own_incarnation()

    def read_own_incarnation(self) -> list[Any]:
        """Read the finite projection without accepting statement text."""
        return self._call("OWN_INCARNATION")

    def writer_admission(self, database_id: int, login: str) -> list[Any]:
        return self._call("WRITER_ADMISSION", dict(database_id=database_id, login=login))

    def principals(self, name: str, sid: bytes) -> list[Any]:
        if type(sid) is not bytes:
            self._failed = True
            raise ValueError(ERROR)
        return self._call("PRINCIPALS", dict(name=name, sid=sid.hex()))

    def permissions(self, writer_id: int, public_id: int, *, limit: int) -> list[Any]:
        return self._call("PERMISSIONS", dict(writer_id=writer_id, public_id=public_id, limit=limit))

    def member(self, object_id: int) -> list[Any]:
        return self._call("MEMBER", dict(object_id=object_id))

    def members(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._call("BATCH_MEMBERS", dict(object_ids=list(object_ids)))

    def object_properties(self, schema_id: int, table: str) -> list[Any]:
        return self._call("OBJECT_PROPERTIES", dict(schema_id=schema_id, table=table))

    def object_properties_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._call("BATCH_OBJECTS", dict(object_ids=list(object_ids)))

    def features(self, object_id: int) -> list[Any]:
        return self._call("FEATURES", dict(object_id=object_id))

    def features_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._call("BATCH_FEATURES", dict(object_ids=list(object_ids)))

    def columns(self, object_id: int) -> list[Any]:
        return self._call("COLUMNS", dict(object_id=object_id))

    def columns_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._call("BATCH_COLUMNS", dict(object_ids=list(object_ids)))

    def observe_selected(self) -> Any:
        return self._call("OBSERVE_SELECTED")
