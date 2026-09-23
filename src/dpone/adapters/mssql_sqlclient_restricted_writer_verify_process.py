"""Retained restricted-writer verification process and its bounded protocol."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Never

from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import FRAMES, RestrictedWriterVerifyFrames
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


class RestrictedWriterVerifyProcessUnknown(TdsLaunchUnknown):
    def __init__(self, process: RestrictedWriterVerifyProcess) -> None:
        self.process = process
        super().__init__(process)


class RestrictedWriterVerifyProcess:
    """Own the verification IPC and custody until exact remote settlement."""

    def __init__(
        self,
        custody,
        executor,
        channel,
        reservation,
        startup_deadline,
        operation_deadline,
        contract,
        *,
        frames: RestrictedWriterVerifyFrames = FRAMES,
        custody_waiter: Callable[[object, object, object, float], None] | None = None,
    ) -> None:
        self._custody, self._executor, self._channel = custody, executor, channel
        self._reservation = reservation
        self._contract = contract
        self._frames, self._custody_waiter = frames, custody_waiter
        self._startup_deadline, self._operation_deadline = startup_deadline, operation_deadline
        self._registration: Any | None = None
        self._credential_sent = self._opening_read = self._authorized = self._result_read = self._eof = (
            self._settled
        ) = False
        self._opening: Any | None = None
        self._opening_payload: bytes | None = None
        self._credential_exposed = False
        self._credential_buffer: tuple[int, int] | None = None
        self._cleanup_attempted = False

    def await_custody(self) -> None:
        try:
            if self._custody_waiter is None:
                raise ValueError("mssql_native.sqlclient_restricted_writer_verify_invalid")
            self._custody_waiter(self._custody, self._executor, self._executor.identity, self._startup_deadline)
        except BaseException:
            self._fail()

    def _fail(self) -> Never:
        try:
            self.cleanup()
        except BaseException:
            pass
        raise RestrictedWriterVerifyProcessUnknown(self) from None

    def registration(self, *, deadline: float):
        if self._registration is not None:
            self._fail()
        try:
            observed = self._contract.decode_startup(
                self._frames.read(self._channel, min(deadline, self._startup_deadline))
            )
            if observed != self._executor.identity or observed.pid != self._custody.process.pid:
                raise ValueError
            self._registration = self._contract.registration(self._reservation, observed)
            return self._registration
        except BaseException:
            self._fail()

    def send_credentials(self, payload: bytearray, *, deadline: float) -> None:
        if self._registration is None or self._credential_sent:
            self._fail()
        self._credential_sent = True
        self._credential_exposed = True
        self._credential_buffer = (id(payload), len(payload))
        try:
            self._frames.write(self._channel, payload, min(deadline, self._operation_deadline))
        except BaseException:
            self._fail()

    def scrub_credentials(self, payload: bytearray) -> None:
        if self._credential_buffer != (id(payload), len(payload)) or any(payload):
            self._fail()
        self._credential_exposed = False
        self._credential_buffer = None

    def assert_credentials_scrubbed(self) -> None:
        if self._credential_exposed:
            self._fail()

    def writer_session(self, *, deadline: float):
        if not self._credential_sent or self._credential_exposed or self._opening_read:
            self._fail()
        self._opening_read = True
        try:
            payload = self._frames.read(self._channel, min(deadline, self._operation_deadline))
            opening = self._contract.decode_opening(payload)
            self._opening_payload, self._opening = payload, opening
            return opening
        except BaseException:
            self._fail()

    def authorize_probe(self, opening: object, *, deadline: float) -> None:
        if not self._opening_read or self._authorized or opening is not self._opening or self._opening_payload is None:
            self._fail()
        self._authorized = True
        try:
            self._frames.write(
                self._channel,
                self._contract.encode_authorization(self._opening_payload),
                min(deadline, self._operation_deadline),
            )
        except BaseException:
            self._fail()

    def result(self, *, deadline: float):
        if (
            not self._credential_sent
            or self._credential_exposed
            or self._credential_buffer is not None
            or not self._authorized
            or self._result_read
        ):
            self._fail()
        self._result_read = True
        try:
            return self._contract.decode_result(
                self._frames.read(self._channel, min(deadline, self._operation_deadline))
            )
        except BaseException:
            self._fail()

    def require_eof_and_zero(self, *, deadline: float):
        if not self._result_read or self._eof or self._registration is None:
            self._fail()
        try:
            effective = min(deadline, self._operation_deadline)
            self._frames.wait(self._channel, effective)
            if self._channel.recv(1) != b"":
                raise ValueError
            self._eof = True
            ceiling = self._executor.request_settlement(natural_deadline=effective)
            if not self._executor.done.wait(max(0.0, ceiling - time.monotonic())):
                raise TimeoutError
            exit_ = self._executor.exit
            if (
                self._executor.failed
                or exit_ is None
                or exit_.identity != self._registration.process
                or not exit_.reaped
                or exit_.exit_code != 0
                or not self._custody._closed
            ):
                raise ValueError
            self._settled = True
            return exit_
        except BaseException:
            self._fail()

    def cleanup(self):
        if self._settled:
            return self._executor.exit
        if not self._cleanup_attempted:
            self._cleanup_attempted = True
            ceiling = self._executor.request()
        else:
            ceiling = self._executor.cleanup_deadline
            if ceiling is None:
                raise RestrictedWriterVerifyProcessUnknown(self)
        if not self._executor.done.wait(max(0.0, ceiling - time.monotonic())):
            raise RestrictedWriterVerifyProcessUnknown(self)
        exit_ = self._executor.exit
        if self._executor.failed or exit_ is None or not exit_.reaped or not self._custody._closed:
            raise RestrictedWriterVerifyProcessUnknown(self)
        self._settled = True
        return exit_

    def contain(self, *, deadline: float) -> None:
        del deadline
        self.cleanup()

    def close(self) -> None:
        if not self._settled:
            raise TdsLaunchUnknown(self) from None
