"""Phased SqlClient process transport; SQL authority stays in the supervisor.

All phase calls are one-shot even after partial I/O. Implementations cap supplied
deadlines by the original launch deadlines. Raw session and result messages are
observations for independent validation, never settlement or publication proof.
"""

from typing import Literal, Protocol

from dpone.contracts.mssql_tds_api import SqlClientInputDescriptor, SqlClientLaunch, SqlClientReady
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity


class SqlClientProcess(Protocol):
    """One supervisor thread owns the original pidfd and all parent pipe ends."""

    @property
    def declared_launch(self) -> SqlClientLaunch:
        """Original immutable launch declaration, not a managed-ready receipt."""

    @property
    def bound_input(self) -> SqlClientInputDescriptor | None:
        """Inherited child-numbered input metadata; None for legacy opaque binding.

        This FD number is not a retained parent handle. Use the immutable record
        for the child's job; original input ownership remains with the caller.
        """

    @property
    def identity(self) -> TdsProcessIdentity:
        """Process incarnation independently identified before managed exec."""

    @property
    def startup_receipt(self) -> SqlClientReady | None:
        """Validated ready body after observed EOF, retained before descriptor close.

        A retained receipt does not turn a failed startup call into permission
        to release credentials; the supervisor must acknowledge registration.
        """

    @property
    def received_result(self) -> bytes | None:
        """Exact bounded body observed through EOF, retained before fallible close.

        Framing/EOF failure leaves this unset. Bytes remain available after
        post-EOF deadline rejection or close failure but still require original
        binding validation before any persistence. This observation proves
        neither local exit nor SQL settlement.
        """

    def startup(self, *, deadline: float) -> None:
        """Validate the managed ready frame and EOF against the admitted launch."""

    def send_credentials(self, body: bytes, *, deadline: float) -> None:
        """After durable registration ACK, send privately once and close the pipe."""

    def observe_session(self, *, deadline: float) -> bytes:
        """Return a bounded raw session observation for independent SQL validation."""

    def receive_session_or_result(self, *, deadline: float) -> tuple[Literal["session", "result"], bytes]:
        """After job delivery, observe announcement or early result under one deadline.

        A returned result is retained and permanently forbids a later grant. It
        still requires domain validation; premature success is not accepted here.
        """

    def send_grant(self, body: bytes, *, deadline: float) -> None:
        """After durable grant-intent ACK, send once; partial delivery is unknown."""

    def receive_result(self, *, deadline: float) -> bytes:
        """Return bounded raw result at EOF; retain it before decoding or teardown."""

    def wait(self, *, deadline: float) -> TdsChildExit:
        """Exclusively reap the direct child; this does not settle a SQL writer."""

    def terminate(self, *, deadline: float) -> TdsChildExit:
        """Contain using the held pidfd under one nonrenewable termination budget."""

    def close(self) -> None:
        """Release owned descriptors after proven local containment."""
