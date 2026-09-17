"""Once-only trusted dbt command observation under an admitted invocation.

This recorder attests the qualified synchronous delegate's normal return. It
neither discovers SQL sessions nor grants source custody, credentials or runtime
qualification. The bootstrap must authenticate those authorities before wiring it.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from threading import TIMEOUT_MAX, Event, Lock

from dpone.adapters.native_generation_invocation_auth import (
    InvocationOriginalReader,
    authenticate_invocation,
    verify_invocation_paths,
)
from dpone.contracts.native_generation_invocation import (
    AuthenticatedInvocationPlan,
    TrustedDbtInvocationCompletion,
    encode_trusted_dbt_invocation_completion,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeOriginalKind, NativeOriginalSubject
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceExecutorBinding,
    decode_source_executor_binding,
    encode_source_executor_binding,
)
from dpone.ports.dbt_publishing import DbtCommandResult, DbtCommandRunner
from dpone.ports.native_originals import (
    BoundNativeOriginalPublisher,
    NativeOriginalBindingPort,
    NativeOriginalReaderPort,
)


class TrustedDbtInvocationRecorder:
    """Consume one fixed BUILD/QUALITY sequence and retain immutable completion.

    A sequence mutex covers every synchronous delegate call. A short state lock
    allows close to latch UNKNOWN without waiting again for an overdue delegate.
    Publication uses a separate mutex so storage latency cannot obstruct close.
    """

    def __init__(
        self,
        *,
        delegate: DbtCommandRunner,
        executor: SourceExecutorBinding,
        command: OriginalRef,
        toolchain: OriginalRef,
        qualification: OriginalRef,
        publish_original: BoundNativeOriginalPublisher,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
        originals: NativeOriginalReaderPort,
        bindings: NativeOriginalBindingPort,
        subject: NativeOriginalSubject,
        max_metadata_bytes: int,
    ) -> None:
        self._executor = decode_source_executor_binding(encode_source_executor_binding(executor))
        self._command = OriginalRef(command.locator, command.sha256)
        self._toolchain = OriginalRef(toolchain.locator, toolchain.sha256)
        self._qualification = OriginalRef(qualification.locator, qualification.sha256)
        self._reader = InvocationOriginalReader(
            originals=originals, bindings=bindings, subject=subject, max_bytes=max_metadata_bytes
        )
        self._reader.require_generation(self._executor)
        self._delegate = delegate
        self._publish = publish_original
        self._clock = clock
        self._monotonic = monotonic_clock
        self._sequence = Lock()
        self._state = Lock()
        self._publication = Lock()
        self._close_requested = Event()
        self._failure: str | None = None
        self._next = 0
        self._succeeded = 0
        self._started: float | None = None
        self._started_at: str | None = None
        self._deadline: float | None = None
        self._close_deadline: float | None = None
        self._profile: Path | None = None
        self._profile_identity: tuple[int, int] | None = None
        self._completion: bytes | None = None
        self._publication_attempted = False

    def require_executor(self, executor: SourceExecutorBinding) -> None:
        """Compare the complete bound identity without I/O or admission authority."""
        if decode_source_executor_binding(encode_source_executor_binding(executor)) != self._executor:
            raise NativeSourceCustodyError("invocation executor differs from its bound identity")

    def validate_before_credentials(self) -> None:
        """Freshly authenticate retained admission before a caller resolves secrets."""
        with self._sequence:
            try:
                self._require_open()
                self._authenticate()
            except BaseException:
                self._fail("invocation authentication failed")
                raise

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        """Admit exactly one locked slot, retaining ownership until normal return."""
        self._require_open()
        with self._sequence:
            self._require_open()
            try:
                authenticated = self._authenticate()
                plan = authenticated.plan
                if self._next >= len(plan.commands):
                    raise NativeSourceCustodyError("trusted invocation has no remaining command slots")
                entry = plan.commands[self._next]
                if type(timeout_seconds) is not int or timeout_seconds != entry.command_timeout_seconds:
                    raise NativeSourceCustodyError("command timeout differs from the locked plan")
                profile = verify_invocation_paths(
                    authenticated, position=self._next, args=args, cwd=cwd, previous_profile=self._profile
                )
                profile_stat = profile.lstat()
                profile_identity = (profile_stat.st_dev, profile_stat.st_ino)
                if self._profile_identity is not None and profile_identity != self._profile_identity:
                    raise NativeSourceCustodyError("invocation profile directory identity changed")
                admitted = self._now()
                with self._state:
                    self._check_open()
                    self._check_deadline(admitted)
                    self._profile = profile
                    self._profile_identity = profile_identity
                    self._next += 1  # Consumed before calling a delegate that may raise.
                result = self._delegate.run(args, cwd=cwd, timeout_seconds=timeout_seconds, redactions=redactions)
                finished = self._now()
                with self._state:
                    if self._failure is not None:
                        raise NativeSourceCustodyError(self._failure)
                    self._check_deadline(finished)
                    if (
                        finished < admitted
                        or finished - admitted > entry.command_timeout_seconds + entry.termination_allowance_seconds
                    ):
                        raise NativeSourceCustodyError("trusted command exceeded its termination allowance")
                    if (
                        type(result) is not DbtCommandResult
                        or type(result.exit_code) is not int
                        or result.exit_code != 0
                    ):
                        raise NativeSourceCustodyError("trusted command did not return an exact zero exit code")
                    self._succeeded += 1
                    if self._succeeded == len(plan.commands):
                        self._capture_completion(finished)
                return result
            except BaseException:
                self._fail("trusted invocation failed; no positive completion")
                raise

    def close(self, *, deadline_monotonic: float) -> None:
        """Close future admission; an overdue admitted call remains UNKNOWN.

        Expiry does not claim cancellation. A later normal delegate return cannot
        clear the UNKNOWN latch. Already completed valid sequences stay valid.
        """
        self._close_requested.set()
        if type(deadline_monotonic) not in (int, float) or not math.isfinite(deadline_monotonic):
            self._fail("UNKNOWN: invalid close deadline")
            raise NativeSourceCustodyError("UNKNOWN: close requires a finite absolute deadline")
        with self._state:
            deadline = deadline_monotonic if self._deadline is None else min(deadline_monotonic, self._deadline)
            if self._completion is None:
                self._close_deadline = deadline if self._close_deadline is None else min(deadline, self._close_deadline)
        try:
            allowance = max(0.0, min(TIMEOUT_MAX, deadline - self._now()))
        except BaseException:
            self._fail("UNKNOWN: close clock failed")
            raise
        if not self._sequence.acquire(timeout=allowance):
            with self._state:
                if self._completion is not None:
                    return
                self._failure = "UNKNOWN: admitted command exceeded close deadline"
            raise NativeSourceCustodyError("UNKNOWN: admitted command remains unresolved after close deadline")
        self._sequence.release()
        with self._state:
            if self._failure is not None and self._failure.startswith("UNKNOWN:"):
                raise NativeSourceCustodyError(self._failure)

    def require_completion(self) -> OriginalRef:
        """Publish once; after any uncertain ACK reconcile only the exact original."""
        with self._publication:
            with self._state:
                if self._failure is not None or self._completion is None:
                    raise NativeSourceCustodyError("trusted invocation has no positive complete sequence")
                payload = self._completion
            reference = OriginalRef(
                f"generations/{self._executor.generation_id}/invocations/{self._executor.invocation_id}/completion.json",
                "sha256:" + sha256(payload).hexdigest(),
            )
            kind: NativeOriginalKind = "trusted_dbt_invocation_completion_v1"
            if not self._publication_attempted:
                self._publication_attempted = True
                published = self._publish(
                    kind=kind, locator=reference.locator, payload=payload, max_bytes=self._reader.max_bytes
                )
                if type(published) is not OriginalRef or published != reference:
                    raise NativeSourceCustodyError(
                        "published completion differs from the immutable invocation identity"
                    )
            if self._reader.read(reference, kind) != payload:
                raise NativeSourceCustodyError("completion readback differs from captured normal return")
            with self._state:
                if self._failure is not None:
                    raise NativeSourceCustodyError(self._failure)
            return reference

    def _authenticate(self) -> AuthenticatedInvocationPlan:
        before = self._now()
        authenticated = authenticate_invocation(
            self._reader,
            executor=self._executor,
            command=self._command,
            toolchain=self._toolchain,
            qualification=self._qualification,
        )
        with self._state:
            self._check_open()
            if self._started is None:
                self._started = before
                self._started_at = self._timestamp()
                self._deadline = before + authenticated.plan.total_termination_budget_seconds
            self._check_deadline(self._now())
        return authenticated

    def _require_open(self) -> None:
        with self._state:
            self._check_open()

    def _check_open(self) -> None:
        if self._close_requested.is_set() or self._failure is not None or self._completion is not None:
            raise NativeSourceCustodyError("trusted invocation admission is closed")

    def _check_deadline(self, now: float) -> None:
        if self._close_deadline is not None and now > self._close_deadline:
            self._failure = "UNKNOWN: admitted command exceeded close deadline"
            raise NativeSourceCustodyError(self._failure)
        if self._started is None or self._deadline is None or now < self._started or now > self._deadline:
            raise NativeSourceCustodyError("trusted invocation exceeded its absolute termination allowance")

    def _fail(self, message: str) -> None:
        with self._state:
            if self._completion is None and self._failure is None:
                self._failure = message
        self._close_requested.set()

    def _capture_completion(self, finished: float) -> None:
        if self._started is None or self._started_at is None:
            raise NativeSourceCustodyError("trusted invocation lacks an observed start")
        self._completion = encode_trusted_dbt_invocation_completion(
            TrustedDbtInvocationCompletion(
                schema="dpone.trusted-dbt-invocation-completion.v1",
                executor=self._executor,
                command=self._command,
                toolchain=self._toolchain,
                qualification=self._qualification,
                started_at=self._started_at,
                finished_at=self._timestamp(),
                elapsed_microseconds=int((finished - self._started) * 1000000),
                exit_code=0,
                command_count=self._succeeded,
            )
        )

    def _now(self) -> float:
        value = self._monotonic()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise NativeSourceCustodyError("invocation monotonic clock must return finite seconds")
        return float(value)

    def _timestamp(self) -> str:
        value = self._clock()
        if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
            raise NativeSourceCustodyError("invocation wall clock must return an aware datetime")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
