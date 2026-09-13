"""One-way startup admission around a retained protected bootstrap original.

The listener owner polls this coordinator before serving requests. Preparation
must verify enrollment and the actual listener under the supplied fixed deadline;
this coordinator supplies ordering and lifetime, not those deployment proofs.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import Generic, Literal, Protocol, TypeVar

from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig, decode_dispatcher_service_config
from dpone.app.composition_dispatcher_service_policy import DispatcherServicePolicy
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.runtime.composition_execution_budget import ExecutionStopSignal

Handler = TypeVar("Handler")
StartupState = Literal["WAITING", "VERIFYING", "ACTIVE", "FAILED", "CLOSED"]


class BootstrapOriginal(Protocol):
    """Protected original whose descriptors remain held through service shutdown."""

    @property
    def document(self) -> bytes: ...

    @property
    def sha256(self) -> str: ...

    def require_current(self) -> None: ...

    def close(self) -> None: ...


class OpenBootstrap(Protocol):
    """Only verified leaf absence may return None; all other failures raise."""

    def __call__(self, path: Path, *, dispatcher_gid: int, deadline: float) -> BootstrapOriginal | None: ...


class DispatcherStartup(Generic[Handler]):
    """Single-threaded coordinator; ACTIVE is published only after final checks.

    No retry follows malformed originals or failed verification. ACTIVE retains
    the original without hot reload, and shutdown never resets the stop signal.
    The listener owner closes this object after its admitted work has finished.
    """

    def __init__(
        self,
        policy: DispatcherServicePolicy,
        *,
        stop: ExecutionStopSignal,
        open_original: OpenBootstrap,
        prepare: Callable[[DispatcherServiceConfig, float], Handler],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._policy = policy
        self._stop = stop
        self._open_original = open_original
        self._prepare = prepare
        self._clock = clock
        self._deadline = clock() + policy.startup_timeout_seconds
        self._state: StartupState = "WAITING"
        self._original: BootstrapOriginal | None = None
        self._handler: Handler | None = None

    @property
    def state(self) -> StartupState:
        """Expose lifecycle progress without enrollment or credential contents."""
        return self._state

    @property
    def handler(self) -> Handler:
        """Return the admitted handler only while startup is active and running."""
        if self._state != "ACTIVE" or self._stop.is_set() or self._handler is None:
            raise CompositionAdmissionError("dispatcher_bootstrap_unavailable")
        return self._handler

    def _require_time(self) -> None:
        now = self._clock()
        if self._stop.is_set() or not math.isfinite(now) or not math.isfinite(self._deadline) or now >= self._deadline:
            raise CompositionAdmissionError("dispatcher_bootstrap_unavailable")

    def poll(self) -> bool:
        """Observe absence or activate once, never renewing the startup deadline."""
        if self._state == "ACTIVE":
            _ = self.handler
            return True
        if self._state != "WAITING":
            raise CompositionAdmissionError("dispatcher_bootstrap_unavailable")
        try:
            self._require_time()
            self._original = self._open_original(
                self._policy.bootstrap_file, dispatcher_gid=self._policy.dispatcher_gid, deadline=self._deadline
            )
            self._require_time()
            if self._original is None:
                return False
            self._state = "VERIFYING"
            config = decode_dispatcher_service_config(
                self._original.document,
                expected_sha256=self._original.sha256,
                bootstrap_uid=self._policy.dispatcher_uid,
                bootstrap_gid=self._policy.dispatcher_gid,
            )
            if config.service_policy != self._policy:
                raise CompositionAdmissionError("dispatcher_bootstrap_unavailable")
            self._require_time()
            handler = self._prepare(config, self._deadline)
            self._require_time()
            self._original.require_current()
            self._require_time()
            if handler is None:
                raise CompositionAdmissionError("dispatcher_bootstrap_unavailable")
            self._handler = handler
            self._state = "ACTIVE"
            return True
        except BaseException as error:
            self._state = "FAILED"
            try:
                self._release()
            except Exception:
                pass
            if not isinstance(error, Exception):
                raise
            raise CompositionAdmissionError("dispatcher_bootstrap_unavailable") from None

    def _release(self) -> None:
        original, self._original = self._original, None
        self._handler = None
        if original is not None:
            original.close()

    def close(self) -> None:
        """Irreversibly close admission and release ownership exactly once."""
        self._state = "CLOSED"
        self._release()
