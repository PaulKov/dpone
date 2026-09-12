"""Bounded one-request Git batch transport with fail-closed process ownership."""

from __future__ import annotations

import hashlib
import os
import re
import selectors
import signal
import subprocess
import time
from pathlib import Path
from subprocess import Popen
from threading import Lock
from typing import IO

from tools.agent_policy.public_clean_policy import Budget
from tools.agent_policy.public_clean_receipts import GateError
from tools.agent_policy.tenant_hygiene import MAX_SOURCE_BLOB_BYTES

OID = re.compile(r"[0-9a-f]{40}\Z")
HEADER = re.compile(rb"([0-9a-f]{40}) blob (0|[1-9][0-9]*)\n\Z")


def git_invocation(*args: str) -> tuple[list[str], dict[str, str]]:
    """Compose the existing offline Git command and environment without widening it."""
    environment = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    environment.update(GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1", GIT_TERMINAL_PROMPT="0")
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    environment.update(GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="")
    return [
        "git",
        "--no-optional-locks",
        "--no-replace-objects",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "protocol.allow=never",
        *args,
    ], environment


class GitBatch:
    """One owned child, one outstanding blob request, no retry or resynchronization."""

    def __init__(self, root: Path, budget: Budget, timeout: float = 30) -> None:
        if not 0 < timeout <= 30:
            raise GateError("GIT_TIMEOUT_INVALID")
        self.root = root
        self.budget = budget
        self.timeout = timeout
        self._process: subprocess.Popen[bytes] | None = None
        self._deadline = 0.0
        self._closed = False
        self._aborted = False
        self._request = Lock()

    def __enter__(self) -> GitBatch:
        if self._process is not None or self._closed:
            raise GateError("GIT_BATCH_CLOSED")
        self.budget.tick()
        command, environment = git_invocation("cat-file", "--batch")
        try:
            self._process = Popen(
                command,
                cwd=self.root,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            assert self._process.stdin is not None and self._process.stdout is not None
            os.set_blocking(self._process.stdin.fileno(), False)
            os.set_blocking(self._process.stdout.fileno(), False)
            self.budget.tick()
            return self
        except OSError as exc:
            self._abort()
            raise GateError("GIT_UNAVAILABLE") from exc
        except BaseException:
            self._abort()
            raise

    def _remaining(self) -> float:
        self.budget.tick()
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise GateError("GIT_TIMEOUT")
        return remaining

    def _wait(self, stream: IO[bytes], event: int) -> None:
        with selectors.DefaultSelector() as selector:
            selector.register(stream, event)
            selector.select(min(self._remaining(), 0.1))

    def _read(self, size: int) -> bytes:
        assert self._process is not None and self._process.stdout is not None
        while True:
            self._remaining()
            try:
                return os.read(self._process.stdout.fileno(), size)
            except BlockingIOError:
                self._wait(self._process.stdout, selectors.EVENT_READ)

    def _exact(self, size: int) -> bytes:
        result = bytearray()
        while len(result) < size:
            chunk = self._read(min(65536, size - len(result)))
            if not chunk:
                raise GateError("GIT_BATCH_TRUNCATED")
            result.extend(chunk)
        return bytes(result)

    def _pending(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        self._remaining()
        try:
            extra = os.read(self._process.stdout.fileno(), 1)
        except BlockingIOError:
            return
        raise GateError("GIT_BATCH_EXTRA" if extra else "GIT_UNAVAILABLE")

    def blob(self, oid: str) -> bytes:
        if self._closed:
            raise GateError("GIT_BATCH_CLOSED")
        if not self._request.acquire(blocking=False):
            self._abort()
            raise GateError("GIT_BATCH_BUSY")
        try:
            if self._closed or self._process is None:
                raise GateError("GIT_BATCH_CLOSED")
            if not isinstance(oid, str) or not OID.fullmatch(oid):
                raise GateError("GIT_ID_INVALID")
            self._deadline = time.monotonic() + self.timeout
            self._pending()
            assert self._process.stdin is not None
            request = memoryview((oid + "\n").encode("ascii"))
            while request:
                self._remaining()
                try:
                    written = os.write(self._process.stdin.fileno(), request)
                    if written <= 0:
                        raise GateError("GIT_UNAVAILABLE")
                    request = request[written:]
                except BlockingIOError:
                    self._wait(self._process.stdin, selectors.EVENT_WRITE)
            header = bytearray()
            while not header.endswith(b"\n"):
                if len(header) >= 128:
                    raise GateError("GIT_BATCH_HEADER")
                header.extend(self._exact(1))
            match = HEADER.fullmatch(header)
            if match is None or match[1].decode("ascii") != oid:
                raise GateError("GIT_BATCH_HEADER")
            size = int(match[2])
            if size > MAX_SOURCE_BLOB_BYTES:
                raise GateError("BLOB_LIMIT")
            raw = self._exact(size)
            if self._exact(1) != b"\n":
                raise GateError("GIT_BATCH_TERMINATOR")
            if hashlib.sha1(b"blob " + str(size).encode("ascii") + b"\0" + raw).hexdigest() != oid:
                raise GateError("GIT_BATCH_IDENTITY")
            self._pending()
            return raw
        except (OSError, ValueError) as exc:
            self._abort()
            raise GateError("GIT_UNAVAILABLE") from exc
        except BaseException:
            self._abort()
            raise
        finally:
            self._request.release()

    def _abort(self) -> None:
        """Best-effort cleanup never masks the original failure or cancellation."""
        if self._aborted:
            return
        self._aborted = True
        self._closed = True
        process = self._process
        if process is None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            process.wait(timeout=1)
        except (OSError, subprocess.TimeoutExpired):
            pass
        for stream in (process.stdin, process.stdout):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if exc_type is not None:
            self._abort()
            return
        try:
            if self._closed or self._process is None:
                raise GateError("GIT_BATCH_CLOSED")
            self._deadline = time.monotonic() + self.timeout
            self._remaining()
            assert self._process.stdin is not None
            self._process.stdin.close()
            if self._read(1):
                raise GateError("GIT_BATCH_EXTRA")
            while self._process.poll() is None:
                try:
                    self._process.wait(timeout=min(self._remaining(), 0.1))
                except subprocess.TimeoutExpired:
                    continue
            self._remaining()
            if self._process.returncode != 0:
                raise GateError("GIT_UNAVAILABLE")
            self._closed = True
            assert self._process.stdout is not None
            self._process.stdout.close()
        except (OSError, ValueError) as exc:
            self._abort()
            raise GateError("GIT_UNAVAILABLE") from exc
        except BaseException:
            self._abort()
            raise
