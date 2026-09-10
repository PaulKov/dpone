"""Safe operational diagnostics for verified command and service-file failures.

Only caller-selected stages and logical service roles cross the log boundary.
OS exception messages, filenames, child arguments and environment values never
form part of a diagnostic.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

PACK_EXEC_FAILED = "DPONE_RUNTIME_PACK_EXEC_FAILED"


class VerifiedPackServiceError(Exception):
    """A stage-specific OS failure whose message exposes no original OS text."""

    def __init__(self, exc: OSError, *, stage: str, service_path: str) -> None:
        self.diagnostic: dict[str, Any] = {
            "error_code": PACK_EXEC_FAILED,
            "status": "failed",
            "stage": stage,
            "exception_type": type(exc).__name__,
            "errno": exc.errno,
            "service_path": service_path,
        }
        super().__init__(diagnostic_message(self.diagnostic))


@contextmanager
def service_file_stage(stage: str, service_path: str) -> Iterator[None]:
    """Attribute an OS error to the operation, without exposing unsafe text."""

    try:
        yield
    except OSError as exc:
        raise VerifiedPackServiceError(exc, stage=stage, service_path=service_path) from None


def diagnostic_message(diagnostic: dict[str, Any]) -> str:
    """Render the bounded diagnostic fields for either CLI or container logs."""

    return (
        f"{PACK_EXEC_FAILED}: stage={diagnostic['stage']} "
        f"exception_type={diagnostic['exception_type']} errno={diagnostic['errno']} "
        f"service_path={diagnostic['service_path']}"
    )


def report_pack_os_error(
    exc: OSError | VerifiedPackServiceError,
    *,
    stage: str = "prepare_verified_command",
    service_path: str = "verified_inputs",
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Emit an actionable safe diagnostic and return the same artifact payload."""

    failure = (
        exc
        if isinstance(exc, VerifiedPackServiceError)
        else VerifiedPackServiceError(exc, stage=stage, service_path=service_path)
    )
    message = diagnostic_message(failure.diagnostic)
    if logger is None:
        print(message, file=sys.stderr, flush=True)
    else:
        logger.error("%s", message)
    return failure.diagnostic


__all__ = [
    "VerifiedPackServiceError",
    "diagnostic_message",
    "report_pack_os_error",
    "service_file_stage",
]
