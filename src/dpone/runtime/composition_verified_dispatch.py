"""Dispatch authenticated composition workloads without any execution fallback.

This module owns both ends of the composition admission marker: it projects
``DPONE_RUNTIME_RELEASE_ADMISSION`` only from authenticated
``dpone.release-set.v3`` release bytes for the verified launcher, and it converts
that marker plus the non-secret supervisor transport into either an exact typed
dispatch request or a stable rejection at execution time. Deployment wire v3 is
never composition authority on its own, because legacy v1/v2 releases with MSSQL
asset outlets use the same deployment wire.

There is no shell, generic or native-v2 fallback. An unrecognized marker, an
absent dispatcher, missing/noncanonical supervisor authority and any unexpected
command shape all reject before the runtime opens a source connection or starts
a child process. Admitted composition commands are exactly the verified runtime
argv shapes:

* native dbt: ``dpone dbt execute-pack <execution-pack> --format json``;
* ordinary transfer: ``dpone run <manifest> --format json [--selector <id>]``.

Both ordinary composition routes (PostgreSQL-to-MSSQL and MSSQL-to-ClickHouse)
share that exact verified prefix. The concrete route stays a decision of the
injected worker over the verified manifest, never a guess from environment,
argument text or shell interpretation.

Rejection reasons are fixed sanitized tokens. Command environment values never
enter a message, a log line, an exception or a request ``repr``.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.release_composition import COMPOSITION_ADMISSION, COMPOSITION_SCHEMA
from dpone.contracts.release_set_authority import admit_release_authority
from dpone.contracts.strict_json import strict_json_object
from dpone.runtime.verified_pack_diagnostics import diagnostic_message

RUNTIME_RELEASE_ADMISSION_ENV = "DPONE_RUNTIME_RELEASE_ADMISSION"
COMPOSITION_SUPERVISOR_B64_ENV = "DPONE_COMPOSITION_SUPERVISOR_B64"
COMPOSITION_DISPATCH_REJECTED = "DPONE_RUNTIME_COMPOSITION_DISPATCH_REJECTED"
COMPOSITION_DISPATCH_STAGE = "composition_dispatch"
_COMPOSITION_AUTHORITY_ROLE = "composition_authority"
_MAX_SUPERVISOR_TRANSPORT_CHARS = 4096
_NATIVE_DBT_COMMAND = ("dpone", "dbt", "execute-pack")
_ORDINARY_COMMAND = ("dpone", "run")
_JSON_FORMAT = ("--format", "json")
_SELECTOR_FLAG = "--selector"

CompositionDispatchKind = Literal["native_dbt", "ordinary_transfer"]


class CompositionDispatchRejection(Exception):
    """Fail-closed refusal to execute, carrying one fixed sanitized reason."""

    def __init__(self, reason: str, *, dispatch_started: bool = False) -> None:
        self.reason = reason
        self.dispatch_started = dispatch_started
        self.diagnostic: dict[str, Any] = {
            "error_code": COMPOSITION_DISPATCH_REJECTED,
            "status": "failed",
            "stage": COMPOSITION_DISPATCH_STAGE,
            "exception_type": type(self).__name__,
            "errno": None,
            "service_path": _COMPOSITION_AUTHORITY_ROLE,
            "reason": reason,
        }
        super().__init__(diagnostic_message(self.diagnostic))


@dataclass(frozen=True, slots=True)
class CompositionDispatchRequest:
    """One admitted composition workload, ready for an injected typed worker.

    ``verified_input`` is the worktree-relative execution pack or manifest path
    already verified by the launcher. ``env`` is the exact child environment and
    is excluded from ``repr`` so no credential can reach a log or traceback.
    """

    kind: CompositionDispatchKind
    argv: tuple[str, ...]
    verified_input: str
    process_selector: str | None
    working_directory: Path
    supervisor: CompositionSupervisorProjection
    env: Mapping[str, str] = field(repr=False)


@runtime_checkable
class CompositionVerifiedDispatcher(Protocol):
    """Injected authority that executes exactly one admitted composition workload."""

    def run(self, request: CompositionDispatchRequest) -> int:
        """Return the child execution status, or raise to reject fail-closed."""


def release_composition_admission(payload: bytes) -> str | None:
    """Project the composition admission marker from authenticated release bytes.

    Only a registered ``dpone.release-set.v3`` envelope whose constituent
    authority holds produces a marker. Legacy v1/v2 releases return ``None`` and
    keep their existing selection and certification diagnostics unchanged. An
    unprojectable composition authority raises ``ValueError`` with a fixed token,
    so the caller can report its own public integrity code without release text.
    """

    try:
        release = strict_json_object(payload)
        if release.get("schema") != COMPOSITION_SCHEMA:
            return None
        authority = admit_release_authority(release)
    except Exception as exc:
        raise ValueError("composition_release_authority") from exc
    if authority.failure is not None:
        raise ValueError("composition_release_authority")
    return authority.dbt_runtime_wire_contract


def composition_dispatch_required(environment: Mapping[str, str]) -> bool:
    """Classify the effective child admission marker for one workload attempt.

    An absent or empty marker keeps the existing native-v2 and ordinary child
    path unchanged. Only the exact authenticated composition marker selects
    supervised dispatch. Any other non-empty value is rejected, so neither a
    forged pod variable nor a future marker can reach a generic child.
    """

    marker = environment.get(RUNTIME_RELEASE_ADMISSION_ENV)
    if marker is None or marker == "":
        return False
    if marker != COMPOSITION_ADMISSION:
        raise CompositionDispatchRejection("unknown_release_admission")
    return True


def composition_dispatch_request(
    *,
    argv: tuple[str, ...],
    working_directory: Path,
    environment: Mapping[str, str],
) -> CompositionDispatchRequest:
    """Build one typed request, or reject before any dispatch or source access.

    Supervisor authority is required first: an authenticated v3 release without
    exact supervisor capability can never execute, whatever its command shape.
    """

    supervisor = _admitted_supervisor(environment)
    kind, verified_input, process_selector = _admitted_command(argv)
    return CompositionDispatchRequest(
        kind=kind,
        argv=argv,
        verified_input=verified_input,
        process_selector=process_selector,
        working_directory=working_directory,
        supervisor=supervisor,
        env=environment,
    )


def run_composition_dispatch(
    dispatcher: CompositionVerifiedDispatcher | None,
    *,
    argv: tuple[str, ...],
    working_directory: Path,
    environment: Mapping[str, str],
) -> int:
    """Run one admitted composition workload through the injected dispatcher.

    Authority is checked before worker availability, so a missing supervisor
    capability is reported truthfully even while no worker root is wired.
    """

    request = composition_dispatch_request(
        argv=argv,
        working_directory=working_directory,
        environment=environment,
    )
    if dispatcher is None:
        raise CompositionDispatchRejection("composition_dispatcher_unavailable")
    try:
        return int(dispatcher.run(request))
    except CompositionDispatchRejection:
        raise
    except Exception as exc:
        raise CompositionDispatchRejection(
            "composition_dispatch_failed",
            dispatch_started=True,
        ) from exc


def report_composition_rejection(
    exc: CompositionDispatchRejection,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Emit the bounded rejection diagnostic and return the same artifact payload."""

    message = diagnostic_message(exc.diagnostic)
    if logger is None:
        print(message, file=sys.stderr, flush=True)
    else:
        logger.error("%s", message)
    return exc.diagnostic


def _admitted_supervisor(environment: Mapping[str, str]) -> CompositionSupervisorProjection:
    raw = environment.get(COMPOSITION_SUPERVISOR_B64_ENV)
    if not isinstance(raw, str) or not raw:
        raise CompositionDispatchRejection("composition_supervisor_authority_missing")
    if len(raw) > _MAX_SUPERVISOR_TRANSPORT_CHARS:
        raise CompositionDispatchRejection("composition_supervisor_authority_oversize")
    try:
        payload = base64.b64decode(raw.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError):
        raise CompositionDispatchRejection("composition_supervisor_authority_invalid") from None
    if base64.b64encode(payload).decode("ascii") != raw:
        raise CompositionDispatchRejection("composition_supervisor_authority_noncanonical") from None
    try:
        value = json.loads(payload.decode("ascii"))
    except (UnicodeDecodeError, ValueError):
        raise CompositionDispatchRejection("composition_supervisor_authority_invalid") from None
    if not isinstance(value, Mapping):
        raise CompositionDispatchRejection("composition_supervisor_authority_invalid") from None
    try:
        projection = CompositionSupervisorProjection.from_mapping(value)
    except (KeyError, TypeError, ValueError):
        raise CompositionDispatchRejection("composition_supervisor_authority_invalid") from None
    if _canonical_supervisor_bytes(projection) != payload:
        raise CompositionDispatchRejection("composition_supervisor_authority_noncanonical") from None
    return projection


def _canonical_supervisor_bytes(projection: CompositionSupervisorProjection) -> bytes:
    return json.dumps(
        projection.to_dict(),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _admitted_command(argv: tuple[str, ...]) -> tuple[CompositionDispatchKind, str, str | None]:
    if argv[:3] == _NATIVE_DBT_COMMAND:
        if len(argv) == 6 and argv[3] and argv[4:] == _JSON_FORMAT:
            return "native_dbt", argv[3], None
        raise CompositionDispatchRejection("composition_command_shape")
    if argv[:2] == _ORDINARY_COMMAND:
        if len(argv) == 5 and argv[2] and argv[3:] == _JSON_FORMAT:
            return "ordinary_transfer", argv[2], None
        if len(argv) == 7 and argv[2] and argv[3:5] == _JSON_FORMAT and argv[5] == _SELECTOR_FLAG and argv[6]:
            return "ordinary_transfer", argv[2], argv[6]
        raise CompositionDispatchRejection("composition_command_shape")
    raise CompositionDispatchRejection("composition_command_unknown")


__all__ = [
    "COMPOSITION_DISPATCH_REJECTED",
    "COMPOSITION_DISPATCH_STAGE",
    "COMPOSITION_SUPERVISOR_B64_ENV",
    "RUNTIME_RELEASE_ADMISSION_ENV",
    "CompositionDispatchKind",
    "CompositionDispatchRejection",
    "CompositionDispatchRequest",
    "CompositionVerifiedDispatcher",
    "composition_dispatch_request",
    "composition_dispatch_required",
    "release_composition_admission",
    "report_composition_rejection",
    "run_composition_dispatch",
]
