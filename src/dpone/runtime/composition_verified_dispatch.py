"""Dispatch authenticated composition workloads without any execution fallback.

Runtime policy only: this module decides whether one prepared command may run as
immutable composition, which typed request a worker receives, and when a result
must be refused. All byte and value semantics live behind the single contract
facade ``dpone.contracts.composition_execution_authority``.

Authority is the authenticated ``VerifiedPackCommand`` environment produced by the
verified launcher from release and deployment bytes. The ambient pod environment
is never authority: it can neither admit, erase nor substitute composition. The
merged child environment is passed onward only so an admitted worker can run its
own child.

There is no shell, generic or native-v2 fallback. An unrecognized marker, an
absent dispatcher, missing/noncanonical supervisor authority, an unsafe verified
input path and any unexpected command shape all reject before the runtime opens a
source connection or starts a child process. Admitted composition commands are
exactly the verified runtime argv shapes:

* native dbt: ``dpone dbt execute-pack <execution-pack> --format json``;
* ordinary transfer: ``dpone run <manifest> --format json [--selector <id>]``.

Both ordinary composition routes (PostgreSQL-to-MSSQL and MSSQL-to-ClickHouse)
share that exact verified prefix. The concrete route stays a decision of the
injected worker over the verified manifest, never a guess from environment,
argument text or shell interpretation.

A worker that reports success must leave valid current-attempt evidence on the
run volume it was given; otherwise the attempt is refused instead of publishing a
passing summary. Rejection reasons are fixed sanitized tokens, and command
environment values never enter a message, a log line, an exception or a request
``repr``.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from dpone.contracts.composition_execution_authority import (
    COMPOSITION_AUTHORITY_REASONS,
    COMPOSITION_SUPERVISOR_B64_ENV,
    RUNTIME_RELEASE_ADMISSION_ENV,
    SUPERVISOR_AUTHORITY_MISSING,
    CompositionSupervisorProjection,
    composition_evidence_object,
    deployment_supervisor_transport,
    is_composition_admission,
    release_composition_admission,
    supervisor_from_transport,
)
from dpone.runtime.verified_pack_diagnostics import diagnostic_message

COMPOSITION_DISPATCH_REJECTED = "DPONE_RUNTIME_COMPOSITION_DISPATCH_REJECTED"
COMPOSITION_DISPATCH_STAGE = "composition_dispatch"
_COMPOSITION_AUTHORITY_ROLE = "composition_authority"
_EVIDENCE_CLOCK_TOLERANCE_SECONDS = 2.0
_NATIVE_DBT_COMMAND = ("dpone", "dbt", "execute-pack")
_ORDINARY_COMMAND = ("dpone", "run")
_JSON_FORMAT = ("--format", "json")
_SELECTOR_FLAG = "--selector"
_UNSAFE_PATH_SEGMENTS = frozenset({"", ".", ".."})
_CONTROL_CHARACTERS = ("\x00", "\n", "\r")

COMPOSITION_AUTHORITY_ENV_KEYS = (RUNTIME_RELEASE_ADMISSION_ENV, COMPOSITION_SUPERVISOR_B64_ENV)

CompositionDispatchKind = Literal["native_dbt", "ordinary_transfer"]


class CompositionDispatchRejection(Exception):
    """Fail-closed refusal to execute, carrying one fixed sanitized reason.

    ``dispatch_started`` is an evidence-preservation obligation, not a detail:
    when it is true the injected worker already ran and may have written partial
    attempt evidence, so the caller must record the failure without overwriting
    ``runtime-evidence.json``. When it is false no worker ran and the caller
    replaces stale evidence so a refused attempt cannot reuse an earlier result.
    """

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
class CompositionRunVolume:
    """The exact attempt-scoped run-volume destinations owned by the executor.

    A worker root writes its evidence and stderr here instead of hard-coding a
    container path, so run-volume ownership stays with the verified executor.
    """

    evidence_path: Path
    stderr_path: Path


@dataclass(frozen=True, slots=True)
class CompositionDispatchRequest:
    """One admitted composition workload, ready for an injected typed worker.

    ``verified_input`` is the worktree-relative execution pack or manifest path
    already verified by the launcher and re-checked at this boundary. ``env`` is
    the exact child environment and is excluded from ``repr`` so no credential can
    reach a log or traceback.
    """

    kind: CompositionDispatchKind
    argv: tuple[str, ...]
    verified_input: str
    process_selector: str | None
    working_directory: Path
    supervisor: CompositionSupervisorProjection
    run_volume: CompositionRunVolume
    env: Mapping[str, str] = field(repr=False)


class CompositionVerifiedDispatcher(Protocol):
    """Injected authority that executes exactly one admitted composition workload."""

    def run(self, request: CompositionDispatchRequest) -> int:
        """Return the child execution status, or raise to reject fail-closed."""


def composition_command_authority(
    *,
    release_payload: bytes,
    deployment_payload: bytes,
) -> Mapping[str, str]:
    """Project the composition environment of one verified command, as one unit.

    The admission marker comes from authenticated release bytes and the pinned
    non-secret supervisor capability from the sealed deployment bytes. A legacy
    v1/v2 release yields an empty mapping, even when its MSSQL asset outlets use
    the same v3 deployment wire. Composition without the exact sealed capability,
    and a legacy release that carries one, both raise ``ValueError`` with a fixed
    token so no command is ever produced.
    """

    admission = release_composition_admission(release_payload)
    transport = deployment_supervisor_transport(deployment_payload, admission=admission)
    if admission is None:
        return {}
    if transport is None:
        # Unreachable while the contract holds; fail closed rather than let a
        # composition release silently degrade to a legacy child.
        raise ValueError(SUPERVISOR_AUTHORITY_MISSING)
    return {
        RUNTIME_RELEASE_ADMISSION_ENV: admission,
        COMPOSITION_SUPERVISOR_B64_ENV: transport,
    }


def composition_dispatch_required(authority: Mapping[str, str]) -> bool:
    """Classify the authenticated command admission marker for one attempt.

    ``authority`` must be the verified ``VerifiedPackCommand`` environment. An
    absent or empty marker keeps the existing native-v2 and ordinary child path
    unchanged. Only the exact composition marker selects supervised dispatch. Any
    other non-empty value is rejected, so no unsupported or future marker can
    reach a generic child.
    """

    marker = authority.get(RUNTIME_RELEASE_ADMISSION_ENV)
    if marker is None or marker == "":
        return False
    if not is_composition_admission(marker):
        raise CompositionDispatchRejection("unknown_release_admission")
    return True


def composition_dispatch_request(
    *,
    argv: tuple[str, ...],
    working_directory: Path,
    authority: Mapping[str, str],
    environment: Mapping[str, str],
    run_volume: CompositionRunVolume,
) -> CompositionDispatchRequest:
    """Build one typed request, or reject before any dispatch or source access.

    Supervisor authority is required first: an authenticated v3 release without
    the exact pinned capability can never execute, whatever its command shape.
    """

    supervisor = _admitted_supervisor(authority)
    kind, verified_input, process_selector = _admitted_command(argv)
    return CompositionDispatchRequest(
        kind=kind,
        argv=argv,
        verified_input=verified_input,
        process_selector=process_selector,
        working_directory=working_directory,
        supervisor=supervisor,
        run_volume=run_volume,
        env=environment,
    )


def run_composition_dispatch(
    dispatcher: CompositionVerifiedDispatcher | None,
    *,
    argv: tuple[str, ...],
    working_directory: Path,
    authority: Mapping[str, str],
    environment: Mapping[str, str],
    run_volume: CompositionRunVolume,
) -> int:
    """Run one admitted composition workload through the injected dispatcher.

    Authority is checked before worker availability, so a missing supervisor
    capability is reported truthfully even while no worker root is wired. A
    successful status is accepted only together with valid current-attempt
    evidence, which keeps a silent worker from publishing a passing summary.
    """

    request = composition_dispatch_request(
        argv=argv,
        working_directory=working_directory,
        authority=authority,
        environment=environment,
        run_volume=run_volume,
    )
    if dispatcher is None:
        raise CompositionDispatchRejection("composition_dispatcher_unavailable")
    dispatched_after = time.time() - _EVIDENCE_CLOCK_TOLERANCE_SECONDS
    try:
        status = int(dispatcher.run(request))
    except CompositionDispatchRejection:
        raise
    except Exception as exc:
        raise CompositionDispatchRejection(
            "composition_dispatch_failed",
            dispatch_started=True,
        ) from exc
    if status == 0:
        _require_current_attempt_evidence(run_volume, since=dispatched_after)
    return status


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


def _admitted_supervisor(authority: Mapping[str, str]) -> CompositionSupervisorProjection:
    try:
        return supervisor_from_transport(authority.get(COMPOSITION_SUPERVISOR_B64_ENV))
    except ValueError as exc:
        raise CompositionDispatchRejection(_authority_reason(exc)) from None


def _authority_reason(exc: ValueError) -> str:
    reason = str(exc)
    return reason if reason in COMPOSITION_AUTHORITY_REASONS else "composition_authority_invalid"


def _require_current_attempt_evidence(run_volume: CompositionRunVolume, *, since: float) -> None:
    try:
        modified = run_volume.evidence_path.stat().st_mtime
        payload = run_volume.evidence_path.read_bytes()
    except OSError:
        raise CompositionDispatchRejection("composition_evidence_missing", dispatch_started=True) from None
    if modified < since:
        raise CompositionDispatchRejection("composition_evidence_stale", dispatch_started=True)
    try:
        composition_evidence_object(payload)
    except ValueError:
        raise CompositionDispatchRejection("composition_evidence_invalid", dispatch_started=True) from None


def _admitted_command(argv: tuple[str, ...]) -> tuple[CompositionDispatchKind, str, str | None]:
    kind, verified_input, process_selector = _admitted_command_shape(argv)
    if not _safe_verified_input(verified_input):
        raise CompositionDispatchRejection("composition_command_path")
    return kind, verified_input, process_selector


def _admitted_command_shape(argv: tuple[str, ...]) -> tuple[CompositionDispatchKind, str, str | None]:
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


def _safe_verified_input(value: str) -> bool:
    """Re-check the launcher-verified relative input at the dispatch boundary."""

    if not value or value.startswith("/") or "\\" in value:
        return False
    if any(control in value for control in _CONTROL_CHARACTERS):
        return False
    return all(segment not in _UNSAFE_PATH_SEGMENTS for segment in value.split("/"))


__all__ = [
    "COMPOSITION_AUTHORITY_ENV_KEYS",
    "COMPOSITION_DISPATCH_REJECTED",
    "COMPOSITION_DISPATCH_STAGE",
    "COMPOSITION_SUPERVISOR_B64_ENV",
    "RUNTIME_RELEASE_ADMISSION_ENV",
    "CompositionDispatchKind",
    "CompositionDispatchRejection",
    "CompositionDispatchRequest",
    "CompositionRunVolume",
    "CompositionVerifiedDispatcher",
    "composition_command_authority",
    "composition_dispatch_request",
    "composition_dispatch_required",
    "report_composition_rejection",
    "run_composition_dispatch",
]
