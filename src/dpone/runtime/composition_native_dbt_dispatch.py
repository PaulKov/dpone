"""Dispatch one admitted native dbt workload to the supervised parent root.

This is the production link between the authenticated v3 dispatch boundary and
the runtime dbt bootstrap. It holds no composition policy of its own: admission,
command shape and supervisor authority were already decided from authenticated
release and deployment bytes, and every attempt, identity, credential, capture
and outcome decision belongs to the injected parent execution root.

The dispatcher exists so that an authenticated composition workload can never
degrade into a generic child. Only the exact verified native dbt argv reaches the
bootstrap. The ordinary transfer cells are refused with a fixed sanitized reason
until their own parent roots are installed, because a supervised workload without
its worker must block rather than run unprotected.

The run volume is the caller's evidence contract: a status is published only
together with the worker's own execution evidence, and a status that disagrees
with that evidence is refused instead of returned.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.ports.composition_dbt import CompositionNativeDbtExecutor
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionDispatchRequest,
    CompositionRunVolume,
)
from dpone.runtime.dbt_execution_bootstrap import execute_dbt_pack

if TYPE_CHECKING:
    from dpone.contracts.composition_execution_authority import CompositionSupervisorProjection

ORDINARY_WORKER_UNAVAILABLE = "composition_ordinary_worker_unavailable"
NATIVE_WORKER_UNAVAILABLE = "composition_native_worker_unavailable"
SUPERVISOR_AUTHORITY = "composition_supervisor_authority"
EVIDENCE_DISAGREEMENT = "composition_evidence_disagreement"
EVIDENCE_WRITE_FAILED = "composition_evidence_write_failed"

ExecutePack = Callable[..., Any]


class CompositionNativeDbtDispatcher:
    """Execute exactly one verified native dbt pack through the parent worker.

    ``supervisor`` is the same sealed capability the execution root was composed
    with. It is required, not derived, so a deployment cannot dispatch a
    supervised attempt against a capability the composed root never accepted.
    """

    def __init__(
        self,
        executor: CompositionNativeDbtExecutor | None,
        *,
        supervisor: CompositionSupervisorProjection,
        execute_pack: ExecutePack = execute_dbt_pack,
    ) -> None:
        self._executor = executor
        self._supervisor = supervisor
        self._execute_pack = execute_pack

    def run(self, request: CompositionDispatchRequest) -> int:
        """Return the worker status, or reject fail-closed before any execution."""
        if request.kind != "native_dbt":
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        self._require_pinned_supervisor(request)
        if self._executor is None:
            raise CompositionDispatchRejection(NATIVE_WORKER_UNAVAILABLE)
        outcome = self._execute_pack(
            request.verified_input,
            environ=dict(request.env),
            runtime_root=Path(request.working_directory),
            composition_executor=self._executor,
        )
        return self._publish(request.run_volume, outcome)

    def _require_pinned_supervisor(self, request: CompositionDispatchRequest) -> None:
        """Require the admitted capability to be the one this cell was composed with.

        ``request.supervisor`` was already parsed from authenticated deployment
        bytes at the dispatch boundary. The execution root independently reparses
        the transport it receives, so a mismatch is refused twice: here before any
        pack byte is read, and again inside the root before admission.
        """
        if request.supervisor != self._supervisor:
            raise CompositionDispatchRejection(SUPERVISOR_AUTHORITY)

    @staticmethod
    def _publish(run_volume: CompositionRunVolume, outcome: Any) -> int:
        """Retain the worker's own evidence, and only an agreeing status."""
        exit_code = getattr(outcome, "exit_code", None)
        payload = outcome.evidence.to_dict() if hasattr(outcome, "evidence") else None
        if isinstance(exit_code, bool) or not isinstance(exit_code, int) or not isinstance(payload, dict):
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        # A zero status is only publishable when the retained evidence and the
        # worker's own verdict both say the attempt passed.
        passing = payload.get("status") == "passed"
        if passing != (exit_code == 0) or passing != bool(outcome.passed):
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        try:
            document = json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2)
            run_volume.evidence_path.write_text(document, encoding="utf-8")
        except (OSError, TypeError, ValueError):
            raise CompositionDispatchRejection(EVIDENCE_WRITE_FAILED, dispatch_started=True) from None
        return exit_code


__all__ = [
    "EVIDENCE_DISAGREEMENT",
    "EVIDENCE_WRITE_FAILED",
    "NATIVE_WORKER_UNAVAILABLE",
    "ORDINARY_WORKER_UNAVAILABLE",
    "SUPERVISOR_AUTHORITY",
    "CompositionNativeDbtDispatcher",
]
