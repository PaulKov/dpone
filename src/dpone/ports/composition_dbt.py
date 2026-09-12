"""Parent execution authority consumed by the shared dbt process engine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dpone.contracts.composition_execution_authority import COMPOSITION_SUPERVISOR_B64_ENV

if TYPE_CHECKING:
    from pathlib import Path

    from dpone.contracts.dbt_runtime import (
        AirflowAttemptCorrelation,
        AirflowRunIdentity,
        DbtExecutionInterval,
        DbtExecutionPack,
    )
    from dpone.ports.dbt_publishing import DbtExecutionOutcome


class CompositionDbtBuildAuthority(Protocol):
    """Reopen protected parent admission immediately before executing dbt build.

    The enclosing worker owns credential issuance, closure, quiescence and
    terminal proofs. Process success alone cannot terminalize a parent attempt.
    """

    def verify_before_build(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> None: ...


class CompositionNativeDbtExecutor(Protocol):
    """Supervised parent root that owns one complete native dbt attempt.

    The runtime bootstrap holds no composition policy. It only detects the pinned
    ``COMPOSITION_SUPERVISOR_B64_ENV`` capability and forwards its exact transport
    value. Parsing that capability and requiring it to match the supervisor the
    root was actually composed with belongs to the root itself.
    """

    def execute_native_pack(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        runtime_root: Path,
        interval: DbtExecutionInterval,
        supervisor_transport: str,
    ) -> DbtExecutionOutcome: ...


__all__ = [
    "COMPOSITION_SUPERVISOR_B64_ENV",
    "CompositionDbtBuildAuthority",
    "CompositionNativeDbtExecutor",
]
