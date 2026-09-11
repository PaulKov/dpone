"""Parent execution authority consumed by the shared dbt process engine."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import AirflowAttemptCorrelation, AirflowRunIdentity, DbtExecutionPack


class CompositionDbtBuildAuthority(Protocol):
    """Reopen protected parent admission immediately before executing dbt build.

    The enclosing worker owns credential issuance, closure, quiescence and
    terminal proofs. Process success alone cannot terminalize a parent attempt.
    """

    def verify_before_build(
        self,
        *,
        pack: DbtExecutionPack,
        manifest: Mapping[str, Any],
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> None: ...
