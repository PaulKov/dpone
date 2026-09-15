"""Preserve execution ambiguity when persisting dbt evidence fails."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.runtime.commit_unknown import CommitUnknownError

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import DbtExecutionEvidence
    from dpone.ports.dbt_publishing import DbtExecutionEvidenceWriter


def persist_execution_evidence(writer: DbtExecutionEvidenceWriter, evidence: DbtExecutionEvidence) -> None:
    """Persist once; post-dispatch write failures cannot become retry permission."""
    try:
        writer.write(evidence)
    except Exception as exc:
        if evidence.build_started:
            raise CommitUnknownError(
                CommitUnknownOutcome(failure_boundary="target_invocation", checkpoint_state="not_advanced")
            ) from exc
        raise
