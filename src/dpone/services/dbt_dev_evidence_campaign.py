"""Application service for one deterministic Airflow dev-evidence campaign."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from dpone.ports.dbt_airflow_evidence import (
    DbtAirflowEvidencePort,
    DbtDevEvidenceCampaignError,
    DbtDevEvidenceCampaignJournalPort,
)
from dpone.services.dbt_dev_evidence_campaign_receipt import (
    DbtDevEvidenceCampaignReceipt,
)
from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest

DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED = "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED"
_WAITING_STATES = frozenset({"queued", "running"})
_TERMINAL_FAILURE_STATES = frozenset({"failed"})


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceCampaignReport:
    """Bounded result of the trusted Airflow campaign controller."""

    evidence_set_id: str
    release_id: str
    deployment_id: str
    workflow_states: tuple[tuple[str, str], ...]
    elapsed_seconds: float

    @property
    def passed(self) -> bool:
        return bool(self.workflow_states) and all(state == "success" for _, state in self.workflow_states)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-dev-evidence-campaign.v1",
            "status": "passed" if self.passed else "failed",
            "passed": self.passed,
            "code": (
                "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED" if self.passed else DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED
            ),
            "evidence_set_id": self.evidence_set_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "workflow_states": [
                {"workflow_id": workflow_id, "state": state} for workflow_id, state in self.workflow_states
            ],
            "elapsed_seconds": self.elapsed_seconds,
        }


class DbtDevEvidenceCampaignService:
    """Trigger all workflow DAGs and wait fail-closed for terminal success."""

    def __init__(
        self,
        *,
        airflow: DbtAirflowEvidencePort,
        journal: DbtDevEvidenceCampaignJournalPort,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._airflow = airflow
        self._journal = journal
        self._monotonic = monotonic
        self._sleep = sleep

    def run(
        self,
        request: DbtDevEvidenceRequest,
        *,
        timeout_seconds: int = 1_800,
        poll_interval_seconds: int = 5,
    ) -> DbtDevEvidenceCampaignReport:
        _validate_limits(
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        request = DbtDevEvidenceRequest.from_mapping(request.to_dict())
        started = self._monotonic()
        states: dict[str, str] = {workflow.workflow_id: "queued" for workflow in request.workflows}
        opened = False
        try:
            self._journal.open(request)
            opened = True
            for workflow in request.workflows:
                self._airflow.trigger(
                    dag_id=workflow.dag_id,
                    dag_run_id=workflow.dag_run_id,
                    conf={
                        "dpone_evidence_authority": {
                            "schema": "dpone.dbt-dev-evidence-authority.v1",
                            "request_id": request.evidence_set_id,
                            "evidence_set_id": request.evidence_set_id,
                            "release_id": request.release_id,
                            "deployment_id": request.deployment_id,
                            "workflow_id": workflow.workflow_id,
                            "dag_id": workflow.dag_id,
                            "dag_run_id": workflow.dag_run_id,
                        }
                    },
                    timeout_seconds=_remaining_seconds(
                        monotonic=self._monotonic,
                        started=started,
                        timeout_seconds=timeout_seconds,
                    ),
                )
            while True:
                for workflow in request.workflows:
                    if states[workflow.workflow_id] == "success":
                        continue
                    state = self._airflow.state(
                        dag_id=workflow.dag_id,
                        dag_run_id=workflow.dag_run_id,
                        timeout_seconds=_remaining_seconds(
                            monotonic=self._monotonic,
                            started=started,
                            timeout_seconds=timeout_seconds,
                        ),
                    )
                    if state not in _WAITING_STATES | _TERMINAL_FAILURE_STATES | {"success"}:
                        states[workflow.workflow_id] = "unknown"
                        raise DbtDevEvidenceCampaignError("Airflow returned an unsupported DAG-run state")
                    states[workflow.workflow_id] = state
                elapsed = self._monotonic() - started
                if all(state == "success" for state in states.values()):
                    report = DbtDevEvidenceCampaignReport(
                        evidence_set_id=request.evidence_set_id,
                        release_id=request.release_id,
                        deployment_id=request.deployment_id,
                        workflow_states=tuple(sorted(states.items())),
                        elapsed_seconds=max(0.0, elapsed),
                    )
                    self._journal.close(
                        _receipt(
                            request=request,
                            status="passed",
                            code="DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
                            states=states,
                        )
                    )
                    return report
                if any(state in _TERMINAL_FAILURE_STATES for state in states.values()):
                    raise DbtDevEvidenceCampaignError("one or more Airflow evidence workflows failed")
                if elapsed >= timeout_seconds:
                    raise DbtDevEvidenceCampaignError("Airflow evidence campaign timed out")
                self._sleep(
                    min(
                        float(poll_interval_seconds),
                        max(0.0, timeout_seconds - elapsed),
                    )
                )
        except DbtDevEvidenceCampaignError as exc:
            status = "failed" if any(state == "failed" for state in states.values()) else "abandoned"
            try:
                self._journal.close(
                    _receipt(
                        request=request,
                        status=status,
                        code=DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED,
                        states=states,
                    )
                )
            except Exception as journal_exc:
                raise DbtDevEvidenceCampaignError(
                    "Airflow evidence campaign could not record terminal closure"
                ) from journal_exc
            raise exc
        except Exception as exc:
            if opened:
                try:
                    self._journal.close(
                        _receipt(
                            request=request,
                            status="abandoned",
                            code=DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED,
                            states=states,
                        )
                    )
                except Exception as journal_exc:
                    raise DbtDevEvidenceCampaignError(
                        "Airflow evidence campaign could not record terminal closure"
                    ) from journal_exc
            raise DbtDevEvidenceCampaignError("Airflow evidence campaign could not be completed") from exc


def _validate_limits(
    *,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, int)
        or not 30 <= timeout_seconds <= 7_200
        or isinstance(poll_interval_seconds, bool)
        or not isinstance(poll_interval_seconds, int)
        or not 1 <= poll_interval_seconds <= 60
        or poll_interval_seconds > timeout_seconds
    ):
        raise DbtDevEvidenceCampaignError("Airflow evidence campaign limits are invalid")


def _remaining_seconds(
    *,
    monotonic: Callable[[], float],
    started: float,
    timeout_seconds: int,
) -> float:
    remaining = float(timeout_seconds) - max(
        0.0,
        monotonic() - started,
    )
    if remaining <= 0:
        raise DbtDevEvidenceCampaignError("Airflow evidence campaign timed out")
    return remaining


def _receipt(
    *,
    request: DbtDevEvidenceRequest,
    status: str,
    code: str,
    states: dict[str, str],
) -> DbtDevEvidenceCampaignReceipt:
    return DbtDevEvidenceCampaignReceipt.build(
        request=request,
        status=status,
        code=code,
        workflow_states=states,
    )


__all__ = [
    "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED",
    "DbtDevEvidenceCampaignError",
    "DbtDevEvidenceCampaignReport",
    "DbtDevEvidenceCampaignService",
]
