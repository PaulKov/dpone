from __future__ import annotations

from dataclasses import dataclass
from typing import Any

AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE = "dpone gitops airflow pod-watch"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodLaunchEvidenceCheck:
    name: str
    passed: bool
    severity: str
    message: str
    path: str
    source: str = AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
            "source": self.source,
        }


__all__ = ["AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE", "GitOpsAirflowPodLaunchEvidenceCheck"]
