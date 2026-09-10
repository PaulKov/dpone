"""Own service files for one verified child attempt on the writable run volume.

The workload tree is never changed. A missing XCom destination means publication
is disabled, including all normalization, directory, read and stat operations.
Each attempt invalidates old results before child execution; startup diagnostics
are best effort and can never turn a failure into a successful return.
"""

from __future__ import annotations

import json
from contextlib import suppress
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_xcom_from_evidence import write_airflow_xcom_from_evidence
from dpone.runtime.verified_pack_diagnostics import (
    VerifiedPackServiceError,
    diagnostic_message,
    service_file_stage,
)


class VerifiedPackServiceFiles:
    """Manage run-volume output and explicitly enabled XCom publication."""

    def __init__(self, output_dir: Path, *, xcom_path: Path | None) -> None:
        self.output_dir = output_dir
        self.evidence_path = output_dir / "runtime-evidence.json"
        self.stderr_path = output_dir / "runtime-stderr.log"
        self.summary_path = output_dir / "runtime-summary.json"
        self.diagnostic_path = output_dir / "runtime-startup-error.json"
        self.xcom_path = xcom_path

    def prepare(self) -> None:
        """Prepare service volumes and discard results from an earlier attempt."""

        with service_file_stage("prepare_run_directory", "run_directory"):
            self.output_dir.mkdir(parents=True, exist_ok=True)
        with service_file_stage("prepare_run_files", "run_directory"):
            for path in (
                self.summary_path,
                self.diagnostic_path,
                self.evidence_path,
                self.stderr_path,
            ):
                path.unlink(missing_ok=True)
        if self.xcom_path is not None:
            with service_file_stage("prepare_xcom_directory", "xcom_directory"):
                self.xcom_path.parent.mkdir(parents=True, exist_ok=True)
            with service_file_stage("prepare_xcom_file", "xcom_return"):
                self.xcom_path.unlink(missing_ok=True)

    def write_summary(self, *, status: str) -> None:
        """Build the existing Airflow summary locally before optional publication."""

        with service_file_stage("write_summary", "runtime_summary"):
            write_airflow_xcom_from_evidence(
                evidence_path=self.evidence_path,
                xcom_output=self.summary_path,
                runtime_evidence_path=str(self.evidence_path),
                stderr_path=str(self.stderr_path),
                status=status,
            )
        if self.xcom_path is not None:
            with service_file_stage("publish_xcom", "xcom_return"):
                self.xcom_path.write_bytes(self.summary_path.read_bytes())

    def record_failure(self, diagnostic: dict[str, Any], *, child_started: bool) -> None:
        """Attempt independent failure artifacts without hiding the initial error.

        Keep partial child stdout for diagnosis. Before starting any child,
        replace old evidence so a failed preparation cannot reuse a prior pass.
        Remove previous summaries even if rebuilding a failure summary fails.
        """

        for path in (self.summary_path, self.xcom_path):
            if path is not None:
                with suppress(OSError):
                    path.unlink(missing_ok=True)
        payload = json.dumps(diagnostic, sort_keys=True) + "\n"
        with suppress(OSError):
            self.diagnostic_path.write_text(payload, encoding="utf-8")
        with suppress(OSError):
            with self.stderr_path.open("a", encoding="utf-8") as handle:
                handle.write(diagnostic_message(diagnostic) + "\n")
        if not child_started:
            with suppress(OSError):
                self.evidence_path.write_text(payload, encoding="utf-8")
        with suppress(OSError, VerifiedPackServiceError):
            self.write_summary(status="failed")


__all__ = ["VerifiedPackServiceFiles"]
