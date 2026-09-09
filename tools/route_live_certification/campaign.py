"""Environment composition root used by real-vendor certification suites."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .contract import WorkflowBinding, fail
from .evidence_writer import PassedCaseObservation, SuiteEvidenceWriter
from .io_authority import json_object
from .registry import release_suites
from .reviewed_case import ReviewedSuite

_ENVIRONMENT_FIELDS = (
    "DPONE_ROUTE_LIVE_EVIDENCE_ROOT",
    "DPONE_ROUTE_LIVE_JUNIT_ROOT",
    "DPONE_ROUTE_LIVE_VENDOR_METADATA",
    "GITHUB_SHA",
    "GITHUB_RUN_ID",
    "GITHUB_RUN_ATTEMPT",
)


@dataclass(frozen=True, slots=True)
class CampaignContext:
    """Immutable writer dependencies shared by one authoritative test run."""

    evidence_root: Path
    junit_root: Path
    binding: WorkflowBinding
    vendor_policy: Mapping[str, object]

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] = os.environ,
    ) -> CampaignContext:
        """Resolve and validate all CI bindings exactly once."""

        values: dict[str, str] = {}
        for field in _ENVIRONMENT_FIELDS:
            value = environment.get(field)
            if not isinstance(value, str) or not value.strip():
                fail(f"campaign.environment_required:{field}")
            values[field] = value
        vendor_path = Path(values["DPONE_ROUTE_LIVE_VENDOR_METADATA"])
        vendors = json_object(
            vendor_path.read_bytes(),
            code="campaign.vendor_metadata_invalid",
        )
        try:
            run_attempt = int(values["GITHUB_RUN_ATTEMPT"])
        except ValueError:
            fail("campaign.run_attempt_invalid")
        return cls(
            evidence_root=Path(values["DPONE_ROUTE_LIVE_EVIDENCE_ROOT"]),
            junit_root=Path(values["DPONE_ROUTE_LIVE_JUNIT_ROOT"]),
            binding=WorkflowBinding(
                values["GITHUB_SHA"],
                values["GITHUB_RUN_ID"],
                run_attempt,
            ),
            vendor_policy=vendors,
        )

    def writer(self, suite: ReviewedSuite) -> SuiteEvidenceWriter:
        """Create a suite-scoped writer from the frozen campaign bindings."""

        if not isinstance(suite, ReviewedSuite):
            fail("campaign.suite_invalid")
        return SuiteEvidenceWriter(
            suite=suite,
            evidence_root=self.evidence_root,
            junit_root=self.junit_root,
            binding=self.binding,
            vendor_policy=self.vendor_policy,
        )


def writer_from_environment(
    suite_id: str,
    *,
    environment: Mapping[str, str] = os.environ,
) -> SuiteEvidenceWriter:
    """Resolve one reviewed writer from explicit CI campaign bindings."""

    suites = {suite.suite_id: suite for suite in release_suites()}
    suite = suites.get(suite_id)
    if suite is None:
        fail(f"campaign.suite_not_reviewed:{suite_id}")
    return CampaignContext.from_environment(environment).writer(suite)


def write_suite_from_environment(
    suite_id: str,
    observations: Sequence[PassedCaseObservation],
    *,
    environment: Mapping[str, str] = os.environ,
) -> tuple[Path, Path]:
    """One-call integration-suite publication with strict environment DI."""

    return writer_from_environment(suite_id, environment=environment).write(observations)


__all__ = [
    "CampaignContext",
    "write_suite_from_environment",
    "writer_from_environment",
]
