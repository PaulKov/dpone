"""Release-authoritative PostgreSQL to MSSQL route certification.

The package keeps the command-line entry point intentionally thin while the
reviewed evidence contract, filesystem authority, JUnit verifier, and suite
consolidator remain independently testable.
"""

from .campaign import CampaignContext, write_suite_from_environment, writer_from_environment
from .contract import CertificationValidationError, WorkflowBinding
from .evidence_writer import PassedCaseObservation, SuiteEvidenceWriter
from .io_authority import require_output_path_disjoint
from .recorder import ObservedImageDigest, RouteLiveObservationRecorder, observed_image_sha256
from .registry import build_inventory, release_suites
from .validator import validate_certification

__all__ = (
    "CertificationValidationError",
    "CampaignContext",
    "ObservedImageDigest",
    "PassedCaseObservation",
    "RouteLiveObservationRecorder",
    "SuiteEvidenceWriter",
    "WorkflowBinding",
    "build_inventory",
    "release_suites",
    "observed_image_sha256",
    "require_output_path_disjoint",
    "validate_certification",
    "write_suite_from_environment",
    "writer_from_environment",
)
