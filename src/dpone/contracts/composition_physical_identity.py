"""One physical guard preimage, preserving the original execution hash semantics.

Callers validate their own closed connector and service contracts before using
this formula. It does not observe, enroll or authorize a physical participant.
"""

from dpone.contracts.airflow_deployment import canonical_fingerprint


def composition_physical_guard_id(*, connector: str, service_id: str, physical_subject_sha256: str) -> str:
    """Derive the existing guard independently of phase, owner and credentials."""
    return canonical_fingerprint(
        {
            "schema": "dpone.composition-physical-domain.v1",
            "connector": connector,
            "service_id": service_id,
            "physical_subject_sha256": physical_subject_sha256,
        }
    )
