"""Composition admission identity validation and stable errors."""

from uuid import UUID

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest


class CompositionAdmissionError(ValueError):
    """Sanitized stable admission failure, without driver or credential text."""

    code = "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


def require_digest(value: object) -> None:
    """Require canonical SHA-256 identity, never arbitrary caller labels."""
    if not is_canonical_sha256_digest(value):
        raise CompositionAdmissionError("digest")


def require_text(value: object, *, maximum: int = 512) -> None:
    """Reject empty, unbounded or control-bearing identity strings."""
    if not isinstance(value, str) or not value or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise CompositionAdmissionError("identity")


def require_ordered_unique(values: tuple[str, ...]) -> None:
    """Canonical ordering prevents different encodings of one resource closure."""
    if not isinstance(values, tuple) or not values or values != tuple(sorted(set(values))):
        raise CompositionAdmissionError("closure")


# Preserve historic public reflection and pickle locators.
CompositionAdmissionError.__module__ = "dpone.contracts.composition_activation"
require_digest.__module__ = "dpone.contracts.composition_activation"
require_text.__module__ = "dpone.contracts.composition_activation"
require_ordered_unique.__module__ = "dpone.contracts.composition_activation"


def require_physical_domain_identity(*, connector: str, service_id: str, physical_subject_sha256: str) -> None:
    """Validate the shared physical-domain tuple before deriving its guard identity.

    Preserve connector, service UUID and digest error precedence. This validates
    only the collision-domain tuple, not a caller's wider target or observation.
    """
    if connector not in {"mssql", "clickhouse"}:
        raise CompositionAdmissionError("physical_connector")
    try:
        valid = str(UUID(service_id)) == service_id
    except (TypeError, ValueError, AttributeError):
        valid = False
    if not valid:
        raise CompositionAdmissionError("protected_service_id")
    require_digest(physical_subject_sha256)
