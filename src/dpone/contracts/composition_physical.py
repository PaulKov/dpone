"""Stable protected domain identities and query-local catalog equivalence.

These are adapter observations, not caller assertions of enrollment. Identity
must be verified against pinned protected authority before constructing them.
"""

from dataclasses import dataclass

from dpone.contracts.composition_identity import (
    CompositionAdmissionError,
    require_digest,
    require_physical_domain_identity,
)
from dpone.contracts.composition_physical_identity import composition_physical_guard_id


@dataclass(frozen=True, slots=True)
class CompositionPhysicalDomain:
    """A platform-enrolled collision domain independent of aliases and credentials."""

    connector: str
    service_id: str
    physical_subject_sha256: str

    def __post_init__(self) -> None:
        require_physical_domain_identity(
            connector=self.connector,
            service_id=self.service_id,
            physical_subject_sha256=self.physical_subject_sha256,
        )

    @property
    def guard_id(self) -> str:
        return composition_physical_guard_id(
            connector=self.connector,
            service_id=self.service_id,
            physical_subject_sha256=self.physical_subject_sha256,
        )


@dataclass(frozen=True, slots=True)
class CompositionDomainObservation:
    """One complete catalog query's equivalence classes and original evidence.

    Equivalence IDs are meaningful only within this observation. Comparing IDs
    from separate alias-specific queries cannot prove targets are distinct.
    ``catalog_sha256`` binds the backend's full helper/dependency/permission
    observation, including unsupported side-effect checks.
    """

    domain: CompositionPhysicalDomain
    slots: tuple[tuple[str, int], ...]
    catalog_sha256: str

    def __post_init__(self) -> None:
        self.domain.__post_init__()
        require_digest(self.catalog_sha256)
        if (
            not isinstance(self.slots, tuple)
            or not 1 <= len(self.slots) <= 8192
            or any(type(pair) is not tuple or len(pair) != 2 for pair in self.slots)
        ):
            raise CompositionAdmissionError("physical_observation_closure")
        subjects = []
        for subject, equivalence in self.slots:
            require_digest(subject)
            if type(equivalence) is not int or equivalence < 0:
                raise CompositionAdmissionError("catalog_equivalence")
            subjects.append(subject)
        if len(set(subjects)) != len(subjects):
            raise CompositionAdmissionError("physical_observation_closure")
