"""Structural shared owner references and complete physical claim originals.

These immutable values authenticate nothing and grant no access, enrollment,
ownership, lifecycle transition or executor permission. Protected adapters must
independently reconstruct the complete catalog and effect observation before
comparing a claim. Owner identity never changes physical collision guards.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Literal

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_physical_identity import composition_physical_guard_id
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    digest,
    document_sha256,
    exact_fields,
    ordered,
    sequence,
    uuid_text,
)


@dataclass(frozen=True, slots=True)
class CompositionOwnerReference:
    """Actual execution activation or qualification run, without authorization.

    Protected storage must enforce both the key and unique ``(kind, id)`` and
    reject rebinding an existing owner to changed original subject bytes.
    """

    owner_kind: Literal["execution", "qualification"]
    owner_id: str

    def __post_init__(self) -> None:
        if type(self.owner_kind) is not str or self.owner_kind not in {"execution", "qualification"}:
            raise CompositionAdmissionError("owner_kind")
        try:
            uuid_text(self.owner_id)
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("owner_id") from None

    @property
    def owner_key(self) -> str:
        """Hash exact canonical UTF-8 identity, without legacy path normalization."""
        self.__post_init__()
        return document_sha256({"schema": "dpone.composition-control-owner-key.v2", **asdict(self)})


@dataclass(frozen=True, slots=True)
class CompositionPhysicalClaim:
    """One complete physical domain's mutation and retained-source requirements.

    Read and write subjects include helper, staging and state effects. A seeded
    source that must remain retained uses the combined role. Neither the role
    nor the observation digest proves source sealing or protected enrollment.
    """

    connector: Literal["mssql", "clickhouse", "postgres"]
    service_id: str
    physical_subject_sha256: str
    observation_sha256: str
    role: Literal["mutation", "retained_source", "mutation_and_retained_source"]
    read_subjects: tuple[str, ...]
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.connector) is not str or self.connector not in {"mssql", "clickhouse", "postgres"}:
            raise CompositionAdmissionError("physical_connector")
        if type(self.role) is not str or self.role not in {
            "mutation",
            "retained_source",
            "mutation_and_retained_source",
        }:
            raise CompositionAdmissionError("physical_claim_role")
        try:
            uuid_text(self.service_id)
            digest(self.physical_subject_sha256)
            digest(self.observation_sha256)
            for subjects in (self.read_subjects, self.write_subjects):
                if type(subjects) is not tuple or len(subjects) > 8192:
                    raise CompositionAdmissionError("physical_claim_subjects")
                for subject in subjects:
                    digest(subject)
                ordered(subjects, maximum=8192, allow_empty=True)
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("physical_claim") from None
        if len(self.read_subjects) + len(self.write_subjects) > 8192:
            raise CompositionAdmissionError("physical_claim_budget")
        if (
            (self.role != "mutation" and not self.read_subjects)
            or (self.role == "retained_source" and self.write_subjects)
            or (self.role != "retained_source" and not self.write_subjects)
        ):
            raise CompositionAdmissionError("physical_claim_role_scope")

    @property
    def guard_id(self) -> str:
        """Use the unchanged physical formula, independent of role and owner."""
        self.__post_init__()
        return composition_physical_guard_id(
            connector=self.connector,
            service_id=self.service_id,
            physical_subject_sha256=self.physical_subject_sha256,
        )

    @property
    def effect_subjects(self) -> tuple[str, ...]:
        """Derive the complete union; it is never an independently writable list."""
        self.__post_init__()
        return tuple(sorted(set(self.read_subjects + self.write_subjects)))

    def to_dict(self) -> dict[str, object]:
        """Return a detached closed nested JSON object, without a wire family."""
        self.__post_init__()
        return {**asdict(self), "read_subjects": list(self.read_subjects), "write_subjects": list(self.write_subjects)}

    @classmethod
    def from_dict(cls, value: object) -> CompositionPhysicalClaim:
        """Require exact ordinary JSON fields and arrays without coercion."""
        try:
            body = exact_fields(value, {field.name for field in fields(cls)})
            return cls(
                **dict(
                    body,
                    **{
                        key: tuple(sequence(body[key], maximum=8192, allow_empty=True))
                        for key in ("read_subjects", "write_subjects")
                    },
                )
            )
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("physical_claim_document") from None
