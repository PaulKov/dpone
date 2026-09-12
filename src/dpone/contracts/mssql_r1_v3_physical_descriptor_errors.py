"""Stable error and migration classifications for the physical descriptor."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1FreshProbeKindV1,
    MssqlR1MigrationDispositionV1,
    MssqlR1MigrationObservationKindV1,
    MssqlR1OutcomeClassV1,
    MssqlR1RedactionClassV1,
    MssqlR1RetryClassV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import MssqlR1PhysicalResourceRefV1

_ERROR = b"dpone-r1-physical-error-condition-v1\0"
_PROBE = b"dpone-r1-physical-migration-probe-v1\0"


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalErrorConditionV1:
    condition_id: str
    error_number: int
    error_state: int
    outcome_class: MssqlR1OutcomeClassV1
    retry_class: MssqlR1RetryClassV1
    fresh_probe_kind: MssqlR1FreshProbeKindV1
    public_blocker_code: str | None
    redaction_class: MssqlR1RedactionClassV1

    def __post_init__(self) -> None:
        _VALIDATE.require_lower_ascii(self.condition_id, "condition ID")
        if not 51001 <= _VALIDATE.require_int(self.error_number, "error number", minimum=51001, maximum=51012) <= 51012:
            raise MssqlR1V3ContractError("error number is outside the exact R1 range")
        _VALIDATE.require_int(self.error_state, "error state", minimum=1, maximum=255)
        _VALIDATE.require_exact_enum(self.outcome_class, MssqlR1OutcomeClassV1, "outcome class")
        _VALIDATE.require_exact_enum(self.retry_class, MssqlR1RetryClassV1, "retry class")
        _VALIDATE.require_exact_enum(self.fresh_probe_kind, MssqlR1FreshProbeKindV1, "fresh probe kind")
        _VALIDATE.require_exact_enum(self.redaction_class, MssqlR1RedactionClassV1, "redaction class")
        public = self.redaction_class is MssqlR1RedactionClassV1.PUBLIC
        if public != (self.public_blocker_code is not None):
            raise MssqlR1V3ContractError("public blocker presence differs from redaction class")
        if self.public_blocker_code is not None:
            _VALIDATE.require_lower_ascii(self.public_blocker_code, "public blocker code")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _ERROR,
            (
                self.condition_id,
                self.error_number,
                self.error_state,
                self.outcome_class,
                self.retry_class,
                self.fresh_probe_kind,
                self.public_blocker_code,
                self.redaction_class,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalErrorConditionV1:
        values = list(decode_canonical_bytes(payload, _ERROR, field_count=8))
        for index, contract, field in (
            (3, MssqlR1OutcomeClassV1, "outcome class"),
            (4, MssqlR1RetryClassV1, "retry class"),
            (5, MssqlR1FreshProbeKindV1, "fresh probe"),
            (7, MssqlR1RedactionClassV1, "redaction class"),
        ):
            values[index] = expect_enum(contract, values[index], field)
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1MigrationProbeV1:
    probe_id: str
    observation_kind: MssqlR1MigrationObservationKindV1
    observation_resource: MssqlR1PhysicalResourceRefV1
    disposition: MssqlR1MigrationDispositionV1
    blocker_code: str | None
    zero_mutation_before_decision: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_lower_ascii(self.probe_id, "migration probe ID")
        _VALIDATE.require_exact_enum(self.observation_kind, MssqlR1MigrationObservationKindV1, "migration observation")
        if type(self.observation_resource) is not MssqlR1PhysicalResourceRefV1:
            raise MssqlR1V3ContractError("migration probe resource is invalid")
        _VALIDATE.require_exact_enum(self.disposition, MssqlR1MigrationDispositionV1, "migration disposition")
        blocking = self.disposition is MssqlR1MigrationDispositionV1.BLOCK
        if blocking != (self.blocker_code is not None):
            raise MssqlR1V3ContractError("migration blocker presence differs from disposition")
        if self.blocker_code is not None:
            _VALIDATE.require_lower_ascii(self.blocker_code, "migration blocker")
        if self.zero_mutation_before_decision is not True:
            raise MssqlR1V3ContractError("migration probe must decide before mutation")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PROBE,
            (
                self.probe_id,
                self.observation_kind,
                self.observation_resource.canonical_bytes,
                self.disposition,
                self.blocker_code,
                self.zero_mutation_before_decision,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1MigrationProbeV1:
        values = list(decode_canonical_bytes(payload, _PROBE, field_count=6))
        values[1] = expect_enum(MssqlR1MigrationObservationKindV1, values[1], "observation kind")
        values[2] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[2], "resource"))
        values[3] = expect_enum(MssqlR1MigrationDispositionV1, values[3], "disposition")
        return cls(*values)  # type: ignore[arg-type]


__all__ = ["MssqlR1MigrationProbeV1", "MssqlR1PhysicalErrorConditionV1"]


def validate_migration_probe_inventory(values: tuple[MssqlR1MigrationProbeV1, ...]) -> None:
    """Require canonical probes with unique case-insensitive semantic identities."""
    _VALIDATE.require_tuple(values, MssqlR1MigrationProbeV1, "migration probes", nonempty=True)
    _VALIDATE.require_canonical(values, "migration probes")
    probe_ids = tuple(item.probe_id.casefold() for item in values)
    if len(set(probe_ids)) != len(probe_ids):
        raise MssqlR1V3ContractError("migration probe IDs are not semantically unique")
