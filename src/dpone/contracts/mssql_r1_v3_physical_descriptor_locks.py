"""Deterministic lock plans for the physical descriptor."""

from __future__ import annotations

from dataclasses import dataclass

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ResourceInstanceSelectorV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1LockCardinalityV1,
    MssqlR1LockKindV1,
    MssqlR1PhysicalResourceRefV1,
)


class MssqlR1LockMechanismV1(StrEnum):
    APPLICATION = "application"
    GUARDED_ROW = "guarded_row"
    TABLE = "table"


class MssqlR1LockActionV1(StrEnum):
    ACQUIRE = "acquire"
    ASSERT_HELD = "assert_held"


class MssqlR1LockModeV1(StrEnum):
    SHARED = "shared"
    UPDATE = "update"
    EXCLUSIVE = "exclusive"


class MssqlR1LockOwnerV1(StrEnum):
    TRANSACTION = "transaction"


class MssqlR1LockTimeoutPolicyV1(StrEnum):
    BOUNDED_ENVIRONMENT = "bounded_environment"


_STEP = b"dpone-r1-physical-lock-step-v1\0"
LOCK_RANK = {kind: index for index, kind in enumerate(MssqlR1LockKindV1, 1)}
LOCK_PHASE = {
    MssqlR1LockMechanismV1.APPLICATION: 1,
    MssqlR1LockMechanismV1.GUARDED_ROW: 2,
    MssqlR1LockMechanismV1.TABLE: 3,
}


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalLockStepV1:
    ordinal: int
    lock_kind: MssqlR1LockKindV1
    resource: MssqlR1PhysicalResourceRefV1
    mechanism: MssqlR1LockMechanismV1
    action: MssqlR1LockActionV1
    mode: MssqlR1LockModeV1
    owner: MssqlR1LockOwnerV1
    cardinality: MssqlR1LockCardinalityV1
    instance_selector: MssqlR1ResourceInstanceSelectorV1
    timeout_policy: MssqlR1LockTimeoutPolicyV1
    requires_updlock: bool
    requires_holdlock: bool
    requires_tablockx: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "lock ordinal")
        for value, contract, field in (
            (self.lock_kind, MssqlR1LockKindV1, "lock kind"),
            (self.mechanism, MssqlR1LockMechanismV1, "lock mechanism"),
            (self.action, MssqlR1LockActionV1, "lock action"),
            (self.mode, MssqlR1LockModeV1, "lock mode"),
            (self.owner, MssqlR1LockOwnerV1, "lock owner"),
            (self.cardinality, MssqlR1LockCardinalityV1, "lock cardinality"),
            (self.timeout_policy, MssqlR1LockTimeoutPolicyV1, "lock timeout policy"),
        ):
            _VALIDATE.require_exact_enum(value, contract, field)
        if (
            type(self.resource) is not MssqlR1PhysicalResourceRefV1
            or type(self.instance_selector) is not MssqlR1ResourceInstanceSelectorV1
        ):
            raise MssqlR1V3ContractError("lock resource/selector type is invalid")
        flags = (self.requires_updlock, self.requires_holdlock, self.requires_tablockx)
        for flag in flags:
            _VALIDATE.require_bool(flag, "lock hint flag")
        exact = {
            MssqlR1LockMechanismV1.APPLICATION: (MssqlR1LockModeV1.EXCLUSIVE, (False, False, False)),
            MssqlR1LockMechanismV1.GUARDED_ROW: (MssqlR1LockModeV1.UPDATE, (True, True, False)),
            MssqlR1LockMechanismV1.TABLE: (MssqlR1LockModeV1.EXCLUSIVE, (False, True, True)),
        }[self.mechanism]
        if (
            self.owner is not MssqlR1LockOwnerV1.TRANSACTION
            or self.timeout_policy is not MssqlR1LockTimeoutPolicyV1.BOUNDED_ENVIRONMENT
            or (self.mode, flags) != exact
        ):
            raise MssqlR1V3ContractError("lock mechanism has a noncanonical shape")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _STEP,
            (
                self.ordinal,
                self.lock_kind,
                self.resource.canonical_bytes,
                self.mechanism,
                self.action,
                self.mode,
                self.owner,
                self.cardinality,
                self.instance_selector.canonical_bytes,
                self.timeout_policy,
                self.requires_updlock,
                self.requires_holdlock,
                self.requires_tablockx,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalLockStepV1:
        values = list(decode_canonical_bytes(payload, _STEP, field_count=13))
        values[1] = expect_enum(MssqlR1LockKindV1, values[1], "lock kind")
        values[2] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[2], "lock resource"))
        for index, contract, field in (
            (3, MssqlR1LockMechanismV1, "mechanism"),
            (4, MssqlR1LockActionV1, "action"),
            (5, MssqlR1LockModeV1, "mode"),
            (6, MssqlR1LockOwnerV1, "owner"),
            (7, MssqlR1LockCardinalityV1, "cardinality"),
            (9, MssqlR1LockTimeoutPolicyV1, "timeout policy"),
        ):
            values[index] = expect_enum(contract, values[index], field)
        values[8] = MssqlR1ResourceInstanceSelectorV1.from_canonical_bytes(expect_bytes(values[8], "selector"))
        return cls(*values)  # type: ignore[arg-type]


def require_lock_order(steps: tuple[MssqlR1PhysicalLockStepV1, ...], subranks: dict[bytes, int]) -> None:
    _VALIDATE.require_tuple(steps, MssqlR1PhysicalLockStepV1, "lock steps")
    _VALIDATE.require_contiguous(steps, "lock steps")
    if any(item.resource.canonical_bytes not in subranks for item in steps):
        raise MssqlR1V3ContractError("lock plan references an undeclared resource")
    keys = tuple(
        (
            LOCK_RANK[item.lock_kind],
            subranks[item.resource.canonical_bytes],
            item.resource.canonical_bytes,
            LOCK_PHASE[item.mechanism],
        )
        for item in steps
    )
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
        raise MssqlR1V3ContractError("lock plan violates global rank/subrank/phase order")
