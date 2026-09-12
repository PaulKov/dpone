"""Cross-contract execution closure for the SQL-free physical descriptor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1TransitionAuthorityV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import MssqlR1PhysicalErrorConditionV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_locks import MssqlR1LockActionV1, MssqlR1PhysicalLockStepV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_replay import MssqlR1ReplayOutcomePolicyV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_requests import (
    MssqlR1OutcomeVariantV1,
    MssqlR1RequestAuthorityV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1AccessKindV1,
    MssqlR1PhysicalResourceAccessV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_transitions import MssqlR1StateTransitionV1

_DOMAIN = b"dpone-r1-physical-execution-semantics-v1\0"


def _canonical_or_empty(values: tuple[object, ...], field: str) -> None:
    if values:
        _VALIDATE.require_canonical(values, field)


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1ExecutionSemanticsV1:
    request_authority: MssqlR1RequestAuthorityV1
    ordered_outcome_variants: tuple[MssqlR1OutcomeVariantV1, ...]
    ordered_lock_steps: tuple[MssqlR1PhysicalLockStepV1, ...]
    ordered_read_set: tuple[MssqlR1PhysicalResourceAccessV1, ...]
    ordered_write_set: tuple[MssqlR1PhysicalResourceAccessV1, ...]
    transition_authority: MssqlR1TransitionAuthorityV1
    ordered_state_transitions: tuple[MssqlR1StateTransitionV1, ...]
    ordered_replay_outcome_policies: tuple[MssqlR1ReplayOutcomePolicyV1, ...]
    ordered_error_conditions: tuple[MssqlR1PhysicalErrorConditionV1, ...]

    def __post_init__(self) -> None:
        if type(self.request_authority) is not MssqlR1RequestAuthorityV1:
            raise MssqlR1V3ContractError("execution semantics requires exact request authority")
        for values, contract, field in (
            (self.ordered_outcome_variants, MssqlR1OutcomeVariantV1, "outcome variants"),
            (self.ordered_lock_steps, MssqlR1PhysicalLockStepV1, "lock steps"),
            (self.ordered_read_set, MssqlR1PhysicalResourceAccessV1, "read set"),
            (self.ordered_write_set, MssqlR1PhysicalResourceAccessV1, "write set"),
            (self.ordered_state_transitions, MssqlR1StateTransitionV1, "state transitions"),
            (self.ordered_replay_outcome_policies, MssqlR1ReplayOutcomePolicyV1, "replay policies"),
            (self.ordered_error_conditions, MssqlR1PhysicalErrorConditionV1, "error conditions"),
        ):
            _VALIDATE.require_tuple(values, contract, field)
        _VALIDATE.require_contiguous(self.ordered_lock_steps, "lock steps")
        _VALIDATE.require_contiguous(self.ordered_state_transitions, "state transitions")
        for values, field in (
            (self.ordered_outcome_variants, "outcome variants"),
            (self.ordered_read_set, "read set"),
            (self.ordered_write_set, "write set"),
            (self.ordered_error_conditions, "error conditions"),
        ):
            _canonical_or_empty(values, field)
        if any(item.access_kind is not MssqlR1AccessKindV1.READ for item in self.ordered_read_set):
            raise MssqlR1V3ContractError("read set contains a mutating access")
        if any(item.access_kind is MssqlR1AccessKindV1.READ for item in self.ordered_write_set):
            raise MssqlR1V3ContractError("write set contains a read access")
        _VALIDATE.require_exact_enum(self.transition_authority, MssqlR1TransitionAuthorityV1, "transition authority")
        if (
            self.transition_authority is MssqlR1TransitionAuthorityV1.SELF_CONTAINED
            and not self.ordered_state_transitions
        ):
            raise MssqlR1V3ContractError("self-contained semantics require transitions")
        if self.transition_authority is MssqlR1TransitionAuthorityV1.READ_ONLY and (
            self.ordered_state_transitions or self.ordered_write_set
        ):
            raise MssqlR1V3ContractError("read-only semantics cannot transition or write")
        if self.transition_authority is MssqlR1TransitionAuthorityV1.CALLER_UOW and (
            self.ordered_state_transitions
            or not (self.ordered_read_set or self.ordered_write_set)
            or any(step.action is not MssqlR1LockActionV1.ASSERT_HELD for step in self.ordered_lock_steps)
        ):
            raise MssqlR1V3ContractError("caller-UoW semantics require pre-held locks and no local transitions")
        conditions = tuple(item.condition_id.casefold() for item in self.ordered_error_conditions)
        if len(set(conditions)) != len(conditions):
            raise MssqlR1V3ContractError("error condition IDs are not semantically unique")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAIN,
            (
                self.request_authority.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_outcome_variants),
                tuple(item.canonical_bytes for item in self.ordered_lock_steps),
                tuple(item.canonical_bytes for item in self.ordered_read_set),
                tuple(item.canonical_bytes for item in self.ordered_write_set),
                self.transition_authority,
                tuple(item.canonical_bytes for item in self.ordered_state_transitions),
                tuple(item.canonical_bytes for item in self.ordered_replay_outcome_policies),
                tuple(item.canonical_bytes for item in self.ordered_error_conditions),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ExecutionSemanticsV1:
        values = list(decode_canonical_bytes(payload, _DOMAIN, field_count=9))
        values[0] = MssqlR1RequestAuthorityV1.from_canonical_bytes(expect_bytes(values[0], "request authority"))
        for index, contract in (
            (1, MssqlR1OutcomeVariantV1),
            (2, MssqlR1PhysicalLockStepV1),
            (3, MssqlR1PhysicalResourceAccessV1),
            (4, MssqlR1PhysicalResourceAccessV1),
            (6, MssqlR1StateTransitionV1),
            (7, MssqlR1ReplayOutcomePolicyV1),
            (8, MssqlR1PhysicalErrorConditionV1),
        ):
            values[index] = tuple(
                contract.from_canonical_bytes(expect_bytes(item, "execution member"))
                for item in expect_tuple(values[index], "execution members")
            )
        values[5] = expect_enum(MssqlR1TransitionAuthorityV1, values[5], "transition authority")
        return cls(*values)  # type: ignore[arg-type]

    def validate_contract(
        self,
        parameters: tuple[Any, ...],
        result: Any,
        declarations: dict[bytes, Any],
        codecs: tuple[Any, ...],
    ) -> None:
        from dpone.contracts.mssql_r1_v3_physical_descriptor_execution_validation import (
            validate_execution_semantics,
        )

        validate_execution_semantics(self, parameters, result, declarations, codecs)


__all__ = ("MssqlR1ExecutionSemanticsV1",)
