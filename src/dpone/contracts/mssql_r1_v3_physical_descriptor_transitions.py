"""Compound state transitions and exact revision equations."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_text,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ComparisonCoordinateV1,
    MssqlR1ComparisonLiteralV1,
    MssqlR1ResourceInstanceSelectorV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1ComparisonSourceV1,
    MssqlR1ExecutionPathV1,
    MssqlR1RevisionRuleKindV1,
    MssqlR1TransitionCardinalityV1,
    MssqlR1TransitionKindV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ValueCardinalityV1,
)

_RULE = b"dpone-r1-physical-revision-rule-v1\0"
_STATE = b"dpone-r1-physical-state-change-v1\0"
_APPLICABILITY = b"dpone-r1-physical-transition-applicability-v1\0"
_TRANSITION = b"dpone-r1-physical-state-transition-v1\0"


def _decode_coordinate(value: object | None, field: str) -> MssqlR1ComparisonCoordinateV1 | None:
    return None if value is None else MssqlR1ComparisonCoordinateV1.from_canonical_bytes(expect_bytes(value, field))


def _encode_operand(value: MssqlR1ComparisonCoordinateV1 | MssqlR1ComparisonLiteralV1 | None) -> bytes | None:
    return None if value is None else value.canonical_bytes


def _decode_operand(value: object | None) -> MssqlR1ComparisonCoordinateV1 | MssqlR1ComparisonLiteralV1 | None:
    if value is None:
        return None
    payload = expect_bytes(value, "revision operand")
    if payload.startswith(b"dpone-r1-physical-comparison-coordinate-v1\0"):
        return MssqlR1ComparisonCoordinateV1.from_canonical_bytes(payload)
    if payload.startswith(b"dpone-r1-physical-comparison-literal-v1\0"):
        return MssqlR1ComparisonLiteralV1.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("revision operand union is unsupported")


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1RevisionRuleV1:
    rule_kind: MssqlR1RevisionRuleKindV1
    current_value: MssqlR1ComparisonCoordinateV1 | None
    expected_value: MssqlR1ComparisonCoordinateV1 | None
    requested_candidate: MssqlR1ComparisonCoordinateV1 | MssqlR1ComparisonLiteralV1 | None
    candidate_value: MssqlR1ComparisonCoordinateV1
    delta: int

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.rule_kind, MssqlR1RevisionRuleKindV1, "revision rule kind")
        if type(self.candidate_value) is not MssqlR1ComparisonCoordinateV1:
            raise MssqlR1V3ContractError("revision candidate must be an exact coordinate")
        candidate = self.candidate_value
        if (
            candidate.source is not MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD
            or candidate.scalar_kind is not MssqlR1ProjectionScalarKindV1.INTEGER
            or candidate.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
            or candidate.nullable
        ):
            raise MssqlR1V3ContractError("revision candidate must be a non-null integer resource field")
        _VALIDATE.require_int(self.delta, "revision delta", maximum=1)
        current = self.current_value
        if current is not None and (
            type(current) is not MssqlR1ComparisonCoordinateV1
            or current.source is not MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD
            or current.resource != candidate.resource
        ):
            raise MssqlR1V3ContractError("revision current must be on the candidate owner")
        allowed_expected = {
            MssqlR1ComparisonSourceV1.REQUEST_FIELD,
            MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
            MssqlR1ComparisonSourceV1.SESSION_BINDING,
        }
        if self.expected_value is not None and (
            type(self.expected_value) is not MssqlR1ComparisonCoordinateV1
            or self.expected_value.source not in allowed_expected
        ):
            raise MssqlR1V3ContractError("revision expected source is unsupported")
        if (
            isinstance(self.requested_candidate, MssqlR1ComparisonCoordinateV1)
            and self.requested_candidate.source not in allowed_expected
        ):
            raise MssqlR1V3ContractError("requested candidate source is unsupported")
        if self.requested_candidate is not None and type(self.requested_candidate) not in {
            MssqlR1ComparisonCoordinateV1,
            MssqlR1ComparisonLiteralV1,
        }:
            raise MssqlR1V3ContractError("requested candidate has an unsupported operand type")
        shapes = {
            MssqlR1RevisionRuleKindV1.CREATE_ONE: (False, False, False, 1),
            MssqlR1RevisionRuleKindV1.EQUAL_REQUEST: (True, True, False, 0),
            MssqlR1RevisionRuleKindV1.ADJACENT: (True, None, False, 1),
            MssqlR1RevisionRuleKindV1.UNCHANGED: (True, False, False, 0),
            MssqlR1RevisionRuleKindV1.NULLABLE_INITIAL: (True, False, False, 1),
            MssqlR1RevisionRuleKindV1.SET_CANDIDATE: (True, True, True, 0),
        }[self.rule_kind]
        actual = (
            current is not None,
            self.expected_value is not None,
            self.requested_candidate is not None,
            self.delta,
        )
        if any(
            expected is not None and expected != observed for expected, observed in zip(shapes, actual, strict=True)
        ):
            raise MssqlR1V3ContractError("revision rule operands do not match its exact equation")
        if self.rule_kind is MssqlR1RevisionRuleKindV1.NULLABLE_INITIAL and (current is None or not current.nullable):
            raise MssqlR1V3ContractError("nullable-initial requires a nullable predecessor")
        if (
            self.rule_kind is not MssqlR1RevisionRuleKindV1.NULLABLE_INITIAL
            and current is not None
            and current.nullable
        ):
            raise MssqlR1V3ContractError("revision predecessor has unexpected nullability")
        operands = tuple(
            value for value in (current, self.expected_value, self.requested_candidate) if value is not None
        )
        if any(
            value.scalar_kind is not candidate.scalar_kind
            or (
                isinstance(value, MssqlR1ComparisonCoordinateV1)
                and value.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
            )
            for value in operands
        ):
            raise MssqlR1V3ContractError("revision operands have incompatible scalar kinds")
        if self.expected_value is not None and self.expected_value.nullable:
            raise MssqlR1V3ContractError("revision expected authority must be non-null")
        if isinstance(self.requested_candidate, MssqlR1ComparisonCoordinateV1) and self.requested_candidate.nullable:
            raise MssqlR1V3ContractError("requested candidate authority must be non-null")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _RULE,
            (
                self.rule_kind,
                _encode_operand(self.current_value),
                _encode_operand(self.expected_value),
                _encode_operand(self.requested_candidate),
                self.candidate_value.canonical_bytes,
                self.delta,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RevisionRuleV1:
        values = list(decode_canonical_bytes(payload, _RULE, field_count=6))
        values[0] = expect_enum(MssqlR1RevisionRuleKindV1, values[0], "rule kind")
        values[1] = _decode_coordinate(values[1], "current value")
        values[2] = _decode_coordinate(values[2], "expected value")
        values[3] = _decode_operand(values[3])
        values[4] = MssqlR1ComparisonCoordinateV1.from_canonical_bytes(expect_bytes(values[4], "candidate value"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1StateChangeV1:
    state_field: MssqlR1ComparisonCoordinateV1
    ordered_predecessor_states: tuple[str, ...]
    candidate_state: str

    def __post_init__(self) -> None:
        if type(self.state_field) is not MssqlR1ComparisonCoordinateV1 or (
            self.state_field.source is not MssqlR1ComparisonSourceV1.RESOURCE_FIELD
            or self.state_field.scalar_kind is not MssqlR1ProjectionScalarKindV1.TEXT
            or self.state_field.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
            or self.state_field.nullable
        ):
            raise MssqlR1V3ContractError("state field must be a non-null scalar text resource field")
        if type(self.ordered_predecessor_states) is not tuple:
            raise MssqlR1V3ContractError("predecessor states must be a tuple")
        for state in (*self.ordered_predecessor_states, self.candidate_state):
            _VALIDATE.require_lower_ascii(state, "state literal")
        if self.ordered_predecessor_states != tuple(sorted(set(self.ordered_predecessor_states))):
            raise MssqlR1V3ContractError("predecessor states must be a canonical set")
        if self.candidate_state in self.ordered_predecessor_states:
            raise MssqlR1V3ContractError("candidate state cannot equal a predecessor state")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _STATE, (self.state_field.canonical_bytes, self.ordered_predecessor_states, self.candidate_state)
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StateChangeV1:
        field, states, candidate = decode_canonical_bytes(payload, _STATE, field_count=3)
        return cls(
            MssqlR1ComparisonCoordinateV1.from_canonical_bytes(expect_bytes(field, "state field")),
            tuple(expect_text(item, "predecessor state") for item in expect_tuple(states, "predecessor states")),
            expect_text(candidate, "candidate state"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1TransitionApplicabilityV1:
    outcome_literal: str
    execution_path: MssqlR1ExecutionPathV1

    def __post_init__(self) -> None:
        _VALIDATE.require_lower_ascii(self.outcome_literal, "transition outcome")
        _VALIDATE.require_exact_enum(self.execution_path, MssqlR1ExecutionPathV1, "execution path")
        if self.execution_path is not MssqlR1ExecutionPathV1.FRESH_MUTATION:
            raise MssqlR1V3ContractError("only fresh mutation may own a transition")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_APPLICABILITY, (self.outcome_literal, self.execution_path))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1TransitionApplicabilityV1:
        outcome, path = decode_canonical_bytes(payload, _APPLICABILITY, field_count=2)
        return cls(outcome, expect_enum(MssqlR1ExecutionPathV1, path, "execution path"))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1StateTransitionV1:
    ordinal: int
    transition_kind: MssqlR1TransitionKindV1
    owner_resource: MssqlR1PhysicalResourceRefV1
    instance_selector: MssqlR1ResourceInstanceSelectorV1
    cardinality: MssqlR1TransitionCardinalityV1
    state_change: MssqlR1StateChangeV1 | None
    ordered_revision_rules: tuple[MssqlR1RevisionRuleV1, ...]
    ordered_applicabilities: tuple[MssqlR1TransitionApplicabilityV1, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "transition ordinal")
        _VALIDATE.require_exact_enum(self.transition_kind, MssqlR1TransitionKindV1, "transition kind")
        _VALIDATE.require_exact_enum(self.cardinality, MssqlR1TransitionCardinalityV1, "transition cardinality")
        if (
            type(self.owner_resource) is not MssqlR1PhysicalResourceRefV1
            or type(self.instance_selector) is not MssqlR1ResourceInstanceSelectorV1
        ):
            raise MssqlR1V3ContractError("transition owner/selector is invalid")
        _VALIDATE.require_tuple(self.ordered_revision_rules, MssqlR1RevisionRuleV1, "revision rules")
        if self.ordered_revision_rules:
            _VALIDATE.require_canonical(self.ordered_revision_rules, "revision rules")
        _VALIDATE.require_tuple(
            self.ordered_applicabilities, MssqlR1TransitionApplicabilityV1, "applicabilities", nonempty=True
        )
        _VALIDATE.require_canonical(self.ordered_applicabilities, "transition applicabilities")
        if self.state_change is not None:
            if (
                type(self.state_change) is not MssqlR1StateChangeV1
                or self.state_change.state_field.resource != self.owner_resource
            ):
                raise MssqlR1V3ContractError("state change differs from transition owner")
        candidates = tuple(item.candidate_value for item in self.ordered_revision_rules)
        if any(item.resource != self.owner_resource for item in candidates):
            raise MssqlR1V3ContractError("revision candidate differs from transition owner")
        if len({item.canonical_bytes for item in candidates}) != len(candidates):
            raise MssqlR1V3ContractError("revision candidate coordinates must be unique")
        predecessors = () if self.state_change is None else self.state_change.ordered_predecessor_states
        if self.transition_kind is MssqlR1TransitionKindV1.APPEND and self.state_change is not None:
            raise MssqlR1V3ContractError("append cannot change state")
        if self.transition_kind is MssqlR1TransitionKindV1.CREATE and predecessors:
            raise MssqlR1V3ContractError("create cannot have predecessor states")
        if self.transition_kind is MssqlR1TransitionKindV1.CAS and self.state_change is not None and not predecessors:
            raise MssqlR1V3ContractError("state-changing CAS requires predecessor states")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _TRANSITION,
            (
                self.ordinal,
                self.transition_kind,
                self.owner_resource.canonical_bytes,
                self.instance_selector.canonical_bytes,
                self.cardinality,
                None if self.state_change is None else self.state_change.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_revision_rules),
                tuple(item.canonical_bytes for item in self.ordered_applicabilities),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StateTransitionV1:
        values = list(decode_canonical_bytes(payload, _TRANSITION, field_count=8))
        values[1] = expect_enum(MssqlR1TransitionKindV1, values[1], "transition kind")
        values[2] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[2], "owner"))
        values[3] = MssqlR1ResourceInstanceSelectorV1.from_canonical_bytes(expect_bytes(values[3], "selector"))
        values[4] = expect_enum(MssqlR1TransitionCardinalityV1, values[4], "cardinality")
        if values[5] is not None:
            values[5] = MssqlR1StateChangeV1.from_canonical_bytes(expect_bytes(values[5], "state change"))
        values[6] = tuple(
            MssqlR1RevisionRuleV1.from_canonical_bytes(expect_bytes(item, "rule"))
            for item in expect_tuple(values[6], "revision rules")
        )
        values[7] = tuple(
            MssqlR1TransitionApplicabilityV1.from_canonical_bytes(expect_bytes(item, "applicability"))
            for item in expect_tuple(values[7], "transition applicabilities")
        )
        return cls(*values)  # type: ignore[arg-type]


def transition_coordinates(
    transition: MssqlR1StateTransitionV1,
) -> tuple[MssqlR1ComparisonCoordinateV1, ...]:
    """Project every authority coordinate carried by a transition."""

    values = list(transition.instance_selector.ordered_coordinates)
    if transition.state_change is not None:
        values.append(transition.state_change.state_field)
    for rule in transition.ordered_revision_rules:
        values.extend(
            item
            for item in (rule.current_value, rule.expected_value, rule.requested_candidate, rule.candidate_value)
            if isinstance(item, MssqlR1ComparisonCoordinateV1)
        )
    return tuple(values)
