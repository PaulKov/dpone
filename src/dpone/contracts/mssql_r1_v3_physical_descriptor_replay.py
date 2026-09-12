"""Outcome-scoped replay proof formulas for physical modules."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ComparisonCoordinateV1,
    MssqlR1ComparisonLiteralV1,
    MssqlR1ResourceExistenceOperandV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1ComparisonOperatorV1,
    MssqlR1ComparisonSourceV1,
    MssqlR1ExecutionPathV1,
    MssqlR1ReplayBooleanOperatorV1,
    MssqlR1ReplayComparatorV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
    MssqlR1ValueCardinalityV1,
)

_EXISTENCE = b"dpone-r1-physical-resource-existence-operand-v1\0"
_PAIR = b"dpone-r1-physical-comparison-pair-v1\0"
_CLAUSE = b"dpone-r1-physical-replay-clause-v1\0"
_GROUP = b"dpone-r1-physical-replay-clause-group-v1\0"
_POLICY = b"dpone-r1-physical-replay-outcome-policy-v1\0"
RECEIPT_NAMES = frozenset(
    "dpone_control_receipt_v3 dpone_open_stage_recovery_receipt_v3 dpone_effect_receipt_v3 "
    "dpone_batch_effect_receipt_v3 dpone_xmin_effect_receipt_v3".split()
)


LeftOperand = MssqlR1ComparisonCoordinateV1 | MssqlR1ResourceExistenceOperandV1
RightOperand = MssqlR1ComparisonCoordinateV1 | MssqlR1ComparisonLiteralV1 | None


def _encode_operand(value: LeftOperand | RightOperand) -> bytes | None:
    return None if value is None else value.canonical_bytes


def _decode_left(value: object) -> LeftOperand:
    payload = expect_bytes(value, "left operand")
    if payload.startswith(b"dpone-r1-physical-comparison-coordinate-v1\0"):
        return MssqlR1ComparisonCoordinateV1.from_canonical_bytes(payload)
    if payload.startswith(_EXISTENCE):
        return MssqlR1ResourceExistenceOperandV1.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("left operand union is unsupported")


def _decode_right(value: object | None) -> RightOperand:
    if value is None:
        return None
    payload = expect_bytes(value, "right operand")
    if payload.startswith(b"dpone-r1-physical-comparison-coordinate-v1\0"):
        return MssqlR1ComparisonCoordinateV1.from_canonical_bytes(payload)
    if payload.startswith(b"dpone-r1-physical-comparison-literal-v1\0"):
        return MssqlR1ComparisonLiteralV1.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("right operand union is unsupported")


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1ComparisonPairV1:
    left: LeftOperand
    operator: MssqlR1ComparisonOperatorV1
    right: RightOperand

    def __post_init__(self) -> None:
        if type(self.left) not in {MssqlR1ComparisonCoordinateV1, MssqlR1ResourceExistenceOperandV1}:
            raise MssqlR1V3ContractError("comparison left operand type is unsupported")
        _VALIDATE.require_exact_enum(self.operator, MssqlR1ComparisonOperatorV1, "comparison operator")
        if self.right is not None and type(self.right) not in {
            MssqlR1ComparisonCoordinateV1,
            MssqlR1ComparisonLiteralV1,
        }:
            raise MssqlR1V3ContractError("comparison right operand type is unsupported")
        existence = self.operator in {MssqlR1ComparisonOperatorV1.ROW_EXISTS, MssqlR1ComparisonOperatorV1.ROW_ABSENT}
        null_check = self.operator in {MssqlR1ComparisonOperatorV1.IS_NULL, MssqlR1ComparisonOperatorV1.IS_NOT_NULL}
        binary = self.operator in {
            MssqlR1ComparisonOperatorV1.EQUAL,
            MssqlR1ComparisonOperatorV1.NOT_EQUAL,
            MssqlR1ComparisonOperatorV1.ADJACENT,
        }
        left_is_existence = type(self.left) is MssqlR1ResourceExistenceOperandV1
        if existence and (not left_is_existence or self.right is not None):
            raise MssqlR1V3ContractError("row existence comparison has an invalid shape")
        if left_is_existence and not existence:
            raise MssqlR1V3ContractError("row existence operand requires a row operator")
        if null_check and (
            type(self.left) is not MssqlR1ComparisonCoordinateV1 or not self.left.nullable or self.right is not None
        ):
            raise MssqlR1V3ContractError("NULL comparison has an invalid shape")
        if binary:
            if type(self.left) is not MssqlR1ComparisonCoordinateV1 or self.right is None:
                raise MssqlR1V3ContractError("binary comparison has an invalid shape")
            if self.left.scalar_kind is not self.right.scalar_kind or (
                isinstance(self.right, MssqlR1ComparisonCoordinateV1)
                and self.left.value_cardinality is not self.right.value_cardinality
            ):
                raise MssqlR1V3ContractError("comparison operands are type-incompatible")
            if (
                isinstance(self.right, MssqlR1ComparisonLiteralV1)
                and self.left.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
            ):
                raise MssqlR1V3ContractError("ordered-set comparison requires two typed coordinates")
            if self.operator is MssqlR1ComparisonOperatorV1.ADJACENT and (
                self.left.scalar_kind.value != "integer"
                or self.left.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
                or isinstance(self.right, MssqlR1ComparisonLiteralV1)
                or self.left.nullable
                or self.right.nullable
            ):
                raise MssqlR1V3ContractError("adjacent requires non-null scalar integer coordinates")
        if not (existence or null_check or binary):
            raise MssqlR1V3ContractError("comparison operator shape is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_PAIR, (_encode_operand(self.left), self.operator, _encode_operand(self.right)))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ComparisonPairV1:
        left, operator, right = decode_canonical_bytes(payload, _PAIR, field_count=3)
        return cls(
            _decode_left(left), expect_enum(MssqlR1ComparisonOperatorV1, operator, "operator"), _decode_right(right)
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ReplayClauseV1:
    ordinal: int
    comparator: MssqlR1ReplayComparatorV1
    ordered_comparisons: tuple[MssqlR1ComparisonPairV1, ...]
    fresh_proof_resource: MssqlR1PhysicalResourceRefV1 | None
    requires_descendant_proof: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "replay clause ordinal")
        _VALIDATE.require_exact_enum(self.comparator, MssqlR1ReplayComparatorV1, "replay comparator")
        _VALIDATE.require_tuple(self.ordered_comparisons, MssqlR1ComparisonPairV1, "comparisons", nonempty=True)
        _VALIDATE.require_canonical(self.ordered_comparisons, "comparisons")
        _VALIDATE.require_bool(self.requires_descendant_proof, "descendant proof flag")
        fresh = self.comparator is MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF
        if (fresh and type(self.fresh_proof_resource) is not MssqlR1PhysicalResourceRefV1) or (
            not fresh and self.fresh_proof_resource is not None
        ):
            raise MssqlR1V3ContractError("fresh proof resource shape differs from comparator")
        if self.requires_descendant_proof and not fresh:
            raise MssqlR1V3ContractError("descendant proof requires fresh coherent proof")
        null_guards = {
            item.left.canonical_bytes
            for item in self.ordered_comparisons
            if item.operator is MssqlR1ComparisonOperatorV1.IS_NOT_NULL
            and type(item.left) is MssqlR1ComparisonCoordinateV1
        }
        for pair in self.ordered_comparisons:
            if pair.operator not in {MssqlR1ComparisonOperatorV1.EQUAL, MssqlR1ComparisonOperatorV1.NOT_EQUAL}:
                continue
            operands = (pair.left, pair.right)
            if any(
                isinstance(item, MssqlR1ComparisonCoordinateV1)
                and item.nullable
                and item.canonical_bytes not in null_guards
                for item in operands
            ):
                raise MssqlR1V3ContractError("nullable equality requires byte-identical non-null guards")
        descendant_rows = {
            pair.left.resource.canonical_bytes
            for pair in self.ordered_comparisons
            if pair.operator is MssqlR1ComparisonOperatorV1.ROW_EXISTS
            and isinstance(pair.left, MssqlR1ResourceExistenceOperandV1)
            and pair.left.resource.resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT
            and pair.left.resource.schema_name == "dpone_authority"
            and pair.left.resource.object_name in RECEIPT_NAMES
        }
        descendant_coordinates = {
            operand.resource.canonical_bytes
            for pair in self.ordered_comparisons
            for operand in (pair.left, pair.right)
            if isinstance(operand, MssqlR1ComparisonCoordinateV1)
            and operand.source is MssqlR1ComparisonSourceV1.DESCENDANT_RECEIPT
            and operand.resource is not None
        }
        if bool(descendant_rows) != self.requires_descendant_proof:
            raise MssqlR1V3ContractError("receipt row-exists and descendant-proof flag must agree")
        if descendant_coordinates - descendant_rows:
            raise MssqlR1V3ContractError("descendant receipt field lacks a qualifying row-exists predicate")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CLAUSE,
            (
                self.ordinal,
                self.comparator,
                tuple(item.canonical_bytes for item in self.ordered_comparisons),
                None if self.fresh_proof_resource is None else self.fresh_proof_resource.canonical_bytes,
                self.requires_descendant_proof,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ReplayClauseV1:
        values = list(decode_canonical_bytes(payload, _CLAUSE, field_count=5))
        values[1] = expect_enum(MssqlR1ReplayComparatorV1, values[1], "comparator")
        values[2] = tuple(
            MssqlR1ComparisonPairV1.from_canonical_bytes(expect_bytes(item, "comparison"))
            for item in expect_tuple(values[2], "comparisons")
        )
        if values[3] is not None:
            values[3] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[3], "proof resource"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ReplayClauseGroupV1:
    ordinal: int
    boolean_operator: MssqlR1ReplayBooleanOperatorV1
    ordered_clauses: tuple[MssqlR1ReplayClauseV1, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "replay group ordinal")
        _VALIDATE.require_exact_enum(self.boolean_operator, MssqlR1ReplayBooleanOperatorV1, "group operator")
        _VALIDATE.require_tuple(self.ordered_clauses, MssqlR1ReplayClauseV1, "replay clauses", nonempty=True)
        _VALIDATE.require_contiguous(self.ordered_clauses, "replay clauses")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _GROUP, (self.ordinal, self.boolean_operator, tuple(item.canonical_bytes for item in self.ordered_clauses))
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ReplayClauseGroupV1:
        ordinal, operator, clauses = decode_canonical_bytes(payload, _GROUP, field_count=3)
        return cls(
            ordinal,  # type: ignore[arg-type]
            expect_enum(MssqlR1ReplayBooleanOperatorV1, operator, "group operator"),
            tuple(
                MssqlR1ReplayClauseV1.from_canonical_bytes(expect_bytes(item, "clause"))
                for item in expect_tuple(clauses, "replay clauses")
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ReplayOutcomePolicyV1:
    outcome_literal: str
    execution_path: MssqlR1ExecutionPathV1
    group_operator: MssqlR1ReplayBooleanOperatorV1
    ordered_groups: tuple[MssqlR1ReplayClauseGroupV1, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_lower_ascii(self.outcome_literal, "replay outcome")
        _VALIDATE.require_exact_enum(self.execution_path, MssqlR1ExecutionPathV1, "execution path")
        _VALIDATE.require_exact_enum(self.group_operator, MssqlR1ReplayBooleanOperatorV1, "policy operator")
        _VALIDATE.require_tuple(self.ordered_groups, MssqlR1ReplayClauseGroupV1, "replay groups", nonempty=True)
        _VALIDATE.require_contiguous(self.ordered_groups, "replay groups")

    @property
    def coordinate(self) -> tuple[str, MssqlR1ExecutionPathV1]:
        return self.outcome_literal, self.execution_path

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _POLICY,
            (
                self.outcome_literal,
                self.execution_path,
                self.group_operator,
                tuple(item.canonical_bytes for item in self.ordered_groups),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ReplayOutcomePolicyV1:
        outcome, path, operator, groups = decode_canonical_bytes(payload, _POLICY, field_count=4)
        return cls(
            outcome,  # type: ignore[arg-type]
            expect_enum(MssqlR1ExecutionPathV1, path, "execution path"),
            expect_enum(MssqlR1ReplayBooleanOperatorV1, operator, "policy operator"),
            tuple(
                MssqlR1ReplayClauseGroupV1.from_canonical_bytes(expect_bytes(item, "group"))
                for item in expect_tuple(groups, "replay groups")
            ),
        )


def replay_clause_coordinates(clause: MssqlR1ReplayClauseV1) -> tuple[MssqlR1ComparisonCoordinateV1, ...]:
    """Project comparison and row-selector coordinates from one replay clause."""

    values: list[MssqlR1ComparisonCoordinateV1] = []
    for pair in clause.ordered_comparisons:
        for operand in (pair.left, pair.right):
            if isinstance(operand, MssqlR1ComparisonCoordinateV1):
                values.append(operand)
            elif isinstance(operand, MssqlR1ResourceExistenceOperandV1):
                values.extend(operand.instance_selector.ordered_coordinates)
    return tuple(values)
