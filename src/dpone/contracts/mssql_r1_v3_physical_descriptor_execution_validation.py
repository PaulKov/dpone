"""Cross-contract validation for physical execution semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.contracts.mssql_r1_v3_codec import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ComparisonCoordinateV1,
    MssqlR1ResourceExistenceOperandV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1ComparisonOperatorV1,
    MssqlR1ComparisonSourceV1,
    MssqlR1ExecutionPathV1,
    MssqlR1ReplayBooleanOperatorV1,
    MssqlR1ReplayComparatorV1,
    MssqlR1RequestBindingKindV1,
    MssqlR1ResourceInstanceSelectorKindV1,
    MssqlR1TransitionAuthorityV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_locks import require_lock_order
from dpone.contracts.mssql_r1_v3_physical_descriptor_replay import replay_clause_coordinates
from dpone.contracts.mssql_r1_v3_physical_descriptor_requests import (
    require_coordinate_authority,
    validate_outcome_variants,
    validate_request_authority,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    ACCESS_MATRIX,
    MssqlR1AccessKindV1,
    MssqlR1PhysicalResourceAccessV1,
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ResourceKindV1,
    MssqlR1ValueCardinalityV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_transitions import transition_coordinates
from dpone.contracts.mssql_r1_v3_schema_modules import (
    MssqlR1FixedResultV3,
    MssqlR1StageScanTemplateV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1ParameterDirectionV3

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import MssqlR1ResourceInstanceSelectorV1
    from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import MssqlR1PhysicalErrorConditionV1
    from dpone.contracts.mssql_r1_v3_physical_descriptor_locks import MssqlR1PhysicalLockStepV1
    from dpone.contracts.mssql_r1_v3_physical_descriptor_replay import (
        MssqlR1ComparisonPairV1,
        MssqlR1ReplayClauseGroupV1,
        MssqlR1ReplayClauseV1,
        MssqlR1ReplayOutcomePolicyV1,
    )
    from dpone.contracts.mssql_r1_v3_physical_descriptor_requests import (
        MssqlR1OutcomeVariantV1,
        MssqlR1RequestAuthorityV1,
    )
    from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import MssqlR1PhysicalResourceDeclarationV1
    from dpone.contracts.mssql_r1_v3_physical_descriptor_transitions import MssqlR1StateTransitionV1
    from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1ProcedureResultContractV3
    from dpone.contracts.mssql_r1_v3_schema_primitives import (
        MssqlR1ResultColumnV3,
        MssqlR1SchemaProcedureParameterV3,
        MssqlR1SupportedCodecEntryV3,
    )


_TRANSITION_SOURCES = frozenset(
    (MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD, MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD)
)


class MssqlR1ExecutionSemanticsView(Protocol):
    @property
    def request_authority(self) -> MssqlR1RequestAuthorityV1: ...
    @property
    def ordered_outcome_variants(self) -> tuple[MssqlR1OutcomeVariantV1, ...]: ...
    @property
    def ordered_lock_steps(self) -> tuple[MssqlR1PhysicalLockStepV1, ...]: ...
    @property
    def ordered_read_set(self) -> tuple[MssqlR1PhysicalResourceAccessV1, ...]: ...
    @property
    def ordered_write_set(self) -> tuple[MssqlR1PhysicalResourceAccessV1, ...]: ...
    @property
    def transition_authority(self) -> MssqlR1TransitionAuthorityV1: ...
    @property
    def ordered_state_transitions(self) -> tuple[MssqlR1StateTransitionV1, ...]: ...
    @property
    def ordered_replay_outcome_policies(self) -> tuple[MssqlR1ReplayOutcomePolicyV1, ...]: ...
    @property
    def ordered_error_conditions(self) -> tuple[MssqlR1PhysicalErrorConditionV1, ...]: ...


@dataclass(frozen=True, slots=True)
class MssqlR1ExecutionValidatorV1:
    execution: MssqlR1ExecutionSemanticsView

    def validate_contract(
        self,
        parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
        result: MssqlR1ProcedureResultContractV3,
        declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
        codecs: tuple[MssqlR1SupportedCodecEntryV3, ...],
    ) -> None:
        if any(item.direction is not MssqlR1ParameterDirectionV3.INPUT for item in parameters):
            raise MssqlR1V3ContractError("physical module parameters must be input-only")
        validate_request_authority(self.execution.request_authority, parameters, codecs)
        validate_outcome_variants(self.execution.ordered_outcome_variants, result)
        self._validate_outcome_paths(type(result) is MssqlR1StageScanTemplateV3)
        self._validate_accesses(declarations)
        self._validate_locks_and_transitions(declarations)
        result_columns = result.ordered_columns if type(result) is MssqlR1FixedResultV3 else ()
        self._validate_coordinates(parameters, result_columns, declarations)
        self._validate_replay(declarations)
        self._validate_sealed_digest()

    def _validate_outcome_paths(self, is_scan: bool) -> None:
        outcomes = tuple(item.outcome_literal for item in self.execution.ordered_outcome_variants)
        policies = tuple(item.coordinate for item in self.execution.ordered_replay_outcome_policies)
        if is_scan:
            if (
                outcomes
                or policies
                or self.execution.transition_authority is not MssqlR1TransitionAuthorityV1.READ_ONLY
                or self.execution.ordered_state_transitions
                or self.execution.ordered_write_set
            ):
                raise MssqlR1V3ContractError("stage scan requires read-only execution without mutation authority")
            return
        applications = {
            (item.outcome_literal, item.execution_path)
            for transition in self.execution.ordered_state_transitions
            for item in transition.ordered_applicabilities
        }
        if not outcomes or not policies or set(outcomes) != {item[0] for item in policies}:
            raise MssqlR1V3ContractError("fixed result requires complete nonempty replay outcome coverage")
        if len(set(policies)) != len(policies):
            raise MssqlR1V3ContractError("replay outcome/path coordinates contain duplicates")
        allowed = {
            MssqlR1TransitionAuthorityV1.SELF_CONTAINED: {
                MssqlR1ExecutionPathV1.FRESH_MUTATION,
                MssqlR1ExecutionPathV1.IDEMPOTENT_REPLAY,
            },
            MssqlR1TransitionAuthorityV1.CALLER_UOW: {
                MssqlR1ExecutionPathV1.FRESH_MUTATION,
                MssqlR1ExecutionPathV1.IDEMPOTENT_REPLAY,
            },
            MssqlR1TransitionAuthorityV1.READ_ONLY: {MssqlR1ExecutionPathV1.READ_ONLY_PROBE},
        }[self.execution.transition_authority]
        if {item[1] for item in policies} != allowed:
            raise MssqlR1V3ContractError("replay policies differ from transition-authority path closure")
        fresh = {item for item in policies if item[1] is MssqlR1ExecutionPathV1.FRESH_MUTATION}
        if self.execution.transition_authority is MssqlR1TransitionAuthorityV1.SELF_CONTAINED:
            if applications != fresh or not fresh:
                raise MssqlR1V3ContractError("transition applicability differs from fresh replay policy coverage")
        elif applications or (
            self.execution.transition_authority is MssqlR1TransitionAuthorityV1.CALLER_UOW and not fresh
        ):
            raise MssqlR1V3ContractError("transition applicability differs from its external authority")

    def _validate_accesses(self, declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1]) -> None:
        for access in (*self.execution.ordered_read_set, *self.execution.ordered_write_set):
            declaration = declarations.get(access.resource.canonical_bytes)
            if (
                declaration is None
                or access.access_kind not in declaration.ordered_allowed_access_kinds
                or access.access_kind.value not in ACCESS_MATRIX[access.resource.resource_kind]
            ):
                raise MssqlR1V3ContractError("module resource access is undeclared or widened")

    def _validate_locks_and_transitions(self, declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1]) -> None:
        require_lock_order(
            self.execution.ordered_lock_steps, {key: item.lock_subrank for key, item in declarations.items()}
        )
        for step in self.execution.ordered_lock_steps:
            declaration = declarations.get(step.resource.canonical_bytes)
            if declaration is None or (step.lock_kind, step.cardinality) != (
                declaration.allowed_lock_kind,
                declaration.lock_cardinality,
            ):
                raise MssqlR1V3ContractError("lock step contradicts its resource declaration")
            _validate_selector(step.instance_selector, declaration)
        writes = {item.resource.canonical_bytes for item in self.execution.ordered_write_set}
        for transition in self.execution.ordered_state_transitions:
            declaration = declarations.get(transition.owner_resource.canonical_bytes)
            if declaration is None or transition.owner_resource.canonical_bytes not in writes:
                raise MssqlR1V3ContractError("transition owner is not a declared write resource")
            _validate_selector(transition.instance_selector, declaration, transition.cardinality.value)

    def _validate_coordinates(
        self,
        parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
        result_columns: tuple[MssqlR1ResultColumnV3, ...],
        declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
    ) -> None:
        coordinates = [item.value_source for item in self.execution.request_authority.ordered_scalar_parameter_bindings]
        coordinates.extend(
            item for step in self.execution.ordered_lock_steps for item in step.instance_selector.ordered_coordinates
        )
        coordinates.extend(
            item
            for transition in self.execution.ordered_state_transitions
            for item in transition_coordinates(transition)
        )
        coordinates.extend(
            item
            for policy in self.execution.ordered_replay_outcome_policies
            for group in policy.ordered_groups
            for clause in group.ordered_clauses
            for item in replay_clause_coordinates(clause)
        )
        for coordinate in coordinates:
            require_coordinate_authority(
                coordinate, self.execution.request_authority, parameters, result_columns, declarations
            )
            self._validate_source_scope(coordinate, declarations)
            if coordinate.resource is not None:
                self._require_read(coordinate.resource, declarations)
        for transition in self.execution.ordered_state_transitions:
            for coordinate in transition_coordinates(transition):
                if coordinate.source in _TRANSITION_SOURCES and coordinate.resource != transition.owner_resource:
                    raise MssqlR1V3ContractError("transition coordinate differs from its exact owner")

    def _validate_source_scope(
        self,
        coordinate: MssqlR1ComparisonCoordinateV1,
        declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
    ) -> None:
        if coordinate.source is MssqlR1ComparisonSourceV1.SESSION_BINDING:
            declaration = None if coordinate.resource is None else declarations.get(coordinate.resource.canonical_bytes)
            if (
                declaration is None
                or declaration.resource.resource_kind is not MssqlR1ResourceKindV1.SESSION
                or coordinate.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
                or coordinate.nullable
            ):
                raise MssqlR1V3ContractError("session binding requires a declared non-null scalar session field")
        if coordinate.source in _TRANSITION_SOURCES:
            owners = {item.owner_resource.canonical_bytes for item in self.execution.ordered_state_transitions}
            if coordinate.resource is None or coordinate.resource.canonical_bytes not in owners:
                raise MssqlR1V3ContractError("transition coordinate has no exact transition owner")

    def _validate_replay(self, declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1]) -> None:
        for policy in self.execution.ordered_replay_outcome_policies:
            for group in policy.ordered_groups:
                for clause in group.ordered_clauses:
                    existence = tuple(
                        pair.left
                        for pair in clause.ordered_comparisons
                        if isinstance(pair.left, MssqlR1ResourceExistenceOperandV1)
                    )
                    for operand in existence:
                        declaration = declarations.get(operand.resource.canonical_bytes)
                        if declaration is None:
                            raise MssqlR1V3ContractError("existence proof references an unbound resource")
                        _validate_selector(operand.instance_selector, declaration)
                        self._require_read(operand.resource, declarations)
                    if clause.comparator is MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF:
                        matches = tuple(item for item in existence if item.resource == clause.fresh_proof_resource)
                        if not matches:
                            raise MssqlR1V3ContractError("fresh coherent proof lacks an exact row identity predicate")
                        if not any(
                            step.resource == clause.fresh_proof_resource
                            and any(step.instance_selector == item.instance_selector for item in matches)
                            for step in self.execution.ordered_lock_steps
                        ):
                            raise MssqlR1V3ContractError("fresh coherent proof lacks a byte-identical lock selector")

    def _require_read(
        self,
        resource: MssqlR1PhysicalResourceRefV1,
        declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
    ) -> None:
        declaration = declarations.get(resource.canonical_bytes)
        edge = MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.READ)
        if (
            declaration is None
            or MssqlR1AccessKindV1.READ not in declaration.ordered_allowed_access_kinds
            or edge not in self.execution.ordered_read_set
        ):
            raise MssqlR1V3ContractError("resource-bearing authority lacks an exact read edge")

    def _validate_sealed_digest(self) -> None:
        if self.execution.request_authority.binding_kind is not MssqlR1RequestBindingKindV1.SEALED_REQUEST_DIGEST:
            return
        if not all(
            _policy_implies_digest(policy, self.execution.request_authority)
            for policy in self.execution.ordered_replay_outcome_policies
        ):
            raise MssqlR1V3ContractError("sealed request digest does not dominate every replay policy branch")


def _validate_selector(
    selector: MssqlR1ResourceInstanceSelectorV1,
    declaration: MssqlR1PhysicalResourceDeclarationV1,
    cardinality: str | None = None,
) -> None:
    keys = tuple(item for item in declaration.ordered_fields if item.instance_key_ordinal is not None)
    singleton = selector.selector_kind is MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON
    if (singleton and keys) or (not singleton and len(selector.ordered_coordinates) != len(keys)):
        raise MssqlR1V3ContractError("resource selector does not cover the complete instance key")
    expected = (
        MssqlR1ValueCardinalityV1.ORDERED_SET
        if selector.selector_kind is MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES
        else MssqlR1ValueCardinalityV1.SCALAR
    )
    allowed_sources = {
        MssqlR1ComparisonSourceV1.REQUEST_FIELD,
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        MssqlR1ComparisonSourceV1.SESSION_BINDING,
        MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
    }
    if any(
        coordinate.source not in allowed_sources
        or coordinate.scalar_kind is not field.scalar_kind
        or coordinate.value_cardinality is not expected
        or coordinate.nullable != field.nullable
        for coordinate, field in zip(selector.ordered_coordinates, keys, strict=True)
    ):
        raise MssqlR1V3ContractError("resource selector does not cover exact typed instance key")
    selected = cardinality or declaration.lock_cardinality.value
    shapes = {
        "one": {("declared_singleton", False), ("single_value", False)},
        "zero_or_one": {("single_value", True)},
        "one_or_more": {("ordered_values", False)},
        "exact_request_set": {("ordered_values", False), ("ordered_values", True)},
    }
    if selected not in shapes or (selector.selector_kind.value, selector.allow_empty) not in shapes[selected]:
        raise MssqlR1V3ContractError("selector shape differs from required cardinality")
    if selected == "exact_request_set" and declaration.lock_cardinality.value != selected:
        raise MssqlR1V3ContractError("exact-request-set selector contradicts resource declaration")


def _is_digest_pair(pair: MssqlR1ComparisonPairV1, authority: MssqlR1RequestAuthorityV1) -> bool:
    return (
        pair.operator is MssqlR1ComparisonOperatorV1.EQUAL
        and isinstance(pair.left, MssqlR1ComparisonCoordinateV1)
        and pair.left.source is MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER
        and pair.left.resource is None
        and pair.left.field_name == authority.request_digest_parameter
        and pair.left.scalar_kind is MssqlR1ProjectionScalarKindV1.DIGEST
        and pair.left.value_cardinality is MssqlR1ValueCardinalityV1.SCALAR
        and not pair.left.nullable
        and isinstance(pair.right, MssqlR1ComparisonCoordinateV1)
        and pair.right.source is MssqlR1ComparisonSourceV1.RESOURCE_FIELD
        and pair.right.resource
        == MssqlR1PhysicalResourceRefV1(
            MssqlR1ResourceKindV1.STATIC_OBJECT,
            "dpone_authority",
            "dpone_sealed_effect_v3",
            None,
        )
        and pair.right.field_name == "request_digest"
        and pair.right.scalar_kind is MssqlR1ProjectionScalarKindV1.DIGEST
        and pair.right.value_cardinality is MssqlR1ValueCardinalityV1.SCALAR
        and not pair.right.nullable
    )


def _clause_implies_digest(clause: MssqlR1ReplayClauseV1, authority: MssqlR1RequestAuthorityV1) -> bool:
    return any(_is_digest_pair(pair, authority) for pair in clause.ordered_comparisons)


def _group_implies_digest(group: MssqlR1ReplayClauseGroupV1, authority: MssqlR1RequestAuthorityV1) -> bool:
    values = tuple(_clause_implies_digest(clause, authority) for clause in group.ordered_clauses)
    return all(values) if group.boolean_operator is MssqlR1ReplayBooleanOperatorV1.ANY else any(values)


def _policy_implies_digest(policy: MssqlR1ReplayOutcomePolicyV1, authority: MssqlR1RequestAuthorityV1) -> bool:
    values = tuple(_group_implies_digest(group, authority) for group in policy.ordered_groups)
    return all(values) if policy.group_operator is MssqlR1ReplayBooleanOperatorV1.ANY else any(values)


def validate_execution_semantics(
    execution: MssqlR1ExecutionSemanticsView,
    parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
    result: MssqlR1ProcedureResultContractV3,
    declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
    codecs: tuple[MssqlR1SupportedCodecEntryV3, ...],
) -> None:
    MssqlR1ExecutionValidatorV1(execution).validate_contract(parameters, result, declarations, codecs)
