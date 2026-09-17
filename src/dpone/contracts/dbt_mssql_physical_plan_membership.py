"""Pure selected-model comparisons, never source acquisition or SQL authority."""

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_mssql_physical import PhysicalModelPlan, PhysicalPlanSet
from dpone.contracts.dbt_mssql_physical_registration_values import RegisteredLimits
from dpone.contracts.dbt_native_execution_policy import (
    require_physical_collation_name,
    require_physical_filegroup_name,
)
from dpone.contracts.dbt_publish_models import DbtModelArtifact
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.contracts.native_identity import OriginalRef


class PhysicalPlanMembershipError(ValueError):
    """Stable, payload-free rejection of incomplete or inconsistent membership."""

    def __init__(self, code: str = "DPONE_PHYSICAL_PLAN_MEMBERSHIP_INVALID") -> None:
        self.code = code
        self.remediation = (
            "Select native_execution.physical_collation.name in the authenticated policy; "
            "verify SQL availability before reservation."
            if code == "DPONE_PHYSICAL_PLAN_COLLATION_UNAVAILABLE"
            else None
        )
        super().__init__(code)


def _require(condition: bool) -> None:
    if not condition:
        raise PhysicalPlanMembershipError()


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PhysicalPlanMembershipError()
    return value


def require_model_plan_membership(
    manifest: Mapping[str, Any],
    *,
    selection: DbtSelectionLock,
    plan_set: PhysicalPlanSet,
    profile: Mapping[str, Any],
    limits: RegisteredLimits,
    logical_target: tuple[str, str],
    resource_bounds: OriginalRef,
    declared_models: tuple[DbtModelArtifact, ...],
) -> None:
    """Compare all selected MODELs after the adapter authenticates graph/source.

    This pure function does not establish authenticity of its supplied mappings.
    Actual managed graph policy must have passed before enrollment uses the result.
    Column order is the retained manifest's declared order, never a sorted digest
    projection. No character collation is inferred from an expected plan/default.
    """
    if type(selection) is not DbtSelectionLock or type(plan_set) is not PhysicalPlanSet:
        raise PhysicalPlanMembershipError()
    selection.__post_init__()
    plan_set.__post_init__()
    limits.__post_init__()
    native = _mapping(profile.get("native_execution"))
    filegroup = require_physical_filegroup_name(native)
    design = _mapping(profile.get("dbt_model_physical_design"))
    allowed_layouts = design.get("allowed_layouts", ["rowstore_none"])
    _require(type(allowed_layouts) is list and all(type(value) is str for value in allowed_layouts))
    nodes = _mapping(manifest.get("nodes"))
    selected_models = []
    for unique_id in selection.selected_graph_unique_ids:
        prefix = unique_id.partition(".")[0]
        section = _mapping(manifest.get("unit_tests", {})) if prefix == "unit_test" else nodes
        node = _mapping(section.get(unique_id))
        _require(node.get("unique_id") == unique_id and node.get("resource_type") == prefix)
        if prefix == "model":
            selected_models.append(unique_id)
    _require(tuple(sorted(selected_models)) == tuple(model.spec.model_unique_id for model in plan_set.models))
    declared = {model.unique_id: model for model in declared_models}
    _require(len(declared) == len(declared_models))
    for model in plan_set.models:
        spec = model.spec
        _require(spec.source_graph_sha256 == selection.graph_contract_sha256)
        _require(spec.filegroup.name == filegroup and spec.resource_bounds == resource_bounds)
        _require(len(spec.columns) <= limits.max_columns)
        node = _mapping(nodes[spec.model_unique_id])
        config = _mapping(node.get("config"))
        if config.get("materialized") != "dpone_managed_table":
            raise PhysicalPlanMembershipError("DPONE_PHYSICAL_PLAN_MANAGED_SOURCE_REQUIRED")
        _require(config.get("enabled") is True)
        target = (node.get("database"), node.get("schema"))
        _require(target == logical_target and target == (spec.relation.database, spec.relation.schema))
        _require(node.get("alias") == spec.relation.table)
        meta = _mapping(config.get("meta"))
        publish = _mapping(_mapping(meta.get("dpone")).get("publish"))
        layout = publish.get("model_storage", "rowstore_none")
        _require(type(layout) is str and layout == spec.layout and layout in allowed_layouts)
        _require(_mapping(config.get("contract")).get("enforced") is True)
        _require(spec.model_unique_id in declared)
        column_contracts = declared[spec.model_unique_id].column_contracts
        _require(
            tuple((column.name, column.data_type, column.nullable) for column in column_contracts)
            == tuple((column.name, column.dtype, column.nullable) for column in spec.columns)
        )
        _columns(node, model, native)


def _columns(node: Mapping[str, Any], model: PhysicalModelPlan, native: Mapping[str, Any]) -> None:
    columns = _mapping(node.get("columns"))
    _require(tuple(columns) == tuple(column.name for column in model.spec.columns))
    for name, expected in zip(columns, model.spec.columns, strict=True):
        raw = _mapping(columns[name])
        _require(type(name) is str and raw.get("name") == name)
        dtype = raw.get("data_type")
        if type(dtype) is not str:
            raise PhysicalPlanMembershipError()
        try:
            canonical = normalize_mssql_physical_type(dtype)
        except ValueError:
            raise PhysicalPlanMembershipError() from None
        _require(dtype == canonical and expected.dtype == canonical)
        constraints = raw.get("constraints", [])
        _require(type(constraints) is list)
        not_null = False
        for constraint in constraints:
            item = _mapping(constraint)
            _require(item.get("type") == "not_null")
            not_null = True
        _require(expected.nullable is (not not_null))
        if canonical.partition("(")[0] in {"char", "varchar", "nchar", "nvarchar"}:
            try:
                selected = require_physical_collation_name(native)
            except ValueError:
                raise PhysicalPlanMembershipError("DPONE_PHYSICAL_PLAN_COLLATION_UNAVAILABLE") from None
            _require(expected.collation == selected)
        else:
            _require(expected.collation is None)
