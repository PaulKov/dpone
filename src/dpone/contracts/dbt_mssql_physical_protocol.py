"""Finite attach wire; unfinished transaction/catalog/receipt producers reject.

This representation contract grants neither session authority nor execution.
The canonical model and executor codecs remain their existing owners.
"""

from collections.abc import Mapping
from typing import cast

from dpone.contracts.dbt_mssql_physical_registration_values import require_registration_digest
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_text,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.dbt_mssql_physical_wire import decode_physical_model_plan
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery
from dpone.contracts.native_source_custody import decode_source_executor_binding

ATTACH_COLUMNS = tuple(
    "wire_version registration_id registration_digest generation_id executor_invocation_id "
    "plan_set_sha256 model_unique_id model_plan_sha256 session_registration_id session_id "
    "guard_epoch source_revision control_program_sha256 executor_json plan_json".split()
)
_PARAMETERS = (
    "registration_id",
    "generation",
    "expected_invocation",
    "plan_set_locator",
    "plan_set_sha256",
    "model_unique_id",
)
ATTACH_SQL = "EXEC [dpone_physical].[physical_attach_session_v1] " + ", ".join(f"@{name}=?" for name in _PARAMETERS)


def attach_parameters() -> tuple[str, ...]:
    """Exact ordered public argument names; there are no transaction controls."""
    return _PARAMETERS


def require_attach_request(
    operation: object,
    parameters: Mapping[str, object],
    delivery: PhysicalTransportDelivery,
) -> tuple[str, ...]:
    """Reject unfrozen operations and substituted scope before cursor creation."""
    if operation != "attach" or type(operation) is not str:
        raise ValueError("physical operation unavailable: protected producer and codec are required")
    if type(parameters) is not dict or set(parameters) != set(_PARAMETERS):
        raise ValueError("physical attach requires exactly its six parameters")
    raw = delivery.to_dict()
    values = tuple(require_physical_text(parameters[name], name) for name in _PARAMETERS)
    expected = (
        raw["registration_id"],
        raw["generation_id"],
        raw["executor_invocation_id"],
        raw["plan_set"]["locator"],
        raw["plan_set"]["sha256"],
    )
    if values[:5] != expected:
        raise ValueError("physical attach differs from delivered command scope")
    return values


def require_attach_row(
    row: tuple[object, ...],
    *,
    delivery: PhysicalTransportDelivery,
    parameters: tuple[str, ...],
) -> None:
    """Validate raw scalars before conversion; never repair names or payloads."""
    if len(row) != len(ATTACH_COLUMNS) or any(value is None for value in row):
        raise ValueError("physical attach requires one complete nonnull row")
    value = dict(zip(ATTACH_COLUMNS, row, strict=True))
    if type(value["wire_version"]) is not int or value["wire_version"] != 1:
        raise ValueError("physical attach wire version differs")
    for name in ("registration_id", "generation_id", "executor_invocation_id", "session_registration_id"):
        require_physical_uuid(value[name], name)
    for name in ("registration_digest", "plan_set_sha256", "model_plan_sha256", "control_program_sha256"):
        require_registration_digest(cast(str, value[name]))
    for name in ("session_id", "guard_epoch", "source_revision"):
        require_sql_positive_integer(value[name], name, bigint=name != "session_id")
    if cast(int, value["source_revision"]) < 2:
        raise ValueError("physical attach requires an enrolled source revision")
    require_physical_text(value["model_unique_id"], "model_unique_id")
    raw = delivery.to_dict()
    comparisons = {
        "registration_id": raw["registration_id"],
        "registration_digest": raw["registration_sha256"],
        "generation_id": raw["generation_id"],
        "executor_invocation_id": raw["executor_invocation_id"],
        "plan_set_sha256": raw["plan_set"]["sha256"],
        "model_unique_id": parameters[5],
        "guard_epoch": raw["guard_epoch"],
    }
    if any(value[key] != expected for key, expected in comparisons.items()):
        raise ValueError("physical attach response differs from delivered identity")
    if type(value["executor_json"]) is not str or type(value["plan_json"]) is not str:
        raise ValueError("physical attach payloads require exact text")
    executor = decode_source_executor_binding(cast(str, value["executor_json"]).encode("utf-8"))
    plan_bytes = cast(str, value["plan_json"]).encode("utf-8")
    plan = decode_physical_model_plan(plan_bytes)
    if (
        str(executor.generation_id) != raw["generation_id"]
        or str(executor.invocation_id) != raw["executor_invocation_id"]
        or executor.guard_epoch != raw["guard_epoch"]
        or {"locator": executor.command.locator, "sha256": executor.command.sha256} != raw["command_plan"]
        or {"locator": executor.profile.locator, "sha256": executor.profile.sha256}
        != raw["trusted_profile"]["reference"]
        or plan.generation_id != raw["generation_id"]
        or plan.model_plan_sha256 != value["model_plan_sha256"]
        or plan.spec.model_unique_id != parameters[5]
        or plan.spec.resource_bounds != executor.profile
        or plan.spec.relation.database != raw["model_database"]["database_name"]
        or plan.spec.relation.schema != raw["model_schema"]
    ):
        raise ValueError("physical attach nested model/executor identity differs")
