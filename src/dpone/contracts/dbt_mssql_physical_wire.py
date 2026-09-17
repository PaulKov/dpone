"""Closed canonical physical-plan original codec, with no admission side effects.

Incoming derived fields are verified by reconstructing the complete canonical
record. Neither a valid digest nor runtime-registration locator grants authority.
"""

from __future__ import annotations

from hashlib import sha256
from typing import cast
from uuid import UUID

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    ManagedPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalPlanSet,
    PhysicalRelation,
)
from dpone.contracts.dbt_mssql_physical_validation import (
    PhysicalPlanError,
    require_physical_text,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef

PHYSICAL_PLAN_SET_KIND = "mssql_physical_plan_set_v1"


def _object(value: object, keys: str) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != set(keys.split()):
        raise PhysicalPlanError("physical record requires its exact closed fields")
    return cast(dict[str, NativeJsonValue], value)


def _array(value: object) -> list[NativeJsonValue]:
    if type(value) is not list or not value:
        raise PhysicalPlanError("physical collection requires a nonempty array")
    return cast(list[NativeJsonValue], value)


def _schema(value: dict[str, NativeJsonValue], expected: str) -> None:
    if value["schema"] != expected:
        raise PhysicalPlanError("physical schema tag is unsupported")


def _ref(value: object) -> OriginalRef:
    obj = _object(value, "locator sha256")
    return OriginalRef(require_physical_text(obj["locator"], "locator"), require_physical_text(obj["sha256"], "sha256"))


def _column(value: object) -> MssqlCatalogColumn:
    obj = _object(value, "name dtype nullable collation")
    if type(obj["nullable"]) is not bool:
        raise PhysicalPlanError("column nullable requires exact bool")
    collation = obj["collation"]
    return MssqlCatalogColumn(
        require_physical_text(obj["name"], "column.name"),
        require_physical_text(obj["dtype"], "column.dtype"),
        obj["nullable"],
        None if collation is None else require_physical_text(collation, "column.collation"),
    )


def _spec(value: object) -> PhysicalModelSpec:
    obj = _object(
        value,
        "schema model_unique_id model_spec_sha256 source_graph_sha256 relation columns layout physical_policy filegroup resource_bounds",
    )
    _schema(obj, "dpone.mssql-physical-model-spec.v1")
    if obj["physical_policy"] != "sqlserver-table-physical-v1":
        raise PhysicalPlanError("physical policy tag is unsupported")
    relation = _object(obj["relation"], "database schema table")
    filegroup = _object(obj["filegroup"], "data_space_id name")
    result = PhysicalModelSpec(
        model_unique_id=require_physical_text(obj["model_unique_id"], "model_unique_id"),
        source_graph_sha256=require_physical_text(obj["source_graph_sha256"], "source_graph_sha256"),
        relation=PhysicalRelation(
            *(require_physical_text(relation[key], key) for key in ("database", "schema", "table"))
        ),
        columns=tuple(_column(column) for column in _array(obj["columns"])),
        layout=require_physical_text(obj["layout"], "layout"),
        filegroup=PhysicalFilegroup(
            require_sql_positive_integer(filegroup["data_space_id"], "data_space_id"),
            require_physical_text(filegroup["name"], "filegroup.name"),
        ),
        resource_bounds=_ref(obj["resource_bounds"]),
    )
    if result.model_spec_sha256 != obj["model_spec_sha256"]:
        raise PhysicalPlanError("model spec digest differs from canonical unsigned spec")
    return result


def _predecessor(value: object) -> AbsentPredecessor | ManagedPredecessor:
    if type(value) is not dict:
        raise PhysicalPlanError("predecessor requires a closed record")
    if value.get("kind") == "ABSENT":
        _object(value, "kind")
        return AbsentPredecessor()
    obj = _object(value, "kind object_id object_create_time local_receipt")
    if obj["kind"] != "MANAGED":
        raise PhysicalPlanError("predecessor kind is unsupported")
    return ManagedPredecessor(
        require_sql_positive_integer(obj["object_id"], "object_id"),
        require_physical_text(obj["object_create_time"], "object_create_time"),
        _ref(obj["local_receipt"]),
    )


def _plan(value: object) -> PhysicalModelPlan:
    obj = _object(
        value,
        "schema generation_id model_plan_sha256 spec predecessor candidate_name helper_name backup_name columnstore_index_name",
    )
    _schema(obj, "dpone.mssql-physical-model-plan.v1")
    result = PhysicalModelPlan(
        require_physical_uuid(obj["generation_id"], "generation_id"),
        _spec(obj["spec"]),
        _predecessor(obj["predecessor"]),
    )
    if result.to_dict() != obj:
        raise PhysicalPlanError("plan names or digest differ from the derived canonical plan")
    return result


def _plan_set(value: object) -> PhysicalPlanSet:
    obj = _object(
        value, "schema generation_id runtime_registration_id workspace_attempt guard profile model_database models"
    )
    _schema(obj, "dpone.mssql-physical-plan-set.v1")
    attempt = _object(obj["workspace_attempt"], "activation_id attempt_id workflow_id write_subjects request_sha256")
    guard = _object(obj["guard"], "guard_id fencing_epoch")
    pin = _object(obj["model_database"], "database_name database_id create_token database_guid")
    return PhysicalPlanSet(
        generation_id=require_physical_uuid(obj["generation_id"], "generation_id"),
        runtime_registration_id=require_physical_uuid(obj["runtime_registration_id"], "runtime_registration_id"),
        workspace_attempt=DbtWorkspaceAttemptRequest(
            activation_id=require_physical_uuid(attempt["activation_id"], "activation_id"),
            attempt_id=require_physical_text(attempt["attempt_id"], "attempt_id"),
            workflow_id=require_physical_text(attempt["workflow_id"], "workflow_id"),
            write_subjects=tuple(
                require_physical_text(item, "write_subject") for item in _array(attempt["write_subjects"])
            ),
            request_sha256=require_physical_text(attempt["request_sha256"], "request_sha256"),
        ),
        guard=DbtWorkspaceGuardEpoch(
            require_physical_text(guard["guard_id"], "guard_id"),
            require_sql_positive_integer(guard["fencing_epoch"], "fencing_epoch", bigint=True),
        ),
        profile=_ref(obj["profile"]),
        model_database=MssqlDatabaseAuthorityPin(
            require_physical_text(pin["database_name"], "database_name"),
            require_sql_positive_integer(pin["database_id"], "database_id"),
            require_physical_text(pin["create_token"], "create_token"),
            UUID(require_physical_uuid(pin["database_guid"], "database_guid")),
        ),
        models=tuple(_plan(model) for model in _array(obj["models"])),
    )


def decode_physical_model_plan(payload: bytes) -> PhysicalModelPlan:
    """Decode one canonical model-plan document without granting admission.

    The attach protocol returns the selected model plan rather than the complete
    plan set.  Keeping that boundary here prevents transports from depending on
    the private record parser or maintaining a second implementation of the
    physical-plan grammar.
    """

    try:
        result = _plan(decode_native_delivery_json(payload))
        if encode_native_delivery_json(result.to_dict()) != payload:
            raise PhysicalPlanError("physical model plan requires exact canonical bytes")
        return result
    except PhysicalPlanError:
        raise
    except (ValueError, DbtPublishingError):
        raise PhysicalPlanError("physical model plan violates its canonical contract") from None


def decode_physical_plan_set(payload: bytes) -> PhysicalPlanSet:
    """Reject malformed, alternate or tampered bytes without partial records.

    Validity is internal consistency only, not graph/receipt authentication,
    policy qualification, current SQL ownership or permission to dispatch.
    """
    try:
        result = _plan_set(decode_native_delivery_json(payload))
        if encode_native_delivery_json(result.to_dict()) != payload:
            raise PhysicalPlanError("physical plan set requires exact canonical bytes")
        return result
    except PhysicalPlanError:
        raise
    except (ValueError, DbtPublishingError):
        raise PhysicalPlanError("physical plan set violates its canonical contract") from None


def encode_physical_plan_set(value: PhysicalPlanSet) -> bytes:
    """Encode a complete plan set and verify it through the same strict boundary."""
    if type(value) is not PhysicalPlanSet:
        raise PhysicalPlanError("encoding requires an exact physical plan set")
    payload = encode_native_delivery_json(value.to_dict())
    decode_physical_plan_set(payload)
    return payload


def physical_plan_set_digest(value: PhysicalPlanSet) -> str:
    """Hash the entire document, retaining every validated nested digest."""
    return "sha256:" + sha256(encode_physical_plan_set(value)).hexdigest()
