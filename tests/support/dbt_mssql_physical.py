"""Synthetic physical-plan vectors, not runtime or qualification evidence."""

import hashlib
import json

from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest

GENERATION = "10000000-0000-0000-0000-000000000001"


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def digest(value):
    return "sha256:" + hashlib.sha256(canonical(value)).hexdigest()


def rehash(document):
    """Refresh only checksums, allowing tests to isolate semantic validation."""
    for plan in document["models"]:
        spec = plan["spec"]
        spec.pop("model_spec_sha256", None)
        spec["model_spec_sha256"] = digest(spec)
        plan.pop("model_plan_sha256", None)
        plan["model_plan_sha256"] = digest(plan)
    return document


def plan_set_document(*, managed=False, layout="rowstore_page"):
    """Construct expected bytes independently of production plan constructors."""
    reference = {"locator": "originals/example", "sha256": "sha256:" + "b" * 64}
    spec = {
        "schema": "dpone.mssql-physical-model-spec.v1",
        "model_unique_id": "model.example.orders",
        "source_graph_sha256": "sha256:" + "a" * 64,
        "relation": {"database": "example", "schema": "models", "table": "orders"},
        "columns": [{"name": "id", "dtype": "int", "nullable": False, "collation": None}],
        "layout": layout,
        "physical_policy": "sqlserver-table-physical-v1",
        "filegroup": {"data_space_id": 1, "name": "PRIMARY"},
        "resource_bounds": reference.copy(),
    }
    spec["model_spec_sha256"] = digest(spec)

    def name(role, prefix):
        value = {
            "schema": "dpone.mssql-physical-object-name.v1",
            "generation_id": GENERATION,
            "model_unique_id": spec["model_unique_id"],
            "role": role,
        }
        return prefix + hashlib.sha256(canonical(value)).hexdigest()

    predecessor = {"kind": "ABSENT"}
    if managed:
        predecessor = {
            "kind": "MANAGED",
            "object_id": 42,
            "object_create_time": "2024-02-29T12:00:00.1234567",
            "local_receipt": reference.copy(),
        }
    plan = {
        "schema": "dpone.mssql-physical-model-plan.v1",
        "generation_id": GENERATION,
        "spec": spec,
        "predecessor": predecessor,
        "candidate_name": name("CANDIDATE", "dpone_c_"),
        "helper_name": name("HELPER", "dpone_h_"),
        "backup_name": name("BACKUP", "dpone_b_") if managed else None,
        "columnstore_index_name": name("CCI", "dpone_i_") if layout == "columnstore" else None,
    }
    plan["model_plan_sha256"] = digest(plan)
    attempt = DbtWorkspaceAttemptRequest.build(
        activation_id=GENERATION,
        attempt_id="sha256:" + "c" * 64,
        workflow_id="orders",
        write_subjects=("sha256:" + "d" * 64,),
    )
    return {
        "schema": "dpone.mssql-physical-plan-set.v1",
        "generation_id": GENERATION,
        "runtime_registration_id": "20000000-0000-0000-0000-000000000001",
        "workspace_attempt": {
            "activation_id": attempt.activation_id,
            "attempt_id": attempt.attempt_id,
            "workflow_id": attempt.workflow_id,
            "write_subjects": list(attempt.write_subjects),
            "request_sha256": attempt.request_sha256,
        },
        "guard": {"guard_id": "physical-example", "fencing_epoch": 1},
        "profile": reference.copy(),
        "model_database": {
            "database_name": "example",
            "database_id": 5,
            "create_token": "2024-02-29T12:00:00.1234567",
            "database_guid": GENERATION,
        },
        "models": [plan],
    }
