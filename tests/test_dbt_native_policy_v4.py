"""Opt-in full native policy validation preserves earlier schema contracts."""

from copy import deepcopy
from hashlib import sha256

import pytest

from dpone.contracts.dbt_publish_schema_contract_policy import policy_v3_schema
from dpone.contracts.dbt_publish_schema_contract_v4 import policy_v4_schema, validate_native_policy_v4
from dpone.contracts.dbt_sqlserver_policy import DBT_PROCESS_TIMEOUT_MIN_SECONDS
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_originals import encode_native_original_storage_authority
from tests.test_native_original_storage_authority import authority as storage_authority

D = "sha256:" + "a" * 64
REF = {"locator": "platform/record.json", "sha256": D}
SELECTION = {"reference": REF, "subject": None}


def native_policy():
    import json

    storage = json.loads(encode_native_original_storage_authority(storage_authority()))
    return {
        "schema": "dpone.dbt-publish-policy.v4",
        "profiles": {
            "local": {
                "source": {"type": "mssql", "connection_ref": "source"},
                "sink": {"type": "clickhouse", "connection_ref": "target", "target_schema": "analytics"},
                "runtime": {
                    "image": "example/image@sha256:" + "b" * 64,
                    "xcom_sidecar_image": "example/sidecar@sha256:" + "c" * 64,
                    "toolchain": "dbt-sqlserver-1.11-core-1.12-certified",
                    "dbt_profile": "native",
                    "dbt_target": "local",
                    "dbt_threads": 1,
                    "dbt_timeout_seconds": DBT_PROCESS_TIMEOUT_MIN_SECONDS,
                },
                "strategy_policy": {
                    "allowed_strategies": ["full_refresh"],
                    "publication_completion_timeout_seconds": 600,
                    "full_refresh": {
                        "authorized": True,
                        "serialized_payload_budget": {"metric": "serialized_payload_v1", "max_bytes": 16777216},
                    },
                },
                "dbt_model_physical_design": {
                    "policy": "sqlserver-table-physical-v1",
                    "allowed_layouts": ["rowstore_none"],
                },
                "native_execution": {
                    "control": {"connection_ref": "control", "schema": "dpone_control", "authority": REF},
                    "originals": {
                        "connection_ref": "originals",
                        "authority": {
                            "locator": "platform/storage.json",
                            "sha256": "sha256:"
                            + sha256(encode_native_original_storage_authority(storage_authority())).hexdigest(),
                        },
                        "policy": storage,
                    },
                    "trusted_execution": {
                        "profile": SELECTION,
                        "toolchain": SELECTION,
                        "qualification": SELECTION,
                        "qualification_policy_id": "synthetic-fixture-only",
                    },
                    "generation": {
                        "storage_root": SELECTION,
                        "capacity_authority": REF,
                        "max_generation_bytes": 3 * 16777216,
                    },
                    "limits": {
                        "max_metadata_bytes": 65536,
                        "original_io_chunk_bytes": 4096,
                        "original_io_total_budget_seconds": 1800,
                        "command_termination_allowance_seconds": 30,
                        "total_termination_budget_seconds": 900,
                    },
                },
            }
        },
        "workflows": {"orders": {"owner": "synthetic"}},
    }


def test_complete_policy_is_canonical_and_does_not_mutate_legacy_schema():
    previous = deepcopy(policy_v3_schema())
    value = native_policy()
    assert validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024) == value
    assert policy_v3_schema() == previous
    assert policy_v4_schema()["properties"]["schema"]["const"] == value["schema"]


@pytest.mark.parametrize(
    "mode",
    [
        "legacy-size",
        "unknown-native",
        "missing-authority",
        "false-integer",
        "float-integer",
        "over-capacity",
        "wrong-storage-hash",
        "chunk-over-bound",
        "unknown-layout",
        "wrong-triple",
    ],
)
def test_invalid_native_policy_cannot_acquire_execution_premises(mode):
    value = native_policy()
    assert validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024) == value
    profile = value["profiles"]["local"]
    native = profile["native_execution"]
    if mode == "legacy-size":
        profile["strategy_policy"]["full_refresh"]["max_source_bytes"] = 16
    elif mode == "unknown-native":
        native["arbitrary_provider"] = {}
    elif mode == "missing-authority":
        del native["control"]["authority"]
    elif mode == "false-integer":
        native["limits"]["max_metadata_bytes"] = True
    elif mode == "float-integer":
        native["limits"]["max_metadata_bytes"] = 1.0
    elif mode == "over-capacity":
        native["generation"]["max_generation_bytes"] = 1
    elif mode == "wrong-storage-hash":
        native["originals"]["authority"]["sha256"] = D
    elif mode == "chunk-over-bound":
        native["limits"]["original_io_chunk_bytes"] = 65537
    elif mode == "unknown-layout":
        profile["dbt_model_physical_design"]["allowed_layouts"] = ["columnstore_archive"]
    else:
        value["schema"] = "dpone.dbt-publish-policy.v3"
    import json

    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(ValueError):
        validate_native_policy_v4(payload, max_bytes=1024 * 1024)


def test_native_policy_preserves_qualified_thread_range_and_existing_invocation_tokens():
    value = native_policy()
    profile = value["profiles"]["local"]
    profile["runtime"]["dbt_threads"] = 2
    profile["authoring_template"] = {
        "project_name": "orders",
        "invocation_target": {"database": "warehouse-prod", "schema": "s" * 256},
        "source_relation": {"database": "warehouse", "schema": "dbo", "name": "orders"},
    }
    assert validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024) == value


@pytest.mark.parametrize("invalid", [" ", "schema\nname", "x" * 257])
def test_native_template_reuses_invocation_target_rejection(invalid):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError
    from dpone.contracts.dbt_invocation import DbtInvocationTarget

    with pytest.raises(DbtPublishingError):
        DbtInvocationTarget(database="warehouse", schema=invalid)
    value = native_policy()
    value["profiles"]["local"]["authoring_template"] = {
        "project_name": "orders",
        "invocation_target": {"database": "warehouse", "schema": invalid},
        "source_relation": {"database": "warehouse", "schema": "dbo", "name": "orders"},
    }
    with pytest.raises((ValueError, DbtPublishingError)):
        validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024)


def catalog_policy():
    value = native_policy()
    value["profiles"]["local"]["native_execution"]["physical_catalog_limits"] = {
        "max_catalog_rows": 1000,
        "max_definition_utf16_bytes": 65536,
        "max_dependency_rows": 1000,
        "max_columns": 256,
    }
    return value


def test_explicit_catalog_limits_are_optional_and_retained_without_defaults():
    legacy = native_policy()
    decoded = validate_native_policy_v4(encode_native_delivery_json(legacy), max_bytes=1024 * 1024)
    assert "physical_catalog_limits" not in decoded["profiles"]["local"]["native_execution"]
    value = catalog_policy()
    assert validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024) == value


@pytest.mark.parametrize(
    "field", ["max_catalog_rows", "max_definition_utf16_bytes", "max_dependency_rows", "max_columns"]
)
@pytest.mark.parametrize("invalid", [None, True, 0, -1, 2147483648])
def test_catalog_policy_limits_require_explicit_sql_integers(field, invalid):
    value = catalog_policy()
    value["profiles"]["local"]["native_execution"]["physical_catalog_limits"][field] = invalid
    with pytest.raises(ValueError):
        validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024)


@pytest.mark.parametrize("mode", ["missing", "unknown", "dependencies", "columns", "null"])
def test_catalog_policy_limits_are_closed_and_cross_checked(mode):
    value = catalog_policy()
    native = value["profiles"]["local"]["native_execution"]
    limits = native["physical_catalog_limits"]
    if mode == "missing":
        del limits["max_catalog_rows"]
    elif mode == "unknown":
        limits["unbounded"] = True
    elif mode == "dependencies":
        limits["max_dependency_rows"] = limits["max_catalog_rows"] + 1
    elif mode == "columns":
        limits["max_columns"] = 255
    else:
        native["physical_catalog_limits"] = None
    with pytest.raises(ValueError):
        validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024)


def test_catalog_policy_limits_accept_explicit_sql_upper_bound():
    value = catalog_policy()
    limits = value["profiles"]["local"]["native_execution"]["physical_catalog_limits"]
    for name in ("max_catalog_rows", "max_definition_utf16_bytes", "max_dependency_rows"):
        limits[name] = 2147483647
    assert validate_native_policy_v4(encode_native_delivery_json(value), max_bytes=1024 * 1024) == value
