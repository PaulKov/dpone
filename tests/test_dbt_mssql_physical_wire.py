"""Independent canonical document vectors and hostile physical-plan input."""

import copy
from dataclasses import replace

import pytest

from dpone.contracts.dbt_mssql_physical_validation import PhysicalPlanError
from dpone.contracts.dbt_mssql_physical_wire import (
    decode_physical_model_plan,
    decode_physical_plan_set,
    encode_physical_plan_set,
    physical_plan_set_digest,
)
from tests.support.dbt_mssql_physical import canonical, digest, plan_set_document, rehash


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_row", "rowstore_page", "columnstore"])
def test_complete_roundtrip_and_external_digest(managed, layout):
    document = plan_set_document(managed=managed, layout=layout)
    payload = canonical(document)
    plan = decode_physical_plan_set(payload)
    assert encode_physical_plan_set(plan) == payload
    assert plan.to_dict() == document
    assert physical_plan_set_digest(plan) == digest(document)


@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_page", "columnstore"])
def test_standalone_model_plan_uses_the_canonical_plan_grammar(layout):
    document = plan_set_document(layout=layout)["models"][0]
    payload = canonical(document)
    plan = decode_physical_model_plan(payload)
    assert plan.to_dict() == document


@pytest.mark.parametrize(
    "alter",
    [
        lambda value: value + b"\n",
        lambda value: value.replace(b'":', b'": '),
    ],
)
def test_standalone_model_plan_rejects_noncanonical_bytes(alter):
    payload = canonical(plan_set_document()["models"][0])
    with pytest.raises(PhysicalPlanError):
        decode_physical_model_plan(alter(payload))


def test_standalone_model_plan_rejects_derived_name_or_digest_drift():
    document = copy.deepcopy(plan_set_document()["models"][0])
    document["candidate_name"] = "substituted"
    with pytest.raises(PhysicalPlanError):
        decode_physical_model_plan(canonical(document))


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("models", 0),
        ("models", 0, "spec"),
        ("models", 0, "spec", "relation"),
        ("models", 0, "spec", "filegroup"),
        ("models", 0, "spec", "columns", 0),
        ("models", 0, "predecessor"),
        ("model_database",),
        ("workspace_attempt",),
        ("guard",),
        ("profile",),
    ],
)
def test_every_object_rejects_unknown_fields(path):
    document = plan_set_document()
    item = document
    for key in path:
        item = item[key]
    item["unexpected"] = None
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(document))


@pytest.mark.parametrize(
    "field,value",
    [
        ("candidate_name", "alternate"),
        ("helper_name", "alternate"),
        ("backup_name", "alternate"),
        ("columnstore_index_name", "alternate"),
        ("model_plan_sha256", "sha256:" + "e" * 64),
        ("generation_id", "30000000-0000-0000-0000-000000000001"),
    ],
)
def test_tampered_plan_is_not_repaired(field, value):
    document = plan_set_document()
    document["models"][0][field] = value
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(document))


@pytest.mark.parametrize(
    "alter",
    [
        lambda value: b" " + value,
        lambda value: value + b"\n",
        lambda value: value.replace(b'":', b'": '),
        lambda value: b"\xef\xbb\xbf" + value,
    ],
)
def test_equivalent_noncanonical_bytes_reject(alter):
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(alter(canonical(plan_set_document())))


def test_duplicate_models_reject():
    document = plan_set_document()
    document["models"].append(copy.deepcopy(document["models"][0]))
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(document))


@pytest.mark.parametrize(
    "field,value",
    [
        ("dtype", "INT"),
        ("dtype", "datetime2"),
        ("dtype", "decimal(2,3)"),
        ("nullable", 1),
        ("nullable", "false"),
        ("collation", "Latin1_General_100_BIN2"),
        ("name", "😀" * 65),
        ("name", "a\x00b"),
    ],
)
def test_rehashed_malformed_columns_still_reject(field, value):
    document = plan_set_document()
    document["models"][0]["spec"]["columns"][0][field] = value
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(rehash(document)))


@pytest.mark.parametrize(
    "field,value",
    [
        ("database_id", True),
        ("database_id", 2147483648),
        ("create_token", "2026-02-29T12:00:00.1234567"),
        ("create_token", "2024-02-29T12:00:00.123456"),
        ("database_guid", "10000000000000000000000000000001"),
        ("database_name", "other"),
    ],
)
def test_invalid_pin_and_internal_database_mismatch_reject(field, value):
    document = plan_set_document()
    document["model_database"][field] = value
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(document))


@pytest.mark.parametrize("value", [False, 0, -1, 9223372036854775808, "1"])
def test_exact_bounded_epoch_rejects(value):
    document = plan_set_document()
    document["guard"]["fencing_epoch"] = value
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(document))


def test_codec_does_not_apply_256_column_admission_limit_or_deduplicate():
    document = plan_set_document()
    columns = document["models"][0]["spec"]["columns"]
    columns[:] = [{"name": f"c{index}", "dtype": "int", "nullable": True, "collation": None} for index in range(257)]
    columns[-1]["name"] = columns[0]["name"]
    payload = canonical(rehash(document))
    result = decode_physical_plan_set(payload)
    assert len(result.models[0].spec.columns) == 257
    assert result.models[0].spec.columns[0].name == result.models[0].spec.columns[-1].name
    assert encode_physical_plan_set(result) == payload


def test_character_collation_required_and_retained():
    document = plan_set_document()
    column = document["models"][0]["spec"]["columns"][0]
    column.update(dtype="nvarchar(80)", collation=None)
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(canonical(rehash(document)))
    column["collation"] = "Latin1_General_100_BIN2"
    result = decode_physical_plan_set(canonical(rehash(document)))
    assert result.models[0].spec.columns[0].collation == column["collation"]


def test_locator_is_not_authenticated_by_codec():
    document = plan_set_document()
    document["runtime_registration_id"] = "90000000-0000-0000-0000-000000000009"
    document["profile"]["locator"] = "unprovisioned/profile"
    result = decode_physical_plan_set(canonical(document))
    assert result.runtime_registration_id == document["runtime_registration_id"]
    assert result.profile.locator == "unprovisioned/profile"


@pytest.mark.parametrize(
    "payload",
    [
        b"{}" * (1024 * 1024),
        b'{"x":"' + b"x" * 4097 + b'"}',
        b'{"x":' + b"[" * 33 + b"0" + b"]" * 33 + b"}",
        b'{"x":0,"x":1}',
        b'{"x":1.2}',
    ],
)
def test_inherited_json_limits_and_duplicate_keys(payload):
    with pytest.raises(PhysicalPlanError):
        decode_physical_plan_set(payload)


def test_all_required_top_level_fields_reject_when_missing():
    original = plan_set_document()
    for field in original:
        document = copy.deepcopy(original)
        del document[field]
        with pytest.raises(PhysicalPlanError):
            decode_physical_plan_set(canonical(document))


def test_direct_plan_set_constructor_enforces_activation_uuid_without_changing_legacy_carrier():
    from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest

    plan = decode_physical_plan_set(canonical(plan_set_document()))
    old = plan.workspace_attempt
    attempt = DbtWorkspaceAttemptRequest.build(
        activation_id="legacy-free-text",
        attempt_id=old.attempt_id,
        workflow_id=old.workflow_id,
        write_subjects=old.write_subjects,
    )
    with pytest.raises(PhysicalPlanError):
        replace(plan, workspace_attempt=attempt)
