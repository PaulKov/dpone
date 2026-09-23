"""Renderer guards and opt-in parity on an already-provisioned SQL gateway.

The live cases never install modules or change permissions. Run only in an
explicitly approved disposable environment, with DPONE_WORKSPACE_JSON_LIVE=1,
DPONE_WORKSPACE_JSON_TEST_DSN and DPONE_WORKSPACE_JSON_TEST_SCHEMA. The supplied
certification principal must be allowed to execute the private helpers; ordinary
runtime roles must not receive this capability. Offline checks are not SQL proof.
"""

import hashlib
import json
import os
from dataclasses import replace

import pytest

from dpone.adapters.dbt_workspace_mssql_gateway_security import (
    render_workspace_gateway_security,
    workspace_gateway_identifier,
)


@pytest.fixture
def workspace_json_live_cursor():
    if os.environ.get("DPONE_WORKSPACE_JSON_LIVE") != "1":
        pytest.skip("UNVERIFIED: workspace SQL JSON certification environment is not enabled")
    pyodbc = pytest.importorskip("pyodbc")
    if not os.environ.get("DPONE_WORKSPACE_JSON_TEST_DSN"):
        pytest.fail("workspace SQL JSON certification DSN is not configured")
    schema = workspace_gateway_identifier(os.environ.get("DPONE_WORKSPACE_JSON_TEST_SCHEMA", ""))
    try:
        connection = pyodbc.connect(os.environ["DPONE_WORKSPACE_JSON_TEST_DSN"], autocommit=True, timeout=10)
    except Exception:
        pytest.fail("workspace SQL JSON certification connection is unavailable", pytrace=False)
    cursor = connection.cursor()
    try:
        yield cursor, schema
    finally:
        cursor.close()
        connection.close()


@pytest.mark.integration_live
@pytest.mark.parametrize("ensure_ascii", [True, False])
@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        "[null,true,false,0,-0,-9223372036854775808,9223372036854775807]",
        '{"z":"/\\\\\\"", "a":"\\b\\f\\n\\r\\t\\u0000\\u001f\\u007f"}',
        '{"value":"Жé水😀"}',
        '{"value":"\\u0416\\u00e9\\u6c34\\ud83d\\ude00"}',
        '{"nested":[{"desired":"{\\"ref\\":\\"Ж/😀\\"}"},{}]}',
        '{"value":"space  "}',
        '{"value":"＼／\\\\／＼/"}',
    ],
)
def test_live_sql_python_canonical_and_digest_parity(workspace_json_live_cursor, raw, ensure_ascii):
    cursor, schema = workspace_json_live_cursor
    expected = json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii)
    cursor.execute(
        f"DECLARE @canonical nvarchar(max), @digest varchar(71); "
        f"EXEC [{schema}].[workspace_json_canonical] ?, @canonical OUTPUT, 0, ?; "
        f"EXEC [{schema}].[workspace_json_utf8_sha256] @canonical, @digest OUTPUT; "
        "SELECT @canonical, @digest;",
        raw,
        ensure_ascii,
    )
    assert tuple(cursor.fetchone()) == (expected, "sha256:" + hashlib.sha256(expected.encode()).hexdigest())


@pytest.mark.integration_live
@pytest.mark.parametrize("raw", ["", 'ASCII /\\"', "Жé水😀", "\x00\x01\x7f\n", "𐀀\uffff\U0010ffff"])
def test_live_sql_python_raw_utf8_digest_parity(workspace_json_live_cursor, raw):
    cursor, schema = workspace_json_live_cursor
    cursor.execute(
        f"DECLARE @digest varchar(71); EXEC [{schema}].[workspace_json_utf8_sha256] ?, @digest OUTPUT; SELECT @digest;",
        raw,
    )
    assert cursor.fetchone()[0] == "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


@pytest.mark.integration_live
@pytest.mark.parametrize(
    "raw",
    [
        '{"a":1,"a":2}',
        '[{"a":1,"a":2}]',
        '"scalar"',
        '{"n":1.0}',
        '{"n":1e1}',
        '{"n":9223372036854775808}',
        '{"value":"\\ud800"}',
        '{"value":"\\udc00"}',
        "[" * 18 + "]" * 18,
        "[" + ",".join(["0"] * 8193) + "]",
    ],
)
def test_live_sql_json_rejects_noncontract_inputs(workspace_json_live_cursor, raw):
    cursor, schema = workspace_json_live_cursor
    with pytest.raises(Exception, match="51000"):
        cursor.execute(
            f"DECLARE @canonical nvarchar(max); EXEC [{schema}].[workspace_json_canonical] ?, @canonical OUTPUT;",
            raw,
        )


@pytest.mark.integration_live
@pytest.mark.parametrize("schema_value", ["dpone.example.v1", "x" * 4001])
def test_live_closed_document_rejects_oversized_schema_with_valid_digest(workspace_json_live_cursor, schema_value):
    from dpone.adapters.dbt_workspace_mssql_gateway_validation import workspace_document_validation

    cursor, schema = workspace_json_live_cursor
    body = {"schema": schema_value, "value": "sample"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    document = {**body, "document_sha256": "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()}
    validation = workspace_document_validation(
        schema,
        variable="document",
        fields={"schema": (1,), "value": (1,), "document_sha256": (1,)},
        schema_name="dpone.example.v1",
        digest_field="document_sha256",
        maximum_bytes=8192,
    )
    command = "DECLARE @document nvarchar(max) = ?; " + validation + " SELECT @document_canonical;"
    if schema_value == "dpone.example.v1":
        cursor.execute(command, json.dumps(document))
        assert json.loads(cursor.fetchone()[0]) == document
    else:
        with pytest.raises(Exception, match="51000"):
            cursor.execute(command, json.dumps(document))


def test_private_json_helpers_are_fixed_bounded_batches():
    from dpone.adapters.dbt_workspace_mssql_gateway_json import render_workspace_gateway_json

    batches = render_workspace_gateway_json("dpone_control")
    assert len(batches) == 3
    for name, sql in zip(("escape", "utf8_sha256", "canonical"), batches, strict=True):
        assert sql.startswith(f"CREATE OR ALTER PROCEDURE [dpone_control].[workspace_json_{name}]")
        assert "EXECUTE AS" not in sql and "sp_executesql" not in sql
        assert "67108864" in sql
    assert "Latin1_General_100_BIN2" in batches[0]
    assert "surrogate" in batches[0]
    assert "SHA2_256" in batches[1]
    assert "_UTF8" not in batches[1]
    assert "duplicate key" in batches[2]
    assert "@depth > 16" in batches[2]
    assert "8192" in batches[2]
    assert "REPLACE(@value COLLATE Latin1_General_100_BIN2" in batches[0]
    assert batches[0].count("STRING_ESCAPE(@value, 'json') COLLATE Latin1_General_100_BIN2") == 2


def test_private_json_helpers_cannot_be_called_by_any_runtime_role():
    sql = render_workspace_gateway_security("dpone_control")
    for name in ("escape", "utf8_sha256", "canonical"):
        for role in (
            "dpone_workspace_runtime",
            "dpone_workspace_legacy",
            "dpone_workspace_registrar",
            "dpone_semantic_guard_runtime",
        ):
            assert f"DENY EXECUTE ON OBJECT::[dpone_control].[workspace_json_{name}] TO [{role}];" in sql


def test_request_validator_retains_original_json_and_verifies_legacy_normalized_hash():
    from dpone.adapters.dbt_workspace_mssql_request_validation import render_workspace_request_validation

    sql = render_workspace_request_validation("dpone_control")
    assert sql.startswith("CREATE OR ALTER PROCEDURE [dpone_control].[workspace_request_require]")
    assert "@normalize_slashes = 1" in sql
    assert "@ensure_ascii = 0" in sql
    assert "@canonical = @request_canonical" in sql and "@request_body" in sql
    assert "workspace request resource partition differs" in sql
    assert "workspace request resource order differs" in sql
    assert "8192" in sql and "16777216" in sql
    grants = render_workspace_gateway_security("dpone_control")
    assert "DENY EXECUTE ON OBJECT::[dpone_control].[workspace_request_require] TO [dpone_workspace_runtime]" in grants


def _request_payload(guard_id):
    from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest
    from dpone.contracts.dbt_workspace_lifecycle import workspace_request_payload
    from tests.test_dbt_workspace_mssql_activation_admission import _request

    original = _request()
    arguments = {key: getattr(original, key) for key in original.__dataclass_fields__ if key != "request_sha256"}
    arguments["resources"] = (replace(original.resources[0], guard_id=guard_id),)
    return workspace_request_payload(DbtWorkspaceActivationRequest.build(**arguments))


def test_legacy_normalized_digest_does_not_replace_original_resource_identity():
    slash = _request_payload("guard:/original")
    backslash = _request_payload("guard:\\original")
    assert slash["request_sha256"] == backslash["request_sha256"]
    assert slash["resources"] != backslash["resources"]


@pytest.mark.integration_live
@pytest.mark.parametrize(
    "guard_id",
    [
        "guard:/plain",
        "guard:\\original",
        "guard:Ж\\水/😀",
        'guard:"quote"',
        "guard:line\nend ",
        "guard:＼original",
        "guard:＼／\\／/mixed",
    ],
)
def test_live_request_retains_raw_resource_and_validates_normalized_digest(workspace_json_live_cursor, guard_id):
    cursor, schema = workspace_json_live_cursor
    payload = _request_payload(guard_id)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    cursor.execute(
        f"DECLARE @canonical nvarchar(max); EXEC [{schema}].[workspace_request_require] ?, @canonical OUTPUT; SELECT @canonical;",
        raw,
    )
    observed = cursor.fetchone()[0]
    assert observed == raw
    assert json.loads(observed)["resources"][0]["guard_id"] == guard_id
    unsigned = {key: value for key, value in payload.items() if key != "request_sha256"}
    raw_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert (raw_digest == payload["request_sha256"]) == ("\\" not in guard_id)


@pytest.mark.integration_live
@pytest.mark.parametrize("value", ["＼", "＼／", "\\／"])
@pytest.mark.parametrize("ensure_ascii", [False, True])
@pytest.mark.parametrize("normalize", [False, True])
def test_live_slash_replacement_is_binary_not_database_width_equivalence(
    workspace_json_live_cursor, value, ensure_ascii, normalize
):
    cursor, schema = workspace_json_live_cursor
    cursor.execute(
        f"DECLARE @quoted nvarchar(max); EXEC [{schema}].[workspace_json_escape] ?, @quoted OUTPUT, ?, ?; SELECT @quoted;",
        value,
        ensure_ascii,
        normalize,
    )
    expected = value.replace("\\", "/") if normalize else value
    assert cursor.fetchone()[0] == json.dumps(expected, ensure_ascii=ensure_ascii)


@pytest.mark.integration_live
@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "wrong_hash",
        "duplicate_subject",
        "empty_partition",
        "nul_guard",
        "oversized_previous",
        "wrong_uuid",
        "resource_extra",
    ],
)
def test_live_request_rejects_forged_closed_payload_even_with_recomputed_digest(workspace_json_live_cursor, mutation):
    from dpone.contracts.airflow_deployment import canonical_fingerprint

    cursor, schema = workspace_json_live_cursor
    payload = _request_payload("guard:original")
    if mutation == "extra":
        payload["unexpected"] = "value"
    elif mutation == "duplicate_subject":
        payload["write_subjects"] *= 2
    elif mutation == "empty_partition":
        payload["resources"][0]["write_subjects"] = []
    elif mutation == "nul_guard":
        payload["resources"][0]["guard_id"] = "guard:\x00unsafe"
    elif mutation == "oversized_previous":
        payload["previous_deployment_id"] = "x" * 4001
    elif mutation == "wrong_uuid":
        payload["activation_id"] = "invalid"
    elif mutation == "resource_extra":
        payload["resources"][0]["unexpected"] = "value"
    payload["request_sha256"] = canonical_fingerprint(
        {key: value for key, value in payload.items() if key != "request_sha256"}
    )
    if mutation == "wrong_hash":
        payload["request_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(Exception, match="51000"):
        cursor.execute(
            f"DECLARE @canonical nvarchar(max); EXEC [{schema}].[workspace_request_require] ?, @canonical OUTPUT;",
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )


def test_security_cutover_is_transactional_and_requires_all_gateway_objects():
    sql = render_workspace_gateway_security("dpone_control")
    assert "SET XACT_ABORT ON;" in sql and "BEGIN TRANSACTION;" in sql and "COMMIT TRANSACTION;" in sql
    assert "OBJECT_ID(N'[dpone_control].[workspace_handover_complete]', N'P')" in sql
    assert "gateway object is missing" in sql
    assert "ownership chain differs" in sql
    assert "EXECUTE AS" not in sql
    assert "workspace gateway schema owner is a runtime role" in sql
    assert "module.uses_ansi_nulls <> 1" in sql and "module.uses_quoted_identifier <> 1" in sql


@pytest.mark.parametrize(
    "table",
    [
        "dbt_workspace_channels",
        "dbt_workspace_handover_claims",
        "dbt_workspace_activations",
        "dbt_workspace_activation_guards",
        "dbt_workspace_activation_write_subjects",
        "dbt_workspace_attempts",
        "dbt_workspace_attempt_guards",
        "semantic_refresh_guards",
    ],
)
def test_no_runtime_role_has_direct_protected_mutation(table):
    sql = render_workspace_gateway_security("dpone_control")
    for role in (
        "dpone_workspace_runtime",
        "dpone_workspace_legacy",
        "dpone_workspace_registrar",
        "dpone_semantic_guard_runtime",
    ):
        assert (
            f"DENY INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON OBJECT::[dpone_control].[{table}] TO [{role}];"
            in sql
        )


def test_disjoint_execute_roles_prevent_managed_runtime_using_legacy_or_registration_api():
    sql = render_workspace_gateway_security("dpone_control")
    assert "GRANT EXECUTE ON OBJECT::[dpone_control].[workspace_handover_claim] TO [dpone_workspace_runtime];" in sql
    for procedure in (
        "workspace_legacy_prepare",
        "workspace_legacy_activate",
        "workspace_register_empty",
        "workspace_adopt_active",
    ):
        assert f"DENY EXECUTE ON OBJECT::[dpone_control].[{procedure}] TO [dpone_workspace_runtime];" in sql
    assert (
        "DENY EXECUTE ON OBJECT::[dpone_control].[workspace_handover_complete] TO [dpone_semantic_guard_runtime];"
        in sql
    )
    assert "GRANT EXECUTE ON SCHEMA" not in sql
    assert "GRANT INSERT" not in sql


@pytest.mark.parametrize("schema", ["", "dbo]; DROP TABLE x;--", "schema.name", "x" * 129, "control space"])
def test_renderer_rejects_untrusted_identifiers(schema):
    with pytest.raises(ValueError):
        render_workspace_gateway_security(schema)
