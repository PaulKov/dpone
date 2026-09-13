"""Retained originals preserve actual catalog preplans without granting authority."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.etl.mssql_schema_preplan import MssqlSchemaPreplanner
from dpone.runtime.etl.mssql_schema_preplan_codec import decode_mssql_schema_preplan, encode_mssql_schema_preplan
from dpone.runtime.sinks.mssql_target_mutation_codec import (
    MssqlRetainedPlanError,
    decode_mssql_target_mutation_plan,
    encode_mssql_target_mutation_plan,
)
from tests.test_mssql_schema_preplan import _admission, _CatalogSink, _CatalogSource, _config, _physical_state


def preplan(ddl=False):
    source = _CatalogSource((("id", "bigint"), ("note", "nvarchar(50)")) if ddl else (("id", "bigint"),))
    config = _config({"schema_evolution": {"enabled": True, "ddl_mode": "online", "apply_safe": True}} if ddl else {})
    value = MssqlSchemaPreplanner().plan(
        config, source=source, sink=_CatalogSink([MssqlCatalogColumn("id", "bigint", True)]), admission=_admission()
    )
    assert source.row_calls == 0
    return value


@pytest.mark.parametrize("ddl", [False, True])
def test_actual_preplanner_roundtrip_preserves_all_hashes(ddl):
    original = preplan(ddl)
    mutation = original.target_mutation_plan
    before = mutation.digest
    document = encode_mssql_schema_preplan(original)
    restored = decode_mssql_schema_preplan(document, sha256(document).digest())
    assert restored == original
    assert restored.target_mutation_plan.digest == before == mutation.digest
    assert restored.target_mutation_plan.expected_before_sha256 == mutation.expected_before_sha256
    assert restored.target_mutation_plan.expected_after_sha256 == mutation.expected_after_sha256
    assert bool(mutation.actions) is ddl
    assert mutation.expectations
    exact = encode_mssql_target_mutation_plan(mutation)
    assert decode_mssql_target_mutation_plan(exact, sha256(exact).digest()) == mutation


def test_retained_preplan_is_detached_from_mutable_diagnostic_report():
    original = replace(preplan(), physical_report={"blockers": [], "preplanned": True})
    document = encode_mssql_schema_preplan(original)
    restored = decode_mssql_schema_preplan(document, sha256(document).digest())
    original.physical_report["blockers"].append("later")
    assert restored.physical_report == {"blockers": [], "preplanned": True}


@pytest.mark.parametrize(
    "codec,decoder",
    [
        (encode_mssql_schema_preplan, decode_mssql_schema_preplan),
        (
            lambda value: encode_mssql_target_mutation_plan(value.target_mutation_plan),
            decode_mssql_target_mutation_plan,
        ),
    ],
)
def test_noncanonical_duplicate_tampered_and_unknown_fields_rejected(codec, decoder):
    document = codec(preplan())
    with pytest.raises(MssqlRetainedPlanError):
        decoder(document, b"x" * 32)
    for raw in (document + b" ", b'{"schema":"a","schema":"b"}'):
        with pytest.raises(MssqlRetainedPlanError):
            decoder(raw, sha256(raw).digest())
    value = strict_json_object(document)
    value["executor_authorized"] = True
    raw = canonical_json_bytes(value)
    with pytest.raises(MssqlRetainedPlanError):
        decoder(raw, sha256(raw).digest())


def test_changed_nested_digest_and_foreign_sql_rejected():
    original = preplan(True).target_mutation_plan
    document = encode_mssql_target_mutation_plan(original)
    value = strict_json_object(document)
    value["mutation_plan"]["actions"][0]["sql"] = "DROP TABLE [DWH].[dbo].[events]"
    raw = canonical_json_bytes(value)
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_target_mutation_plan(raw, sha256(raw).digest())
    forged = replace(
        original,
        actions=(replace(original.actions[0], sql="ALTER TABLE [DWH].[dbo].[foreign] ADD [note] nvarchar(50) NULL"),),
    )
    with pytest.raises(MssqlRetainedPlanError):
        encode_mssql_target_mutation_plan(forged)


def test_actual_authorized_physical_ddl_and_report_roundtrip():
    source = _CatalogSource((("id", "bigint"),))
    config = _config(
        {
            "physical_design": {
                "apply_runtime": True,
                "apply": "safe_window",
                "storage": {"mssql": {"compression": "PAGE"}},
                "reconciliation": {
                    "mode": "safe_window",
                    "approval": {
                        "approved_by": "test",
                        "approved_risks": ["table_settings.compression"],
                        "table": "[DWH].[dbo].[events]",
                    },
                },
            }
        }
    )
    original = MssqlSchemaPreplanner().plan(
        config,
        source=source,
        sink=_CatalogSink([MssqlCatalogColumn("id", "bigint", True)], physical=_physical_state("NONE")),
        admission=_admission(),
    )
    assert original.target_mutation_plan.actions[-1].kind == "physical_design"
    document = encode_mssql_schema_preplan(original)
    assert decode_mssql_schema_preplan(document, sha256(document).digest()) == original
    assert source.row_calls == 0


@pytest.mark.parametrize("field,replacement", [("kind", "arbitrary_ddl"), ("statement_type", "sql"), ("sql", "EXEC p")])
def test_closed_action_vocabulary_even_with_recomputed_document_hash(field, replacement):
    value = strict_json_object(encode_mssql_target_mutation_plan(preplan(True).target_mutation_plan))
    value["mutation_plan"]["actions"][0][field] = replacement
    value["mutation_plan_sha256"] = sha256(canonical_json_bytes(value["mutation_plan"])).hexdigest()
    raw = canonical_json_bytes(value)
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_target_mutation_plan(raw, sha256(raw).digest())


@pytest.mark.parametrize(
    "field,replacement",
    [("representation", "opaque_v1"), ("kind", "other"), ("before_sha256", "A" * 64), ("after_sha256", "f" * 62)],
)
def test_closed_catalog_expectation_shape(field, replacement):
    value = strict_json_object(encode_mssql_target_mutation_plan(preplan().target_mutation_plan))
    value["mutation_plan"]["expectations"][0][field] = replacement
    value["mutation_plan_sha256"] = sha256(canonical_json_bytes(value["mutation_plan"])).hexdigest()
    raw = canonical_json_bytes(value)
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_target_mutation_plan(raw, sha256(raw).digest())


def test_nonfinite_oversized_and_boolean_version_rejected():
    document = encode_mssql_schema_preplan(preplan())
    raw = document.replace(b'"physical_report":null', b'"physical_report":{"bad":NaN}')
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_schema_preplan(raw, sha256(raw).digest())
    raw = b" " * (1024 * 1024 + 1)
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_schema_preplan(raw, sha256(raw).digest())
    value = strict_json_object(encode_mssql_target_mutation_plan(preplan().target_mutation_plan))
    value["mutation_plan"]["version"] = True
    raw = canonical_json_bytes(value)
    with pytest.raises(MssqlRetainedPlanError):
        decode_mssql_target_mutation_plan(raw, sha256(raw).digest())
