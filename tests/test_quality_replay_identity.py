"""Synthetic source-free identity and effective-plan contract tests."""

from dataclasses import replace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.governance.quality_replay_identity import (
    EXCLUDED_LOAD_FIELDS,
    EXCLUDED_OPTIONS,
    QualityReplayIdentityError,
    admission_digest,
    prepare_effective_plan,
    schema_digest,
    validate_effective_plan,
)


def config(**options):
    return LoadConfig("synthetic_source", "synthetic_sink", "public", "items", "analytics", "items", options=options)


def plan(original=None, effective=None):
    original = original or config()
    return prepare_effective_plan(
        original,
        effective or original,
        source_schema=[("id", "integer")],
        payload_schema=[("id", "Int32")],
        target_schema=[("id", "Int32")],
    )


def test_full_refresh_replay_reconstructs_plan_without_source_or_raw_config():
    original = config(query="SELECT 'synthetic literal' AS id", password="synthetic secret")
    effective = replace(original, options={**original.options, "_source_connector": object(), "run_id": "attempt-1"})
    recorded = plan(original, effective)
    replay = replace(original, options={**original.options, "run_id": "attempt-2"})
    assert validate_effective_plan(replay, recorded) == recorded["effective_plan_digest"]
    encoded = canonical_json_bytes(recorded)
    for private in (b"synthetic literal", b"synthetic secret", b"synthetic_source", b"SELECT"):
        assert private not in encoded


@pytest.mark.parametrize("name", sorted(EXCLUDED_OPTIONS))
def test_every_named_runtime_or_secret_option_is_explicitly_excluded(name):
    assert EXCLUDED_OPTIONS[name]
    assert admission_digest(config(**{name: object()})) == admission_digest(config())


@pytest.mark.parametrize("name", sorted(EXCLUDED_LOAD_FIELDS))
def test_every_named_diagnostic_load_field_is_explicitly_excluded(name):
    assert EXCLUDED_LOAD_FIELDS[name]
    assert admission_digest(replace(config(), **{name: 999})) == admission_digest(config())


@pytest.mark.parametrize(
    "changes",
    [
        {"target_table": "other"},
        {"source_table": "other"},
        {"source_conn_id": "other"},
        {"unique_key": ["id"]},
        {"batch_size": 25},
        {"custom_predicate": "id > 0"},
        {"overwrite_type": "exchange"},
    ],
)
def test_semantic_load_changes_break_replay(changes):
    with pytest.raises(QualityReplayIdentityError, match="MISMATCH"):
        validate_effective_plan(replace(config(), **changes), plan())


@pytest.mark.parametrize("name", ["quality", "column_mapping", "schema_contract", "physical_design", "lineage"])
def test_semantic_option_changes_break_replay(name):
    with pytest.raises(QualityReplayIdentityError):
        validate_effective_plan(config(**{name: {"enabled": True}}), plan())


def test_query_change_breaks_identity_and_mapping_order_does_not():
    assert admission_digest(config(query="SELECT 1")) != admission_digest(config(query="SELECT 2"))
    assert admission_digest(config(column_mapping={"a": "x", "b": "y"})) == admission_digest(
        config(column_mapping={"b": "y", "a": "x"})
    )


def test_inline_query_document_binds_sql_and_rejects_external_or_rendered_inputs():
    first = config(query={"mode": "inline", "sql": "SELECT 1"})
    second = config(query={"mode": "inline", "sql": "SELECT 2"})
    assert admission_digest(first) != admission_digest(second)
    for query in (
        {"sql_file": "synthetic.sql"},
        {"mode": "sql_file", "sql_file": "synthetic.sql"},
        {"sql": "SELECT {{ value }}", "render": {"context": {"value": 1}}},
    ):
        with pytest.raises(QualityReplayIdentityError):
            admission_digest(config(query=query))


def test_runtime_manifest_context_and_source_budget_remain_identity_bearing():
    original = config(manifest_dir="/synthetic", repo_root="/synthetic", __dpone_source_byte_budget_v1=1024)
    assert admission_digest(original) != admission_digest(config())


def test_endpoint_option_registry_excludes_secrets_and_rejects_unknowns():
    assert admission_digest(config(source_options={"password": "one", "query": "SELECT 1"})) == admission_digest(
        config(source_options={"password": "two", "query": "SELECT 1"})
    )
    with pytest.raises(QualityReplayIdentityError, match="UNSUPPORTED"):
        admission_digest(config(sink_options={"future_semantics": True}))


@pytest.mark.parametrize("value", [object(), lambda: None, float("nan"), float("inf"), {1: "key"}])
def test_opaque_or_noncanonical_values_fail_without_rendering(value):
    with pytest.raises(QualityReplayIdentityError, match="UNSUPPORTED"):
        admission_digest(config(column_mapping=value))


def test_unknown_field_and_unknown_option_are_rejected_without_echo():
    original = config()
    original.new_field = "synthetic secret"
    with pytest.raises(QualityReplayIdentityError) as error:
        admission_digest(original)
    assert "synthetic secret" not in str(error.value)
    with pytest.raises(QualityReplayIdentityError):
        admission_digest(config(synthetic_unknown=True))


@pytest.mark.parametrize(
    "options", [{"transform": "opaque"}, {"normalization": {"enabled": True}}, {"_new_partitions": [1]}]
)
def test_unsupported_transformations_fail_closed(options):
    with pytest.raises(QualityReplayIdentityError, match="UNSUPPORTED"):
        admission_digest(config(**options))


def test_strategy_override_or_enrichment_cannot_be_laundered_as_identity():
    with pytest.raises(QualityReplayIdentityError, match="UNSUPPORTED"):
        plan(replace(config(), load_strategy=LoadStrategy.INCREMENTAL_APPEND), config())
    with pytest.raises(QualityReplayIdentityError, match="UNSUPPORTED"):
        plan(config(), config(column_mapping={"id": "other"}))


@pytest.mark.parametrize(
    "key",
    [
        "contract_version",
        "admission_digest",
        "effective_config_digest",
        "source_schema_digest",
        "payload_schema_digest",
        "target_schema_digest",
        "effective_plan_digest",
    ],
)
def test_missing_or_tampered_effective_provenance_rejected(key):
    recorded = plan()
    recorded.pop(key)
    with pytest.raises(QualityReplayIdentityError):
        validate_effective_plan(config(), recorded)
    recorded = plan()
    recorded[key] = "sha256:" + "0" * 64
    with pytest.raises(QualityReplayIdentityError):
        validate_effective_plan(config(), recorded)


def test_unknown_provenance_is_rejected():
    recorded = plan()
    recorded["unknown"] = True
    with pytest.raises(QualityReplayIdentityError):
        validate_effective_plan(config(), recorded)


def test_schema_preserves_order_type_and_name():
    original = [("id", "Int32"), ("name", "String")]
    assert schema_digest(original) != schema_digest(list(reversed(original)))
    assert schema_digest(original) != schema_digest([("id", "Int64"), ("name", "String")])
    assert schema_digest(original) != schema_digest([("other", "Int32"), ("name", "String")])
    assert schema_digest(original) == schema_digest(tuple(original))


@pytest.mark.parametrize("schema", [None, {}, [], [("id", "")], [("id", "Int32"), ("id", "Int32")], [(True, "Int32")]])
def test_missing_or_ambiguous_schema_rejected(schema):
    with pytest.raises(QualityReplayIdentityError):
        schema_digest(schema)


def test_cyclic_and_oversized_values_are_bounded():
    cycle = []
    cycle.append(cycle)
    for value in (cycle, "x" * (256 * 1024 + 1)):
        with pytest.raises(QualityReplayIdentityError):
            admission_digest(config(column_mapping=value))
