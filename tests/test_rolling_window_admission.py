"""A window never silently bypasses an explicitly configured legacy policy."""

from types import SimpleNamespace

import pytest

from dpone.contracts.process_errors import ETLProcessError
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.rolling_window_admission import validate_window_admission


@pytest.mark.parametrize(
    "field,value",
    [
        ("with_dedup", True),
        ("dedup_expression", "id"),
        ("reconciliation", True),
        ("reconciliation_policy", object()),
        ("custom_predicate", "1=1"),
        ("portable_scope", {"column": "id"}),
        ("micro_batch_commit", True),
        ("only_new_rows", True),
        ("partition", {"column": "at"}),
        ("load_strategy", "full_refresh"),
    ],
)
def test_active_incompatible_field_is_rejected(field, value):
    with pytest.raises(ETLProcessError, match="rolling_window_unsupported"):
        validate_window_admission(SimpleNamespace(options={}, **{field: value}))


@pytest.mark.parametrize(
    "key",
    [
        "backfill",
        "cdc",
        "diff",
        "scd2",
        "state",
        "schema_contract",
        "reconciliation",
        "source_custom_predicate",
        "pre_sql",
        "post_sql",
    ],
)
def test_configured_unimplemented_option_is_rejected(key):
    with pytest.raises(ETLProcessError, match="rolling_window_unsupported"):
        validate_window_admission(SimpleNamespace(options={key: {"enabled": True}}))


def test_quality_policy_is_not_silently_ignored_before_custom_factory():
    def forbidden(_):
        pytest.fail("custom factory called before governance admission")

    load = SimpleNamespace(options={"rolling_window": {}, "quality": {"acceptance": {"enabled": True}}})
    process = SimpleNamespace(config=SimpleNamespace(name="window", load_config=load))
    with pytest.raises((ETLProcessError, ValueError), match="quality|acceptance"):
        DefaultProcessRunner(window_runtime_factory=forbidden).run(process)


def test_inert_policies_and_normalized_defaults_are_admitted():
    validate_window_admission(
        SimpleNamespace(
            load_strategy="replace",
            with_dedup=False,
            partition={},
            options={"partition": {}, "backfill": {}, "merge_policy": "auto"},
        )
    )


@pytest.mark.parametrize("name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_schema_chunking_contract_matches_runtime(name):
    import json
    from pathlib import Path

    import jsonschema

    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / name).read_text())
    rule = schema["definitions"]["native_transfer_execution"]["properties"]["chunking"]
    validator = jsonschema.Draft7Validator(rule)
    assert not list(validator.iter_errors({"parallelism": 4, "mode": "bounded_window", "checkpointing": "resumable"}))
    for invalid in (
        {"parallelism": True},
        {"parallelism": 0},
        {"parallelism": 65},
        {"checkpointing": "disabled"},
        {"unknown": 1},
    ):
        assert list(validator.iter_errors(invalid))


def test_chunking_without_window_fails_before_legacy_hydration():
    def forbidden():
        pytest.fail("legacy hydration must not ignore bounded-window declaration")

    process = SimpleNamespace(
        config=SimpleNamespace(
            name="bad",
            ensure_runtime_bindings=forbidden,
            load_config=SimpleNamespace(options={"native_transfer": {"execution": {"chunking": {}}}}),
        )
    )
    with pytest.raises(ETLProcessError, match="rolling_window_required"):
        DefaultProcessRunner().run(process)
