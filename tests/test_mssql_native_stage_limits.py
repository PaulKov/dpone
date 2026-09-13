"""Independent worker settings preserve legacy resource and evidence identity."""

from dataclasses import asdict

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.contracts.native_delivery_observations import normalize_delivery_limits
from dpone.manifest.mssql_native_policy import native_limits
from tests.test_mssql_native_policy import config

LEGACY = dict(
    max_total_encoded_bytes=1000000,
    stage_allocated_bytes_stop_threshold=2000000,
    max_rows=65536,
    max_bytes=16777216,
    max_row_bytes=1048576,
    max_pending=2,
    max_staging_tables=1024,
    parallelism=2,
)


@pytest.mark.parametrize("encoding,importing", [(None, None), (2, 2), (None, 2), (2, None)])
def test_legacy_identity_and_positional_construction(encoding, importing):
    limits = NativeChunkLimits(*LEGACY.values(), encoding_parallelism=encoding, import_parallelism=importing)
    assert limits.to_dict() == LEGACY
    assert limits.effective_encoding_parallelism == limits.effective_import_parallelism == 2
    assert limits.retained_work_capacity == 4
    assert limits.spool_payload_bound == 5 * limits.max_bytes
    assert normalize_delivery_limits(LEGACY) == LEGACY
    with pytest.raises(TypeError):
        NativeChunkLimits(*LEGACY.values(), 3)


@pytest.mark.parametrize(
    "encoding,importing,expected", [(3, None, (3, 2)), (None, 4, (2, 4)), (1, 3, (1, 3)), (64, 1, (64, 1))]
)
def test_asymmetric_policy(encoding, importing, expected):
    limits = NativeChunkLimits(**LEGACY, encoding_parallelism=encoding, import_parallelism=importing)
    assert (limits.effective_encoding_parallelism, limits.effective_import_parallelism) == expected
    assert limits.retained_work_capacity == max(expected) + 2
    assert limits.spool_payload_bound == (max(expected) + 3) * limits.max_bytes
    record = {**LEGACY, "encoding_parallelism": expected[0], "import_parallelism": expected[1]}
    assert limits.to_dict() == record
    assert normalize_delivery_limits(record, schema_version=2) == record
    with pytest.raises(ValueError, match="exact_limits_required"):
        normalize_delivery_limits(record)
    assert (
        native_limits(config(encoding_parallelism=encoding or 2, import_parallelism=importing or 2)).to_dict()[
            "encoding_parallelism"
        ]
        == expected[0]
    )


@pytest.mark.parametrize("field", ["encoding_parallelism", "import_parallelism"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 65, "2", 2.0])
def test_strict_overrides(field, value):
    with pytest.raises(ValueError, match=f"invalid_limit:{field}"):
        NativeChunkLimits(**LEGACY, **{field: value})
    with pytest.raises(ValueError, match=f"invalid_limit:{field}"):
        native_limits(config(**{field: value}))


@pytest.mark.parametrize("field", ["encoding_parallelism", "import_parallelism"])
def test_manifest_null_is_not_python_fallback(field):
    assert NativeChunkLimits(**LEGACY, **{field: None}).to_dict() == LEGACY
    with pytest.raises(ValueError, match=f"invalid_limit:{field}"):
        native_limits(config(**{field: None}))


@pytest.mark.parametrize("version", [True, "2", 3, None])
def test_unknown_report_versions_rejected(version):
    with pytest.raises(ValueError):
        normalize_delivery_limits(LEGACY, schema_version=version)


def test_v2_does_not_silently_admit_noncanonical_or_defaulted_records():
    with pytest.raises(ValueError):
        normalize_delivery_limits(LEGACY, schema_version=2)
    with pytest.raises(ValueError):
        normalize_delivery_limits({**LEGACY, "encoding_parallelism": 2, "import_parallelism": 2}, schema_version=2)
    with pytest.raises(ValueError):
        normalize_delivery_limits({**LEGACY, "encoding_parallelism": 3, "import_parallelism": None}, schema_version=2)
    with pytest.raises(ValueError):
        normalize_delivery_limits(asdict(NativeChunkLimits(**LEGACY)))


@pytest.mark.parametrize("encoding,importing", [(1, 3), (3, 1)])
def test_real_spawn_and_source_free_recovery_preserve_policy(tmp_path, encoding, importing):
    import os
    from dataclasses import replace
    from threading import Barrier

    from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
    from dpone.contracts.bounded_window import WindowContractError
    from tests.test_mssql_native_chunks_execution import Target, setup

    target = Target(Barrier(importing, timeout=20) if importing > 1 else None)
    executor, plan, lease, wire = setup(tmp_path, target, encoding_parallelism=encoding, import_parallelism=importing)
    result = executor.stage(plan, iter([(7,)] * 3), wire, lease)
    assert result.rows == 3 and target.peak <= importing
    assert all(o["worker"] != os.getpid() for o in result.observations if o["phase"] == "encode")
    assert len(set(target.files)) == 1
    journal = NativeChunkJournal(executor.store, lease, plan)
    before = executor.store.load(journal.key)
    assert executor.recover(plan, lease) == result
    assert executor.store.load(journal.key) == before
    executor.limits = replace(executor.limits, encoding_parallelism=2)
    with pytest.raises(WindowContractError, match="resource_limits_changed"):
        executor.recover(plan, lease)
    assert executor.store.load(journal.key) == before


def test_legacy_journal_recovers_with_explicit_equal_overrides_without_rewrite(tmp_path):
    from dataclasses import replace

    from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
    from tests.test_mssql_native_chunks_execution import Target, setup

    executor, plan, lease, wire = setup(tmp_path, Target())
    result = executor.stage(plan, iter([(7,)]), wire, lease)
    journal = NativeChunkJournal(executor.store, lease, plan)
    before = executor.store.load(journal.key)
    executor.limits = replace(executor.limits, encoding_parallelism=2, import_parallelism=2)
    assert executor.recover(plan, lease) == result
    assert executor.store.load(journal.key) == before


@pytest.mark.parametrize("name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_both_schemas_admit_only_strict_stage_overrides(name):
    import json
    from pathlib import Path

    import jsonschema

    schema = json.loads((Path("src/dpone/schema") / name).read_text())
    definition = schema["definitions"]["native_transfer_execution"]
    validator = jsonschema.Draft7Validator({**definition, "definitions": schema["definitions"]})
    for value in (1, 64):
        execution = config(encoding_parallelism=value, import_parallelism=value).options["native_transfer"]["execution"]
        assert not list(validator.iter_errors(execution))
    for value in (None, True, "2", 0, 65):
        execution = config(encoding_parallelism=value).options["native_transfer"]["execution"]
        assert list(validator.iter_errors(execution))


@pytest.mark.parametrize(
    "override", [{}, {"encoding_parallelism": 2}, {"encoding_parallelism": 3, "import_parallelism": 1}]
)
def test_policy_round_trip_preserves_canonical_values(override):
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    raw = config(**override).options["native_transfer"]["execution"]
    policy = NativeTransferExecutionPolicy.from_mapping(raw)
    output = policy.to_dict()
    assert all(value is not None for value in output["native_chunks"].values())
    assert NativeTransferExecutionPolicy.from_mapping(output).to_dict() == output
    assert policy.native_chunks.to_dict() == native_limits(config(**override)).to_dict()
