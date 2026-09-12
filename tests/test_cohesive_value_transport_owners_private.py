"""Stable value identity and both COPY lifecycles across defining-owner moves."""

import importlib
import inspect
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime
from typing import get_args, get_type_hints

import pytest


@pytest.mark.parametrize(
    "module_name,symbol",
    [
        ("dpone.contracts.postgres_mssql_type_derivation", "PostgresMssqlTypeDecisionAuthorityV1"),
        ("dpone.contracts.postgres_mssql_type_derivation", "derive_type_decision"),
        ("dpone.contracts.mssql_r1_v3_receipt", "MssqlR1ReceiptObservationV3"),
        ("dpone.contracts.mssql_r1_v3_receipt_projection", "receipt_body_for_observation"),
        ("dpone.runtime.connectors.postgres_copy_stream", "PostgresCopyFileExporter"),
        ("dpone.runtime.connectors.postgres_copy_stream", "_write_complete_chunk"),
    ],
)
def test_concrete_definition_belongs_to_cohesive_owner(module_name, symbol):
    module = importlib.import_module(module_name)
    assert hasattr(module, symbol), f"Missing concrete owner: {module_name}.{symbol}"
    assert getattr(module, symbol).__module__ == module_name


def test_policy_and_derivation_share_one_runtime_value_type():
    from dpone.contracts import postgres_mssql_type_authority as policy
    from dpone.contracts import postgres_mssql_type_derivation as derivation

    decision = derivation.PostgresMssqlTypeDecisionAuthorityV1
    assert policy.PostgresMssqlTypeDecisionAuthorityV1 is decision
    assert get_args(get_type_hints(policy.PostgresMssqlTypePolicyAuthorityV1)["ordered_decisions"])[0] is decision
    assert get_type_hints(derivation.derive_type_decision)["return"] is decision
    assert tuple(field.name for field in fields(decision)) == (
        "decision_id",
        "source_shape",
        "stage_shape",
        "target_shape",
        "codec",
        "normalization",
        "value_admission",
        "loss_policy",
        "equality_policy",
        "hash_policy",
    )
    parameters = getattr(decision, "__dataclass_params__", None)
    assert parameters is not None and parameters.frozen
    assert tuple(decision.__slots__) == tuple(field.name for field in fields(decision))
    assert "PostgresMssqlTypeDecisionAuthorityV1" not in policy.__all__
    assert "derive_type_decision" not in policy.__all__


def test_observation_port_and_projection_share_the_validated_value_type():
    from dpone.adapters import mssql_r1_v3_receipt as adapter
    from dpone.contracts import mssql_r1_v3_effect as effect
    from dpone.contracts import mssql_r1_v3_quality as quality
    from dpone.contracts import mssql_r1_v3_receipt as values
    from dpone.contracts import mssql_r1_v3_receipt_projection as projection
    from dpone.ports.mssql_r1_v3_effect_runtime import MssqlR1TransactionV3

    observation = values.MssqlR1ReceiptObservationV3
    observe_hints = get_type_hints(
        adapter.MssqlR1ReceiptObservationV3Port.observe,
        globalns=vars(adapter),
        localns={
            "MssqlR1TransactionV3": MssqlR1TransactionV3,
            "MssqlR1EffectAttemptEnvelopeV3": effect.MssqlR1EffectAttemptEnvelopeV3,
            "MssqlQualityEvidenceV3": quality.MssqlQualityEvidenceV3,
            "MssqlR1ReceiptObservationV3": observation,
        },
    )
    assert observe_hints["return"] is observation
    projection_hints = get_type_hints(
        projection.receipt_body_for_observation,
        globalns=vars(projection),
        localns={
            "MssqlR1EffectAttemptEnvelopeV3": effect.MssqlR1EffectAttemptEnvelopeV3,
            "MssqlQualityEvidenceV3": quality.MssqlQualityEvidenceV3,
            "MssqlR1ReceiptObservationV3": observation,
        },
    )
    assert projection_hints["observation"] is observation
    assert tuple(field.name for field in fields(observation)) == ("committed_at", "batch_target_row_count_before")
    value = observation(datetime(2026, 1, 1, tzinfo=UTC), 0)
    assert value.batch_target_row_count_before == 0
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(value, "batch_target_row_count_before", 1)
    for invalid in (-1, True):
        with pytest.raises(ValueError):
            observation(datetime(2026, 1, 1, tzinfo=UTC), invalid)
    with pytest.raises(ValueError):
        observation(datetime(2026, 1, 1), None)


def test_released_stream_api_remains_present_and_keyword_only():
    from dpone.runtime.connectors import postgres_copy_stream as module

    assert {"PostgresCopyStreamExporter", "build_copy_to_stdout_sql"} <= set(module.__all__)
    parameters = inspect.signature(module.PostgresCopyStreamExporter.export).parameters
    assert tuple(parameters) == ("self", "query_sql", "columns", "format", "chunk_size", "estimated_rows")
    assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in tuple(parameters)[2:])
    assert parameters["format"].default == "CSV"
    assert parameters["chunk_size"].default == 16 * 1024 * 1024
    assert parameters["estimated_rows"].default is None
    assert module.build_copy_to_stdout_sql("SELECT 1") == "COPY (SELECT 1) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)"


@pytest.mark.parametrize("finish", [True, False])
def test_stream_cursor_lifetime_is_lazy_and_closes_on_early_stop(finish):
    from dpone.runtime.connectors.postgres_copy_stream import PostgresCopyStreamExporter

    events = []

    class Connection:
        @contextmanager
        def cursor(self):
            events.append("cursor-open")
            try:
                yield self
            finally:
                events.append("cursor-close")

        @contextmanager
        def copy(self, query):
            assert query.startswith("COPY (")
            events.append("copy-open")
            try:
                yield self
            finally:
                events.append("copy-close")

        def read(self):
            return next(chunks, b"")

    chunks = iter((memoryview(b"first\n"), b"second\n"))
    artifact = PostgresCopyStreamExporter(Connection()).export("SELECT 1", columns=("value",))
    assert events == []
    iterator = artifact.iter_bytes()
    assert next(iterator) == b"first\n"
    assert events == ["cursor-open", "copy-open"]
    if finish:
        assert list(iterator) == [b"second\n"]
    else:
        close = getattr(iterator, "close", None)
        assert callable(close)
        close()
    assert events == ["cursor-open", "copy-open", "copy-close", "cursor-close"]


@pytest.mark.parametrize("progress", [0, -1, True, None, 4])
def test_file_write_progress_error_code_survives_owner_move(progress):
    from dpone.runtime.connectors import postgres_copy_stream as module

    class Writer:
        def write(self, payload):
            assert bytes(payload) == b"abc"
            return progress

    with pytest.raises(OSError, match=r"^postgres_copy_file\.write_progress_invalid$"):
        module._write_complete_chunk(Writer(), b"abc")


def test_connector_import_keeps_copy_transport_lazy_in_fresh_process():
    import subprocess
    import sys

    script = (
        "import importlib, sys\n"
        "transport = 'dpone.runtime.connectors.postgres_copy_stream'\n"
        "assert transport not in sys.modules\n"
        "importlib.import_module('dpone.runtime.connectors.postgres')\n"
        "assert transport not in sys.modules, 'connector eagerly imported COPY transport'\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
