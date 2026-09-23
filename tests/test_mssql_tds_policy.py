"""Closed TDS policy admission without vendor imports or live source access."""

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan
from dpone.manifest.mssql_native_policy import native_limits, native_requested, native_transport_policy


def policy_mapping(**changes):
    return dict(backend="mssql_python", input="rows", max_worker_address_space_bytes=64 << 20, **changes)


def config(transport_marker=...):
    chunks = dict(max_total_encoded_bytes=1 << 30, stage_allocated_bytes_stop_threshold=1 << 30)
    if transport_marker is not ...:
        chunks["transport"] = transport_marker
    return SimpleNamespace(
        options={
            "native_transfer": {
                "execution": {
                    "chunking": {"mode": "bounded_stream"},
                    "native_chunks": chunks,
                }
            }
        }
    )


def test_policy_defaults_are_explicit_immutable_and_roundtrip():
    policy = NativeBulkTransportPolicy.from_mapping(policy_mapping())
    assert policy.to_dict() == dict(
        policy_mapping(),
        batch_rows=65536,
        startup_timeout_seconds=30,
        operation_timeout_seconds=300,
        terminate_timeout_seconds=10,
        drop_timeout_seconds=30,
    )
    assert NativeBulkTransportPolicy.from_mapping(policy.to_dict()) == policy
    with pytest.raises(FrozenInstanceError):
        policy.input = "arrow"


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [],
        False,
        {"backend": "mssql_python"},
        dict(policy_mapping(), extra=1),
        dict(policy_mapping(), backend="bcp"),
        dict(policy_mapping(), input="auto"),
    ],
)
def test_invalid_explicit_policy_fails_without_fallback(value):
    with pytest.raises(ValueError, match="mssql_native.transport"):
        native_transport_policy(config(value))
    with pytest.raises(ValueError, match="mssql_native.transport"):
        native_limits(config(value))


@pytest.mark.parametrize(
    "field,low,high",
    [
        ("max_worker_address_space_bytes", 64 << 20, 16 << 30),
        ("batch_rows", 1, 65536),
        ("startup_timeout_seconds", 1, 120),
        ("operation_timeout_seconds", 1, 3600),
        ("terminate_timeout_seconds", 1, 60),
        ("drop_timeout_seconds", 1, 300),
    ],
)
def test_integer_bounds_and_no_coercion(field, low, high):
    for value in (False, True, str(low), float(low), None, low - 1, high + 1):
        with pytest.raises(ValueError, match=f"mssql_native.transport_invalid:{field}"):
            NativeBulkTransportPolicy.from_mapping(dict(policy_mapping(), **{field: value}))
    for value in (low, high):
        mapping = (
            dict(policy_mapping(), startup_timeout_seconds=1, **{field: value})
            if field != "startup_timeout_seconds"
            else dict(policy_mapping(), **{field: value})
        )
        assert getattr(NativeBulkTransportPolicy.from_mapping(mapping), field) == value


def test_startup_must_fit_operation_deadline():
    with pytest.raises(ValueError, match="mssql_native.transport_invalid:operation_timeout_seconds"):
        NativeBulkTransportPolicy.from_mapping(policy_mapping(startup_timeout_seconds=31, operation_timeout_seconds=30))


def test_omission_preserves_limits_and_plan_shape():
    assert native_transport_policy(config()) is None
    policy = NativeBulkTransportPolicy.from_mapping(policy_mapping())
    assert native_limits(config(policy.to_dict())).to_dict() == native_limits(config()).to_dict()
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    assert list(plan.to_dict()) == [
        "run_id",
        "target_id",
        "source_query_id",
        "window_fingerprint",
        "schema_fingerprint",
        "wire_fingerprint",
    ]
    explicit = NativeChunkPlan("run", "target", "query", "window", "schema", "wire", transport=policy)
    assert explicit.to_dict() == dict(plan.to_dict(), transport=policy.to_dict())
    assert explicit != plan


def test_python_plan_rejects_unparsed_transport():
    with pytest.raises(ValueError, match="mssql_native.transport_invalid"):
        NativeChunkPlan("run", "target", "query", "window", "schema", "wire", transport=policy_mapping())


@pytest.mark.parametrize("transport", [None, {}, policy_mapping()])
def test_transport_marker_alone_cannot_silently_choose_generic_route(transport):
    partial = SimpleNamespace(options={"native_transfer": {"execution": {"native_chunks": {"transport": transport}}}})
    assert native_requested(partial)


def test_general_execution_policy_preserves_explicit_native_transport():
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    cfg = config(policy_mapping())
    execution = cfg.options["native_transfer"]["execution"]
    parsed = NativeTransferExecutionPolicy.from_mapping(execution)
    expected = NativeBulkTransportPolicy.from_mapping(policy_mapping()).to_dict()
    assert parsed.to_dict()["native_chunks"]["transport"] == expected
    assert NativeTransferExecutionPolicy.from_mapping(parsed.to_dict()).to_dict() == parsed.to_dict()


def test_general_execution_policy_omits_absent_native_transport():
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    parsed = NativeTransferExecutionPolicy.from_mapping(config().options["native_transfer"]["execution"])
    assert "transport" not in parsed.to_dict()["native_chunks"]


def test_python_execution_policy_rejects_transport_without_chunks():
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    policy = NativeBulkTransportPolicy.from_mapping(policy_mapping())
    with pytest.raises(ValueError, match="mssql_native.transport_requires_native_chunks"):
        NativeTransferExecutionPolicy(native_bulk_transport=policy)


def test_python_execution_policy_rejects_unparsed_transport():
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    with pytest.raises(ValueError, match="mssql_native.transport_invalid"):
        NativeTransferExecutionPolicy(native_chunks=native_limits(config()), native_bulk_transport=policy_mapping())


def test_execution_policy_preserves_normal_annotation_reflection():
    from typing import get_type_hints

    from dpone.contracts.mssql_native_chunks import NativeChunkLimits
    from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy

    annotations = get_type_hints(NativeTransferExecutionPolicy)
    assert annotations["native_chunks"] == NativeChunkLimits | None
    assert annotations["native_bulk_transport"] == NativeBulkTransportPolicy | None
