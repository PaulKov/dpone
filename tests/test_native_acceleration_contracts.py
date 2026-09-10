from __future__ import annotations

import json
import sys
import tomllib
from importlib.metadata import version as distribution_version
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.commands.runtime_native_accel_cmd import cmd_runtime_native_accel_doctor
from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.bulk_wire import BulkWirePlanner, BulkWirePolicy
from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder
from dpone.runtime.decision_audit import (
    CompositeDecisionPublisher,
    DecisionAuditPolicy,
    RuntimeDecisionContext,
    RuntimeDecisionSummary,
)
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.native_acceleration import (
    NATIVE_ACCELERATION_BATCH_SCHEMA_VERSION,
    NATIVE_ACCELERATION_SCHEMA_VERSION,
    NativeAccelerationPolicy,
    NativeAccelerationRegistry,
)
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest


def test_bulk_wire_policy_accepts_acceleration_mode() -> None:
    policy = BulkWirePolicy.from_options(
        {
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "required"},
                }
            }
        }
    )

    assert policy.acceleration == NativeAccelerationPolicy(mode="required")
    assert policy.to_dict()["acceleration"] == {"mode": "required"}


def test_bulk_wire_plan_evidence_contains_acceleration_decision() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "auto"},
                }
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    evidence = contract.to_evidence()

    assert evidence["acceleration"]["schema_version"] == NATIVE_ACCELERATION_SCHEMA_VERSION
    assert evidence["acceleration"]["requested_mode"] == "auto"
    assert evidence["acceleration"]["selected_backend"] in {"python_reference", "native_accelerated"}
    assert "fallback_reason" in evidence["acceleration"]


def test_strategy_plan_surfaces_native_acceleration_evidence() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options={
                "columns": [{"name": "id", "type": "int"}],
                "native_transfer": {
                    "wire": {
                        "mode": "typed_binary",
                        "source_native_format": "bcp_native",
                        "binary_format": "native",
                        "acceleration": {"mode": "off"},
                    }
                },
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        )
    )

    acceleration = plan.native_ingest_settings["bulk_wire"]["acceleration"]

    assert plan.fast_path_id == "mssql_bcp_native_to_clickhouse_native"
    assert acceleration["selected_backend"] == "python_reference"
    assert acceleration["warning_codes"] == ["native_acceleration_disabled"]
    assert "Native acceleration is disabled; Python reference transcode will cap throughput." in plan.bottleneck_hints


def test_required_native_acceleration_fails_before_stream_creation(tmp_path: Path) -> None:
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "required"},
                }
            }
        },
    )

    with pytest.raises(ValueError, match="native_acceleration_required_unavailable"):
        NativeWireTranscoder(registry=NativeAccelerationRegistry(module_loader=lambda: None)).to_clickhouse_binary(
            artifact,
            [("id", "int nullable")],
        )


def test_auto_native_acceleration_falls_back_to_python_reference(tmp_path: Path) -> None:
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "auto"},
                }
            }
        },
    )

    stream = NativeWireTranscoder(registry=NativeAccelerationRegistry(module_loader=lambda: None)).to_clickhouse_binary(
        artifact,
        [("id", "int nullable")],
        clickhouse_schema=[("id", "Int32")],
    )

    assert stream.native_acceleration_evidence.selected_backend == "python_reference"
    assert stream.native_acceleration_evidence.fallback_reason == "native_acceleration_package_missing"
    assert b"".join(stream.iter_bytes())
    assert stream.native_acceleration_evidence.rows == 1


def test_auto_native_acceleration_falls_back_from_legacy_provider_without_batch_contract(tmp_path: Path) -> None:
    legacy_module = SimpleNamespace(
        __version__="0.73.0",
        capabilities=lambda: {
            "schema_version": NATIVE_ACCELERATION_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "mssql_bcp_native_to_clickhouse_native",
                    "source_format": "mssql-bcp-native",
                    "target_format": "Native",
                    "certified": True,
                    "supported_types": ["int"],
                    "native_wire_revision": 2,
                }
            ],
        },
        transcode=lambda request: [b"legacy-native-bytes"],
    )
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "auto"},
                }
            }
        },
    )

    stream = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: legacy_module)
    ).to_clickhouse_binary(
        artifact,
        [("id", "int nullable")],
        clickhouse_schema=[("id", "Int32")],
    )

    assert stream.native_acceleration_evidence.selected_backend == "python_reference"
    assert stream.native_acceleration_evidence.fallback_reason == "native_acceleration_batch_contract_missing"
    assert b"".join(stream.iter_bytes())


def test_auto_native_acceleration_fallback_is_published_to_decision_audit(tmp_path: Path) -> None:
    audit = InMemoryLoadStepAuditStorage()
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "auto"},
                }
            }
        },
    )
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
        logger=SimpleNamespace(info=lambda *_args, **_kwargs: None, warning=lambda *_args, **_kwargs: None),
        summary=RuntimeDecisionSummary(),
    )

    with RuntimeDecisionContext.activate(publisher):
        stream = NativeWireTranscoder(
            registry=NativeAccelerationRegistry(module_loader=lambda: None)
        ).to_clickhouse_binary(
            artifact,
            [("id", "int nullable")],
            clickhouse_schema=[("id", "Int32")],
        )

    assert stream.native_acceleration_evidence.selected_backend == "python_reference"
    records = [record for record in audit.records if record.step_id == "native_transfer.acceleration"]
    assert len(records) == 1
    details = records[0].details
    assert details["selected"] == "python_reference"
    assert details["fallback_reason"] == "native_acceleration_package_missing"


def test_auto_native_acceleration_uses_certified_backend_when_available(monkeypatch, tmp_path: Path) -> None:
    native_payload = next(
        ClickHouseNativeEncoder(
            [("id", "int nullable")],
            target_schema=[("id", "Nullable(Int32)")],
        ).iter_batches([{"id": 42}])
    )
    fake_module = SimpleNamespace(
        __version__="0.28.0-test",
        capabilities=lambda: {
            "schema_version": NATIVE_ACCELERATION_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "mssql_bcp_native_to_clickhouse_native",
                    "source_format": "mssql-bcp-native",
                    "target_format": "Native",
                    "certified": True,
                    "supported_platforms": ["test"],
                    "supported_types": ["int"],
                    "native_wire_revision": 2,
                }
            ],
        },
        transcode=lambda request: [b"legacy-compatible-native-bytes"],
        transcode_batches=lambda request: [
            {
                "schema_version": NATIVE_ACCELERATION_BATCH_SCHEMA_VERSION,
                "payload": native_payload,
                "rows": 1,
            }
        ],
    )
    monkeypatch.setitem(sys.modules, "dpone_native_accel", fake_module)
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "auto"},
                }
            }
        },
    )

    stream = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: fake_module)
    ).to_clickhouse_binary(
        artifact,
        [("id", "int nullable")],
        clickhouse_schema=[("id", "Int32")],
    )

    assert b"".join(stream.iter_bytes()) == native_payload
    assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"
    assert stream.native_acceleration_evidence.accelerator_version == "0.28.0-test"
    registry = NativeAccelerationRegistry(module_loader=lambda: fake_module)
    decision = registry.decide(
        policy=NativeAccelerationPolicy(mode="required"),
        source_format="mssql-bcp-native",
        target_format="Native",
        source_types=("int",),
    )
    assert list(registry.transcode(decision=decision, request={})) == [b"legacy-compatible-native-bytes"]


def test_native_acceleration_rejects_legacy_or_malformed_provider_batches(monkeypatch, tmp_path: Path) -> None:
    fake_module = SimpleNamespace(
        __version__="0.28.0-test",
        capabilities=lambda: {
            "schema_version": NATIVE_ACCELERATION_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "mssql_bcp_native_to_clickhouse_native",
                    "source_format": "mssql-bcp-native",
                    "target_format": "Native",
                    "certified": True,
                    "supported_platforms": ["test"],
                    "supported_types": ["int"],
                    "native_wire_revision": 2,
                }
            ],
        },
        transcode_batches=lambda request: [b"native-bytes"],
    )
    monkeypatch.setitem(sys.modules, "dpone_native_accel", fake_module)
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": "required"},
                }
            }
        },
    )
    stream = NativeWireTranscoder(
        registry=NativeAccelerationRegistry(module_loader=lambda: fake_module)
    ).to_clickhouse_binary(
        artifact,
        [("id", "int nullable")],
        clickhouse_schema=[("id", "Int32")],
    )

    with pytest.raises(RuntimeError, match="native_acceleration_batch_invalid"):
        b"".join(stream.iter_bytes())

    assert stream.native_wire_evidence.failure_code == "native_wire_transcode_failed"
    assert stream.sink_binary_evidence.failure_code == "sink_binary_encode_failed"
    assert stream.native_acceleration_evidence.failure_code == "native_acceleration_backend_failed"


def test_native_accel_doctor_json_reports_optional_package_status(capsys) -> None:
    code = cmd_runtime_native_accel_doctor(
        SimpleNamespace(format="json"),
        ctx=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    captured = capsys.readouterr().out

    assert code == 0
    assert '"schema_version": "dpone.native_transfer.acceleration.v1"' in captured
    assert '"selected_backend":' in captured


def test_public_schema_exposes_native_acceleration_policy() -> None:
    for path in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads(Path(path).read_text(encoding="utf-8"))
        wire = schema["definitions"]["native_transfer_wire_policy"]["properties"]

        assert wire["acceleration"]["$ref"] == "#/definitions/native_acceleration_policy"
        assert schema["definitions"]["native_acceleration_policy"]["properties"]["mode"]["enum"] == [
            "auto",
            "off",
            "required",
        ]


def test_project_metadata_exposes_acceleration_extra() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["version"] == "0.77.0"
    assert pyproject["project"]["optional-dependencies"]["accel"] == ["dpone-native-accel==0.77.0"]


def test_native_accel_provider_version_matches_distribution_metadata() -> None:
    import dpone_native_accel

    provider_pyproject = tomllib.loads(Path("packages/dpone-native-accel/pyproject.toml").read_text(encoding="utf-8"))
    expected_version = provider_pyproject["project"]["version"]

    assert dpone_native_accel.__version__ == expected_version
    assert distribution_version("dpone-native-accel") == expected_version


def _native_artifact(tmp_path: Path, *, source_options: dict) -> SimpleNamespace:
    payload = tmp_path / "one-row.bcp"
    payload.write_bytes(
        (4).to_bytes(1, byteorder="little", signed=True) + (42).to_bytes(4, byteorder="little", signed=True)
    )
    schema = [("id", "int nullable")]
    bulk_wire_contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=schema,
        source_options=source_options,
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )
    native_contract = build_mssql_bcp_native_contract(
        schema=schema,
        query="SELECT [id] FROM [dbo].[orders]",
        target_format="Native",
    )
    return SimpleNamespace(
        file_path=payload,
        columns=["id"],
        estimated_rows=1,
        cleanup=lambda: None,
        bulk_wire_contract=bulk_wire_contract,
        native_wire_contract=native_contract,
    )


@pytest.mark.parametrize("revision", [None, 1, "2", True, 3])
@pytest.mark.parametrize("mode", ["auto", "required"])
def test_old_native_provider_cannot_bypass_corrected_profile(tmp_path, revision, mode):
    backend = {
        "backend_id": "legacy",
        "source_format": "mssql-bcp-native",
        "target_format": "Native",
        "certified": True,
        "supported_types": ["int"],
    }
    if revision is not None:
        backend["native_wire_revision"] = revision

    def forbidden(request):
        pytest.fail("Incompatible provider was invoked")

    module = SimpleNamespace(capabilities=lambda: {"backends": [backend]}, transcode_batches=forbidden)
    registry = NativeAccelerationRegistry(module_loader=lambda: module)
    decision = registry.decide(
        policy=NativeAccelerationPolicy(mode=mode),
        source_format="mssql-bcp-native",
        target_format="Native",
        source_types=["int nullable"],
    )
    assert decision.fallback_reason == "native_acceleration_profile_revision_unsupported"
    artifact = _native_artifact(
        tmp_path,
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "acceleration": {"mode": mode},
                }
            }
        },
    )
    if mode == "required":
        artifact.file_path.unlink()
        with pytest.raises(ValueError, match="native_acceleration_required_unavailable"):
            NativeWireTranscoder(registry=registry).to_clickhouse_binary(artifact, [("id", "int nullable")])
    else:
        stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(artifact, [("id", "int nullable")])
        assert b"".join(stream.iter_bytes())
        assert stream.native_acceleration_evidence.selected_backend == "python_reference"
