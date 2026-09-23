"""SqlClient importer binds P10f to durable custody and failed settlement."""

from copy import deepcopy
from dataclasses import asdict, fields, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_native_importer import (
    SqlClientFailedAttemptSettlement,
    SqlClientFailedEligibility,
    SqlClientInputCustody,
    SqlClientNativeChunkImporter,
    SqlClientNativeImportUnknown,
)
from dpone.contracts.bounded_window import WindowTransientError
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeBulkTransportPolicy, NativeChunkPlan
from dpone.contracts.mssql_sqlclient_evidence_types import evidence_name
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptDisposition,
    SqlClientFailedAttemptSettlementReceipt,
)
from dpone.contracts.mssql_sqlclient_native_chunk import bind_sqlclient_native_chunk
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SCHEMA,
    canonical_stage_id,
    plan_sha256,
    sqlclient_physical_stage,
    validate_native_chunk_receipt,
)
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from tests.test_mssql_sqlclient_terminal_projection import _project as _p10f_project
from tests.test_mssql_tds_writer_launch import setup as setup


def _plan(projection) -> NativeChunkPlan:
    return NativeChunkPlan(
        projection.attempt.run_id,
        projection.attempt.target_key,
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )


def _project(setup, monkeypatch):
    terminal, projection = _p10f_project(setup, monkeypatch)
    plan = _plan(projection)
    attempt = replace(projection.attempt, plan_sha256=plan_sha256(plan))
    attempt_sha256 = attempt_identity_digest(attempt)

    def receipt(value):
        return replace(
            value,
            attempt_sha256=attempt_sha256,
            relative_name=evidence_name(attempt_sha256, value.kind, value.payload_sha256),
        )

    rebound = bind_sqlclient_native_chunk(
        attempt=attempt,
        attempt_sha256=attempt_sha256,
        stage=projection.stage,
        object_identity=projection.object_identity,
        rows=projection.rows,
        encoded_bytes=projection.encoded_bytes,
        file_sha256=projection.file_sha256,
        typed_digest=projection.typed_digest,
        typed_sum=projection.typed_sum,
        registration_receipt=receipt(projection.registration_receipt),
        verification_receipt=receipt(projection.verification_receipt),
        lifecycle_verification_sha256=projection.lifecycle_verification_sha256,
        lifecycle_revision=projection.lifecycle_revision,
        worker_build_sha256=projection.worker_build_sha256,
        implementation_sha256=projection.implementation_sha256,
        helper_implementation_sha256=projection.helper_implementation_sha256,
        directory_key=directory_key(attempt),
        directory_coordinate=projection.directory_coordinate,
    )
    return terminal, rebound


def _file(projection) -> EncodedNativeFile:
    return EncodedNativeFile(
        Path("/sealed/input.native"),
        projection.attempt.ordinal,
        projection.rows,
        projection.encoded_bytes,
        projection.file_sha256,
        projection.typed_digest,
    )


def _lease(projection):
    return SimpleNamespace(target_id=projection.attempt.target_key, owner="owner", fence=7)


def _attempt(projection):
    return f"{projection.attempt.run_id}-{projection.attempt.ordinal}-{projection.attempt.attempt}"


def _custody(projection, **override):
    plan = _plan(projection)
    facts = dict(
        plan_sha256=plan_sha256(plan),
        target_id=plan.target_id,
        run_id=plan.run_id,
        window_fingerprint=plan.window_fingerprint,
        attempt_id=_attempt(projection),
        ordinal=projection.attempt.ordinal,
        rows=projection.rows,
        encoded_bytes=projection.encoded_bytes,
        file_sha256=projection.file_sha256,
        typed_digest=projection.typed_digest,
        durable_object_id="custody-object",
        durable_location_sha256="d" * 64,
    )
    facts.update(override)
    return SqlClientInputCustody.bind(**facts)


def _eligibility(projection, **override):
    plan = _plan(projection)
    facts = dict(
        plan_sha256=plan_sha256(plan),
        target_id=plan.target_id,
        run_id=plan.run_id,
        window_fingerprint=plan.window_fingerprint,
        attempt_id=_attempt(projection),
        lease_owner="owner",
        lease_fence=7,
        lifecycle_phase="failed",
        publication_eligible=False,
        observation_sha256="e" * 64,
    )
    facts.update(override)
    return SqlClientFailedEligibility.bind(**facts)


def _settlement():
    return SqlClientFailedAttemptSettlementReceipt.bind(
        request_sha256="a" * 64,
        retirement_receipt_sha256="b" * 64,
        operation_key="c" * 64,
        disposition=SqlClientFailedAttemptDisposition.RETRY_READY,
    )


class _FailedSettlement:
    def __init__(self, settle=None):
        self._settle = settle or (lambda *_args: _settlement())

    def settle(self, plan, attempt_id, lease):
        return self._settle(plan, attempt_id, lease)


def _importer(projection, **override):
    values = dict(
        projection_type=type(projection),
        retain_input=lambda *args: _custody(projection),
        observe_custody=lambda *args: _custody(projection),
        execute=lambda *args: projection,
        inspect_projection=lambda *args: projection,
        failed_settlement=_FailedSettlement(),
        allocated_bytes=lambda: 0,
    )
    values.update(override)
    return SqlClientNativeChunkImporter(**values)


def _tampered(value, field, replacement):
    candidate = object.__new__(type(value))
    for item in fields(value):
        object.__setattr__(candidate, item.name, getattr(value, item.name))
    object.__setattr__(candidate, field, replacement)
    return candidate


@pytest.mark.parametrize(
    "field,replacement",
    (
        ("path", "/not-a-path"),
        ("ordinal", True),
        ("ordinal", -1),
        ("rows", True),
        ("rows", -1),
        ("rows", 2**63),
        ("encoded_bytes", True),
        ("encoded_bytes", -1),
        ("encoded_bytes", 2**63),
        ("file_sha256", "A" * 64),
        ("file_sha256", "a" * 63),
        ("typed_digest", "g" * 64),
        ("typed_digest", "b" * 65),
    ),
)
def test_invalid_encoded_file_is_rejected_before_any_callback(field, replacement):
    calls = []
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    file = EncodedNativeFile(Path("/sealed/input.native"), 0, 1, 1, "a" * 64, "b" * 64)
    importer = SqlClientNativeChunkImporter(
        projection_type=object,
        retain_input=lambda *args: calls.append("retain"),
        observe_custody=lambda *args: calls.append("observe_custody"),
        execute=lambda *args: calls.append("execute"),
        inspect_projection=lambda *args: calls.append("inspect"),
        failed_settlement=_FailedSettlement(lambda *args: calls.append("settle")),
        allocated_bytes=lambda: 0,
    )
    file = _tampered(file, field, replacement)
    with pytest.raises(SqlClientNativeImportUnknown):
        importer.import_file(plan, file, "run-0-0", SimpleNamespace(target_id="target", owner="owner", fence=7))
    assert calls == []


def test_import_and_source_free_inspection_reconstruct_byte_identical_receipt(setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    calls = []
    importer = _importer(
        projection,
        retain_input=lambda *args: calls.append("retain") or _custody(projection),
        execute=lambda *args: calls.append("execute") or projection,
        observe_custody=lambda *args: calls.append("custody") or _custody(projection),
        inspect_projection=lambda *args: calls.append("projection") or projection,
        allocated_bytes=lambda: 17,
    )
    receipt = importer.import_file(_plan(projection), _file(projection), _attempt(projection), _lease(projection))
    assert importer.inspect(_plan(projection), receipt, _lease(projection)) == receipt
    assert receipt.consumed_part_evidence["input_custody"]["custody_sha256"] == _custody(projection).custody_sha256
    assert receipt.consumed_part_evidence["plan_sha256"] == plan_sha256(_plan(projection))
    evidence = validate_native_chunk_receipt(receipt, projection=projection)
    assert evidence.schema == SCHEMA
    assert evidence.typed_sum == projection.typed_sum
    assert evidence.registration_receipt == projection.registration_receipt
    assert evidence.verification_receipt == projection.verification_receipt
    assert "native_typed_sum" in receipt.consumed_part_evidence
    assert "typed_sum" not in receipt.consumed_part_evidence
    assert sqlclient_physical_stage(receipt) == projection.stage
    assert receipt.stage_id == canonical_stage_id(projection.object_identity)
    assert receipt.stage_id != projection.attempt.table
    assert calls == ["retain", "execute", "custody", "projection"]
    assert importer.allocated_bytes() == 17


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("native_typed_sum", 1),
        ("projection_sha256", "f" * 64),
        ("verification_payload_sha256", "e" * 64),
        ("lifecycle_revision", 999),
    ),
)
def test_canonical_receipt_tamper_is_rejected_by_importer(field, replacement, setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    importer = _importer(projection)
    receipt = importer.import_file(_plan(projection), _file(projection), _attempt(projection), _lease(projection))
    evidence = deepcopy(dict(receipt.consumed_part_evidence))
    evidence[field] = replacement
    changed = replace(receipt, consumed_part_evidence=evidence)

    with pytest.raises(SqlClientNativeImportUnknown):
        importer.inspect(_plan(projection), changed, _lease(projection))


@pytest.mark.parametrize("replay", ("plan", "window"))
def test_self_consistent_cross_parent_replay_is_rejected(replay, setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    plan = _plan(projection)
    importer = _importer(projection)
    receipt = importer.import_file(plan, _file(projection), _attempt(projection), _lease(projection))
    evidence = validate_native_chunk_receipt(receipt)
    custody_facts = asdict(evidence.input_custody)
    custody_facts.pop("custody_sha256")
    replacement = "f" * 64 if replay == "plan" else "other-window"
    custody_facts["plan_sha256" if replay == "plan" else "window_fingerprint"] = replacement
    custody = SqlClientInputCustody.bind(**custody_facts)
    rebound = replace(
        evidence,
        plan_sha256=replacement if replay == "plan" else evidence.plan_sha256,
        window_fingerprint=replacement if replay == "window" else evidence.window_fingerprint,
        input_custody=custody,
    )
    changed = replace(receipt, consumed_part_evidence=rebound.to_mapping())
    observed_custody = SqlClientInputCustody(**asdict(custody))
    importer = _importer(projection, observe_custody=lambda *args: observed_custody)

    with pytest.raises(SqlClientNativeImportUnknown):
        importer.inspect(plan, changed, _lease(projection))


@pytest.mark.parametrize("field,replacement", (("custody_sha256", "f" * 64), ("plan_sha256", "f" * 64)))
def test_tampered_exact_type_custody_is_unknown_before_execute(setup, monkeypatch, field, replacement):
    _, projection = _project(setup, monkeypatch)
    calls = []
    importer = _importer(
        projection,
        retain_input=lambda *args: _tampered(_custody(projection), field, replacement),
        execute=lambda *args: calls.append("execute"),
    )
    with pytest.raises(SqlClientNativeImportUnknown):
        importer.import_file(_plan(projection), _file(projection), _attempt(projection), _lease(projection))
    assert calls == []


def test_inspection_rejects_arbitrary_or_cross_plan_custody(setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    importer = _importer(projection)
    receipt = importer.import_file(_plan(projection), _file(projection), _attempt(projection), _lease(projection))
    importer = _importer(
        projection,
        observe_custody=lambda *args: _custody(projection, durable_object_id="substitute"),
    )
    with pytest.raises(SqlClientNativeImportUnknown):
        importer.inspect(_plan(projection), receipt, _lease(projection))


@pytest.mark.parametrize("failure", (WindowTransientError("retry"), KeyboardInterrupt(), SystemExit(2)))
def test_callback_transient_and_cancellation_propagate_unchanged(setup, monkeypatch, failure):
    _, projection = _project(setup, monkeypatch)

    def fail(*args):
        raise failure

    importer = _importer(projection, execute=fail)
    with pytest.raises(type(failure)) as observed:
        importer.import_file(_plan(projection), _file(projection), _attempt(projection), _lease(projection))
    assert observed.value is failure


def test_settlement_delegates_exact_coordinates_to_one_nominal_capability(setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    calls = []
    plan = _plan(projection)
    attempt_id = _attempt(projection)
    lease = _lease(projection)

    def settle(candidate_plan, candidate_attempt, candidate_lease):
        calls.append((candidate_plan, candidate_attempt, candidate_lease))
        return _settlement()

    _importer(projection, failed_settlement=_FailedSettlement(settle)).settle(plan, attempt_id, lease)
    assert calls == [(plan, attempt_id, lease)]


def test_legacy_v1_failed_records_remain_read_only_compatible(setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)
    eligibility = _eligibility(projection)
    receipt = SqlClientFailedAttemptSettlement.bind(
        eligibility_sha256=eligibility.eligibility_sha256,
        operation_key="c" * 64,
        durable_receipt_sha256="a" * 64,
        settled=True,
    )
    assert eligibility.schema.endswith("v1")
    assert receipt.eligibility_sha256 == eligibility.eligibility_sha256


def test_tampered_settlement_receipt_is_rejected(setup, monkeypatch):
    _, projection = _project(setup, monkeypatch)

    def settle(*_args):
        return _tampered(_settlement(), "operation_key", "f" * 64)

    with pytest.raises(SqlClientNativeImportUnknown):
        _importer(projection, failed_settlement=_FailedSettlement(settle)).settle(
            _plan(projection), _attempt(projection), _lease(projection)
        )
