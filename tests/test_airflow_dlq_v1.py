from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from dpone.contracts.dlq import DlqPolicy, DlqReason
from dpone.ops.data_contract_evidence import DataContractEvidenceBundleWriter
from dpone.ops.dlq import DlqService
from dpone.ops.dlq_replay import DlqReplayPolicy, DlqReplayService
from dpone.ops.dlq_retention import DlqRetentionService
from dpone.ops.dlq_store import DlqFileStore, DlqIntegrityError, DlqPathError, DlqRecordTooLarge
from dpone.ops.quarantine import QuarantineService
from dpone.readiness.schema_contracts import SchemaContract
from dpone.type_system import ContractEnforcementService


class _Resolver:
    identity = "fixture-resolver:v1"

    def __init__(self, rows: dict[str, dict[str, object]]) -> None:
        self.rows = rows

    def resolve(self, record_ref: str) -> dict[str, object]:
        return dict(self.rows[record_ref])


class _Sink:
    identity = "fixture-sink:v1"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.applied: list[tuple[dict[str, object], str]] = []

    def apply(self, row: dict[str, object], *, idempotency_key: str) -> None:
        if self.fail:
            raise RuntimeError("sink unavailable")
        self.applied.append((dict(row), idempotency_key))


def _service(tmp_path: Path, *, pii_policy: str = "reference_only") -> DlqService:
    policy = DlqPolicy.from_config({"directory": str(tmp_path / "dlq"), "pii_policy": pii_policy})
    return DlqService(
        DlqFileStore(
            policy.directory,
            max_record_bytes=policy.max_record_bytes,
            max_diagnostic_bytes=policy.max_diagnostic_bytes,
            max_index_bytes=policy.max_index_bytes,
        ),
        policy=policy,
    )


def _put(service: DlqService, *, run_id: str = "run-01", row_index: int = 0):
    return service.put(
        run_id=run_id,
        load_id="load-01",
        row={"id": row_index, "email": "pii-canary@example.com", "nested": {"token": "secret-canary"}},
        row_index=row_index,
        reason=DlqReason.schema_type_mismatch(column="email"),
        diagnostics={"expected_type": "email", "actual_type": "string", "actual_value": "pii-canary@example.com"},
    )


def test_policy_validates_safe_bounds_and_production_preserve() -> None:
    with pytest.raises(ValueError, match="retention_days"):
        DlqPolicy.from_config({"retention_days": 0})
    with pytest.raises(ValueError, match="max_diagnostic_bytes"):
        DlqPolicy.from_config({"max_record_bytes": 4096, "max_diagnostic_bytes": 8192})
    with pytest.raises(ValueError, match="preserve"):
        DlqPolicy.from_config({"pii_policy": "preserve"}, environment="prod-eu")
    with pytest.raises(ValueError, match="boolean"):
        DlqPolicy.from_config({"enabled": "false"})
    with pytest.raises(ValueError, match="directory"):
        DlqPolicy.from_config({"directory": ""})


def test_reference_only_record_and_index_never_persist_row_values(tmp_path: Path) -> None:
    service = _service(tmp_path)
    record = _put(service)

    artifact_text = "\n".join(path.read_text(encoding="utf-8") for path in (tmp_path / "dlq").rglob("*.json"))
    assert "pii-canary@example.com" not in artifact_text
    assert "secret-canary" not in artifact_text
    assert "actual_value" not in artifact_text
    assert record.payload.policy == "reference_only"
    assert record.payload.value is None
    assert record.sha256.startswith("sha256:")
    assert service.summary("run-01")["reasons"] == {"schema.type_mismatch": 1}
    compatibility_export = QuarantineService(tmp_path / "dlq").export(run_id="run-01")
    assert compatibility_export.entries[0].row == {}
    assert compatibility_export.entries[0].reason == "schema.type_mismatch"


def test_masked_record_preserves_shape_but_not_scalars(tmp_path: Path) -> None:
    record = _put(_service(tmp_path, pii_policy="masked"))

    assert record.payload.value == {
        "email": "[REDACTED]",
        "id": "[REDACTED]",
        "nested": {"token": "[REDACTED]"},
    }


def test_file_store_rejects_checksum_drift_and_symlink_escape(tmp_path: Path) -> None:
    service = _service(tmp_path)
    record = _put(service)
    path = next((tmp_path / "dlq" / "runs" / "run-01" / "records").glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason"]["code"] = "schema.required_null"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DlqIntegrityError, match="checksum"):
        service.records("run-01")

    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "dlq" / "runs" / "escaped"
    link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(DlqPathError):
        service.store.records("escaped")
    assert record.record_id


def test_file_store_rejects_corrupt_index_and_path_traversal(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _put(service)
    service.summary("run-01")
    index = tmp_path / "dlq" / "runs" / "run-01" / "index.json"
    payload = json.loads(index.read_text(encoding="utf-8"))
    payload["records"] = []
    index.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DlqIntegrityError, match="INDEX_CHECKSUM_MISMATCH"):
        service.records("run-01")
    with pytest.raises(DlqPathError):
        service.records("../outside")


def test_replay_is_plan_first_and_acknowledges_only_after_sink_success(tmp_path: Path) -> None:
    service = _service(tmp_path)
    record = _put(service)
    service.summary("run-01")
    replay = DlqReplayService(service.store)
    plan = replay.plan(
        run_id="run-01",
        resolver_identity=_Resolver.identity,
        target_identity=_Sink.identity,
    )
    resolver = _Resolver({record.record_ref: {"id": 1, "email": "fixed@example.com"}})

    with pytest.raises(RuntimeError, match="sink unavailable"):
        replay.execute(plan, resolver=resolver, sink=_Sink(fail=True))
    assert service.store.is_acknowledged(record) is False

    sink = _Sink()
    result = replay.execute(plan, resolver=resolver, sink=sink)
    assert result.applied is True
    assert result.replayed_rows == 1
    assert service.store.is_acknowledged(record) is True
    assert sink.applied[0][1] == plan.items[0].idempotency_key

    second = replay.execute(plan, resolver=resolver, sink=sink)
    assert second.replayed_rows == 0
    assert second.already_applied_rows == 1
    assert len(sink.applied) == 1


def test_replay_plan_is_bounded_and_executor_identity_is_pinned(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = _put(service, row_index=1)
    _put(service, row_index=2)
    service.summary("run-01")
    replay = DlqReplayService(service.store)

    with pytest.raises(RuntimeError, match="PLAN_LIMIT_EXCEEDED"):
        replay.plan(
            run_id="run-01",
            resolver_identity=_Resolver.identity,
            target_identity=_Sink.identity,
            policy=DlqReplayPolicy(max_records=1, max_bytes=1_048_576),
        )

    plan = replay.plan(
        run_id="run-01",
        resolver_identity=_Resolver.identity,
        target_identity=_Sink.identity,
    )
    wrong_sink = _Sink()
    wrong_sink.identity = "different-sink:v1"
    with pytest.raises(RuntimeError, match="IDENTITY_MISMATCH"):
        replay.execute(
            plan,
            resolver=_Resolver({first.record_ref: {"id": 1}}),
            sink=wrong_sink,
        )
    with pytest.raises(RuntimeError, match="PLAN_CHECKSUM_MISMATCH"):
        replay.execute(
            replace(plan, target_identity="tampered-target:v1"),
            resolver=_Resolver({first.record_ref: {"id": 1}}),
            sink=_Sink(),
        )


def test_retention_is_plan_first_and_protects_pending_and_pinned_records(tmp_path: Path) -> None:
    service = _service(tmp_path)
    pending = _put(service, row_index=1)
    applied = _put(service, row_index=2)
    service.store.acknowledge(applied, plan_id="sha256:" + "1" * 64, idempotency_key="sha256:" + "2" * 64)
    now = datetime.now(UTC) + timedelta(days=31)

    retention = DlqRetentionService(service.store)
    plan = retention.plan(now=now, pinned_record_ids={applied.record_id})
    assert plan.delete_record_ids == ()
    assert set(plan.protected_record_ids) == {pending.record_id, applied.record_id}

    unpinned = retention.plan(now=now)
    assert unpinned.delete_record_ids == (applied.record_id,)
    result = retention.apply(unpinned)
    assert result.deleted_record_ids == (applied.record_id,)
    assert service.store.record(pending.record_id).record_id == pending.record_id


def test_retention_refuses_snapshot_drift(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = _put(service, row_index=1)
    service.store.acknowledge(first, plan_id="sha256:" + "1" * 64, idempotency_key="sha256:" + "2" * 64)
    plan = DlqRetentionService(service.store).plan(now=datetime.now(UTC) + timedelta(days=31))
    _put(service, row_index=2)

    with pytest.raises(RuntimeError, match="SNAPSHOT_DRIFT"):
        DlqRetentionService(service.store).apply(plan)


def test_retention_rechecks_active_and_pinned_protection_at_apply(tmp_path: Path) -> None:
    service = _service(tmp_path)
    record = _put(service)
    service.store.acknowledge(
        record,
        plan_id="sha256:" + "1" * 64,
        idempotency_key="sha256:" + "2" * 64,
    )
    retention = DlqRetentionService(service.store)
    plan = retention.plan(now=datetime.now(UTC) + timedelta(days=31))

    with pytest.raises(RuntimeError, match="RECORD_PROTECTED"):
        retention.apply(plan, active_record_ids={record.record_id})


def test_run_and_index_limits_fail_closed(tmp_path: Path) -> None:
    policy = DlqPolicy.from_config({"directory": str(tmp_path / "limited"), "max_records_per_run": 1})
    store = DlqFileStore(
        policy.directory,
        max_record_bytes=policy.max_record_bytes,
        max_diagnostic_bytes=policy.max_diagnostic_bytes,
        max_index_bytes=100,
    )
    service = DlqService(store, policy=policy)
    _put(service)
    with pytest.raises(ValueError, match="RUN_RECORD_LIMIT_EXCEEDED"):
        _put(service, row_index=2)
    with pytest.raises(DlqRecordTooLarge, match="INDEX_TOO_LARGE"):
        service.summary("run-01")


def test_contract_evidence_uses_safe_projection_and_explicit_outcomes(tmp_path: Path) -> None:
    service = _service(tmp_path)
    contract = SchemaContract.from_config(
        {
            "enforcement": "quarantine",
            "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False}},
        }
    )
    enforcement = ContractEnforcementService(quarantine=service).enforce(
        rows=[{"amount": "10.00", "email": "accepted-canary@example.com"}, {"amount": "bad"}],
        contract=contract,
        run_id="run-evidence",
        load_id="load-evidence",
    )

    artifact = DataContractEvidenceBundleWriter(tmp_path / "evidence").write(
        run_id="run-evidence", pipeline="orders", enforcement=enforcement
    )
    text = artifact.json_path.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert "accepted-canary@example.com" not in text
    assert '"target_rows"' not in text
    assert '"actual_value"' not in text
    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "passed_with_quarantine"
    assert payload["dlq"]["reasons"] == {"schema.type_mismatch": 1}


def test_legacy_quarantine_replay_cannot_claim_unapplied_rows(tmp_path: Path) -> None:
    service = QuarantineService(tmp_path)
    service.put(run_id="run-legacy", load_id="load-legacy", row={"id": 1}, reason="type_error")

    result = service.replay(run_id="run-legacy", yes=True)

    assert result.applied is False
    assert result.replayed_rows == 0
    assert result.error_code == "DPONE_DLQ_REPLAY_EXECUTOR_REQUIRED"


def test_public_record_and_index_schemas_accept_emitted_artifacts(tmp_path: Path) -> None:
    service = _service(tmp_path)
    record = _put(service)
    service.summary("run-01")
    repo = Path(__file__).resolve().parents[1]
    pairs = (
        (
            repo / "docs/schemas/dlq/dpone.dlq.v1.schema.json",
            next((tmp_path / "dlq" / "runs" / "run-01" / "records").glob("*.json")),
        ),
        (
            repo / "docs/schemas/dlq/dpone.dlq-index.v1.schema.json",
            tmp_path / "dlq" / "runs" / "run-01" / "index.json",
        ),
    )
    for schema_path, artifact_path in pairs:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(artifact)

    replay_schema = json.loads(
        (repo / "docs/schemas/dlq/dpone.dlq-replay-plan.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(replay_schema).validate(
        DlqReplayService(service.store)
        .plan(
            run_id="run-01",
            resolver_identity=_Resolver.identity,
            target_identity=_Sink.identity,
        )
        .to_dict()
    )
    service.store.acknowledge(
        record,
        plan_id="sha256:" + "1" * 64,
        idempotency_key="sha256:" + "2" * 64,
    )
    retention_schema = json.loads(
        (repo / "docs/schemas/dlq/dpone.dlq-retention-plan.v1.schema.json").read_text(encoding="utf-8")
    )
    retention_plan = DlqRetentionService(service.store).plan(now=datetime.now(UTC) + timedelta(days=31))
    Draft202012Validator(retention_schema).validate(retention_plan.to_dict())
