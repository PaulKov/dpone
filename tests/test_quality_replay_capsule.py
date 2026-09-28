"""Synthetic durable evidence structure and tamper-rejection contracts."""

import json

import pytest

from dpone.contracts.quality_replay import QualityReplayCapsule, ReplayQualityEvidenceError


def core():
    return {
        "policy_snapshot_id": "a" * 64,
        "admission_digest": "b" * 64,
        "effective_plan": {},
        "run_id": "synthetic-run",
        "load_id": "synthetic-load",
        "source_probe": {"row_count": 2, "typed_hash": None},
        "target_probe": {"row_count": 2, "typed_hash": None},
        "report": {"passed": True},
        "acceptance": {},
        "binding": {"operation_id": "synthetic-operation"},
    }


def test_round_trip_completion_preserves_prepared_identity():
    prepared = QualityReplayCapsule.prepare(core())
    complete = prepared.advance("COMPLETE", authority_version=3)
    assert prepared.core_digest == complete.core_digest
    assert complete.state == "COMPLETE"
    assert QualityReplayCapsule.parse(complete.payload) == complete


@pytest.mark.parametrize("field", ["policy_snapshot_id", "binding", "source_probe"])
def test_modified_core_is_rejected(field):
    raw = json.loads(QualityReplayCapsule.prepare(core()).payload)
    raw["core"][field] = "changed"
    with pytest.raises(ReplayQualityEvidenceError):
        QualityReplayCapsule.parse(json.dumps(raw))


@pytest.mark.parametrize("raw", ['{"version":1,"version":1}', "{}", '{"x":NaN}'])
def test_untrusted_encoding_is_rejected(raw):
    with pytest.raises(ReplayQualityEvidenceError):
        QualityReplayCapsule.parse(raw)


def test_failed_evidence_cannot_be_completed():
    failed = QualityReplayCapsule.prepare(core()).advance("FAILED", authority_version=1)
    with pytest.raises(ReplayQualityEvidenceError):
        failed.advance("COMPLETE", authority_version=2)


def test_completion_chain_tamper_fails():
    raw = json.loads(QualityReplayCapsule.prepare(core()).advance("COMPLETE", authority_version=1).payload)
    raw["completion"][0]["previous_digest"] = "f" * 64
    with pytest.raises(ReplayQualityEvidenceError):
        QualityReplayCapsule.parse(json.dumps(raw))


def test_bounded_capsule_rejects_oversize_before_storage():
    value = core()
    value["report"] = {"data": "x" * (256 * 1024)}
    with pytest.raises(ReplayQualityEvidenceError):
        QualityReplayCapsule.prepare(value)
