from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_quality import (
    MssqlBatchQualityEvidenceV3,
    MssqlXminQualityEvidenceV3,
)

DIGEST = bytes.fromhex("ab" * 32)


def test_batch_quality_requires_complete_candidate_and_hash_parity() -> None:
    evidence = MssqlBatchQualityEvidenceV3(
        probe_contract_digest=DIGEST,
        staged_rows=10,
        typed_scan_rows=10,
        candidate_target_rows=10,
        candidate_sidecar_rows=10,
        non_null_keys=10,
        unique_keys=10,
        canonical_reencode_matches=10,
        sidecar_hash_matches=10,
    )

    assert evidence.canonical_bytes.startswith(b"dpone-r1-quality-v3\0")
    assert evidence.digest.hex() == "0a7de1e51d180bafd9637d40f0305ec69c95dd3af53cb58b4fcb83cfc366daf0"
    with pytest.raises(MssqlR1V3ContractError, match="Batch quality equations"):
        replace(evidence, unique_keys=9)


def test_xmin_quality_enforces_sets_counts_anti_joins_and_checkpoint() -> None:
    evidence = MssqlXminQualityEvidenceV3(
        probe_contract_digest=DIGEST,
        affected_key_count=10,
        expected_present_keys=8,
        observed_target_present_keys=8,
        observed_sidecar_present_keys=8,
        expected_absent_keys=2,
        observed_target_absent_keys=2,
        observed_sidecar_absent_keys=2,
        survivor_hash_matches=8,
        complete_key_count=13,
        target_row_count_after=13,
        target_row_count_before=10,
        inserted_count=5,
        deleted_count=2,
        delta_no_effect_count=1,
        unchanged_row_count=6,
        delta_keys_not_in_complete=0,
        target_keys_missing_from_complete=0,
        complete_keys_missing_from_target=0,
        checkpoint_predecessor_matches=True,
    )

    assert evidence.updated_count == 2
    assert evidence.unchanged_outside_delta_count == 5
    assert evidence.digest.hex() == "5a52e6dad37720317cf6a59e30812af3c54b6e9046a3345b09464995e1e77744"
    for field, value, message in (
        ("affected_key_count", 7, "affected-key equation"),
        ("delta_keys_not_in_complete", 1, "anti-join"),
        ("target_keys_missing_from_complete", 1, "anti-join"),
        ("complete_keys_missing_from_target", 1, "anti-join"),
        ("target_row_count_after", 12, "target cardinality"),
        ("unchanged_row_count", 5, "unchanged-row equation"),
        ("unchanged_row_count", 7, "unchanged-row equation"),
        ("checkpoint_predecessor_matches", False, "checkpoint predecessor"),
    ):
        with pytest.raises(MssqlR1V3ContractError, match=message):
            replace(evidence, **{field: value})


def test_xmin_quality_rejects_sql_bigint_overflow_and_negative_derived_sets() -> None:
    values = dict(
        probe_contract_digest=DIGEST,
        affected_key_count=0,
        expected_present_keys=0,
        observed_target_present_keys=0,
        observed_sidecar_present_keys=0,
        expected_absent_keys=0,
        observed_target_absent_keys=0,
        observed_sidecar_absent_keys=0,
        survivor_hash_matches=0,
        complete_key_count=0,
        target_row_count_after=0,
        target_row_count_before=0,
        inserted_count=0,
        deleted_count=0,
        delta_no_effect_count=0,
        unchanged_row_count=0,
        delta_keys_not_in_complete=0,
        target_keys_missing_from_complete=0,
        complete_keys_missing_from_target=0,
        checkpoint_predecessor_matches=True,
    )
    MssqlXminQualityEvidenceV3(**values)
    with pytest.raises(MssqlR1V3ContractError, match="SQL bigint"):
        MssqlXminQualityEvidenceV3(**{**values, "affected_key_count": 2**63})
    with pytest.raises(MssqlR1V3ContractError, match="present-key partition"):
        MssqlXminQualityEvidenceV3(
            **{
                **values,
                "affected_key_count": 1,
                "expected_present_keys": 1,
                "observed_target_present_keys": 1,
                "observed_sidecar_present_keys": 1,
                "survivor_hash_matches": 1,
                "inserted_count": 1,
                "delta_no_effect_count": 1,
                "complete_key_count": 1,
                "target_row_count_after": 1,
            }
        )
