"""Closed Batch/XMin target-quality evidence for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    QUALITY_CODEC_VERSION,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_enum,
    expect_text,
    require_count,
    require_digest,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

_QUALITY_DOMAIN = b"dpone-r1-quality-v3\0"
_BATCH_COUNT_FIELDS = (
    "staged_rows",
    "typed_scan_rows",
    "candidate_target_rows",
    "candidate_sidecar_rows",
    "non_null_keys",
    "unique_keys",
    "canonical_reencode_matches",
    "sidecar_hash_matches",
)
_XMIN_COUNT_FIELDS = (
    "affected_key_count",
    "expected_present_keys",
    "observed_target_present_keys",
    "observed_sidecar_present_keys",
    "expected_absent_keys",
    "observed_target_absent_keys",
    "observed_sidecar_absent_keys",
    "survivor_hash_matches",
    "complete_key_count",
    "target_row_count_after",
    "target_row_count_before",
    "inserted_count",
    "deleted_count",
    "delta_no_effect_count",
    "unchanged_row_count",
    "delta_keys_not_in_complete",
    "target_keys_missing_from_complete",
    "complete_keys_missing_from_target",
)


@dataclass(frozen=True, slots=True)
class MssqlBatchQualityEvidenceV3:
    probe_contract_digest: bytes
    staged_rows: int
    typed_scan_rows: int
    candidate_target_rows: int
    candidate_sidecar_rows: int
    non_null_keys: int
    unique_keys: int
    canonical_reencode_matches: int
    sidecar_hash_matches: int
    codec_version: str = QUALITY_CODEC_VERSION
    source_mode: SourceMode = SourceMode.BATCH_FULL_REFRESH

    def __post_init__(self) -> None:
        require_digest(self.probe_contract_digest, "probe_contract_digest")
        for name in _BATCH_COUNT_FIELDS:
            require_count(getattr(self, name), name)
        if self.codec_version != QUALITY_CODEC_VERSION or self.source_mode is not SourceMode.BATCH_FULL_REFRESH:
            raise MssqlR1V3ContractError("Batch quality discriminator is invalid")
        expected = self.staged_rows
        observed = (
            self.typed_scan_rows,
            self.candidate_target_rows,
            self.candidate_sidecar_rows,
            self.non_null_keys,
            self.unique_keys,
            self.canonical_reencode_matches,
            self.sidecar_hash_matches,
        )
        if any(value != expected for value in observed):
            raise MssqlR1V3ContractError("Batch quality equations are not satisfied")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _QUALITY_DOMAIN,
            (
                self.codec_version,
                self.source_mode,
                self.probe_contract_digest,
                self.staged_rows,
                self.typed_scan_rows,
                self.candidate_target_rows,
                self.candidate_sidecar_rows,
                self.non_null_keys,
                self.unique_keys,
                self.canonical_reencode_matches,
                self.sidecar_hash_matches,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlBatchQualityEvidenceV3:
        values = decode_canonical_bytes(payload, _QUALITY_DOMAIN, field_count=11)
        codec = expect_text(values[0], "codec")
        mode = expect_enum(SourceMode, values[1], "source_mode")
        if codec != QUALITY_CODEC_VERSION or mode is not SourceMode.BATCH_FULL_REFRESH:
            raise MssqlR1V3ContractError("Batch quality discriminator is invalid")
        return cls(*values[2:])  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlXminQualityEvidenceV3:
    probe_contract_digest: bytes
    affected_key_count: int
    expected_present_keys: int
    observed_target_present_keys: int
    observed_sidecar_present_keys: int
    expected_absent_keys: int
    observed_target_absent_keys: int
    observed_sidecar_absent_keys: int
    survivor_hash_matches: int
    complete_key_count: int
    target_row_count_after: int
    target_row_count_before: int
    inserted_count: int
    deleted_count: int
    delta_no_effect_count: int
    unchanged_row_count: int
    delta_keys_not_in_complete: int
    target_keys_missing_from_complete: int
    complete_keys_missing_from_target: int
    checkpoint_predecessor_matches: bool
    codec_version: str = QUALITY_CODEC_VERSION
    source_mode: SourceMode = SourceMode.XMIN_CURRENT_STATE

    def __post_init__(self) -> None:
        require_digest(self.probe_contract_digest, "probe_contract_digest")
        for name in _XMIN_COUNT_FIELDS:
            require_count(getattr(self, name), name)
        if self.codec_version != QUALITY_CODEC_VERSION or self.source_mode is not SourceMode.XMIN_CURRENT_STATE:
            raise MssqlR1V3ContractError("XMin quality discriminator is invalid")
        if not isinstance(self.checkpoint_predecessor_matches, bool) or not self.checkpoint_predecessor_matches:
            raise MssqlR1V3ContractError("XMin checkpoint predecessor does not match")
        if self.expected_present_keys != self.inserted_count + self.updated_count + self.delta_no_effect_count:
            raise MssqlR1V3ContractError("XMin present-key partition is invalid")
        if self.affected_key_count != self.expected_present_keys + self.expected_absent_keys:
            raise MssqlR1V3ContractError("XMin affected-key equation is invalid")
        if self.expected_absent_keys != self.deleted_count:
            raise MssqlR1V3ContractError("XMin deleted-key equation is invalid")
        if (
            self.observed_target_present_keys != self.expected_present_keys
            or self.observed_sidecar_present_keys != self.expected_present_keys
            or self.survivor_hash_matches != self.expected_present_keys
        ):
            raise MssqlR1V3ContractError("XMin survivor evidence is incomplete")
        if (
            self.observed_target_absent_keys != self.expected_absent_keys
            or self.observed_sidecar_absent_keys != self.expected_absent_keys
        ):
            raise MssqlR1V3ContractError("XMin deleted-key absence evidence is incomplete")
        if self.target_row_count_before - self.deleted_count + self.inserted_count != self.target_row_count_after:
            raise MssqlR1V3ContractError("XMin target cardinality equation is invalid")
        if self.target_row_count_after != self.complete_key_count:
            raise MssqlR1V3ContractError("XMin final target cardinality differs from complete keys")
        expected_unchanged = self.complete_key_count - self.expected_present_keys + self.delta_no_effect_count
        if self.unchanged_row_count != expected_unchanged:
            raise MssqlR1V3ContractError("XMin unchanged-row equation is invalid")
        if any(
            (
                self.delta_keys_not_in_complete,
                self.target_keys_missing_from_complete,
                self.complete_keys_missing_from_target,
            )
        ):
            raise MssqlR1V3ContractError("XMin anti-join evidence is non-zero")
        if self.unchanged_outside_delta_count < 0:
            raise MssqlR1V3ContractError("XMin unchanged-row partition is invalid")

    @property
    def updated_count(self) -> int:
        updated = self.expected_present_keys - self.inserted_count - self.delta_no_effect_count
        if updated < 0:
            raise MssqlR1V3ContractError("XMin present-key partition is invalid")
        return updated

    @property
    def unchanged_outside_delta_count(self) -> int:
        return self.unchanged_row_count - self.delta_no_effect_count

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _QUALITY_DOMAIN,
            (
                self.codec_version,
                self.source_mode,
                self.probe_contract_digest,
                self.affected_key_count,
                self.expected_present_keys,
                self.observed_target_present_keys,
                self.observed_sidecar_present_keys,
                self.expected_absent_keys,
                self.observed_target_absent_keys,
                self.observed_sidecar_absent_keys,
                self.survivor_hash_matches,
                self.complete_key_count,
                self.target_row_count_after,
                self.target_row_count_before,
                self.inserted_count,
                self.deleted_count,
                self.delta_no_effect_count,
                self.unchanged_row_count,
                self.delta_keys_not_in_complete,
                self.target_keys_missing_from_complete,
                self.complete_keys_missing_from_target,
                self.checkpoint_predecessor_matches,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlXminQualityEvidenceV3:
        values = decode_canonical_bytes(payload, _QUALITY_DOMAIN, field_count=22)
        codec = expect_text(values[0], "codec")
        mode = expect_enum(SourceMode, values[1], "source_mode")
        if codec != QUALITY_CODEC_VERSION or mode is not SourceMode.XMIN_CURRENT_STATE:
            raise MssqlR1V3ContractError("XMin quality discriminator is invalid")
        return cls(*values[2:])  # type: ignore[arg-type]


MssqlQualityEvidenceV3 = MssqlBatchQualityEvidenceV3 | MssqlXminQualityEvidenceV3


def decode_quality_evidence(payload: bytes) -> MssqlQualityEvidenceV3:
    try:
        return MssqlBatchQualityEvidenceV3.from_canonical_bytes(payload)
    except MssqlR1V3ContractError:
        return MssqlXminQualityEvidenceV3.from_canonical_bytes(payload)


__all__ = [
    "MssqlBatchQualityEvidenceV3",
    "MssqlQualityEvidenceV3",
    "MssqlXminQualityEvidenceV3",
    "decode_quality_evidence",
]
