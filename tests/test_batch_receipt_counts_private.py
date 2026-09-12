"""Receipt counters must retain the declared SQL count and quality authority."""

from dataclasses import fields, replace

import pytest

from dpone.contracts.mssql_r1_v3_codec import MssqlR1V3ContractError, canonical_bytes
from dpone.contracts.mssql_r1_v3_receipt import _BATCH_BODY_DOMAIN, MssqlBatchEffectReceiptBodyV3
from tests import test_postgres_mssql_r1_v3_contracts as fixtures


def _body():
    request, _ = fixtures._batch_request()
    quality = fixtures.MssqlBatchQualityEvidenceV3(fixtures.D3, 2, 2, 2, 2, 2, 2, 2, 2)
    return MssqlBatchEffectReceiptBodyV3(
        fixtures.D2,
        fixtures.D2,
        request.artifacts[0].canonical_bytes,
        request.artifacts[0].manifest_digest,
        2,
        0,
        2,
        quality.canonical_bytes,
        quality.digest,
    )


@pytest.mark.parametrize("field", ["target_row_count_before", "target_row_count_after"])
@pytest.mark.parametrize("invalid", [-1, 2**63, True, False, 2.0, "2", None])
def test_batch_receipt_rejects_non_count_target_cardinality(field, invalid):
    with pytest.raises(MssqlR1V3ContractError, match="non-negative SQL bigint"):
        replace(_body(), **{field: invalid})


@pytest.mark.parametrize("invalid", [0, 1, 3, 2**63 - 1])
def test_batch_receipt_after_count_must_match_its_quality_evidence(invalid):
    with pytest.raises(MssqlR1V3ContractError, match="target count differs from quality"):
        replace(_body(), target_row_count_after=invalid)


@pytest.mark.parametrize("before", [0, 1, 2**63 - 1])
def test_batch_receipt_valid_prior_counts_preserve_canonical_round_trip(before):
    body = replace(_body(), target_row_count_before=before)
    assert MssqlBatchEffectReceiptBodyV3.from_canonical_bytes(body.canonical_bytes) == body


@pytest.mark.parametrize("field", ["target_row_count_before", "target_row_count_after"])
@pytest.mark.parametrize("invalid", [-1, True, False, "2", None])
def test_batch_receipt_wire_decoder_rejects_invalid_target_counts(field, invalid):
    body = _body()
    values = tuple(invalid if item.name == field else getattr(body, item.name) for item in fields(body))
    wire = canonical_bytes(_BATCH_BODY_DOMAIN, values)
    with pytest.raises(MssqlR1V3ContractError, match="non-negative SQL bigint"):
        MssqlBatchEffectReceiptBodyV3.from_canonical_bytes(wire)


@pytest.mark.parametrize("invalid", [0, 1, 3, 2**63 - 1])
def test_batch_receipt_wire_decoder_rejects_contradictory_quality_count(invalid):
    body = _body()
    values = tuple(
        invalid if item.name == "target_row_count_after" else getattr(body, item.name) for item in fields(body)
    )
    with pytest.raises(MssqlR1V3ContractError, match="target count differs from quality"):
        MssqlBatchEffectReceiptBodyV3.from_canonical_bytes(canonical_bytes(_BATCH_BODY_DOMAIN, values))


@pytest.mark.parametrize(
    ("field", "message"),
    [("manifest_digest", "manifest proof is inconsistent"), ("quality_digest", "quality bytes/digest/kind mismatch")],
)
def test_existing_manifest_and_quality_error_precedence_is_preserved(field, message):
    body = _body()
    assert getattr(body, field) != bytes(32)
    with pytest.raises(MssqlR1V3ContractError, match=message):
        replace(body, target_row_count_before=-1, **{field: bytes(32)})
