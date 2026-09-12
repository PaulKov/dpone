"""Immutable receipt decoding remains independent from request construction."""

import subprocess
import sys
from datetime import timedelta
from importlib import import_module

from dpone.contracts.mssql_r1_v3_receipt import MssqlBatchEffectReceiptBodyV3, MssqlR1EffectReceiptV3
from tests import test_postgres_mssql_r1_v3_contracts as fixtures


def test_projection_builds_and_checks_the_frozen_batch_receipt():
    projection = import_module("dpone.contracts.mssql_r1_v3_receipt_projection")
    request, attempt = fixtures._batch_request()
    quality = fixtures.MssqlBatchQualityEvidenceV3(fixtures.D3, 2, 2, 2, 2, 2, 2, 2, 2)
    body = MssqlBatchEffectReceiptBodyV3(
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
    receipt = projection.build_receipt_v3(
        attempt,
        body,
        committed_at=fixtures.NOW + timedelta(minutes=2),
        admitted_authorities=fixtures._admitted(request),
    )
    assert receipt.digest.hex() == "0a79707643bf95dbab3906555e780554060998e59135c51924cebc6ffcd84608"
    assert projection.validate_receipt_for_request_v3(request, receipt) is None
    decoded = MssqlR1EffectReceiptV3.from_canonical_bytes(receipt.canonical_bytes)
    assert type(decoded.body) is MssqlBatchEffectReceiptBodyV3
    assert decoded.canonical_bytes == receipt.canonical_bytes


def test_receipt_model_import_does_not_load_request_projection():
    code = """
import sys
from dpone.contracts import mssql_r1_v3_receipt
assert 'dpone.contracts.mssql_r1_v3_receipt_projection' not in sys.modules
assert 'dpone.contracts.mssql_r1_v3_effect' not in sys.modules
assert 'dpone.contracts.mssql_r1_v3_plan' not in sys.modules
assert 'dpone.contracts.mssql_r1_v3_transaction_authority' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
