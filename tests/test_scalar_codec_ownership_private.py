"""Scalar framing stays below aggregates; aggregate decoding preserves identity."""

import ast
import inspect
from datetime import timedelta

import pytest

from dpone.contracts import mssql_r1_v3_codec as codec
from dpone.contracts import mssql_r1_v3_errors as codec_leaf
from dpone.contracts.mssql_r1_v3_receipt import MssqlBatchEffectReceiptBodyV3
from dpone.contracts.mssql_r1_v3_receipt_projection import build_receipt_v3
from tests import test_postgres_mssql_r1_v3_contracts as fixtures


def _values():
    request, attempt = fixtures._batch_request()
    quality = fixtures.MssqlBatchQualityEvidenceV3(fixtures.D3, 2, 2, 2, 2, 2, 2, 2, 2)
    artifact = request.artifacts[0]
    body = MssqlBatchEffectReceiptBodyV3(
        fixtures.D2,
        fixtures.D2,
        artifact.canonical_bytes,
        artifact.manifest_digest,
        2,
        0,
        2,
        quality.canonical_bytes,
        quality.digest,
    )
    receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=fixtures.NOW + timedelta(minutes=2),
        admitted_authorities=fixtures._admitted(request),
    )
    return request, receipt


def test_scalar_framing_has_no_aggregate_domain_dependencies():
    tree = ast.parse(inspect.getsource(codec_leaf))
    dependencies = [
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("dpone.contracts.")
    ]
    assert dependencies == []


@pytest.mark.parametrize("index", [0, 1])
def test_subclass_decode_keeps_existing_concrete_result_identity(index):
    value = _values()[index]
    concrete = type(value)
    derived = type("DerivedAggregate", (concrete,), {})
    decoded = derived.from_canonical_bytes(value.canonical_bytes)
    assert type(decoded) is concrete
    assert decoded == value
    assert decoded.canonical_bytes == value.canonical_bytes


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("damage", ["empty", "truncated", "trailing"])
def test_aggregate_decode_keeps_strict_frame_rejection(index, damage):
    value = _values()[index]
    encoded = value.canonical_bytes
    invalid = {"empty": b"", "truncated": encoded[:-1], "trailing": encoded + b"x"}[damage]
    with pytest.raises(codec.MssqlR1V3ContractError):
        type(value).from_canonical_bytes(invalid)
