"""Receipt views retain declared stage material without encoding or reordering."""

from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_r1_v3_receipt import MssqlBatchEffectReceiptBodyV3, MssqlXminEffectReceiptBodyV3
from dpone.contracts.mssql_r1_v3_receipt_projection import receipt_stage_manifests


@pytest.mark.parametrize("batch", [True, False])
def test_receipt_stage_projection_retains_exact_declared_order(batch):
    # Deliberately distinct declared bytes/digests prove that this view neither
    # re-encodes a manifest nor recomputes its digest; admission is a separate step.
    if batch:
        body = object.__new__(MssqlBatchEffectReceiptBodyV3)
        object.__setattr__(body, "manifest_bytes", b"batch-material")
        object.__setattr__(body, "manifest_digest", b"declared-batch-digest")
        expected = ((b"batch-material",), (b"declared-batch-digest",))
    else:
        body = object.__new__(MssqlXminEffectReceiptBodyV3)
        for name, value in (
            ("delta_manifest_bytes", b"delta-material"),
            ("complete_keys_manifest_bytes", b"keys-material"),
            ("delta_manifest_digest", b"declared-delta-digest"),
            ("complete_keys_manifest_digest", b"declared-keys-digest"),
        ):
            object.__setattr__(body, name, value)
        expected = ((b"delta-material", b"keys-material"), (b"declared-delta-digest", b"declared-keys-digest"))
    assert receipt_stage_manifests(SimpleNamespace(body=body)) == expected
