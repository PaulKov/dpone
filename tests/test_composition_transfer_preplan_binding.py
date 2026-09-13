"""V2 pins the complete trusted preplan while preserving legacy original bytes."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_composition_transaction_fence import binding

DIGEST = "sha256:" + "a" * 64


def test_v2_preplan_original_roundtrip_and_legacy_cannot_gain_authority():
    legacy = binding()
    current = replace(legacy, preplan_document_sha256=DIGEST)
    assert strict_json_object(legacy.document)["schema"].endswith(".v1")
    assert strict_json_object(current.document)["schema"].endswith(".v2")
    assert current.digest != legacy.digest
    assert CompositionMssqlOperationBinding.from_bytes(current.document, current.attempt) == current
    assert current.require_preplan() == DIGEST
    with pytest.raises(CompositionAdmissionError, match="preplan"):
        legacy.require_preplan()


@pytest.mark.parametrize("digest", ["", "a" * 64, "sha256:" + "A" * 64, 1])
def test_v2_invalid_digest_rejected(digest):
    with pytest.raises(CompositionAdmissionError):
        replace(binding(), preplan_document_sha256=digest)


def test_v1_cannot_smuggle_a_v2_envelope_reference():
    value = binding()
    body = strict_json_object(value.document)
    body["preplan_document_sha256"] = DIGEST
    with pytest.raises(CompositionAdmissionError, match="binding_original"):
        CompositionMssqlOperationBinding.from_bytes(canonical_json_bytes(body), value.attempt)


@pytest.mark.parametrize("foreign_attempt", [False, True])
def test_registrar_pins_only_matching_trusted_envelope_before_sql(tmp_path, foreign_attempt):
    from dpone.app.composition_transfer_execution import composition_transfer_binding_registrar
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from dpone.runtime.composition_transfer_preplan_store import CompositionTransferPreplanStore
    from tests.test_composition_transfer_preplan_store import envelope, originals

    tmp_path.chmod(0o700)
    attempt, _, write, _, operation, _, _, _ = originals()
    _, document = envelope()
    reference = CompositionTransferPreplanStore(tmp_path).capture(attempt, document)
    calls = []
    selected = replace(attempt, try_number=attempt.try_number + 1) if foreign_attempt else attempt
    register = composition_transfer_binding_registrar(
        lambda *args, **kwargs: calls.append((args, kwargs)), attempt=selected, write=write
    )
    admission = MssqlTransactionAdmission(operation=operation)
    if foreign_attempt:
        with pytest.raises(CompositionAdmissionError, match="preplan_original"):
            register(admission, reference.mutation_plan_sha256, preplan_reference=reference)
        assert calls == []
    else:
        register(admission, reference.mutation_plan_sha256, preplan_reference=reference)
        assert calls == [
            (
                (attempt, operation, write, reference.mutation_plan_sha256),
                {"preplan_document_sha256": "sha256:" + reference.document_sha256.hex()},
            )
        ]


def test_shared_operation_decoder_preserves_stable_original_and_binding_bytes():
    from dpone.contracts.composition_mssql_binding import decode_stable_operation_document, stable_operation_document

    value = binding()
    original = stable_operation_document(value.operation)
    assert stable_operation_document(decode_stable_operation_document(original)) == original
    assert CompositionMssqlOperationBinding.from_bytes(value.document, value.attempt).document == value.document


@pytest.mark.parametrize("damage", ["extra", "uppercase", "lease", "invocation", "short_digest"])
def test_shared_operation_decoder_rejects_noncanonical_nested_originals(damage):
    from dpone.contracts.composition_mssql_binding import decode_stable_operation_document, stable_operation_document

    body = strict_json_object(stable_operation_document(binding().operation))
    if damage == "extra":
        body["foreign"] = True
    elif damage == "uppercase":
        body["operation_key"] = "AB" * 32
    elif damage == "lease":
        body["lease_expires_at_utc"] = None
    elif damage == "invocation":
        body["attempt"]["request"]["invocation"]["foreign"] = True
    else:
        body["scope_hash"] = "ab"
    with pytest.raises(ValueError):
        decode_stable_operation_document(canonical_json_bytes(body))
