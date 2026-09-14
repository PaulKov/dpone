"""Genuine producer receipts exercise wrapper attempt identity and result scope."""

import pytest

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact


@pytest.fixture
def validated(tmp_path):
    path = tmp_path / "source.bcp"
    path.write_bytes(b"1\t\x1dE\n")
    schema = (("id", "int"), ("value", "nvarchar(max)"))
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {"id": {"type": "integer"}}})
    raw = FileExportArtifact(
        str(path), ["id", "value"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=1
    )
    validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapped = ContractValidatedFileArtifact(raw, contract=contract, schema=schema, run_id="test", load_id="test")
    return raw, wrapped, schema, contract


def test_success_requires_exact_non_bool_count(validated):
    _raw, wrapped, _schema, _contract = validated
    with wrapped.file_validation_attempt("a" * 32) as attempt:
        assert attempt.binding.receipt.rows_validated == 1
        with pytest.raises(ValueError):
            attempt.complete(True)
        with pytest.raises(ValueError):
            attempt.complete(2)
        attempt.complete(1)
    assert wrapped.validation_summary.accepted_rows == 1
    with pytest.raises(RuntimeError):
        attempt.verify_unchanged()


def test_exception_after_complete_resets_latest_summary(validated):
    _raw, wrapped, _schema, _contract = validated
    with pytest.raises(OSError):
        with wrapped.file_validation_attempt("b" * 32) as attempt:
            attempt.complete(1)
            raise OSError("durability failure")
    assert wrapped.validation_summary.accepted_rows == 0
    assert wrapped.validation_summary.validation_mode == "opaque_file"


def test_busy_attempt_does_not_reset_active_summary(validated):
    _raw, wrapped, _schema, _contract = validated
    with wrapped.file_validation_attempt("c" * 32) as attempt:
        attempt.complete(1)
        with pytest.raises(RuntimeError, match="attempt_in_progress"):
            with wrapped.file_validation_attempt("d" * 32):
                pytest.fail("busy entry")
        assert wrapped.validation_summary.accepted_rows == 1


def test_genuine_reissue_before_binding_allowed_after_binding_rejected(validated):
    raw, wrapped, schema, contract = validated
    fresh = validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    with wrapped.file_validation_attempt("e" * 32) as attempt:
        assert attempt.binding.receipt is fresh
        validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
        with pytest.raises(RuntimeError, match="receipt_replaced"):
            attempt.verify_unchanged()


def test_original_bytes_changed_cannot_complete(validated):
    raw, wrapped, _schema, _contract = validated
    with wrapped.file_validation_attempt("f" * 32) as attempt:
        from pathlib import Path

        Path(raw.file_path).write_bytes(b"2\t\x1dE\n")
        with pytest.raises(RuntimeError):
            attempt.complete(1)
    assert wrapped.validation_summary.accepted_rows == 0


def test_receipt_reissued_during_verification_never_completes(validated):
    from dpone.runtime.file_artifact_authority import FileVerificationBudget

    raw, wrapped, schema, contract = validated
    armed = False

    def clock():
        nonlocal armed
        if armed:
            armed = False
            validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
        return 0.0

    budget = FileVerificationBudget(clock, 10, 1000)
    with pytest.raises(RuntimeError, match="receipt_replaced"):
        with wrapped.file_validation_attempt("1" * 32, verification_budget=budget) as attempt:
            armed = True
            attempt.complete(1)
    assert wrapped.validation_summary.accepted_rows == 0


def test_complete_rejects_deadline_elapsed_during_contract_serialization(validated):
    from dpone.runtime.file_artifact_authority import FileVerificationBudget

    _raw, wrapped, _schema, contract = validated
    now = [0.0]

    class ObservedColumns(dict):
        armed = False

        def items(self):
            if self.armed:
                now[0] = 20.0
            return super().items()

    columns = ObservedColumns(contract.columns)
    # SchemaContract is frozen; supply an equal mapping through its public replacement API.
    from dataclasses import replace

    replacement = replace(contract, columns=columns)
    raw = _raw
    wrapped = ContractValidatedFileArtifact(raw, contract=replacement, schema=_schema, run_id="test", load_id="test")
    budget = FileVerificationBudget(lambda: now[0], 10, 1000)
    with pytest.raises(TimeoutError):
        with wrapped.file_validation_attempt("2" * 32, verification_budget=budget) as attempt:
            columns.armed = True
            attempt.complete(1)
    assert wrapped.validation_summary.accepted_rows == 0
