"""Independent committed transfer evidence: no live route certification."""

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app.composition_transfer_observation import CompositionTransferCommittedObserver
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.test_mssql_composition_transaction_fence import binding


def test_missing_retained_payload_cannot_enable_success_capability():
    observer = CompositionTransferCommittedObserver(
        read_binding=lambda attempt: binding(),
        transaction=lambda: None,
        read_receipt=lambda connection, bound: None,
        require_target=lambda *args: None,
        read_payload=None,
    )
    assert not observer.proves_outcome
    with pytest.raises(CompositionAdmissionError, match="transfer_payload_unavailable"):
        observer(binding().attempt)


def observation_case(tmp_path, target_rows):
    from dpone.runtime.consumed_payload_evidence import canonical_native_contract_sha256
    from tests.test_composition_transfer_payload import payload, store
    from tests.test_mssql_generic_transaction_governance import _receipt

    retained = store(tmp_path)
    bound = binding()
    retained.capture(bound.attempt, payload(tmp_path))
    source = retained.read(bound.attempt)
    columns = [
        dict(
            wire_name="id", target_name="id", target_type="int", nullable=True, collation=None, generation_contract=None
        ),
        dict(
            wire_name="amount",
            target_name="amount",
            target_type="decimal(12,2)",
            nullable=True,
            collation=None,
            generation_contract=None,
        ),
    ]
    evidence = source.evidence()
    native = canonical_native_contract_sha256(
        columns, source_wire_contract_sha256s=[evidence.parts[0].wire_contract_sha256]
    )
    complete = evidence.with_native_rows(2, native_contract_sha256=native)
    receipt = _receipt(bound.operation)
    receipt = replace(
        receipt,
        mutation_plan_sha256=bound.mutation_plan_sha256,
        payload_evidence=replace(
            receipt.payload_evidence,
            manifest_sha256=bytes.fromhex(complete.manifest_sha256),
            native_contract_sha256=bytes.fromhex(native),
            declared_rows=2,
            actual_raw_rows=2,
            actual_native_rows=2,
        ),
        metrics=replace(receipt.metrics, inserted_rows=2, total_rows=2),
    )

    class Cursor:
        def execute(self, sql, *args):
            # DB-API manual mode already uses IMPLICIT_TRANSACTIONS. An explicit
            # BEGIN nests it and defeats the observer's exact transaction fence.
            assert "BEGIN TRANSACTION" not in sql.upper()
            if "CURRENT_TRANSACTION_ID()" in sql:
                self.rows = [(17, 1, 1)]
            elif "sys.columns" in sql:
                self.rows = [("id", "int", 4, 10, 0, 1, None), ("amount", "decimal", 9, 12, 2, 1, None)]
            elif sql.startswith("SELECT ["):
                self.rows = list(target_rows)
            else:
                self.rows = []

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.rows.pop(0) if self.rows else None

        def close(self):
            pass

    @contextmanager
    def transaction():
        yield SimpleNamespace(cursor=lambda: Cursor(), rollback=lambda: None, autocommit=False)

    observer = CompositionTransferCommittedObserver(
        read_binding=lambda attempt: bound,
        transaction=transaction,
        read_receipt=lambda connection, bound: receipt,
        require_target=lambda *args: None,
        read_payload=retained.read,
    )
    return observer, bound


def test_committed_originals_and_typed_rows_match_independently(tmp_path):
    from decimal import Decimal

    observer, bound = observation_case(tmp_path, [(2, None), (1, Decimal("10.25"))])
    result = observer(bound.attempt)
    assert result.state == "SUCCEEDED"
    assert result.evidence_document and b"source_content_sha256" in result.evidence_document


@pytest.mark.parametrize("rows", [[(1, "10.26"), (2, None)], [(1, "10.25"), (1, "10.25")], [(1, "10.25")]])
def test_content_mutation_duplicate_or_missing_row_cannot_succeed(tmp_path, rows):
    from decimal import Decimal

    observer, bound = observation_case(tmp_path, [(key, Decimal(value) if value else None) for key, value in rows])
    assert observer(bound.attempt).state == "COMMIT_UNKNOWN"


def test_other_receipt_payload_fails_closed(tmp_path):
    observer, bound = observation_case(tmp_path, [])
    receipt = observer._receipt(None, bound)
    observer._receipt = lambda *args: replace(
        receipt, payload_evidence=replace(receipt.payload_evidence, manifest_sha256=b"x" * 32)
    )
    with pytest.raises(CompositionAdmissionError, match="transfer_payload_receipt"):
        observer(bound.attempt)
