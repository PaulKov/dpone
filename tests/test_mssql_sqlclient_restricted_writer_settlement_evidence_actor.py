"""P9b remote-settlement evidence is create-only and one-shot."""

from collections.abc import Iterator
from contextlib import contextmanager
from time import monotonic

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_restricted_writer_settlement_evidence_actor import (
    RestrictedWriterSettlementEvidenceActor,
)
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterSettlementOperations,
    RestrictedWriterSettlementRecord,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    encode_request,
    remote_settlement_payload,
    validate_result,
)
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

OPERATIONS = RestrictedWriterSettlementOperations(encode_request, validate_result, remote_settlement_payload)


def test_actor_persists_exact_record_once_and_rejects_replay(tmp_path):
    attempt, operation = "a" * 64, "b" * 64

    @contextmanager
    def writer() -> Iterator[CreateOnlyEvidenceWriterV1]:
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    deadline = monotonic() + 3.0
    actor = pool.open(
        lambda d, c: RestrictedWriterSettlementEvidenceActor(writer, attempt, operation, OPERATIONS, d, c),
        deadline=deadline,
    )
    record = RestrictedWriterSettlementRecord(attempt, operation, b'{"schema":"test"}')
    try:
        receipt = actor.write(record, deadline=deadline)
        assert actor.observation.receipt == receipt
        assert (tmp_path / receipt.relative_name).read_bytes() == record.payload
        with pytest.raises(TdsJournalActorUnknown):
            actor.write(record, deadline=deadline)
    finally:
        actor.close(deadline=deadline)
        pool.close(deadline=monotonic() + 2.0)


def test_actor_rejects_foreign_subject_before_persistence(tmp_path):
    attempt, operation = "a" * 64, "b" * 64

    @contextmanager
    def writer() -> Iterator[CreateOnlyEvidenceWriterV1]:
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    deadline = monotonic() + 3.0
    actor = pool.open(
        lambda d, c: RestrictedWriterSettlementEvidenceActor(writer, attempt, operation, OPERATIONS, d, c),
        deadline=deadline,
    )
    try:
        with pytest.raises(TdsJournalActorUnknown):
            actor.write(RestrictedWriterSettlementRecord("c" * 64, operation, b"{}"), deadline=deadline)
        assert list(tmp_path.iterdir()) == []
    finally:
        actor.close(deadline=deadline)
        pool.close(deadline=monotonic() + 2.0)
