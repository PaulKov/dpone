from contextlib import contextmanager
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_restricted_writer_verify_evidence_actor import RestrictedWriterVerifyEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import RestrictedWriterVerifyEvidenceOperations
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_evidence import (
    ORDER,
    RestrictedWriterVerifyEvidenceRecord,
    evidence_payload,
)
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request


def payload(kind, operation_id):
    facts = {
        ORDER[0]: {"request_sha256": "a" * 64, "parent_sha256": "b" * 64},
        ORDER[1]: {"public_sha256": "a" * 64, "implementation_sha256": "b" * 64},
        ORDER[2]: {"process_pid": 42},
        ORDER[3]: {"credential_frame": "dpone.sqlclient.restricted-writer-verify-credentials.v1"},
        ORDER[4]: {"result_sha256": "a" * 64, "session_authority_sha256": "b" * 64},
        ORDER[5]: {"process_pid": 42, "exit_code": 0, "reaped": True},
    }
    return evidence_payload(kind, operation_id, **facts[kind])


def test_actor_persists_six_records_in_exact_order_with_ack_identity():
    writes = []

    @contextmanager
    def sink():
        yield SimpleNamespace(write=lambda name, payload: writes.append((name, payload)))

    operation_id = verify_request().operation_id
    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda deadline, clock: RestrictedWriterVerifyEvidenceActor(
            sink, operation_id, deadline, clock, RestrictedWriterVerifyEvidenceOperations()
        ),
        deadline=monotonic() + 2,
    )
    for kind in ORDER:
        record = RestrictedWriterVerifyEvidenceRecord(operation_id, kind, payload(kind, operation_id))
        receipt = actor.write(record, deadline=monotonic() + 2)
        assert actor.observation.receipts[-1] is receipt
    assert len(writes) == 6
    actor.close(deadline=monotonic() + 2)


def test_wrong_order_never_reaches_writer():
    writes = []

    @contextmanager
    def sink():
        yield SimpleNamespace(write=lambda *args: writes.append(args))

    operation_id = verify_request().operation_id
    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda deadline, clock: RestrictedWriterVerifyEvidenceActor(
            sink, operation_id, deadline, clock, RestrictedWriterVerifyEvidenceOperations()
        ),
        deadline=monotonic() + 2,
    )
    wrong = RestrictedWriterVerifyEvidenceRecord(operation_id, ORDER[1], payload(ORDER[1], operation_id))
    with pytest.raises(ValueError):
        actor.write(wrong, deadline=monotonic() + 2)
    assert writes == []
    actor.close(deadline=monotonic() + 2)
