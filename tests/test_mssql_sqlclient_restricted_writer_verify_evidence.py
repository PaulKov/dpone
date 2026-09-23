from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_restricted_writer_verify_evidence import (
    ORDER,
    RestrictedWriterVerifyEvidenceKind,
    RestrictedWriterVerifyEvidenceObservation,
    RestrictedWriterVerifyEvidenceReceipt,
    RestrictedWriterVerifyEvidenceRecord,
    evidence_payload,
)
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request


def test_six_kinds_are_closed_and_receipts_are_deterministic():
    operation_id = verify_request().operation_id
    receipts = tuple(
        RestrictedWriterVerifyEvidenceRecord(
            operation_id,
            kind,
            evidence_payload(
                kind,
                operation_id,
                **{
                    ORDER[0]: {"request_sha256": "a" * 64, "parent_sha256": "b" * 64},
                    ORDER[1]: {"public_sha256": "a" * 64, "implementation_sha256": "b" * 64},
                    ORDER[2]: {"process_pid": 42},
                    ORDER[3]: {"credential_frame": "dpone.sqlclient.restricted-writer-verify-credentials.v1"},
                    ORDER[4]: {"result_sha256": "a" * 64, "session_authority_sha256": "b" * 64},
                    ORDER[5]: {"process_pid": 42, "exit_code": 0, "reaped": True},
                }[kind],
            ),
        ).receipt
        for kind in ORDER
    )
    assert RestrictedWriterVerifyEvidenceObservation(operation_id, receipts).receipts == receipts
    assert len({receipt.relative_name for receipt in receipts}) == 6
    with pytest.raises(ValueError):
        RestrictedWriterVerifyEvidenceReceipt(
            receipts[0].operation_id,
            receipts[0].kind,
            "nested/forged.json",
            receipts[0].payload_sha256,
            receipts[0].size,
        )


def test_payload_is_nonsecret_and_kind_bound():
    operation_id = verify_request().operation_id
    payload = evidence_payload(
        RestrictedWriterVerifyEvidenceKind.RESULT,
        operation_id,
        result_sha256="a" * 64,
        session_authority_sha256="b" * 64,
    )
    assert b"password" not in payload and b"username" not in payload and b"host" not in payload
    with pytest.raises(ValueError):
        RestrictedWriterVerifyEvidenceRecord(operation_id, RestrictedWriterVerifyEvidenceKind.LOCAL_EXIT, payload)
    assert sha256(payload).hexdigest()


@pytest.mark.parametrize("extra", ("host", "username", "password", "connection", "digest", "business"))
def test_each_evidence_kind_rejects_extra_or_secret_fields(extra):
    operation_id = verify_request().operation_id
    with pytest.raises(ValueError):
        evidence_payload(
            RestrictedWriterVerifyEvidenceKind.CREDENTIAL_INTENT,
            operation_id,
            credential_frame="dpone.sqlclient.restricted-writer-verify-credentials.v1",
            **{extra: "forbidden"},
        )
