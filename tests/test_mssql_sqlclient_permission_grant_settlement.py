"""Settlement evidence binds one P7 hold to one local release transcript."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind as DepartureKind,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceRecord,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    PermissionGrantRemoteSettlementEvidenceKind as RK,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    PermissionGrantSettlementEvidenceKind as K,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    PermissionGrantSettlementEvidenceReceipt,
    PermissionGrantSettlementEvidenceRecord,
    decode_permission_grant_local_exit,
    encode_permission_grant_remote_settlement,
    permission_grant_settlement_subject,
    release_payloads,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireKind, encode_permission_message
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_permission_grant_parent_evidence import evidence_fixture


def fixture():
    binding, parent_subject, parent = evidence_fixture()
    held_kind = next(kind for kind in parent if kind.value == "held_ready")
    held_receipt = PermissionGrantParentEvidenceRecord(
        parent_subject, held_kind, parent[held_kind], binding=binding
    ).receipt
    result_kind = next(kind for kind in parent if kind.value == "result")
    result_receipt = PermissionGrantParentEvidenceRecord(
        parent_subject, result_kind, parent[result_kind], binding=binding
    ).receipt
    subject = permission_grant_settlement_subject(held_receipt, result_receipt, binding)
    digest = parent[result_kind]
    result_sha = __import__("hashlib").sha256(digest).hexdigest()
    body = {"evidence_sha256": result_sha}
    release = encode_permission_message(binding, PermissionWireKind.RELEASE, 5, body)
    released = encode_permission_message(binding, PermissionWireKind.RELEASED, 5, body)
    exit_value = TdsChildExit(binding.startup.process, 0, True)
    records = (
        PermissionGrantSettlementEvidenceRecord(subject, K.RELEASE_INTENT, release, binding=binding),
        PermissionGrantSettlementEvidenceRecord(subject, K.RELEASED, released, binding=binding),
        PermissionGrantSettlementEvidenceRecord(
            subject,
            K.LOCAL_EXIT,
            canonical_json_bytes(
                {
                    "schema": "dpone.sqlclient.permission-grant-local-exit.v1",
                    "process": {
                        "host_sha256": exit_value.identity.host_sha256,
                        "boot_id": exit_value.identity.boot_id,
                        "pid": exit_value.identity.pid,
                        "start_ticks": exit_value.identity.start_ticks,
                    },
                    "exit_code": 0,
                    "reaped": True,
                }
            ),
            binding=binding,
        ),
    )
    return binding, records, exit_value


def test_exact_three_record_chain_roundtrips():
    binding, records, exit_value = fixture()
    assert [record.kind for record in records] == list(K)[:3]
    assert release_payloads(records[0], records[1]) == (records[0].payload, records[1].payload)
    assert decode_permission_grant_local_exit(records[2]) == exit_value
    assert all("payload" not in repr(record) for record in records)
    assert all(record.receipt.subject == records[0].subject for record in records)


def test_receipt_names_are_filesystem_safe_and_bind_the_complete_subject():
    binding, records, _ = fixture()
    receipts = tuple(record.receipt for record in records)
    assert all(len(f".{receipt.relative_name}.stage".encode()) <= 255 for receipt in receipts)

    changed_subject = replace(records[0].subject, attempt_sha256="f" * 64)
    changed = PermissionGrantSettlementEvidenceRecord(
        changed_subject,
        records[0].kind,
        records[0].payload,
        binding=binding,
    ).receipt
    assert changed.relative_name != receipts[0].relative_name
    with pytest.raises(ValueError):
        PermissionGrantSettlementEvidenceReceipt(
            changed.subject,
            changed.kind,
            receipts[0].relative_name,
            changed.payload_sha256,
            changed.byte_count,
        )


@pytest.mark.parametrize("index", range(3))
def test_binding_or_payload_tamper_is_rejected(index):
    binding, records, _ = fixture()
    record = records[index]
    with pytest.raises(ValueError):
        PermissionGrantSettlementEvidenceRecord(
            record.subject,
            record.kind,
            record.payload + b" ",
            binding=binding,
        )
    changed = replace(binding, operation_deadline_ns=binding.operation_deadline_ns + 1)
    with pytest.raises(ValueError):
        PermissionGrantSettlementEvidenceRecord(record.subject, record.kind, record.payload, binding=changed)


@pytest.mark.parametrize("change", ["ordinal", "digest"])
def test_wrong_release_ordinal_or_result_digest_is_rejected(change):
    binding, records, _ = fixture()
    record = records[0]
    body = {"evidence_sha256": "f" * 64 if change == "digest" else record.subject.result_sha256}
    ordinal = 6 if change == "ordinal" else 5
    payload = encode_permission_message(binding, PermissionWireKind.RELEASE, ordinal, body)
    with pytest.raises(ValueError):
        PermissionGrantSettlementEvidenceRecord(record.subject, record.kind, payload, binding=binding)


def remote_record():
    from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
    from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
        PermissionGrantDepartureEvidenceContext,
    )
    from dpone.contracts.mssql_tds_result import attempt_identity_digest
    from tests.test_mssql_sqlclient_permission_grant_departure import grant_evidence_payloads

    binding, records, _ = fixture()
    request, result, payloads = grant_evidence_payloads()
    context = PermissionGrantDepartureEvidenceContext(request.plan)
    receipts = tuple(
        SqlClientDepartureEvidenceRecord(
            request.plan.helper_id,
            attempt_identity_digest(request.plan.attempt),
            kind,
            payloads[kind],
            permission_grant_context=context,
        ).receipt
        for kind in DepartureKind
    )
    payload = encode_permission_grant_remote_settlement(result, receipts)
    return PermissionGrantSettlementEvidenceRecord(records[0].subject, RK.REMOTE_SETTLEMENT, payload, binding=binding)


def test_remote_settlement_binds_exact_verifier_result_and_six_receipts():
    record = remote_record()
    assert record.receipt.kind is RK.REMOTE_SETTLEMENT
