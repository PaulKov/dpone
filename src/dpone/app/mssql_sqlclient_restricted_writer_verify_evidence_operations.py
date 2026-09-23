"""Exact evidence DTO operations injected into P9 storage and orchestration."""

from typing import Any, cast

from dpone.contracts.mssql_sqlclient_restricted_writer_verify_evidence import (
    ORDER,
    RestrictedWriterVerifyEvidenceKind,
    RestrictedWriterVerifyEvidenceObservation,
    RestrictedWriterVerifyEvidenceReceipt,
    RestrictedWriterVerifyEvidenceRecord,
    evidence_payload,
)

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


class RestrictedWriterVerifyEvidenceOperations:
    """Validate exact evidence values at application composition boundaries."""

    order = ORDER
    receipt_type = RestrictedWriterVerifyEvidenceReceipt
    observation_type = RestrictedWriterVerifyEvidenceObservation
    request_type: type

    def observation(self, operation_id, receipts: tuple[object, ...]):
        return RestrictedWriterVerifyEvidenceObservation(
            operation_id, cast(tuple[RestrictedWriterVerifyEvidenceReceipt, ...], receipts)
        )

    def check_observation(self, value: object):
        if type(value) is not RestrictedWriterVerifyEvidenceObservation:
            raise ValueError(ERROR)
        return RestrictedWriterVerifyEvidenceObservation(value.operation_id, value.receipts)

    def validate_record(self, value: object, operation_id, ordinal: int):
        if type(value) is not RestrictedWriterVerifyEvidenceRecord:
            raise ValueError(ERROR)
        value.__post_init__()
        if value.operation_id != operation_id or ordinal >= len(ORDER) or value.kind is not ORDER[ordinal]:
            raise ValueError(ERROR)
        return value

    def receipt(self, record: object):
        if type(record) is not RestrictedWriterVerifyEvidenceRecord:
            raise ValueError(ERROR)
        return record.receipt

    def receipts(self, observation: object) -> tuple[object, ...]:
        if type(observation) is not RestrictedWriterVerifyEvidenceObservation:
            raise ValueError(ERROR)
        return observation.receipts

    def write_parts(self, record: object, receipt: object) -> tuple[str, bytes]:
        if (
            type(record) is not RestrictedWriterVerifyEvidenceRecord
            or type(receipt) is not RestrictedWriterVerifyEvidenceReceipt
        ):
            raise ValueError(ERROR)
        return receipt.relative_name, record.payload

    def evidence_record(self, request: object, kind: object, facts: dict[str, object]):
        if type(request) is not self.request_type or kind not in ORDER:
            raise ValueError(ERROR)
        exact_kind = cast(RestrictedWriterVerifyEvidenceKind, kind)
        exact_request = cast(Any, request)
        return RestrictedWriterVerifyEvidenceRecord(
            exact_request.operation_id,
            exact_kind,
            evidence_payload(exact_kind, exact_request.operation_id, **facts),
        )
