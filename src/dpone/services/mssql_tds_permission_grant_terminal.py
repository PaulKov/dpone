"""Credential-free terminal value for a settled SQL permission grant."""

from dataclasses import dataclass

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    PermissionGrantRemoteSettlementEvidenceKind,
    PermissionGrantSettlementEvidenceKind,
    PermissionGrantSettlementEvidenceReceipt,
    TdsDirectorySnapshot,
)
from dpone.contracts.mssql_tds_validation import _hash

ERROR = "mssql_native.sqlclient_permission_remote_settlement_unknown"


@dataclass(frozen=True, slots=True)
class PermissionGrantSettled:
    """Credential-free terminal GRANT settlement; never writer authority."""

    directory: TdsDirectorySnapshot
    local_evidence: tuple[PermissionGrantSettlementEvidenceReceipt, ...]
    verifier_receipts: tuple
    verifier_authority_sha256: str
    schema: str = "dpone.sqlclient.permission-grant-settled.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.directory) is not TdsDirectorySnapshot:
                raise ValueError
            self.directory.__post_init__()
            local_kinds = (
                PermissionGrantSettlementEvidenceKind.RELEASE_INTENT,
                PermissionGrantSettlementEvidenceKind.RELEASED,
                PermissionGrantSettlementEvidenceKind.LOCAL_EXIT,
                PermissionGrantRemoteSettlementEvidenceKind.REMOTE_SETTLEMENT,
            )
            if type(self.local_evidence) is not tuple or len(self.local_evidence) != 4:
                raise ValueError
            for receipt, kind in zip(self.local_evidence, local_kinds, strict=True):
                if type(receipt) is not PermissionGrantSettlementEvidenceReceipt or receipt.kind is not kind:
                    raise ValueError
                receipt.__post_init__()
            if len({receipt.subject for receipt in self.local_evidence}) != 1:
                raise ValueError
            if type(self.verifier_receipts) is not tuple or len(self.verifier_receipts) != 6:
                raise ValueError
            for receipt, kind in zip(self.verifier_receipts, SqlClientDepartureEvidenceKind, strict=True):
                if type(receipt) is not SqlClientDepartureEvidenceReceipt or receipt.kind is not kind:
                    raise ValueError
                receipt.__post_init__()
            if len({(receipt.helper_id, receipt.attempt_sha256) for receipt in self.verifier_receipts}) != 1:
                raise ValueError
            _hash(self.verifier_authority_sha256)
            if (
                self.verifier_authority_sha256 != self.verifier_authority_sha256.lower()
                or self.schema != "dpone.sqlclient.permission-grant-settled.v1"
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError(ERROR)
