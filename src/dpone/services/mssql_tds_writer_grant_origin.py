"""Exact durable permission-grant prerequisite captured for writer launch."""

from dpone.services.mssql_tds_permission_grant import PermissionGrantHeldOwner
from dpone.services.mssql_tds_permission_grant_release import PermissionGrantLocallyReleased
from dpone.services.mssql_tds_permission_grant_settlement import _SettlementOwner
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantParentEvidenceKind,
    PermissionGrantParentEvidenceReceipt,
)


def capture_writer_grant_result(association: object) -> tuple[PermissionGrantParentEvidenceReceipt, str]:
    """Return the exact acknowledged GRANT result and an immutable digest copy."""
    grant_owner = object.__getattribute__(association, "_verify_owner_ref")
    if type(grant_owner) is not _SettlementOwner:
        raise ValueError("mssql_native.sqlclient_writer_admission_unknown")
    grant_local = object.__getattribute__(grant_owner, "local")
    if type(grant_local) is not PermissionGrantLocallyReleased:
        raise ValueError("mssql_native.sqlclient_writer_admission_unknown")
    grant_held = object.__getattribute__(grant_local, "held_owner")
    if type(grant_held) is not PermissionGrantHeldOwner:
        raise ValueError("mssql_native.sqlclient_writer_admission_unknown")
    receipt = object.__getattribute__(grant_held, "result_receipt")
    if (
        object.__getattribute__(association, "_remote_settlement_owner") is not grant_owner
        or object.__getattribute__(grant_owner, "local") is not grant_local
        or object.__getattribute__(grant_local, "held_owner") is not grant_held
        or object.__getattribute__(grant_held, "association") is not association
        or type(receipt) is not PermissionGrantParentEvidenceReceipt
        or object.__getattribute__(grant_held, "result_receipt")
        is not object.__getattribute__(grant_held, "_result_receipt_ref")
        or receipt.kind is not PermissionGrantParentEvidenceKind.RESULT
    ):
        raise ValueError("mssql_native.sqlclient_writer_admission_unknown")
    PermissionGrantParentEvidenceReceipt.__post_init__(receipt)
    return receipt, receipt.payload_sha256


__all__ = ("capture_writer_grant_result",)
