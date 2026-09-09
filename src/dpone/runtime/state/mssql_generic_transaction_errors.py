"""Typed rejections emitted by the generic SQL Server transaction catalog.

Only the finite ``THROW`` tokens authored by dpone's operation-claim batch
and its operation trigger are normalized here.  Driver, transport, and
unrecognized vendor failures deliberately remain their original exception so
operators do not lose diagnostic or commit-outcome information.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from dpone.contracts.mssql_transaction_governance import MssqlOperationClaimRejected

OPERATION_CLAIM_REJECTION_CODES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "DPONE_LOAD_OPERATION_LEASE_EXPIRED_OWNER_REACQUIRE_REQUIRED": (
            "mssql_transaction.operation_lease_expired_owner_reacquire_required"
        ),
        "DPONE_LOAD_OPERATION_STALE_ATTEMPT_GENERATION": ("mssql_transaction.operation_stale_attempt_generation"),
        "DPONE_LOAD_OPERATION_IDENTITY_COLLISION": ("mssql_transaction.operation_identity_collision"),
        "DPONE_LOAD_OPERATION_LEASE_MODE_MISMATCH": ("mssql_transaction.operation_lease_mode_mismatch"),
        "DPONE_LOAD_OPERATION_LEASE_EXPIRED": "mssql_transaction.operation_lease_expired",
        "DPONE_LOAD_OPERATION_ALREADY_OWNED": "mssql_transaction.operation_already_owned",
        "DPONE_LOAD_OPERATION_EPOCH_INVALID": "mssql_transaction.operation_epoch_invalid",
        "DPONE_LOAD_OPERATION_IMMUTABLE": "mssql_transaction.operation_immutable",
    }
)
_OPERATION_CLAIM_TOKEN = re.compile(r"(?<![A-Z0-9_])DPONE_LOAD_OPERATION_[A-Z_]+(?![A-Z0-9_])")


def normalize_operation_claim_error(error: BaseException) -> MssqlOperationClaimRejected | None:
    """Return a typed rejection only for an exact allowlisted catalog token."""

    vendor_tokens = _OPERATION_CLAIM_TOKEN.findall(str(error))
    if len(vendor_tokens) != 1:
        return None
    vendor_token = vendor_tokens[0]
    code = OPERATION_CLAIM_REJECTION_CODES.get(vendor_token)
    if code is None:
        return None
    return MssqlOperationClaimRejected(code=code, vendor_token=vendor_token)


__all__ = [
    "MssqlOperationClaimRejected",
    "OPERATION_CLAIM_REJECTION_CODES",
    "normalize_operation_claim_error",
]
