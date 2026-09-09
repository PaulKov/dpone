"""Load-identity composition for the default runtime hydrator."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def build_load_identity_service(*, audit_storage: Any, etl_logger: Any) -> Any:
    """Build the one load identity authority shared by the whole processor."""

    def warn_receipt_backed_commit_audit_failure(_record: Any) -> None:
        etl_logger.warning(
            "event=dpone.load_audit_commit_persistence_failed "
            "outcome=committed receipt_backed=true repair_source=commit_receipt"
        )

    service_type = import_module("dpone.runtime.lineage.audit").LoadIdentityService
    return service_type(
        audit_storage=audit_storage,
        on_receipt_backed_commit_audit_failure=warn_receipt_backed_commit_audit_failure,
    )
