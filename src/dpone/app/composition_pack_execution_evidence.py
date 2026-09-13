"""Retain verified terminal and recovery evidence for one pack execution.

Serialization never establishes execution authority. Successful remote results
are independently decoded before counts or original documents become evidence.
Existing imports from composition_pack_execution_dispatcher remain supported.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from dpone.app.composition_clickhouse_execution import CompositionClickHouseResult
from dpone.app.composition_execution_cells import MSSQL_CLICKHOUSE_FULL_REFRESH_V1
from dpone.app.composition_transfer_execution import CompositionTransferResult
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_remote_transfer_result import RemoteTransferResult, decode_result
from dpone.contracts.strict_json import strict_json_object
from dpone.runtime.composition_native_dbt_dispatch import EVIDENCE_DISAGREEMENT, EVIDENCE_WRITE_FAILED
from dpone.runtime.composition_verified_dispatch import CompositionDispatchRejection, CompositionRunVolume


def publish_pack_exec_evidence(run_volume: CompositionRunVolume, cell: str, result: Any) -> int:
    """Retain worker metrics only after the parent root already sealed OUTCOME."""

    if type(result) is CompositionTransferResult:
        rows = result.rows_written
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        extra: dict[str, Any] = {"rows_written": rows}
    elif type(result) is RemoteTransferResult:
        try:
            verified = decode_result(result.document, result.sha256, attempt=result.attempt)
            if cell != MSSQL_CLICKHOUSE_FULL_REFRESH_V1 or verified != result:
                raise ValueError("remote result mismatch")
            extra = {
                "publication_state": "PUBLISHED",
                "rows": verified.rows,
                "remote_result_sha256": verified.sha256,
                "remote_result_document": strict_json_object(verified.document),
            }
        except (CompositionAdmissionError, TypeError, ValueError):
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True) from None
    elif type(result) is CompositionClickHouseResult:
        if result.publication_state != "PUBLISHED":
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        extra = {"publication_state": result.publication_state, "rows": len(result.rows)}
    else:
        raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
    write_pack_execution_evidence(run_volume, cell, {"status": "passed", **extra})
    return 0


def write_pack_execution_evidence(run_volume: CompositionRunVolume, cell: str, extra: Mapping[str, Any]) -> None:
    payload = {"kind": "dpone.composition.pack-exec-evidence.v1", "cell": cell, **extra}
    try:
        document = json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2)
        run_volume.evidence_path.write_text(document, encoding="utf-8")
    except (OSError, TypeError, ValueError):
        raise CompositionDispatchRejection(EVIDENCE_WRITE_FAILED, dispatch_started=True) from None
