"""Physical row-count guard for contract-enforced ClickHouse row streams."""

from __future__ import annotations

from typing import Any

from dpone.runtime.etl.contract_artifacts import ContractEnforcedStreamingArtifact


def verify_streaming_staging_count(sink: Any, staging_config: Any, payload: Any, staged_rows: int) -> None:
    """Reject a completed validated stream when physical staging differs.

    The connector's returned insert count alone is not a publication receipt.
    Count the completed staging table before allowing finalization.
    """

    if not isinstance(getattr(payload, "artifact", None), ContractEnforcedStreamingArtifact):
        return
    physical_rows = sink._count(staging_config)
    if isinstance(physical_rows, bool) or not isinstance(physical_rows, int) or physical_rows != staged_rows:
        raise RuntimeError("clickhouse_streaming_staging_count_mismatch")
