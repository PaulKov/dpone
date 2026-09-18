"""Deterministic identities for external ClickHouse publication."""

from __future__ import annotations

from dpone.contracts.clickhouse_cluster_publication import digest_payload


class ExternalContractError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{code}:{detail}")


def require_digest(value: str, label: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise ExternalContractError("IDENTITY_INVALID", f"{label} digest must be lowercase SHA-256")


def derive_target_key(cluster: str, database: str, target: str) -> str:
    return digest_payload({"cluster": cluster, "database": database, "target": target})


def derive_operation_id(*, scheduler_invocation: str, target_key: str, normalized_plan_digest: str) -> str:
    if not scheduler_invocation:
        raise ExternalContractError("IDENTITY_INVALID", "scheduler invocation is required")
    require_digest(target_key, "target key")
    require_digest(normalized_plan_digest, "plan")
    return digest_payload(
        {
            "protocol_version": 1,
            "scheduler_invocation": scheduler_invocation,
            "target_key": target_key,
            "normalized_plan_digest": normalized_plan_digest,
        }
    )


def derive_member_id(shard_num: int, replica_num: int) -> str:
    if shard_num <= 0 or replica_num <= 0:
        raise ExternalContractError("IDENTITY_INVALID", "member coordinates must be positive")
    return digest_payload({"shard_num": shard_num, "replica_num": replica_num})


def derive_generation_id(*, operation_id: str, artifact_sha256: str, schema_digest: str, row_count: int) -> str:
    require_digest(operation_id, "operation")
    require_digest(artifact_sha256, "artifact")
    require_digest(schema_digest, "schema")
    if isinstance(row_count, bool) or row_count < 0:
        raise ExternalContractError("IDENTITY_INVALID", "generation row count must be non-negative")
    return digest_payload(
        {
            "operation_id": operation_id,
            "artifact_sha256": artifact_sha256,
            "schema_digest": schema_digest,
            "row_count": row_count,
        }
    )


__all__ = [
    "ExternalContractError",
    "derive_generation_id",
    "derive_member_id",
    "derive_operation_id",
    "derive_target_key",
    "require_digest",
]
