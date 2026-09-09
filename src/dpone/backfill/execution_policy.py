"""Pure helper policies for backfill runtime execution."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_contract import (
    BACKFILL_OPERATION_SCOPE_OPTION,
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    execution_policy_from_load_config,
    issue_backfill_runtime_authority,
    normalize_backfill_execution_policy,
    normalize_retry_policy,
    validate_backfill_advisor_options,
)
from dpone.backfill.execution_policy_models import BackfillExecutionPolicy, BackfillStatePolicy
from dpone.backfill.models import chunk_spec_from_options
from dpone.backfill.predicates import BackfillPredicateRenderer
from dpone.backfill.state import CHUNK_STATUS_FAILED, CHUNK_STATUS_SUCCESS

if TYPE_CHECKING:
    from dpone.backfill.models import BackfillChunk
    from dpone.backfill.state import BackfillLedger
    from dpone.config.load_config import LoadConfig

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


def plan_hash(
    chunks: tuple[BackfillChunk, ...],
    *,
    execution_policy: BackfillExecutionPolicy | None = None,
    campaign_contract: Mapping[str, Any] | None = None,
) -> str:
    payload: Any = [chunk.to_jsonable() for chunk in chunks]
    if execution_policy is not None:
        payload = {
            "chunks": payload,
            "execution_policy_sha256": execution_policy.digest,
        }
    if campaign_contract is not None:
        payload = {"backfill_plan": payload, "campaign_contract": dict(campaign_contract)}
    return _hash_json(payload)


def config_hash(
    *,
    dataset: str,
    inner_mode: str | None = None,
    spec: Any = None,
    execution_policy: BackfillExecutionPolicy | None = None,
    campaign_contract: Mapping[str, Any] | None = None,
) -> str:
    if execution_policy is not None:
        payload: dict[str, Any] = {
            "dataset": dataset,
            "execution_policy_sha256": execution_policy.digest,
        }
        if campaign_contract is not None:
            payload["campaign_contract"] = dict(campaign_contract)
        return _hash_json(payload)
    if inner_mode is None or spec is None:
        raise ValueError("config_hash requires execution_policy or inner_mode and spec")
    return _hash_json(
        {
            "dataset": dataset,
            "inner_mode": inner_mode,
            "chunk_config": spec.to_jsonable(),
        }
    )


def combined_predicate(user_predicate: str | None, chunk_predicate: str) -> str:
    user = str(user_predicate or "").strip()
    if not user:
        return chunk_predicate
    return f"({user}) AND ({chunk_predicate})"


def render_chunk_predicate(backfill_options: Mapping[str, Any], chunk: BackfillChunk) -> str:
    spec = chunk_spec_from_options(backfill_options)
    if spec is None:
        raise ValueError("backfill chunk predicate rendering requires the immutable chunk spec")
    dialect = str(backfill_options.get("predicate_dialect") or "generic")
    return BackfillPredicateRenderer.for_dialect(dialect).render(spec, chunk)


def lease_expires_at(load_config: LoadConfig) -> datetime:
    policy = execution_policy_from_load_config(load_config)
    return datetime.now(_UTC) + timedelta(minutes=policy.lease_ttl_minutes)


def row_count_verification(ledger: BackfillLedger) -> dict[str, Any]:
    mismatches = [
        {
            "index": record.index,
            "start": record.start,
            "end": record.end,
            "rows_extracted": record.rows_extracted,
            "rows_loaded": record.rows_loaded,
        }
        for record in ledger.chunks
        if record.status == CHUNK_STATUS_SUCCESS and record.rows_extracted != record.rows_loaded
    ]
    return {
        "check": "row_count_parity",
        "status": "passed" if not mismatches else "warning",
        "mismatched_chunks": mismatches,
    }


def select_pending_chunks(
    chunks: tuple[BackfillChunk, ...],
    *,
    ledger: BackfillLedger,
    retry_policy: str | None,
) -> list[BackfillChunk]:
    """Select executable chunks for resume/retry mode."""

    policy = normalize_retry_policy(retry_policy)
    if policy == "failed_only":
        failed = {record.index for record in ledger.chunks if record.status == CHUNK_STATUS_FAILED}
        return [chunk for chunk in chunks if chunk.index in failed]
    committed = ledger.committed_indexes()
    return [chunk for chunk in chunks if chunk.index not in committed]


def _hash_json(value: Any) -> str:
    material = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


__all__ = [
    "BACKFILL_OPERATION_SCOPE_OPTION",
    "BACKFILL_RUNTIME_AUTHORITY_OPTION",
    "BackfillExecutionPolicy",
    "BackfillStatePolicy",
    "combined_predicate",
    "config_hash",
    "execution_policy_from_load_config",
    "issue_backfill_runtime_authority",
    "lease_expires_at",
    "normalize_backfill_execution_policy",
    "normalize_retry_policy",
    "plan_hash",
    "render_chunk_predicate",
    "row_count_verification",
    "select_pending_chunks",
    "validate_backfill_advisor_options",
]
