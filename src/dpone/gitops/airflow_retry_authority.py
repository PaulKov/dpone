"""Compile immutable Airflow retry authority from canonical process routes."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.provider_execution_contract import (
    MAX_CERTIFIED_TASK_RETRIES,
    RETRY_AUTHORITY_MODE,
    RETRY_AUTHORITY_SCHEMA,
)

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.manifest.loader import ManifestLoaderRouter


def certify_airflow_retry_authority(
    process_configs: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Return fixed authority only when every process has the exact safe route."""

    processes = tuple(process_configs)
    if not processes or not all(_is_retry_safe_process(process) for process in processes):
        return None
    return {
        "schema": RETRY_AUTHORITY_SCHEMA,
        "mode": RETRY_AUTHORITY_MODE,
        "max_task_retries": MAX_CERTIFIED_TASK_RETRIES,
    }


def certify_manifest_airflow_retry_authority(
    manifest_path: Path,
) -> dict[str, Any] | None:
    """Load one validated manifest and certify all of its process routes."""

    loaded = ManifestLoaderRouter().load(manifest_path, metadata_only=True)
    return certify_airflow_retry_authority(process.raw_config for process in loaded.processes)


def _is_retry_safe_process(process: Mapping[str, Any]) -> bool:
    source = _mapping(process.get("source"))
    source_options = _mapping(source.get("options"))
    xmin_execution = _mapping(source_options.get("xmin_execution"))
    sink = _mapping(process.get("sink"))
    strategy = _mapping(sink.get("strategy"))
    backfill = _mapping(strategy.get("backfill"))
    backfill_state = _mapping(backfill.get("state"))
    state = _mapping(process.get("state"))
    unique_key = strategy.get("unique_key")
    return all(
        (
            canonical_endpoint_type(str(source.get("type") or "")) == "postgres",
            source_options.get("incremental_strategy") == "xmin",
            xmin_execution.get("mode") == "initial",
            canonical_endpoint_type(str(sink.get("type") or "")) == "mssql",
            strategy.get("mode") == "backfill",
            isinstance(unique_key, list) and bool(unique_key),
            _has_retry_safe_target_publication(strategy, backfill),
            backfill.get("retry_policy") == "non_committed",
            backfill_state.get("backend") == "audit_schema",
            backfill_state.get("require_distributed_lock") is True,
            canonical_endpoint_type(str(state.get("type") or "")) == "mssql",
            state.get("atomicity") == "target_atomic",
            state.get("provisioning") == "external",
        )
    )


def _has_retry_safe_target_publication(
    strategy: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> bool:
    """Accept only target-fenced merge or exact retained-shadow publication."""

    if backfill.get("inner_mode") == "incremental_merge":
        return True
    publication = _mapping(backfill.get("publication"))
    return all(
        (
            backfill.get("inner_mode") == "incremental_append",
            strategy.get("only_new_rows") is False,
            publication.get("mode") == "shadow_swap",
            publication.get("retain_backup") is True,
        )
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "certify_airflow_retry_authority",
    "certify_manifest_airflow_retry_authority",
]
