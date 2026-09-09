"""Pure deterministic table-name policy for MSSQL shadow publication."""

from __future__ import annotations

import hashlib

_SHADOW_SUFFIX = "__dpone_initial_shadow"
_BACKUP_SUFFIX = "__dpone_initial_backup"
_CAMPAIGN_DIGEST_LENGTH = 12


def publication_table_names(
    table: str,
    *,
    run_key: str,
    artifact_scope: str,
) -> tuple[str, str]:
    """Resolve shadow and backup table names for one campaign."""

    shadow_suffix, backup_suffix = _artifact_suffixes(scope=artifact_scope, run_key=run_key)
    return (
        _derived_name(table, shadow_suffix),
        _derived_name(table, backup_suffix),
    )


def publication_contract_suffixes(artifact_scope: str) -> tuple[str, str]:
    """Describe campaign suffixes before a concrete run key is planned."""

    return _artifact_suffixes(scope=artifact_scope, run_key=None)


def _artifact_suffixes(*, scope: str, run_key: str | None) -> tuple[str, str]:
    if scope == "stable":
        return _SHADOW_SUFFIX, _BACKUP_SUFFIX
    if scope != "campaign":
        raise ValueError("mssql_backfill_publication.artifact_scope_invalid")
    token = (
        "{campaign_sha256_12}"
        if run_key is None
        else hashlib.sha256(run_key.encode("utf-8")).hexdigest()[:_CAMPAIGN_DIGEST_LENGTH]
    )
    return f"__dpone_{token}_shadow", f"__dpone_{token}_backup"


def _derived_name(table: str, suffix: str) -> str:
    candidate = f"{table}{suffix}"
    if len(candidate) <= 128:
        return candidate
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:12]
    return f"{table[: 128 - len(suffix) - len(digest) - 1]}_{digest}{suffix}"


__all__ = ["publication_contract_suffixes", "publication_table_names"]
