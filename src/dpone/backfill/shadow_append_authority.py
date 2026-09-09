"""Unforgeable in-memory authority for one campaign-owned shadow target."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.backfill.execution_policy import execution_policy_from_load_config

SHADOW_APPEND_AUTHORITY_OPTION = "__dpone_backfill_shadow_append_authority"
_ISSUER = object()


@dataclass(frozen=True, slots=True, init=False)
class ShadowAppendAuthority:
    """Proof that the orchestrator redirected a governed chunk to its shadow."""

    run_key: str
    live_database: str
    live_schema: str
    live_table: str
    shadow_table: str
    execution_policy_sha256: str

    def __init__(
        self,
        *,
        run_key: str,
        live_database: str,
        live_schema: str,
        live_table: str,
        shadow_table: str,
        execution_policy_sha256: str,
        _issuer: object,
    ) -> None:
        if _issuer is not _ISSUER:
            raise TypeError("shadow append authority can only be issued by the backfill publisher")
        for field, value in {
            "run_key": run_key,
            "live_database": live_database,
            "live_schema": live_schema,
            "live_table": live_table,
            "shadow_table": shadow_table,
            "execution_policy_sha256": execution_policy_sha256,
        }.items():
            object.__setattr__(self, field, value)

    def __deepcopy__(self, _memo: dict[int, Any]) -> ShadowAppendAuthority:
        return self


def issue_shadow_append_authority(
    load_config: Any,
    *,
    run_key: str,
    live_table: str,
    shadow_table: str,
) -> ShadowAppendAuthority:
    """Issue proof only for the normalized shadow-publication contract."""

    policy = execution_policy_from_load_config(load_config)
    if policy.publication.mode != "shadow_swap" or policy.inner_mode != "incremental_append":
        raise ValueError("shadow append authority requires shadow_swap incremental_append policy")
    live_database = str(getattr(load_config, "target_database", "") or "").strip()
    live_schema = str(getattr(load_config, "target_schema", "") or "").strip()
    if not all(str(value or "").strip() for value in (run_key, live_database, live_schema, live_table, shadow_table)):
        raise ValueError("shadow append authority coordinates must be non-empty")
    return ShadowAppendAuthority(
        run_key=run_key,
        live_database=live_database,
        live_schema=live_schema,
        live_table=live_table,
        shadow_table=shadow_table,
        execution_policy_sha256=policy.digest,
        _issuer=_ISSUER,
    )


def require_shadow_append_authority(load_config: Any) -> ShadowAppendAuthority | None:
    """Return a valid publisher-issued proof, or ``None`` for ordinary loads."""

    options = getattr(load_config, "options", {}) or {}
    authority = options.get(SHADOW_APPEND_AUTHORITY_OPTION) if isinstance(options, dict) else None
    if authority is None:
        return None
    if not isinstance(authority, ShadowAppendAuthority):
        raise ValueError("backfill shadow append requires runtime-issued authority")
    coordinates = (
        str(getattr(load_config, "target_database", "") or "").strip(),
        str(getattr(load_config, "target_schema", "") or "").strip(),
        str(getattr(load_config, "target_table", "") or "").strip(),
    )
    if coordinates[2] != authority.shadow_table:
        raise ValueError("backfill shadow append target does not match runtime-issued authority")
    if coordinates[:2] != (authority.live_database, authority.live_schema):
        raise ValueError("backfill shadow append coordinates do not match runtime-issued authority")
    return authority


__all__ = [
    "SHADOW_APPEND_AUTHORITY_OPTION",
    "ShadowAppendAuthority",
    "issue_shadow_append_authority",
    "require_shadow_append_authority",
]
