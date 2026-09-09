"""Durable campaign authority for portable PostgreSQL/MSSQL chunk scopes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route
from dpone.contracts.portable_relation_scope import resolve_portable_relation_scope
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBindingError,
    PortableScopeColumnContract,
    bind_portable_scope,
)

CAMPAIGN_PORTABLE_SCOPE_SCHEMA = "dpone.backfill.portable-scope-columns.v1"


@dataclass(frozen=True, slots=True)
class CampaignPortableScopeBinding:
    """One catalog-certified column contract shared by every campaign AST."""

    columns: PortableScopeColumnContract
    identity_bound: bool

    def to_jsonable(self) -> dict[str, Any]:
        """Return the versioned durable ledger representation."""

        return {
            "schema": CAMPAIGN_PORTABLE_SCOPE_SCHEMA,
            "identity_bound": self.identity_bound,
            "source": {
                "name": self.columns.source_name,
                "type": self.columns.source_type,
                "collation": self.columns.source_collation,
            },
            "target": {
                "name": self.columns.target_name,
                "type": self.columns.target_type,
                "collation": self.columns.target_collation,
            },
        }

    def identity_contract(self) -> dict[str, Any] | None:
        """Bind new campaign hashes while preserving pre-contract ledgers."""

        if not self.identity_bound:
            return None
        contract = self.to_jsonable()
        contract.pop("identity_bound")
        return contract

    @classmethod
    def from_jsonable(cls, value: Any) -> CampaignPortableScopeBinding | None:
        """Parse a persisted contract, returning ``None`` for a legacy ledger."""

        if value is None:
            return None
        if not isinstance(value, Mapping) or value.get("schema") != CAMPAIGN_PORTABLE_SCOPE_SCHEMA:
            _invalid()
        source = value.get("source")
        target = value.get("target")
        identity_bound = value.get("identity_bound")
        if not isinstance(source, Mapping) or not isinstance(target, Mapping) or not isinstance(identity_bound, bool):
            _invalid()
        columns = PortableScopeColumnContract(
            source_name=_required_text(source.get("name")),
            source_type=_required_text(source.get("type")),
            source_collation=_optional_text(source.get("collation")),
            target_name=_required_text(target.get("name")),
            target_type=_required_text(target.get("type")),
            target_collation=_optional_text(target.get("collation")),
        )
        return cls(columns=columns, identity_bound=identity_bound)


def bind_campaign_portable_scope(load_config: Any, campaign: CampaignPortableScopeBinding | None) -> Any:
    """Derive the exact AST proof before a chunk crosses process IPC."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return load_config
    if campaign is None:
        raise PortableScopeBindingError(
            "portable_scope.binding.campaign_contract_required",
            "process-isolated chunk dispatch requires a durable campaign column contract",
        )
    binding = bind_portable_scope(scope, campaign.columns)
    options = dict(getattr(load_config, "options", {}) or {})
    options[PORTABLE_SCOPE_BINDING_OPTION] = binding
    return replace(load_config, portable_scope=scope, options=options)


def compose_campaign_binding_identity(
    contract: Mapping[str, Any] | None,
    campaign: CampaignPortableScopeBinding | None,
) -> Mapping[str, Any] | None:
    """Add an identity-bound portable contract without flattening owners."""

    binding = campaign.identity_contract() if campaign is not None else None
    if binding is None:
        return contract
    if contract is None:
        return {"portable_scope_binding": binding}
    return {
        "campaign_contract": dict(contract),
        "portable_scope_binding": binding,
    }


def resolve_campaign_portable_scope_binding(
    load_config: Any,
    *,
    column: str,
    persisted: Mapping[str, Any] | None,
    identity_bound: bool,
    resolver: Any,
) -> CampaignPortableScopeBinding | None:
    """Reuse durable authority or certify one parent-side catalog contract."""

    campaign = CampaignPortableScopeBinding.from_jsonable(persisted)
    if campaign is not None or not is_postgres_mssql_backfill_route(load_config):
        return campaign
    if not callable(resolver):
        return None
    return CampaignPortableScopeBinding(
        columns=resolver(load_config, column),
        identity_bound=identity_bound,
    )


def persist_campaign_portable_scope_binding(
    store: Any,
    ledger: Any,
    campaign: CampaignPortableScopeBinding,
) -> Any:
    """Persist a missing legacy contract under the caller's campaign fence."""

    current = store.load(ledger.run_key) or ledger
    persisted = CampaignPortableScopeBinding.from_jsonable(current.portable_scope_column_contract)
    if persisted is not None and persisted != campaign:
        raise RuntimeError("backfill.portable_scope_campaign_contract_changed")
    if persisted is None:
        current.portable_scope_column_contract = campaign.to_jsonable()
        store.save(current)
    reloaded = store.load(ledger.run_key) or current
    if CampaignPortableScopeBinding.from_jsonable(reloaded.portable_scope_column_contract) != campaign:
        raise RuntimeError("backfill.portable_scope_campaign_contract_not_durable")
    return reloaded


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        _invalid()
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


def _invalid() -> None:
    raise PortableScopeBindingError(
        "portable_scope.binding.campaign_contract_invalid",
        "persisted campaign column contract is incomplete or unsupported",
    )


__all__ = [
    "CAMPAIGN_PORTABLE_SCOPE_SCHEMA",
    "CampaignPortableScopeBinding",
    "bind_campaign_portable_scope",
    "compose_campaign_binding_identity",
    "persist_campaign_portable_scope_binding",
    "resolve_campaign_portable_scope_binding",
]
