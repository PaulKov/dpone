"""Approved stable-contract registry for benchmark risk scoring.

High fan-in is healthy for small DTOs and explicit ports when the contract is
stable, documented, and intentionally shared. The benchmark keeps those cases
visible in raw metrics while excluding them from god-module risk penalties.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class StableContract:
    """A high fan-in module approved as an intentional shared contract."""

    module: str
    kind: str
    owner: str
    rationale: str

    def to_jsonable(self, *, fan_in: int | None = None) -> dict[str, Any]:
        """Return JSON-safe contract metadata with optional fan-in evidence."""

        payload = asdict(self)
        if fan_in is not None:
            payload["fan_in"] = fan_in
        return payload


APPROVED_STABLE_CONTRACTS: tuple[StableContract, ...] = (
    StableContract(
        module="dpone.commands.output_json",
        kind="output serialization port",
        owner="developer-experience",
        rationale="Small JSON renderer shared by CLI command handlers.",
    ),
    StableContract(
        module="dpone.commands.output_text",
        kind="output text rendering port",
        owner="developer-experience",
        rationale="Small redacted text renderer shared by CLI command handlers.",
    ),
    StableContract(
        module="dpone.runtime.sources.extract_result",
        kind="source extraction DTO",
        owner="runtime",
        rationale="Immutable source result contract passed from sources into ETL orchestration.",
    ),
    StableContract(
        module="dpone.runtime.sinks.load_payload",
        kind="sink load payload DTO",
        owner="runtime",
        rationale="Immutable payload contract passed from ETL orchestration into sinks.",
    ),
    StableContract(
        module="dpone.runtime.sinks.load_result",
        kind="sink load result DTO",
        owner="runtime",
        rationale="Immutable sink result contract returned by load strategies.",
    ),
    StableContract(
        module="dpone.config.load_strategy",
        kind="load strategy enum contract",
        owner="runtime",
        rationale="Stable enum shared by manifest compilation, sources, sinks, and orchestration.",
    ),
)

_STABLE_CONTRACTS_BY_MODULE = {contract.module: contract for contract in APPROVED_STABLE_CONTRACTS}


def stable_contract_for(module: str) -> StableContract | None:
    """Return approved stable-contract metadata for a module, when present."""

    return _STABLE_CONTRACTS_BY_MODULE.get(module)


def is_approved_stable_contract(module: str) -> bool:
    """Return whether a module is an approved high fan-in contract."""

    return stable_contract_for(module) is not None


__all__ = [
    "APPROVED_STABLE_CONTRACTS",
    "StableContract",
    "is_approved_stable_contract",
    "stable_contract_for",
]
