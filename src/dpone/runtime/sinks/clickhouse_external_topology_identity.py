"""Fail-closed queue aliases and direct endpoints for external members."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from dpone.ports.clickhouse_external_replication import ExternalContractError, ExternalMember

Endpoint = tuple[str, str, int]
ConnectionEndpoint = tuple[str, int]


def admit_member_endpoints(
    rows: Sequence[Sequence[Any]],
    members: Sequence[ExternalMember],
    resolve_endpoint: Callable[..., Endpoint],
) -> tuple[dict[str, str], dict[str, Endpoint]]:
    """Return injective queue aliases and direct endpoints for one inventory."""

    aliases: dict[str, str] = {}
    endpoints: dict[str, Endpoint] = {}
    endpoint_owners: dict[ConnectionEndpoint, str] = {}
    for row, member in zip(rows, members, strict=True):
        member_aliases = {str(row[0]).strip(), str(row[1]).strip()} - {""}
        if not member_aliases:
            _invalid("member has no queue identity alias")
        for alias in member_aliases:
            owner = aliases.get(alias)
            if owner is not None and owner != member.member_id:
                _invalid("queue identity alias is ambiguous")
            aliases[alias] = member.member_id
        endpoint = _endpoint(resolve_endpoint(str(row[0]), str(row[1]), int(row[2])))
        connection_endpoint = (endpoint[0] or endpoint[1], endpoint[2])
        owner = endpoint_owners.get(connection_endpoint)
        if owner is not None and owner != member.member_id:
            _invalid("resolved direct endpoint is ambiguous")
        endpoints[member.member_id] = endpoint
        endpoint_owners[connection_endpoint] = member.member_id
    return aliases, endpoints


def _endpoint(value: object) -> Endpoint:
    if not isinstance(value, tuple) or len(value) != 3:
        _invalid("resolved direct endpoint is invalid")
    host, address, port = value
    if not isinstance(port, int) or isinstance(port, bool) or port <= 0 or port > 65535:
        _invalid("resolved direct endpoint port is invalid")
    normalized = (str(host).strip(), str(address).strip(), port)
    if not normalized[0] and not normalized[1]:
        _invalid("resolved direct endpoint has no host identity")
    return normalized


def _invalid(detail: str) -> None:
    raise ExternalContractError("INVENTORY_INVALID", detail)


__all__ = ["Endpoint", "admit_member_endpoints"]
