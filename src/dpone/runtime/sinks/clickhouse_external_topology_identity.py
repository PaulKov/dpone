"""Fail-closed queue aliases and direct endpoints for external members."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, NoReturn

Endpoint = tuple[str, str, int]
ConnectionEndpoint = tuple[str, int]
Invalid = Callable[[str], NoReturn]


def admit_member_endpoints(
    rows: Sequence[Sequence[Any]],
    member_ids: Sequence[str],
    resolve_endpoint: Callable[..., Endpoint],
    invalid: Invalid,
) -> tuple[dict[str, str], dict[str, Endpoint]]:
    """Return injective queue aliases and direct endpoints for one inventory."""

    aliases: dict[str, str] = {}
    endpoints: dict[str, Endpoint] = {}
    endpoint_owners: dict[ConnectionEndpoint, str] = {}
    for row, member_id in zip(rows, member_ids, strict=True):
        member_aliases = {str(row[0]).strip(), str(row[1]).strip()} - {""}
        if not member_aliases:
            invalid("member has no queue identity alias")
        for alias in member_aliases:
            owner = aliases.get(alias)
            if owner is not None and owner != member_id:
                invalid("queue identity alias is ambiguous")
            aliases[alias] = member_id
        endpoint = _endpoint(resolve_endpoint(str(row[0]), str(row[1]), int(row[2])), invalid)
        connection_endpoint = (endpoint[0] or endpoint[1], endpoint[2])
        owner = endpoint_owners.get(connection_endpoint)
        if owner is not None and owner != member_id:
            invalid("resolved direct endpoint is ambiguous")
        endpoints[member_id] = endpoint
        endpoint_owners[connection_endpoint] = member_id
    return aliases, endpoints


def _endpoint(value: object, invalid: Invalid) -> Endpoint:
    if not isinstance(value, tuple) or len(value) != 3:
        invalid("resolved direct endpoint is invalid")
    host, address, port = value
    if not isinstance(port, int) or isinstance(port, bool) or port <= 0 or port > 65535:
        invalid("resolved direct endpoint port is invalid")
    normalized = (str(host).strip(), str(address).strip(), port)
    if not normalized[0] and not normalized[1]:
        invalid("resolved direct endpoint has no host identity")
    return normalized


__all__ = ["Endpoint", "admit_member_endpoints"]
