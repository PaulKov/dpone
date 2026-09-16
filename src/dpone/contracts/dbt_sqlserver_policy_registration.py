"""Descriptive graph identity; construction alone does not register a policy."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DbtSqlserverGraphRegistration:
    """Exact graph ID/hash pair resolved by the finite graph registry.

    This is deliberately a graph identity, not a future full policy registration:
    it makes no assertion about publish roots, package/control deployment,
    qualification, or execution permission. Consumers must resolve its pair.
    """

    graph_policy_id: str
    graph_policy_sha256: str
