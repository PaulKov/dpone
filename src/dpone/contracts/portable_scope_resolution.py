"""Runtime resolution of one AST with its pre-admitted catalog proof."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.contracts.portable_relation_scope import (
    PortableRelationScope,
    resolve_portable_relation_scope,
)
from dpone.contracts.portable_scope_binding import (
    PortableScopeBinding,
    require_portable_scope_binding,
)


@dataclass(frozen=True, slots=True)
class ResolvedPortableScope:
    """Immutable runtime pair of typed AST and its exact catalog binding."""

    scope: PortableRelationScope
    binding: PortableScopeBinding

    def __post_init__(self) -> None:
        self.binding.require_scope(self.scope)


def resolve_bound_portable_scope(load_config: Any) -> ResolvedPortableScope | None:
    """Resolve an optional scope and require its pre-admitted binding once."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return None
    return ResolvedPortableScope(
        scope=scope,
        binding=require_portable_scope_binding(load_config, scope),
    )


__all__ = ["ResolvedPortableScope", "resolve_bound_portable_scope"]
