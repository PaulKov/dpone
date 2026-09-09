"""Pure state-bootstrap authoring policy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.errors import RuntimeConfigurationError

STATELESS_LOAD_STRATEGIES = frozenset({LoadStrategy.FULL_REFRESH, LoadStrategy.REPLACE, LoadStrategy.BACKFILL})


def validate_disabled_state(load_config: LoadConfig) -> None:
    """Reject stateful strategies when state persistence is disabled."""

    if load_config.load_strategy in STATELESS_LOAD_STRATEGIES:
        return
    allowed = ", ".join(sorted(strategy.value for strategy in STATELESS_LOAD_STRATEGIES))
    raise RuntimeConfigurationError(
        "state.type='disabled' is only supported for stateless load strategies "
        f"({allowed}); got {load_config.load_strategy.value!r}"
    )


def legacy_state_connection_id(
    state_type: str,
    state_cfg: Mapping[str, Any],
    sink_cfg: Mapping[str, Any],
) -> str:
    """Resolve the legacy connection identifier without endpoint I/O."""

    connection_id = str(state_cfg.get("connection_id") or (sink_cfg or {}).get("connection_id") or "").strip()
    if connection_id:
        return connection_id
    raise RuntimeConfigurationError(f"state.type={state_type!r} requires state.connection_id or a sink connection_id")


__all__ = [
    "STATELESS_LOAD_STRATEGIES",
    "legacy_state_connection_id",
    "validate_disabled_state",
]
