"""Pure, strategy-aware SQL Server target behaviour policy.

The policy consumes only an immutable catalog snapshot and the already
normalized load-strategy contract.  It therefore runs before hooks, source
COPY/export, staging, and target mutation without issuing connector calls.
"""

from __future__ import annotations

from dpone.config.mssql_strategy_contract_models import MSSQLLoadStrategyContract
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlForeignKeyState,
    MssqlSchemaCatalogSnapshot,
)

_CASCADE_ACTIONS = frozenset({"CASCADE", "SET_NULL", "SET_DEFAULT"})
_DML_EVENTS = frozenset({"INSERT", "UPDATE", "DELETE"})


class MssqlTargetBehaviorError(RuntimeError):
    """Stable fail-closed diagnostic for an unsupported target behaviour."""


def mssql_target_behavior_blockers(
    snapshot: MssqlSchemaCatalogSnapshot,
    *,
    strategy: MSSQLLoadStrategyContract | None,
) -> tuple[str, ...]:
    """Classify catalog-only target behaviour before any source row I/O.

    ``strategy=None`` is deliberately conservative: an unknown mutation
    contract cannot prove that an enabled inbound foreign key is safe.
    """

    if not snapshot.exists:
        return ()
    behavior = snapshot.behavior
    unsupported = {
        "temporal": behavior.temporal_type,
        "ledger": behavior.ledger_type,
        "memory_optimized": behavior.memory_optimized,
        "filetable": behavior.filetable,
        "graph_node": behavior.graph_node,
        "graph_edge": behavior.graph_edge,
    }
    enabled = sorted(name for name, value in unsupported.items() if value)
    blockers: list[str] = []
    if enabled:
        blockers.append("unsupported_table_behavior:" + ",".join(enabled))
    for trigger in snapshot.triggers:
        events = {event.upper() for event in trigger.events}
        if not trigger.disabled and not trigger.system_shipped and (not events or events & _DML_EVENTS):
            blockers.append(f"enabled_dml_trigger:{trigger.name}")
    for foreign_key in snapshot.foreign_keys:
        blocker = _inbound_foreign_key_blocker(foreign_key, strategy)
        if blocker is not None:
            blockers.append(blocker)
    return tuple(blockers)


def require_ordinary_mssql_target_behavior(
    snapshot: MssqlSchemaCatalogSnapshot,
    *,
    strategy: MSSQLLoadStrategyContract | None,
) -> None:
    """Raise one stable typed blocker for any unsupported target behaviour."""

    blockers = mssql_target_behavior_blockers(snapshot, strategy=strategy)
    if blockers:
        raise MssqlTargetBehaviorError(f"mssql_target_contract.{';'.join(blockers)}")


def _inbound_foreign_key_blocker(
    foreign_key: MssqlForeignKeyState,
    strategy: MSSQLLoadStrategyContract | None,
) -> str | None:
    if foreign_key.direction not in {"inbound", "self"} or foreign_key.disabled:
        return None
    actions = {
        foreign_key.update_action.upper(),
        foreign_key.delete_action.upper(),
    }
    cascading = sorted(actions & _CASCADE_ACTIONS)
    if cascading:
        return f"inbound_cascading_foreign_key:{foreign_key.name}:{','.join(cascading)}"
    if _proves_no_action_inbound_safe(foreign_key, strategy):
        return None
    mode = strategy.mode if strategy is not None else "unknown"
    return f"inbound_foreign_key_destructive_strategy:{foreign_key.name}:{mode}"


def _proves_no_action_inbound_safe(
    foreign_key: MssqlForeignKeyState,
    strategy: MSSQLLoadStrategyContract | None,
) -> bool:
    """Prove that the route never deletes or updates referenced columns."""

    if strategy is None:
        return False
    if strategy.mode == "incremental_append":
        return True
    referenced = {name.casefold() for name in foreign_key.referenced_columns}
    if strategy.mode == "incremental_merge" and strategy.incremental_merge is not None:
        policy = strategy.incremental_merge
        return policy.merge_policy == "update_insert" and referenced <= {name.casefold() for name in policy.unique_key}
    if strategy.mode == "snapshot_diff" and strategy.snapshot_diff is not None:
        policy = strategy.snapshot_diff
        return policy.delete_policy != "hard_delete" and referenced <= {name.casefold() for name in policy.unique_key}
    if strategy.mode == "scd2" and strategy.scd2 is not None:
        return referenced <= {name.casefold() for name in strategy.scd2.unique_key}
    if strategy.mode == "backfill" and strategy.backfill is not None:
        policy = strategy.backfill
        if policy.inner_mode != "incremental_merge" or policy.inner_policy is None:
            return False
        merge = policy.inner_policy
        return getattr(merge, "merge_policy", None) == "update_insert" and referenced <= {
            name.casefold() for name in getattr(merge, "unique_key", ())
        }
    return False


__all__ = [
    "MssqlTargetBehaviorError",
    "mssql_target_behavior_blockers",
    "require_ordinary_mssql_target_behavior",
]
