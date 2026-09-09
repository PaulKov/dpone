"""Fail-closed post-hook replay policy for governed MSSQL transactions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any

from dpone.governance.hooks import HookGraph
from dpone.runtime.governance.hook_graph_runtime import hooks_config, manifest_dir, repo_root

MSSQL_HOOK_GRAPH_SHA256_OPTION = "__dpone_mssql_hook_graph_sha256"


class MssqlHookReplayPolicyError(RuntimeError):
    """An inline hook cannot be resumed exactly after an ambiguous boundary."""

    code = "DPONE_MSSQL_HOOK_REPLAY_BLOCKED"


@dataclass(frozen=True, slots=True)
class MssqlHookGraphContract:
    """Canonical resolved graph evidence bound into the route fingerprint."""

    sha256: str

    def __post_init__(self) -> None:
        if len(self.sha256) != 64:
            raise ValueError("mssql_transaction.hook_graph_digest_invalid")


def require_replay_safe_mssql_hooks(load_config: Any) -> MssqlHookGraphContract:
    """Reject every inline hook until a provider-issued receipt exists.

    An author-declared ``mutates_source=false`` is not a read-only authority:
    SQL Server ``SELECT`` can consume sequences or invoke side-effecting remote
    functions, and the normal hook connector is writable.  Contract v1 has no
    durable per-hook outbox, provider idempotency key, or read-only-session
    capability. Pre-hooks also run after catalog preplanning and could invalidate
    the frozen source/target schema. No inline hook can therefore be certified.
    """

    graph, contract = mssql_hook_graph_contract(load_config)
    unsupported = tuple(
        (phase, action.id) for phase in ("pre_hook", "post_hook") for action in graph.ordered_phase(phase)
    )
    if unsupported:
        names = ",".join(f"{phase}:{hook_id}" for phase, hook_id in unsupported)
        raise MssqlHookReplayPolicyError(
            "mssql_transaction.hook_exactly_once_capability_required:"
            f"{names}: inline hooks require a preplan-safe provider phase and durable idempotency/read-only receipt"
        )
    return contract


def bind_replay_safe_mssql_hook_graph(load_config: Any) -> Any:
    """Bind the canonical replay-safe hook graph into route identity options."""

    contract = require_replay_safe_mssql_hooks(load_config)
    options = dict(getattr(load_config, "options", {}) or {})
    options[MSSQL_HOOK_GRAPH_SHA256_OPTION] = contract.sha256
    return replace(load_config, options=options)


def mssql_hook_graph_contract(load_config: Any) -> tuple[HookGraph, MssqlHookGraphContract]:
    """Resolve SQL files and hash the canonical hook graph, not authored paths."""

    graph = HookGraph.from_config(
        hooks_config(load_config),
        manifest_dir=manifest_dir(load_config),
        repo_root=repo_root(load_config),
    )
    payload = {
        phase: [_canonical_action(action) for action in graph.ordered_phase(phase)]
        for phase in ("pre_hook", "post_hook")
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return graph, MssqlHookGraphContract(hashlib.sha256(encoded).hexdigest())


def _canonical_action(action: Any) -> dict[str, Any]:
    return {
        "autocommit": action.autocommit,
        "connector": action.connector,
        "depends_on": list(action.depends_on),
        "execution": {"airflow": action.execution.airflow, "cli": action.execution.cli},
        "id": action.id,
        "kind": action.kind,
        "lineage": {"inputs": list(action.lineage.inputs), "outputs": list(action.lineage.outputs)},
        "mutates_source": action.mutates_source,
        "retry_policy": action.retry_policy,
        "sql_hash": action.sql_hash,
        "type": action.type,
    }


__all__ = [
    "MSSQL_HOOK_GRAPH_SHA256_OPTION",
    "MssqlHookGraphContract",
    "MssqlHookReplayPolicyError",
    "bind_replay_safe_mssql_hook_graph",
    "mssql_hook_graph_contract",
    "require_replay_safe_mssql_hooks",
]
