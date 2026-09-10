"""Transaction-bound old-out/new-in execution, without commit or retry policy."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.contracts.native_mssql_switch import NativeSwitchPlan, NativeSwitchRejected, NativeSwitchResult
from dpone.runtime.sinks.mssql_native_switch.catalog import (
    NativeSwitchCatalog,
    count,
    interval_parameters,
    qualified,
    quote,
)
from dpone.runtime.sinks.mssql_native_switch.planner import plan_native_switch

if TYPE_CHECKING:
    from dpone.ports.native_mssql_switch import NativeSwitchTransaction


def execute_native_switch(plan: NativeSwitchPlan, *, transaction: NativeSwitchTransaction) -> NativeSwitchResult:
    """Revalidate, count replaced rows, switch old out and prepared in once.

    Caller resolves the exact receipt before granting authority and supplies
    its complete prepared integrity verifier, invoked under held SQL locks. SQL table locks protect content/catalog revalidation
    through caller completion; they intentionally serialize all three tables.
    A successful return is uncommitted. Any failure, including after the first
    SWITCH, propagates unchanged for complete caller rollback. The executor
    cannot fall back, create a receipt, clean up or settle a commit outcome.
    """
    binding = plan.owner_binding
    transaction.assert_authority(binding)
    catalog = NativeSwitchCatalog(transaction)
    state = catalog.transaction_state()
    if (
        state["state"] != 1
        or state["depth"] != 1
        or state["database_id"] != binding.database.database_id
        or state["xact_abort"] != 16384
        or state["isolation"] != 4
        or type(state["session_id"]) is not int
        or state["session_id"] <= 0
    ):
        raise NativeSwitchRejected("transaction_authority_invalid")
    if catalog.database() != binding.database:
        raise NativeSwitchRejected("database_binding_mismatch")
    # TABLOCKX + HOLDLOCK also excludes independent non-dpone writers and DDL.
    # A transaction port must remain on this session; no reconnect/retry allowed.
    for obj in sorted((binding.target, binding.prepared, binding.switch_out), key=lambda obj: obj.object_id):
        count(transaction, f"SELECT COUNT_BIG(*) AS row_count FROM {qualified(binding, obj)} WITH (TABLOCKX, HOLDLOCK)")
    current = catalog.snapshot(binding, interval=plan.interval)
    eligibility = plan_native_switch(current, interval=plan.interval, owner_binding=binding)
    if eligibility.plan is None:
        raise NativeSwitchRejected(*eligibility.reasons)
    if eligibility.plan != plan:
        raise NativeSwitchRejected("catalog_drift")
    transaction.verify_prepared(plan)
    column = quote(plan.interval.column)
    target = qualified(binding, binding.target)
    replaced = count(
        transaction,
        f"SELECT COUNT_BIG(*) AS row_count FROM {target} WHERE {column} >= ? AND {column} < ?",
        interval_parameters(plan.interval),
    )
    transaction.assert_authority(binding)
    if catalog.transaction_state() != state:
        raise NativeSwitchRejected("transaction_authority_invalid")
    partition = plan.partition_number
    transaction.execute(
        f"ALTER TABLE {target} SWITCH PARTITION {partition} TO {qualified(binding, binding.switch_out)} PARTITION {partition}"
    )
    transaction.execute(
        f"ALTER TABLE {qualified(binding, binding.prepared)} SWITCH PARTITION {partition} TO {target} PARTITION {partition}"
    )
    return NativeSwitchResult(replaced_rows=replaced, inserted_rows=current.prepared_rows)
