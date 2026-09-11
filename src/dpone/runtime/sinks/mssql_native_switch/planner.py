"""Compatible entry point for canonical finite-partition SWITCH admission."""

from __future__ import annotations

from dpone.contracts.native_mssql_switch import (
    NativeSwitchBinding,
    NativeSwitchEligibility,
    NativeSwitchInterval,
    NativeSwitchSnapshot,
)
from dpone.contracts.native_mssql_switch import (
    NativeSwitchPlan as NativeSwitchPlan,
)
from dpone.contracts.native_mssql_switch import (
    plan_native_switch as _plan_native_switch,
)


def plan_native_switch(
    snapshot: NativeSwitchSnapshot,
    *,
    interval: NativeSwitchInterval,
    owner_binding: NativeSwitchBinding,
) -> NativeSwitchEligibility:
    """Return a frozen plan or stable reasons; never derive scope from rows.

    Synthetic snapshots are useful for tests. Execution always reacquires a
    catalog-adapter snapshot under locks; this pure function grants no SQL rights.
    """
    return _plan_native_switch(snapshot, interval=interval, owner_binding=owner_binding)
