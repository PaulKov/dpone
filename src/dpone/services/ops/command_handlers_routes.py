"""Compatibility facade for route ops command handlers."""

from __future__ import annotations

from .command_handlers_release_context import OpsServiceCatalog
from .command_handlers_routes_certification import (
    cmd_route_certification_pack,
    cmd_route_certify,
    cmd_route_certify_release,
    cmd_route_live_certification,
    cmd_route_rc_execute,
    cmd_route_rc_orchestrator,
    cmd_route_release_gate,
)
from .command_handlers_routes_readiness import (
    cmd_route_data_quality,
    cmd_route_readiness,
    cmd_route_reconciliation_repair,
    cmd_route_schema_evolution,
)
from .command_handlers_routes_refresh import (
    cmd_route_refresh_capture_snapshots,
    cmd_route_refresh_execute,
    cmd_route_refresh_plan,
    cmd_route_refresh_verify,
)
from .command_handlers_routes_state import (
    cmd_route_execution_ledger,
    cmd_route_run_supervisor,
    cmd_route_state_promote,
)

__all__ = [
    "OpsServiceCatalog",
    "cmd_route_certification_pack",
    "cmd_route_certify",
    "cmd_route_certify_release",
    "cmd_route_data_quality",
    "cmd_route_execution_ledger",
    "cmd_route_live_certification",
    "cmd_route_rc_execute",
    "cmd_route_rc_orchestrator",
    "cmd_route_readiness",
    "cmd_route_reconciliation_repair",
    "cmd_route_refresh_capture_snapshots",
    "cmd_route_refresh_execute",
    "cmd_route_refresh_plan",
    "cmd_route_refresh_verify",
    "cmd_route_release_gate",
    "cmd_route_run_supervisor",
    "cmd_route_schema_evolution",
    "cmd_route_state_promote",
]
