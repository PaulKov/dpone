"""Compatibility facade for route ops parser registration."""

from __future__ import annotations

from dpone.commands.ops_parsers_route_rc import (
    register_route_rc_execute_parser,
    register_route_rc_orchestrator_parser,
)
from dpone.commands.ops_parsers_routes_certification import (
    register_route_certify_parser,
    register_route_live_certification_parser,
    register_route_release_gate_parser,
)
from dpone.commands.ops_parsers_routes_readiness import (
    register_route_data_quality_parser,
    register_route_readiness_parser,
    register_route_reconciliation_repair_parser,
    register_route_schema_evolution_parser,
)
from dpone.commands.ops_parsers_routes_refresh import (
    register_route_refresh_capture_snapshots_parser,
    register_route_refresh_execute_parser,
    register_route_refresh_plan_parser,
    register_route_refresh_verify_parser,
)
from dpone.commands.ops_parsers_routes_state import (
    register_route_execution_ledger_parser,
    register_route_run_supervisor_parser,
    register_route_state_promote_parser,
)

__all__ = [
    "register_route_certify_parser",
    "register_route_data_quality_parser",
    "register_route_execution_ledger_parser",
    "register_route_live_certification_parser",
    "register_route_rc_execute_parser",
    "register_route_rc_orchestrator_parser",
    "register_route_readiness_parser",
    "register_route_reconciliation_repair_parser",
    "register_route_refresh_capture_snapshots_parser",
    "register_route_refresh_execute_parser",
    "register_route_refresh_plan_parser",
    "register_route_refresh_verify_parser",
    "register_route_release_gate_parser",
    "register_route_run_supervisor_parser",
    "register_route_schema_evolution_parser",
    "register_route_state_promote_parser",
]
