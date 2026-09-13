"""Factory registration cannot authorize parent activation."""

from types import SimpleNamespace

import pytest

from dpone.app.composition_execution_cells import build_installed_execution_capabilities
from dpone.contracts.composition_activation import CompositionAdmissionError


def test_registered_but_unwired_cell_rejects_before_activation_side_effects():
    calls = []
    plan = SimpleNamespace(
        __post_init__=lambda: None,
        sources=SimpleNamespace(release_id="same"),
        require_installed_cells=lambda cells: calls.append("registry_checked"),
        workloads=(SimpleNamespace(execution_cell="mssql_clickhouse_full_refresh_v1"),),
    )
    context = SimpleNamespace(__post_init__=lambda: None, release_id="same")
    with pytest.raises(CompositionAdmissionError, match="complete_execution_capability_unavailable"):
        build_installed_execution_capabilities().require_execution(plan, context)
        calls.append("reserve_or_drain")
    assert calls == ["registry_checked"]
