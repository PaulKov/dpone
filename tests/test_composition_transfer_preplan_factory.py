"""Production factories must install trusted planning and observation together."""

from types import SimpleNamespace

import pytest

from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.app.composition_transfer_execution_factory import build_composition_transfer_execution_dependencies
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from tests.composition_mssql_gate_helpers import SERVICE
from tests.test_composition_transfer_preplan_store import originals


def arguments(tmp_path):
    tmp_path.chmod(0o700)
    attempt, manifest, write, *_ = originals()

    def connection(kind, database):
        return ResolvedBindingConnection(
            CredentialsConfig(host="fixture", database=database),
            {},
            ResolvedConnectionDescriptor(kind, {"database": database, "composition_service_id": SERVICE}),
        )

    def no_sql():
        pytest.fail("factory construction opened SQL")

    args = dict(
        control=CompositionDbtControlAuthority(no_sql, SERVICE, "Control"),
        read_active=lambda: None,
        sink_target=connection("mssql", "DWH"),
        state_target=connection("mssql", "State"),
        read_plan=lambda selected: SimpleNamespace(sources=SimpleNamespace(subject_sha256=attempt.plan_sha256)),
        verify_operation=lambda *args: None,
        state_config={
            "type": "mssql",
            "database": "State",
            "schema": "etl",
            "atomicity": "target_atomic",
            "provisioning": "external",
        },
        payload_root=tmp_path,
        verified_manifest=manifest,
        source_target=connection("postgres", "source"),
        parent_context=SimpleNamespace(environment="test"),
    )
    return args, attempt, write


def test_real_factory_installs_both_boundaries_without_sql_or_early_journal(tmp_path):
    args, attempt, write = arguments(tmp_path)
    dependencies = build_composition_transfer_execution_dependencies(**args)
    assert not (tmp_path / "preplans").exists()
    assert callable(dependencies.preplan_factory)
    observer = dependencies.outcome_observer._observe
    assert callable(observer._verify_preplan) and observer.proves_outcome
    service = dependencies.preplan_factory(attempt, write)
    assert service._attempt == attempt and service._write == write


@pytest.mark.parametrize("missing", ["verified_manifest", "source_target", "parent_context"])
def test_real_factory_refuses_payload_observer_without_trusted_inputs(tmp_path, missing):
    args, _, _ = arguments(tmp_path)
    args[missing] = None
    with pytest.raises(CompositionAdmissionError, match="preplan_configuration"):
        build_composition_transfer_execution_dependencies(**args)
