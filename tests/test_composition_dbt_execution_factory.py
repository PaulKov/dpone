"""Production composition of the protected native dbt worker dependencies.

Only construction is exercised here: every collaborator must be the real
protected adapter bound to the same verified control authority, sealed supervisor
capability and issued child environment. Opening SQL Server, allocating a Linux
child identity and running dbt remain live obligations and are UNVERIFIED here.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pytest

from dpone.adapters.composition_dbt_capture import LinuxDbtBuildRunner, ProtectedDbtCapture
from dpone.adapters.composition_dbt_capture_store import MssqlDbtCaptureStore
from dpone.adapters.composition_dbt_process_boundary import LinuxDbtProcessBoundary
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_dbt_materialization import MssqlDbtMaterializationObserver
from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.app.composition_dbt_execution import CompositionDbtExecutionDependencies
from dpone.app.composition_dbt_execution_factory import (
    CompositionDbtControlAuthority,
    build_composition_dbt_execution_dependencies,
)
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.dbt_invocation import DbtInvocationContext
from tests.composition_mssql_gate_helpers import attempt

SERVICE = "3f2c1b7a-5d4e-4a91-8b26-0c7d9e1f2a34"
SUPERVISOR = CompositionSupervisorProjection(
    persistent_volume_claim="dpone-composition-supervisor",
    child_uid_start=1_000_000,
    child_gid_start=1_000_000,
    child_identity_count=1_000_000,
)


def _dependencies(**overrides: Any) -> CompositionDbtExecutionDependencies:
    return build_composition_dbt_execution_dependencies(
        supervisor=overrides.pop("supervisor", SUPERVISOR),
        control=overrides.pop(
            "control",
            CompositionDbtControlAuthority(
                connection_factory=lambda: pytest.fail("no control connection may be opened here"),
                expected_service_id=SERVICE,
                control_database="DponeControl",
            ),
        ),
        read_active=lambda: pytest.fail("no parent may be read here"),
        target=object(),  # type: ignore[arg-type]
        open_materialization_target=lambda *_: pytest.fail("no target may be opened here"),
        require_materialization_target=lambda *_: None,
        observe_undispatched_closure=lambda *_: b"{}",
        **overrides,
    )


def test_every_collaborator_is_the_real_protected_adapter() -> None:
    dependencies = _dependencies()

    assert type(dependencies) is CompositionDbtExecutionDependencies
    assert type(dependencies.attempts) is MssqlCompositionAttemptStore
    assert type(dependencies.gate) is MssqlCompositionLoginGate
    assert type(dependencies.boundary) is LinuxDbtProcessBoundary
    assert dependencies.supervisor == SUPERVISOR
    assert dependencies.expected_service_id == SERVICE
    assert dependencies.outcome_proof_writer is persist_execution_proof


def test_capture_store_receives_only_the_authority_bound_source_verifier() -> None:
    dependencies = _dependencies()

    def verify(attempt: Any) -> Any:
        raise AssertionError("derivation belongs to the capture authority")

    store = dependencies.capture_store(verify)

    assert type(store) is MssqlDbtCaptureStore
    assert store._verify_source is verify
    assert store._verify_closure is not None


def test_capture_runs_the_child_through_the_issued_environment() -> None:
    dependencies = _dependencies()
    issued = {"DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD": "issued"}
    store = dependencies.capture_store(lambda _attempt: None)

    capture = dependencies.capture(store, lambda: issued)

    assert type(capture) is ProtectedDbtCapture
    assert type(capture._runner) is LinuxDbtBuildRunner
    assert dict(capture._runner._environment(object())) == issued


def test_production_preflight_factory_binds_exact_issued_child_environment(tmp_path: Path) -> None:
    observed: dict[str, object] = {}
    identity = attempt()
    credentials = MssqlIssuedCredentials(
        "dpone_v3_" + identity.attempt_sha256[7:],
        b"a" * 16,
        "issued-preflight-secret",
    )

    class CompletedProcess:
        stdout = io.BytesIO(b"issued-preflight-secret")
        stderr = io.BytesIO(b"issued-preflight-secret")

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return 0

    def popen(args: tuple[str, ...], **kwargs: object) -> CompletedProcess:
        observed["args"] = args
        observed.update(kwargs)
        return CompletedProcess()

    dependencies = _dependencies(preflight_popen=popen)
    runner = dependencies.preflight_command_runner_factory(identity, credentials)
    target = (tmp_path / "attempt" / "preflight" / "target").absolute()
    result = runner.run(
        ("dbt", "parse", "--target-path", str(target)),
        cwd=tmp_path,
        timeout_seconds=10,
        redactions=(credentials.login_name, credentials.password),
    )

    expected_home = target.parent.parent / "home"
    assert observed["env"] == {
        **DbtInvocationContext.canonical().environment(home=str(expected_home)),
        "DBT_ENV_SECRET_DPONE_COMPOSITION_USER": credentials.login_name,
        "DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD": credentials.password,
    }
    assert credentials.login_name not in observed["args"]
    assert credentials.password not in observed["args"]
    assert credentials.password not in result.stdout
    assert credentials.password not in result.stderr
    assert credentials.password not in repr(runner)


def test_materialization_observer_reopens_source_contracts_per_call() -> None:
    dependencies = _dependencies()

    def read_contracts(attempt: Any) -> Any:
        raise AssertionError("contracts belong to the capture authority")

    observer = dependencies.materializations(read_contracts)

    assert type(observer) is MssqlDbtMaterializationObserver
    assert observer._read_contracts is read_contracts


def test_child_identities_come_from_the_sealed_supervisor_projection() -> None:
    dependencies = _dependencies(supervisor_root=Path("/srv/dpone/composition"))
    allocator = dependencies.boundary._identities

    assert allocator.root == Path("/srv/dpone/composition")
    assert (allocator.uid_start, allocator.gid_start, allocator.count) == (
        SUPERVISOR.child_uid_start,
        SUPERVISOR.child_gid_start,
        SUPERVISOR.child_identity_count,
    )
    assert dependencies.boundary.output_root == Path("/srv/dpone/composition/run")


def test_the_default_dbt_executable_is_the_inspected_environment_binary() -> None:
    assert _dependencies().dbt_executable == current_environment_dbt_executable()


def test_a_relative_supervisor_root_is_refused_before_any_allocation() -> None:
    with pytest.raises(DbtCaptureError, match="capture_allocation_path"):
        _dependencies(supervisor_root=Path("var/lib/dpone"))
