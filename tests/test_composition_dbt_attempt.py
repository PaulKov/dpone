"""Exact parent identity and memory-only issued dbt credential regressions."""

from dataclasses import replace

import pytest
import yaml

from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
from dpone.app.composition_credentials import IssuedDbtProfileRenderer, issued_dbt_process_factory
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.contracts.dbt_relation_writes import selected_relation_writes
from dpone.contracts.dbt_workspace_control import dbt_relation_write_subject
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.services.composition_dbt_attempt import CompositionDbtAttemptLifecycle, build_composition_dbt_attempt
from tests.composition_mssql_gate_helpers import attempt, occurrence
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest


def selected():
    pack = _pack()
    active = occurrence()
    workload = replace(active.request.workloads[0], workload_id=f"dbt__{pack.workflow_id}")
    writes = tuple(
        sorted(
            dbt_relation_write_subject(w)
            for w in selected_relation_writes(
                project_path=pack.project_subdir, execution=pack, manifest=_preflight_manifest()
            )
        )
    )
    workload = replace(workload, write_subjects=writes)
    ordinary = replace(active.request.workloads[1], workload_id="z_ordinary")
    request = replace(
        active.request,
        workloads=(workload, ordinary),
        resources=(
            replace(active.request.resources[0], write_subjects=tuple(sorted(writes + ordinary.write_subjects))),
        ),
    )
    active = replace(active, request=request, receipt=replace(active.receipt, request_sha256=request.request_sha256))
    return pack, active


def run_identity(active):
    workload = active.request.workloads[0]
    return AirflowRunIdentity(
        active.request.release_id,
        active.request.deployment_id,
        AirflowArtifactIdentity(workload.workload_id, workload.pack_sha256),
    )


def test_parent_attempt_binds_exact_pack_plan_and_scheduler():
    pack, active = selected()
    scheduler = AirflowAttemptCorrelation("dag", "execute", "run", 2, 3)
    result = build_composition_dbt_attempt(
        active, pack=pack, run_identity=run_identity(active), airflow_attempt=scheduler
    )
    assert result.activation_request_sha256 == active.request.request_sha256
    assert (result.pack_sha256, result.plan_sha256) == (active.request.workloads[0].pack_sha256, pack.pack_sha256)
    assert (result.dag_run_id, result.task_id, result.try_number, result.map_index) == ("run", "execute", 2, 3)
    assert result.guard_epochs == active.receipt.guard_epochs


@pytest.mark.parametrize("change", ["pack", "state", "cell"])
def test_parent_rejects_wrong_workload_before_admission(change):
    pack, active = selected()
    original_identity = run_identity(active)
    if change == "state":
        active = replace(active, receipt=replace(active.receipt, state="RETIRING"))
    else:
        workload = active.request.workloads[0]
        values = {
            "pack": {"pack_sha256": "sha256:" + "f" * 64},
            "cell": {"execution_cell": "postgres_mssql_full_refresh_v1"},
            "constituent": {"constituent_id": "standalone"},
        }
        request = replace(active.request, workloads=(replace(workload, **values[change]), active.request.workloads[1]))
        active = replace(
            active, request=request, receipt=replace(active.receipt, request_sha256=request.request_sha256)
        )
    with pytest.raises(CompositionAdmissionError):
        build_composition_dbt_attempt(
            active,
            pack=pack,
            run_identity=original_identity,
            airflow_attempt=AirflowAttemptCorrelation("dag", "execute", "run", 1, -1),
        )


def material():
    identity = attempt()
    credentials = MssqlIssuedCredentials("dpone_v3_" + identity.attempt_sha256[7:], b"a" * 16, "new-secret")
    target = ResolvedBindingConnection(
        CredentialsConfig(
            host="sql",
            database="warehouse",
            schema="dbo",
            username="ambient",
            password="old-secret",
            additional_params={"Trusted_Connection": "yes"},
        ),
        {},
        ResolvedConnectionDescriptor("mssql", {}),
    )
    return identity, credentials, target


def test_profiles_contain_only_environment_references_and_no_ambient_secret():
    identity, credentials, target = material()
    profile = _pack().profile
    target.credentials.database = profile.database
    target.credentials.schema = profile.schema
    rendered = IssuedDbtProfileRenderer(
        attempt=identity, connection_ref=profile.connection_ref, target=target, credentials=credentials
    ).render(profile, _pack().adapter_runtime)
    assert b"new-secret" not in rendered.content and b"old-secret" not in rendered.content
    output = yaml.safe_load(rendered.content)[profile.profile_name]["outputs"][profile.target_name]
    assert output["authentication"] == "sql"
    assert "env_var('DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD')" in output["password"]
    assert "new-secret" in rendered.redaction_values
    assert target.credentials.username == "ambient"


def test_wrong_issued_attempt_is_rejected():
    identity, credentials, target = material()
    with pytest.raises(CompositionAdmissionError):
        IssuedDbtProfileRenderer(attempt=attempt(2), connection_ref="target", target=target, credentials=credentials)


def test_child_environment_is_detached_and_issued_values_override_ambient():
    identity, credentials, _ = material()
    captured = {}

    def popen(*args, **kwargs):
        captured.update(kwargs)
        return object()

    launch = issued_dbt_process_factory(popen, attempt=identity, credentials=credentials)
    environment = {"PATH": "/bin", "DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD": "ambient"}
    launch(("dbt", "build"), env=environment)
    assert captured["env"]["DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD"] == "new-secret"
    assert environment["DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD"] == "ambient"


@pytest.mark.parametrize("failure", [None, "writes", "active", "receipt", "scheduler", "pack"])
def test_prebuild_revalidates_real_writes_and_protected_fences(failure):
    pack, active = selected()
    scheduler = AirflowAttemptCorrelation("dag", "execute", "run", 1, -1)
    identity = build_composition_dbt_attempt(
        active, pack=pack, run_identity=run_identity(active), airflow_attempt=scheduler
    )
    events = []

    class Reader:
        def read_exact(self, value):
            events.append("read_attempt")
            return CompositionAttemptReceipt(value, "COMMIT_UNKNOWN" if failure == "receipt" else "RUNNING")

    def read_active():
        events.append("read_active")
        return replace(active, receipt=replace(active.receipt, state="RETIRING")) if failure == "active" else active

    manifest = _preflight_manifest()
    if failure == "writes":
        for unique_id in pack.selection_lock.selected_graph_unique_ids:
            if unique_id.startswith("model."):
                manifest["nodes"][unique_id]["alias"] = "unowned"
    lifecycle = CompositionDbtAttemptLifecycle(
        attempt=identity,
        occurrence=active,
        pack=pack,
        attempts=Reader(),
        read_active=read_active,
        read_preflight_manifest=lambda value: manifest if value == identity else {},
    )
    kwargs = dict(
        pack=_pack(warning_policy="allow") if failure == "pack" else pack,
        run_identity=run_identity(active),
        airflow_attempt=replace(scheduler, try_number=2) if failure == "scheduler" else scheduler,
    )
    if failure is None:
        lifecycle.verify_before_build(**kwargs)
        assert events == ["read_active", "read_attempt"]
    else:
        with pytest.raises(CompositionAdmissionError):
            lifecycle.verify_before_build(**kwargs)


def test_unknown_connection_and_missing_child_environment_fail_closed():
    identity, credentials, target = material()
    renderer = IssuedDbtProfileRenderer(
        attempt=identity, connection_ref="target", target=target, credentials=credentials
    )
    with pytest.raises(CompositionAdmissionError, match="credential_scope"):
        renderer.resolve("other")
    launch = issued_dbt_process_factory(lambda *args, **kwargs: None, attempt=identity, credentials=credentials)
    with pytest.raises(CompositionAdmissionError, match="child_environment"):
        launch(("dbt", "build"))
