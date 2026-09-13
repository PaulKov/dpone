"""Least-privilege worker assembly; SQL/credentials are explicit offline doubles."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app import composition_remote_clickhouse_worker as worker
from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRequest
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowDeploymentIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
from dpone.contracts.composition_control import dbt_relation_write_subject
from dpone.contracts.composition_dispatch_v2 import DispatchV2Response
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.dbt_relation_writes import transfer_relation_write
from dpone.contracts.dbt_runtime import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    AIRFLOW_RUN_IDENTITY_ENV,
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    TRY_NUMBER_ENV,
    airflow_attempt_from_environment,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_snapshot_helpers import digest, occurrence
from tests.test_composition_clickhouse_execution_root import _manifest
from tests.test_composition_remote_transfer_result import result_body


def setup_worker(monkeypatch, tmp_path, state="ACTIVE"):
    manifest = _manifest()
    original = occurrence(state)
    write = transfer_relation_write(project_path=".", workflow_id="flow", workload_id="a_native", manifest=manifest)
    subject = dbt_relation_write_subject(write)
    old = original.request
    workloads = (replace(old.workloads[0], write_subjects=(subject,)), old.workloads[1])
    resources = tuple(
        replace(r, write_subjects=(subject,)) if r.connector == "clickhouse" else r for r in old.resources
    )
    parent = replace(
        old,
        context=replace(old.context, previous_deployment_id=digest("previous")),
        workloads=workloads,
        resources=resources,
    )
    retained = CompositionActivationOccurrence(
        parent, CompositionActivationReceipt(parent.request_sha256, state, original.receipt.guard_epochs)
    )
    plan = SimpleNamespace(
        workloads=workloads,
        writes=(write,),
        sources=SimpleNamespace(
            subject_sha256=parent.source_subject_sha256,
            transfer_manifests=(("a_native", canonical_json_bytes(manifest)),),
        ),
    )
    identity = AirflowDeploymentIdentity(parent.release_id, parent.deployment_id, parent.activation_id)
    run = AirflowRunIdentity(
        parent.release_id, parent.deployment_id, AirflowArtifactIdentity("a_native", workloads[0].pack_sha256)
    )
    env = {
        AIRFLOW_DEPLOYMENT_IDENTITY_ENV: identity.to_json(),
        AIRFLOW_RUN_IDENTITY_ENV: run.to_json(),
        DAG_ID_ENV: "dag",
        DAG_RUN_ID_ENV: "run",
        TRY_NUMBER_ENV: "1",
        DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: "control",
        "DPONE_CACHE_ROOT": str(tmp_path),
    }
    binding = {
        "schema": "dpone.composition-dispatcher-binding.v1",
        "dispatcher_id": parent.activation_id,
        "connection_ref": "dispatch",
        "service_configuration_sha256": digest("configuration"),
    }
    entries = {
        name: {"type": kind, "connection": {}}
        for name, kind in [("source", "mssql"), ("target", "clickhouse"), ("control", "mssql"), ("api", "api")]
    }
    entries["target"]["connection"]["composition_dispatcher"] = binding
    aliases = {"mssql-source": "source", "ch-sink": "target", "control": "control", "dispatch": "api"}
    resolved, calls, sql_options = [], [], []
    credentials = SimpleNamespace(
        endpoint="https://127.0.0.1:443",
        token="x" * 43,
        ssl_ca_location=None,
        connect_timeout=10,
        send_receive_timeout=90,
        database="control",
    )
    control = SimpleNamespace(
        credentials=credentials,
        descriptor=SimpleNamespace(
            connection_type="mssql", properties={"composition_service_id": parent.activation_id}
        ),
    )
    api = SimpleNamespace(credentials=credentials, descriptor=SimpleNamespace(connection_type="api", properties={}))

    def resolve(alias):
        resolved.append(alias)
        assert alias in {"control", "dispatch"}, "source or target credentials resolved"
        return control if alias == "control" else api

    runtime = SimpleNamespace(
        environment=parent.context.environment,
        release_id=parent.release_id,
        deployment_id=parent.deployment_id,
        authority_subject_sha256=parent.context.runtime_context_sha256,
        resolver=SimpleNamespace(resolve=resolve),
        binding_set={"bindings": {k: {"connection_ref": v} for k, v in aliases.items()}},
        connection_registry={"connections": entries},
    )
    monkeypatch.setattr(
        worker,
        "RuntimeConnectionContextLoader",
        lambda **kw: SimpleNamespace(load=lambda actual: calls.append(dict(actual)) or runtime),
    )
    monkeypatch.setattr(worker, "reopen_composition_plan", lambda *a: plan)
    monkeypatch.setattr(
        worker,
        "CompositionAuthorityConnections",
        lambda **kw: (
            sql_options.append(("connections", kw["control_schema"]))
            or SimpleNamespace(control_connection=lambda context: calls.append(context) or object())
        ),
    )

    def store(factory, **kwargs):
        sql_options.append(("store", kwargs["control_schema"]))

        def read(identifier):
            assert identifier == parent.activation_id
            factory()
            return retained

        return SimpleNamespace(read=read)

    monkeypatch.setattr(worker, "MssqlCompositionActivationStore", store)
    monkeypatch.setattr(
        worker, "DispatchV2HttpClient", lambda *a, **kw: SimpleNamespace(call=lambda req: calls.append(req))
    )
    request = SimpleNamespace(env=env, kind="ordinary_transfer", working_directory=tmp_path)
    typed = CompositionClickHouseExecutionRequest(
        manifest, plan.sources.subject_sha256, run, airflow_attempt_from_environment(env, run)
    )
    return SimpleNamespace(
        request=request,
        manifest=manifest,
        runtime=runtime,
        entries=entries,
        retained=retained,
        resolved=resolved,
        calls=calls,
        typed=typed,
        plan=plan,
        credentials=credentials,
        sql_options=sql_options,
    )


@pytest.mark.parametrize("state", ["ACTIVE", "RETIRING", "RETIRED"])
def test_remote_assembly_reads_retained_parent_without_source_target_or_admission(monkeypatch, tmp_path, state):
    value = setup_worker(monkeypatch, tmp_path, state)
    root = worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert root.can_execute_attempt()
    assert root._candidate.activation_request_sha256 == value.retained.request.request_sha256
    assert value.resolved == ["control", "dispatch"]
    assert value.calls[0] == value.request.env
    assert value.calls[1].runtime_context_sha256 == value.retained.request.context.runtime_context_sha256


def test_missing_signed_property_is_only_local_fallback_and_ignores_ambient(monkeypatch, tmp_path):
    value = setup_worker(monkeypatch, tmp_path)
    del value.entries["target"]["connection"]["composition_dispatcher"]
    monkeypatch.setenv("DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF", "ambient")
    assert worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest) is None
    assert value.resolved == []


@pytest.mark.parametrize("bad", [None, {}, {"schema": "wrong"}])
def test_explicit_invalid_dispatcher_never_falls_back(monkeypatch, tmp_path, bad):
    value = setup_worker(monkeypatch, tmp_path)
    value.entries["target"]["connection"]["composition_dispatcher"] = bad
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert value.resolved == []


@pytest.mark.parametrize(
    "alias,target",
    [
        ("control", "source"),
        ("control", "target"),
        ("dispatch", "source"),
        ("dispatch", "target"),
        ("dispatch", "control"),
    ],
)
def test_registry_alias_collisions_reject_before_credentials(monkeypatch, tmp_path, alias, target):
    value = setup_worker(monkeypatch, tmp_path)
    value.runtime.binding_set["bindings"][alias]["connection_ref"] = target
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert value.resolved == []


@pytest.mark.parametrize("field", ["environment", "release_id", "deployment_id", "authority_subject_sha256"])
def test_runtime_parent_drift_never_sends(monkeypatch, tmp_path, field):
    value = setup_worker(monkeypatch, tmp_path)
    setattr(value.runtime, field, "foreign" if field == "environment" else digest("foreign"))
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert "dispatch" not in value.resolved


@pytest.mark.parametrize(
    "field,bad",
    [("connect_timeout", True), ("connect_timeout", 31), ("send_receive_timeout", 0), ("send_receive_timeout", 901)],
)
def test_api_limits_require_explicit_bounded_integers(monkeypatch, tmp_path, field, bad):
    value = setup_worker(monkeypatch, tmp_path)
    setattr(value.credentials, field, bad)
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)


def test_typed_manifest_or_plan_drift_rejects_before_rpc(monkeypatch, tmp_path):
    value = setup_worker(monkeypatch, tmp_path)
    root = worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    for typed in [replace(value.typed, plan_sha256=digest("foreign")), replace(value.typed, manifest={})]:
        with pytest.raises(CompositionAdmissionError):
            root.execute(typed)
    assert len(value.calls) == 2


def direct_root(call):
    from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation

    candidate, _ = result_body()
    run = AirflowRunIdentity(
        digest("release"), digest("deployment"), AirflowArtifactIdentity(candidate.workload_id, candidate.pack_sha256)
    )
    scheduler = AirflowAttemptCorrelation(
        "dag", candidate.task_id, candidate.dag_run_id, candidate.try_number, candidate.map_index
    )
    typed = CompositionClickHouseExecutionRequest(_manifest(), candidate.plan_sha256, run, scheduler)
    root = worker.RemoteClickHouseExecutionRoot(
        expected=typed,
        candidate=candidate,
        dispatcher_id="10000000-0000-4000-8000-000000000001",
        runtime_authority_sha256=digest("runtime"),
        client=SimpleNamespace(call=call),
    )
    return root, typed


def test_success_returns_only_real_decoded_evidence_and_submits_once():
    from uuid import UUID

    from dpone.contracts.composition_remote_transfer_result import RemoteTransferResult

    calls = []
    candidate, body = result_body()

    def call(request):
        calls.append(request)
        return DispatchV2Response.for_request(request, canonical_json_bytes(body), status="SUCCEEDED")

    root, typed = direct_root(call)
    result = root.execute(typed)
    assert type(result) is RemoteTransferResult and result.rows == 2 and result.attempt == candidate
    assert calls[0].attempt == candidate and UUID(calls[0].request_id).version == 4
    assert calls[0].payload_spec == (0, None)
    assert not root.can_execute_attempt()
    with pytest.raises(CompositionAdmissionError):
        root.execute(typed)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "status,state", [("IN_PROGRESS", "RUNNING"), ("UNKNOWN", "COMMIT_UNKNOWN"), ("FAILED", "FAILED")]
)
def test_non_success_never_becomes_local_result_or_retries(status, state):
    from tests.test_composition_remote_transfer_result import ref, status_body

    calls = []
    _, body = status_body(state)
    if state == "FAILED":
        receipt = body["receipt"]["document"]
        for field in ("closed_gates_sha256", "quiescence_sha256", "outcome_evidence_sha256"):
            receipt[field] = digest(field)
        body["receipt"] = ref(canonical_json_bytes(receipt))

    def call(request):
        calls.append(request)
        return DispatchV2Response.for_request(request, canonical_json_bytes(body), status=status)

    root, typed = direct_root(call)
    for _ in range(2):
        if not calls:
            with pytest.raises(worker.RemoteClickHouseNonSuccess) as error:
                root.execute(typed)
            assert error.value.response.status == status
            assert error.value.response.evidence_document == canonical_json_bytes(body)
            assert error.value.response_document == error.value.response.to_bytes()
            assert "receipt" not in str(error.value)
        else:
            with pytest.raises(CompositionAdmissionError, match="remote_clickhouse_unknown"):
                root.execute(typed)
    assert len(calls) == 1


def test_transport_exception_is_redacted_and_never_retried():
    calls = []

    def call(request):
        calls.append(request)
        raise RuntimeError("private-secret-detail")

    root, typed = direct_root(call)
    for _ in range(2):
        with pytest.raises(CompositionAdmissionError) as error:
            root.execute(typed)
        assert "private-secret-detail" not in str(error.value)
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ["missing_control", "source_sha", "workloads", "manifest"])
def test_projection_or_sealed_plan_failures_never_resolve_api(monkeypatch, tmp_path, kind):
    value = setup_worker(monkeypatch, tmp_path)
    if kind == "missing_control":
        del value.request.env[DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV]
        monkeypatch.setenv(DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV, "control")
    elif kind == "source_sha":
        value.plan.sources.subject_sha256 = digest("foreign")
    elif kind == "workloads":
        value.plan.workloads = (value.plan.workloads[0],)
    else:
        value.plan.sources.transfer_manifests = (("a_native", b"{}"),)
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert "dispatch" not in value.resolved


def test_real_tls_client_uses_verified_context_and_explicit_credential_limits():
    import ssl

    resolved = SimpleNamespace(
        descriptor=SimpleNamespace(connection_type="api"),
        credentials=SimpleNamespace(
            endpoint="https://127.0.0.1:443",
            token="x" * 43,
            ssl_ca_location=None,
            connect_timeout=7,
            send_receive_timeout=80,
        ),
    )
    client = worker._client(resolved)
    assert client._context.verify_mode == ssl.CERT_REQUIRED and client._context.check_hostname
    assert client._context.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert (client._accept, client._execution, client._cleanup) == (7, 80, 60)


def test_foreign_activation_projection_cannot_resolve_api(monkeypatch, tmp_path):
    value = setup_worker(monkeypatch, tmp_path)
    identity = AirflowDeploymentIdentity(
        value.retained.request.release_id, value.retained.request.deployment_id, "10000000-0000-4000-8000-000000000009"
    )
    value.request.env[AIRFLOW_DEPLOYMENT_IDENTITY_ENV] = identity.to_json()
    with pytest.raises(CompositionAdmissionError):
        worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
    assert "dispatch" not in value.resolved


def test_non_success_exception_rejects_foreign_correlation_and_success():
    from tests.test_composition_dispatch_v2 import request
    from tests.test_composition_remote_transfer_result import status_body

    wire = request()
    _, body = status_body()
    response = DispatchV2Response.for_request(wire, canonical_json_bytes(body), status="IN_PROGRESS")
    with pytest.raises(CompositionAdmissionError):
        worker.RemoteClickHouseNonSuccess(replace(response, subject_sha256=digest("foreign")), request=wire)
    _, body = result_body()
    success = DispatchV2Response.for_request(wire, canonical_json_bytes(body), status="SUCCEEDED")
    with pytest.raises(CompositionAdmissionError):
        worker.RemoteClickHouseNonSuccess(success, request=wire)


@pytest.mark.parametrize("schema", ["custom_control", "invalid; SQL"])
def test_signed_control_schema_is_validated_and_shared_by_both_sql_collaborators(monkeypatch, tmp_path, schema):
    value = setup_worker(monkeypatch, tmp_path)
    resolved = value.runtime.resolver.resolve("control")
    value.resolved.clear()
    resolved.descriptor.properties["composition_control_schema"] = schema
    observations = value.sql_options
    monkeypatch.setenv("DPONE_CONTROL_SCHEMA", "ambient_schema")
    if schema == "custom_control":
        assert worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
        assert observations == [("connections", schema), ("store", schema)]
    else:
        with pytest.raises(CompositionAdmissionError):
            worker.compose_remote_clickhouse_pack_root(request=value.request, manifest=value.manifest)
        assert observations == []
