"""Handler ordering and real response codecs; boundary doubles certify no live route."""

import os
from contextlib import contextmanager
from dataclasses import asdict, replace
from threading import Event
from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_transfer_handler as module
from dpone.app.composition_dispatcher_context import StagedDispatcherAttempt
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_dispatch_v2 import DispatchV2Request, decode_response
from dpone.contracts.composition_dispatcher_binding import CompositionDispatcherBinding
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.contracts.composition_remote_transfer_result import decode_result, evidence_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.composition_execution_budget import CompositionExecutionBudget
from tests.test_composition_clickhouse_custody_enrollment import v2_body
from tests.test_composition_clickhouse_supervisor_enrollment import enrolled
from tests.test_composition_dispatcher_service_config import DIGEST, IDENTIFIER, decode, document
from tests.test_composition_remote_transfer_result import result_body


def harness(monkeypatch, states=(None, "SUCCEEDED")):
    attempt, result = result_body()
    run = AirflowRunIdentity(DIGEST, DIGEST, AirflowArtifactIdentity(attempt.workload_id, attempt.pack_sha256))
    airflow = AirflowAttemptCorrelation(
        "dag", attempt.task_id, attempt.dag_run_id, attempt.try_number, attempt.map_index
    )
    request = DispatchV2Request(
        IDENTIFIER,
        IDENTIFIER,
        DIGEST,
        "EXECUTE_TRANSFER",
        canonical_json_bytes(
            {
                "attempt_document": {"schema": "dpone.composition-attempt.v1", **asdict(attempt)},
                "attempt_sha256": attempt.attempt_sha256,
                "airflow_run_identity": run.to_dict(),
                "airflow_attempt": airflow.to_dict(),
            }
        ),
    )
    body = v2_body()
    body["facts"]["linux"]["capture_custody"] = {
        "identity_maps": {"uid_map": [[0, 0, 4294967295]], "gid_map": [[0, 0, 4294967295]]},
        "root_identity": {"device": 1, "inode": 8, "uid": 101, "gid": 101, "mode": 448},
    }
    enrollment = enrolled(body)
    value = document()
    value.update(
        dispatcher_uid=101,
        dispatcher_gid=101,
        supervisor_enrollment_sha256=enrollment.enrollment_sha256,
        capture_root="/capture",
        capture_root_identity=body["facts"]["linux"]["capture_custody"]["root_identity"],
    )
    config = decode(value, bootstrap_uid=101, bootstrap_gid=101)
    events, arguments = [], {}
    budget = CompositionExecutionBudget(900, stop_event=Event())
    target = SimpleNamespace(
        descriptor=SimpleNamespace(
            connection_type="clickhouse", properties={"composition_service_id": body["service_id"]}
        )
    )

    def resolve(ref):
        events.append("resolve:" + ref)
        return target

    context = SimpleNamespace(
        binding=CompositionDispatcherBinding(config.dispatcher_id, "dispatcher", config.sha256),
        occurrence=SimpleNamespace(runtime_context_sha256=DIGEST),
        runtime=SimpleNamespace(authority_subject_sha256=DIGEST, resolver=SimpleNamespace(resolve=resolve)),
        plan=SimpleNamespace(sources=SimpleNamespace(subject_sha256=attempt.plan_sha256)),
        target_binding_ref="target",
    )
    selected = StagedDispatcherAttempt(context, attempt, SimpleNamespace(connection_ref="target"), b"name: transfer\n")
    loader = SimpleNamespace(load_attempt=lambda selector, candidate: events.append("load") or selected)
    receipts = [
        None
        if state is None
        else CompositionAttemptReceipt(attempt, state, *([DIGEST] * 3 if state in {"SUCCEEDED", "FAILED"} else []))
        for state in states
    ]

    class Authority:
        control = SimpleNamespace(
            connection_factory=object(), control_schema="dpone_control", expected_service_id=IDENTIFIER
        )

        def __init__(self, selected, **kwargs):
            events.append("authority")
            arguments["authority"] = kwargs

        def inspect(self, run_identity, airflow_attempt):
            assert (run_identity, airflow_attempt) == (run, airflow)
            events.append("inspect")
            return receipts.pop(0)

        def read_active(self):
            pytest.fail("read_active belongs to real root, not handler")

    @contextmanager
    def transaction(*args):
        events.append("ledger-open")
        yield object()
        events.append("ledger-close")

    class Files:
        def __enter__(self):
            events.append("files-open")
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            events.append("files-close")

    class Custody:
        def __init__(self, **kwargs):
            events.append("custody")
            arguments["custody"] = kwargs

        def require_host(self):
            events.append("host")

        def require_enrollment_in(self, *args):
            pass

        def open_files(self):
            return Files()

    raw = canonical_json_bytes(result)

    def read_result(candidate):
        events.append("terminal")
        assert candidate == attempt
        return decode_result(raw, evidence_digest(raw), attempt=attempt)

    cell = SimpleNamespace(dependencies=object(), terminal=SimpleNamespace(read_result=read_result))

    def build(**kwargs):
        events.append("cell")
        arguments["cell"] = kwargs
        return cell

    class Root:
        def __init__(self, dependencies):
            assert dependencies is cell.dependencies

        def execute(self, invocation):
            events.append("execute")
            arguments["invocation"] = invocation
            return SimpleNamespace(rows=("never trust local result",))

    monkeypatch.setattr(module, "DispatcherAttemptAuthority", Authority)
    monkeypatch.setattr(module, "composition_control_transaction", transaction)
    monkeypatch.setattr(
        module, "read_service_enrollment", lambda ledger, service: events.append("enrollment") or enrollment
    )
    monkeypatch.setattr(module, "DispatcherCaptureCustody", Custody)
    monkeypatch.setattr(
        module, "RemoteClickHouseLocalSupervisor", lambda **kwargs: arguments.setdefault("supervisor", kwargs)
    )
    monkeypatch.setattr(module, "build_protected_clickhouse_cell", build)
    monkeypatch.setattr(module, "CompositionClickHouseExecutionRoot", Root)
    return SimpleNamespace(
        config=config,
        loader=loader,
        request=request,
        selected=selected,
        budget=budget,
        events=events,
        arguments=arguments,
        cell=cell,
        root=Root,
        receipts=receipts,
        enrollment=enrollment,
    )


def test_new_execution_uses_exact_selection_budget_and_retained_result(monkeypatch):
    case = harness(monkeypatch)
    response = module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert decode_response(response.to_bytes(), case.request).status == "SUCCEEDED"
    assert strict_json_object(response.evidence_document)["rows"] == 2
    assert case.events.index("inspect") < case.events.index("resolve:target") < case.events.index("enrollment")
    assert case.events.index("ledger-close") < case.events.index("custody") < case.events.index("cell")
    assert case.events.index("execute") < case.events.index("terminal") < case.events.index("files-close")
    cell = case.arguments["cell"]
    assert cell["manifest"] == case.selected.manifest
    assert cell["begin_cleanup"] == case.budget.begin_cleanup
    assert cell["absolute_deadline"] == case.budget.io_deadline
    assert cell["require_effect"] == case.budget.require_effect
    assert case.arguments["custody"]["deadline"] == case.budget.execution_deadline
    assert case.arguments["custody"]["absolute_deadline"] == case.budget.io_deadline
    assert case.arguments["supervisor"]["absolute_deadline"] == case.budget.io_deadline
    assert isinstance(cell["source_connector_factory"], module.BudgetedMssqlConnectorFactory)


@pytest.mark.parametrize(
    ("state", "status"), [("RUNNING", "IN_PROGRESS"), ("COMMIT_UNKNOWN", "UNKNOWN"), ("FAILED", "FAILED")]
)
def test_existing_non_success_is_read_only_before_target_or_host(monkeypatch, state, status):
    case = harness(monkeypatch, (state,))
    response = module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert response.status == status
    assert case.events == ["load", "authority", "inspect"]


def test_existing_success_reads_terminal_without_execution(monkeypatch):
    case = harness(monkeypatch, ("SUCCEEDED",))
    assert module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget).status == "SUCCEEDED"
    assert "execute" not in case.events and case.events[-1] == "files-close"


@pytest.mark.parametrize(
    ("state", "status"), [("RUNNING", "UNKNOWN"), ("COMMIT_UNKNOWN", "UNKNOWN"), ("FAILED", "FAILED")]
)
def test_execute_error_reopens_actual_receipt_after_explicit_cleanup(monkeypatch, state, status):
    case = harness(monkeypatch, (None, state))

    def fail(self, request):
        case.events.append("execute-error")
        raise RuntimeError("credential-secret")

    monkeypatch.setattr(case.root, "execute", fail)
    response = module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert response.status == status and b"credential-secret" not in response.to_bytes()
    assert case.budget.remaining_cleanup() > 0
    assert case.events[-2:] == ["inspect", "files-close"]


def test_missing_receipt_after_error_never_fabricates_unknown(monkeypatch):
    case = harness(monkeypatch, (None, None))
    monkeypatch.setattr(case.root, "execute", lambda *args: (_ for _ in ()).throw(RuntimeError("secret")))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown") as error:
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "secret" not in str(error.value) and case.events[-1] == "files-close"


def test_root_return_without_terminal_success_cannot_create_success(monkeypatch):
    case = harness(monkeypatch, (None, "COMMIT_UNKNOWN"))
    response = module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert response.status == "UNKNOWN" and "terminal" not in case.events


@pytest.mark.parametrize("change", ["dispatcher", "authority", "binding", "runtime", "attempt"])
def test_foreign_selection_rejects_before_control_target_or_host(monkeypatch, change):
    case = harness(monkeypatch)
    if change == "dispatcher":
        case.request = replace(case.request, dispatcher_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    elif change == "authority":
        case.request = replace(case.request, runtime_authority_sha256="sha256:" + "b" * 64)
    elif change == "binding":
        case.selected.context.binding = replace(
            case.selected.context.binding, service_configuration_sha256="sha256:" + "b" * 64
        )
    elif change == "runtime":
        case.selected.context.runtime.authority_subject_sha256 = "sha256:" + "b" * 64
    else:
        case.loader.load_attempt = lambda *args: replace(
            case.selected, attempt=replace(case.selected.attempt, try_number=99)
        )
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "authority" not in case.events


@pytest.mark.parametrize("change", ["digest", "uid", "gid", "destination", "inode", "mode"])
def test_configuration_must_match_enrollment_before_host_and_files(monkeypatch, change):
    case = harness(monkeypatch)
    value = strict_json_object(case.config.document)
    if change == "digest":
        value["supervisor_enrollment_sha256"] = DIGEST
    elif change in {"uid", "gid"}:
        value["dispatcher_" + change] += 1
        value["capture_root_identity"][change] += 1
    elif change == "destination":
        value["capture_root"] = "/other"
    else:
        # Enrollment remains canonical and valid while disagreeing with startup.
        body = case.enrollment.body
        body["facts"]["linux"]["capture_custody"]["root_identity"][change] += 1
        altered = enrolled(body)
        value["supervisor_enrollment_sha256"] = altered.enrollment_sha256
        monkeypatch.setattr(module, "read_service_enrollment", lambda *args: altered)
    case.config = decode(value, bootstrap_uid=value["dispatcher_uid"], bootstrap_gid=value["dispatcher_gid"])
    case.selected.context.binding = replace(
        case.selected.context.binding, service_configuration_sha256=case.config.sha256
    )
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "custody" not in case.events and "cell" not in case.events


def test_terminal_read_failure_is_transport_unknown_and_files_close(monkeypatch):
    case = harness(monkeypatch)
    case.cell.terminal.read_result = lambda *args: (_ for _ in ()).throw(RuntimeError("credential-secret"))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert case.events[-1] == "files-close"


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_late_terminal_receipt_cannot_be_relabelled_unknown(monkeypatch, state):
    case = harness(monkeypatch, (None, state))
    now = [100.0]
    case.budget = CompositionExecutionBudget(1, stop_event=Event(), clock=lambda: now[0])

    def late(self, request):
        now[0] = 102.0
        raise RuntimeError("lost acknowledgement")

    monkeypatch.setattr(case.root, "execute", late)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "terminal" not in case.events and case.events[-1] == "files-close"


def test_expired_execution_preserves_actual_running_receipt(monkeypatch):
    case = harness(monkeypatch, (None, "RUNNING"))
    now = [100.0]
    case.budget = CompositionExecutionBudget(1, stop_event=Event(), clock=lambda: now[0])

    def late(self, request):
        now[0] = 102.0
        raise RuntimeError("lost acknowledgement")

    monkeypatch.setattr(case.root, "execute", late)
    response = module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert response.status == "UNKNOWN" and case.budget.remaining_cleanup() == 59.0


def test_stopped_admission_cannot_resolve_target_after_absence(monkeypatch):
    case = harness(monkeypatch)
    case.budget.stop()
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert case.events == ["load", "authority", "inspect"]


def test_cell_build_failure_closes_open_files_and_never_executes(monkeypatch):
    case = harness(monkeypatch)
    monkeypatch.setattr(
        module, "build_protected_clickhouse_cell", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("secret"))
    )
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "execute" not in case.events and case.events[-1] == "files-close"


def test_configuration_fields_cannot_override_retained_original(monkeypatch):
    case = harness(monkeypatch)
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherTransferHandler(replace(case.config, capture_root=case.config.context_root), case.loader)
    assert case.events == []


def test_failed_authority_inspection_never_resolves_business_credentials(monkeypatch):
    case = harness(monkeypatch)
    monkeypatch.setattr(
        module.DispatcherAttemptAuthority, "inspect", lambda *args: (_ for _ in ()).throw(RuntimeError("secret"))
    )
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert case.events == ["load", "authority"]


def test_corrupt_terminal_original_cannot_become_success(monkeypatch):
    case = harness(monkeypatch)
    _, body = result_body()
    body["rows"] += 1
    case.cell.terminal.read_result = lambda *args: SimpleNamespace(document=canonical_json_bytes(body))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert case.events[-1] == "files-close"


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_terminal_deadline_is_rechecked_after_response_codec(monkeypatch, state):
    case = harness(monkeypatch, (state,))
    now = [100.0]
    case.budget = CompositionExecutionBudget(1, stop_event=Event(), clock=lambda: now[0])
    case.budget.begin_cleanup()
    original = module.DispatchV2Response.for_request

    def late_codec(cls, *args, **kwargs):
        response = original(*args, **kwargs)
        case.events.append("codec-completed")
        now[0] = 102.0
        return response

    monkeypatch.setattr(module.DispatchV2Response, "for_request", classmethod(late_codec))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
        module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
    assert "codec-completed" in case.events
    assert case.budget.remaining_cleanup() == 58.0


def test_failed_file_entry_closes_owned_descriptor_exactly_once(monkeypatch, tmp_path):
    case = harness(monkeypatch)
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)

    class EntryFailure:
        def __enter__(self):
            raise RuntimeError("fresh custody observation failed")

        def __exit__(self, *args):
            self.close()

        def close(self):
            case.events.append("descriptor-close")
            os.close(descriptor)

    monkeypatch.setattr(module.DispatcherCaptureCustody, "open_files", lambda self: EntryFailure())
    try:
        with pytest.raises(CompositionAdmissionError, match="dispatcher_transfer_unknown"):
            module.DispatcherTransferHandler(case.config, case.loader)(case.request, case.budget)
        assert case.events.count("descriptor-close") == 1
        with pytest.raises(OSError):
            os.fstat(descriptor)
        assert "cell" not in case.events and "execute" not in case.events
    finally:
        # The initial RED must not itself leak a test descriptor.
        if "descriptor-close" not in case.events:
            os.close(descriptor)
