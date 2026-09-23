"""Frozen public constructor/method call shapes and defining module identities."""

import importlib
import inspect

import pytest

SIGNATURES = {
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess": "(process: 'subprocess.Popen[bytes]', handle: 'LinuxTdsProcess | None', descriptors: 'tuple[int, ...]', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.__init__": "(self, process: 'subprocess.Popen[bytes]', handle: 'LinuxTdsProcess | None', descriptors: 'tuple[int, ...]', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.check_owner": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.close_descriptor": "(self, descriptor: 'int') -> 'None'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.wait": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.terminate": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_child_process:TdsChildProcess.close": "(self, *, descriptors_first: 'bool' = False) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:UnresolvedPythonTdsLaunch": "(process: 'subprocess.Popen[bytes]', descriptors: 'tuple[int, ...]', handle: 'LinuxTdsProcess | None', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:UnresolvedPythonTdsLaunch.__init__": "(self, process: 'subprocess.Popen[bytes]', descriptors: 'tuple[int, ...]', handle: 'LinuxTdsProcess | None', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_child_process:UnresolvedPythonTdsLaunch.contain": "(self, *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_tds_child_process:UnresolvedPythonTdsLaunch.close": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_process:TdsProcessTermination": "(identity: 'TdsProcessIdentity', reaped: 'bool', exit_code: 'int | None') -> None",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess": "(identity: 'TdsProcessIdentity', fd: 'int', ops: 'Any') -> 'None'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.__init__": "(self, identity: 'TdsProcessIdentity', fd: 'int', ops: 'Any') -> 'None'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.admit": "() -> 'None'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.identify": "(pid: 'int') -> 'TdsProcessIdentity'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.acquire": "(identity: 'TdsProcessIdentity', *, _ops: 'Any' = None) -> 'LinuxTdsProcess'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.contain": "(self, *, deadline: 'float', direct_child: 'bool') -> 'TdsProcessTermination'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.wait": "(self, *, deadline: 'float', direct_child: 'bool') -> 'TdsProcessTermination'",
    "dpone.adapters.mssql_tds_process:LinuxTdsProcess.close": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorkerLauncher": "(*, policy: 'NativeBulkTransportPolicy', identity: 'TdsAttemptIdentity', python_executable: 'Path', package_root: 'Path', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorkerLauncher.__init__": "(self, *, policy: 'NativeBulkTransportPolicy', identity: 'TdsAttemptIdentity', python_executable: 'Path', package_root: 'Path', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorkerLauncher.assert_installation": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorkerLauncher.spawn": "(self, *, startup_deadline: 'float', operation_deadline: 'float') -> 'PythonTdsWorker'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker": "(launcher, process, handle, control_fd: 'int', startup_fd: 'int', cache=None) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.__init__": "(self, launcher, process, handle, control_fd: 'int', startup_fd: 'int', cache=None) -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.startup": "(self, *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.send": "(self, body: 'bytes', *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.receive": "(self, *, expected_attempt_sha256: 'str', expected_input: 'TdsInputReceipt', deadline: 'float') -> 'TdsWorkerResult'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.wait": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.terminate": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_supervisor_process:PythonTdsWorker.close": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorLauncher": "(*, python_executable: 'Path', package_root: 'Path', implementation_sha256: 'str', admission: 'bytes', max_address_space_bytes: 'int', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorLauncher.__init__": "(self, *, python_executable: 'Path', package_root: 'Path', implementation_sha256: 'str', admission: 'bytes', max_address_space_bytes: 'int', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorLauncher.assert_installation": "(self) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorLauncher.spawn": "(self, *, startup_deadline: 'float', operation_deadline: 'float') -> 'PythonTdsCoordinatorProcess'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess": "(process: 'subprocess.Popen[bytes]', handle: 'LinuxTdsProcess', descriptors: 'tuple[int, ...]', *, package_root: 'Path', implementation_sha256: 'str', launch_nonce: 'bytes', startup_deadline: 'float', operation_deadline: 'float', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.__init__": "(self, process: 'subprocess.Popen[bytes]', handle: 'LinuxTdsProcess', descriptors: 'tuple[int, ...]', *, package_root: 'Path', implementation_sha256: 'str', launch_nonce: 'bytes', startup_deadline: 'float', operation_deadline: 'float', cache: 'TemporaryDirectory[str] | None' = None) -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.startup": "(self, *, deadline: 'float') -> 'TdsCoordinatorStartup'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.deliver_credentials": "(self, body: 'bytes', *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.observe_authority": "(self, *, deadline: 'float') -> 'bytes'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.deliver_grant": "(self, body: 'bytes', *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.receive_result": "(self, *, deadline: 'float') -> 'bytes'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.wait": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.terminate": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_tds_coordinator_process:PythonTdsCoordinatorProcess.close": "(self) -> 'None'",
    "dpone.adapters.mssql_sqlclient_departure_launch:PythonSqlClientDepartureLauncher": "(*, python_executable: 'Path', package_root: 'Path', implementation_sha256: 'str', admission: 'bytes', max_address_space_bytes: 'int', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_sqlclient_departure_launch:PythonSqlClientDepartureLauncher.__init__": "(self, *, python_executable: 'Path', package_root: 'Path', implementation_sha256: 'str', admission: 'bytes', max_address_space_bytes: 'int', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_sqlclient_departure_launch:PythonSqlClientDepartureLauncher.assert_installation": "(self) -> 'None'",
    "dpone.adapters.mssql_sqlclient_departure_launch:PythonSqlClientDepartureLauncher.spawn": "(self, *, startup_deadline: 'float', operation_deadline: 'float') -> 'SqlClientDepartureProcess'",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess": "(process: Any, handle: dpone.adapters.mssql_tds_process.LinuxTdsProcess, descriptors: tuple[int, ...], *, expected: dpone.contracts.mssql_tds_coordinator_ipc.TdsCoordinatorStartup, startup_deadline: float, operation_deadline: float, cache: tempfile.TemporaryDirectory[str] | None = None) -> None",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.__init__": "(self, process: Any, handle: dpone.adapters.mssql_tds_process.LinuxTdsProcess, descriptors: tuple[int, ...], *, expected: dpone.contracts.mssql_tds_coordinator_ipc.TdsCoordinatorStartup, startup_deadline: float, operation_deadline: float, cache: tempfile.TemporaryDirectory[str] | None = None) -> None",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.startup": "(self, *, deadline: float) -> dpone.contracts.mssql_tds_coordinator_ipc.TdsCoordinatorStartup",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.send_request": "(self, body: bytes, *, deadline: float) -> None",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.receive_result": "(self, *, deadline: float) -> bytes",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.wait": "(self, *, deadline: float) -> dpone.contracts.mssql_tds_worker.TdsChildExit",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.terminate": "(self, *, deadline: float) -> dpone.contracts.mssql_tds_worker.TdsChildExit",
    "dpone.adapters.mssql_sqlclient_departure_process:SqlClientDepartureProcess.close": "(self) -> None",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess": "(child: 'subprocess.Popen', channel: 'socket.socket', expected: 'TdsCoordinatorStartup', *, startup_deadline: 'float', operation_deadline: 'float', termination_timeout: 'float') -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess.__init__": "(self, child: 'subprocess.Popen', channel: 'socket.socket', expected: 'TdsCoordinatorStartup', *, startup_deadline: 'float', operation_deadline: 'float', termination_timeout: 'float') -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess.assert_current": "(self) -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess.startup": "(self) -> 'TdsCoordinatorStartup'",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess.contain": "(self, *, deadline: 'float | None' = None) -> 'TdsChildExit'",
    "dpone.adapters.mssql_sqlclient_observe_process:SqlClientObserveProcess.close": "(self) -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:PythonSqlClientObserveLauncher": "(*, python_executable: 'Path', package_root: 'Path', implementation_sha256: 'str', admission: 'bytes', max_address_space_bytes: 'int', dependency_paths: 'tuple[Path, ...]' = ()) -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:PythonSqlClientObserveLauncher.assert_installation": "(self) -> 'None'",
    "dpone.adapters.mssql_sqlclient_observe_process:PythonSqlClientObserveLauncher.spawn": "(self, *, startup_deadline: 'float', operation_deadline: 'float', termination_timeout: 'float') -> 'SqlClientObserveProcess'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess": "(process: 'Any', handle: 'LinuxTdsProcess', descriptors: 'tuple[int, ...]', *, launch: 'SqlClientLaunch', cache: 'TemporaryDirectory[str] | None' = None, bound_input: 'SqlClientInputDescriptor | None' = None) -> 'None'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.__init__": "(self, process: 'Any', handle: 'LinuxTdsProcess', descriptors: 'tuple[int, ...]', *, launch: 'SqlClientLaunch', cache: 'TemporaryDirectory[str] | None' = None, bound_input: 'SqlClientInputDescriptor | None' = None) -> 'None'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.startup": "(self, *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.send_credentials": "(self, body: 'bytes', *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.observe_session": "(self, *, deadline: 'float') -> 'bytes'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.receive_session_or_result": "(self, *, deadline: 'float') -> \"tuple[Literal['session', 'result'], bytes]\"",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.send_grant": "(self, body: 'bytes', *, deadline: 'float') -> 'None'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.receive_result": "(self, *, deadline: 'float') -> 'bytes'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.wait": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.terminate": "(self, *, deadline: 'float') -> 'TdsChildExit'",
    "dpone.adapters.mssql_sqlclient_process:SqlClientChildProcess.close": "(self) -> 'None'",
    "dpone.services.mssql_tds_supervisor:TdsWorkerFailure": "(code: 'TdsAttemptError') -> 'None'",
    "dpone.services.mssql_tds_supervisor:TdsWorkerFailure.__init__": "(self, code: 'TdsAttemptError') -> 'None'",
    "dpone.services.mssql_tds_supervisor:TdsSupervisionUnknown": "(worker: 'TdsManagedWorker | None', *, unresolved_launch: 'TdsUnresolvedLaunch | None' = None) -> 'None'",
    "dpone.services.mssql_tds_supervisor:TdsSupervisionUnknown.__init__": "(self, worker: 'TdsManagedWorker | None', *, unresolved_launch: 'TdsUnresolvedLaunch | None' = None) -> 'None'",
    "dpone.services.mssql_tds_supervisor:run_tds_worker": "(writer: 'TdsJournalGateway', launcher: 'TdsWorkerLauncher', expected_input: 'TdsInputReceipt', request: 'Callable[[], bytes]', *, operation_deadline: 'float', startup_timeout: 'float', termination_timeout: 'float', clock: 'Callable[[], float]' = <built-in function monotonic>) -> 'TdsWorkerResult'",
    "dpone.services.mssql_sqlclient_supervision:before": "(deadline: 'float', clock: 'Callable[[], float]') -> 'None'",
    "dpone.services.mssql_sqlclient_supervision:validate_local_exit": "(observed: 'TdsChildExit', expected: 'TdsProcessIdentity | None') -> 'None'",
    "dpone.services.mssql_sqlclient_supervision:SqlClientSupervisionUnknown": "(retained: '_SqlClientExecution') -> 'None'",
    "dpone.services.mssql_sqlclient_supervision:SqlClientSupervisionUnknown.__init__": "(self, retained: '_SqlClientExecution') -> 'None'",
    "dpone.services.mssql_sqlclient_supervision:SqlClientSupervisionUnknown.close": "(self, *, deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:before": "(deadline: 'float', clock: 'Callable[[], float]') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:checked_exit": "(value: 'TdsChildExit', process: 'TdsProcessIdentity | None') -> 'TdsChildExit'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention": "(writer: 'TdsCoordinatorGateway', evidence: 'TdsCoordinatorEvidenceGateway', pool: 'TdsActorPool', current: 'TdsCoordinatorSnapshot', request: 'TdsCreateRequest', admission: 'bytes', session_nonce: 'bytes', child: 'TdsManagedCoordinatorProcess | None' = None, unresolved_launch: 'TdsUnresolvedLaunch | None' = None, process: 'TdsProcessIdentity | None' = None, startup: 'TdsCoordinatorStartup | None' = None, registration: 'bytes | None' = None, authority: 'TdsCoordinatorAuthority | None' = None, grant: 'TdsCoordinatorGrant | None' = None, raw_result: 'bytes | None' = None, response: 'TdsCreateResponse | None' = None, local_exit: 'TdsChildExit | None' = None, receipts: 'dict[Kind, TdsCoordinatorEvidenceReceipt]' = <factory>, code: 'TdsAttemptError' = <TdsAttemptError.FENCING: 'fencing'>, containment_deadline: 'float | None' = None, containment_budget_captured: 'bool' = False, child_close_attempted: 'bool' = False, child_closed: 'bool' = False, evidence_poisoned: 'bool' = False, journal_poisoned: 'bool' = False, evidence_closed: 'bool' = False, writer_closed: 'bool' = False, parent_assertion: 'Callable[[float], None] | None' = None, parent_poisoned: 'bool' = False, parent_asserting: 'bool' = False, unresolved_close_attempted: 'bool' = False, unresolved_closed: 'bool' = False) -> None",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.assert_parent": "(self, deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.assert_authority": "(self, deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.advance": "(self, event: 'TdsCoordinatorEvent', deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.persist": "(self, kind: 'Kind', payload: 'bytes', deadline: 'float') -> 'TdsCoordinatorEvidenceReceipt'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.capture_result": "(self) -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.close_child": "(self) -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.close_unresolved": "(self) -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.close_evidence": "(self, deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.close_writer": "(self, deadline: 'float') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorRetention.local_proof": "(self) -> 'TdsCoordinatorLocalExit'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorSupervisionUnknown": "(retained: 'TdsCoordinatorRetention') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorSupervisionUnknown.__init__": "(self, retained: 'TdsCoordinatorRetention') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorFailure": "(code: 'TdsAttemptError', retained: 'TdsCoordinatorRetention') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:TdsCoordinatorFailure.__init__": "(self, code: 'TdsAttemptError', retained: 'TdsCoordinatorRetention') -> 'None'",
    "dpone.app.mssql_tds_coordinator_supervision:fail_coordinator": "(retained: 'TdsCoordinatorRetention', original: 'BaseException', *, termination_timeout: 'float', clock: 'Callable[[], float]') -> 'NoReturn'",
    "dpone.app.mssql_sqlclient_departure_supervision:SqlClientDepartureOutcome": "(plan: 'SqlClientDeparturePlan | SqlClientDeparturePlanV2', request: 'SqlClientDepartureRequest | SqlClientDepartureRequestV2', result: 'SqlClientDepartureResult | SqlClientDepartureResultV2', local_exit: 'TdsChildExit', receipts: 'tuple[SqlClientDepartureEvidenceReceipt, ...]') -> None",
    "dpone.app.mssql_sqlclient_departure_supervision:SqlClientCreateDepartureOutcome": "(create_outcome: 'TdsCoordinatorCreateOutcome', helper_outcome: 'SqlClientDepartureOutcome') -> None",
    "dpone.app.mssql_sqlclient_departure_supervision:strict_exit": "(value: 'TdsChildExit', process: 'TdsProcessIdentity | None') -> 'TdsChildExit'",
    "dpone.app.mssql_sqlclient_departure_supervision:SqlClientCreateDepartureUnknown": "(retained: '_DepartureRetention') -> 'None'",
    "dpone.app.mssql_sqlclient_departure_supervision:SqlClientCreateDepartureUnknown.__init__": "(self, retained: '_DepartureRetention') -> 'None'",
    "dpone.app.mssql_sqlclient_departure_supervision:SqlClientCreateDepartureUnknown.close": "(self, *, deadline: 'float') -> 'None'",
}


@pytest.mark.parametrize("qualified,expected", SIGNATURES.items())
def test_original_public_signature_and_defining_module(qualified, expected):
    module, path = qualified.split(":")
    value = importlib.import_module(module)
    for component in path.split("."):
        value = getattr(value, component)
    assert str(inspect.signature(value)) == expected
    if "." not in path:
        assert value.__module__ == module


@pytest.fixture
def departure_fixture(tmp_path, monkeypatch):
    # Reuse the existing real-pipe launcher fixture; fake process is explicitly
    # local fault injection, never a successful SQL or kernel authority.
    from tests.test_mssql_sqlclient_departure_launch import setup

    fixture = setup.__wrapped__(tmp_path, monkeypatch)
    value = next(fixture)
    try:
        yield value
    finally:
        try:
            next(fixture)
        except StopIteration:
            pass


def test_constructor_failure_has_one_original_adoption(departure_fixture, monkeypatch):
    from dpone.adapters import mssql_sqlclient_departure_launch as module
    from dpone.adapters.mssql_tds_child_process import TdsChildProcess
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    adopted, constructor_arguments = [], []
    original = TdsChildProcess.__init__

    def adopt(self, *args, **kwargs):
        adopted.append(self)
        original(self, *args, **kwargs)

    def reject(process, *args, **kwargs):
        constructor_arguments.append(process)
        raise ValueError("constructor failed")

    monkeypatch.setattr(TdsChildProcess, "__init__", adopt)
    monkeypatch.setattr(module, "SqlClientDepartureProcess", reject)
    with pytest.raises(TdsLaunchUnknown) as caught:
        departure_fixture.launcher.spawn(startup_deadline=10.25, operation_deadline=20.5)
    retained = caught.value.launch
    assert len(adopted) == 1
    assert retained._resources is adopted[0] is constructor_arguments[0]
    assert retained.process is departure_fixture.process
    retained.contain(deadline=30.0)
    retained.close()


def test_observe_thread_start_failure_retains_original_socket_pair(departure_fixture, monkeypatch):
    from dpone.adapters import mssql_sqlclient_observe_process as module
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    root = departure_fixture.args["package_root"]
    (root / "dpone/app/mssql_sqlclient_observe_bootstrap.py").write_text("# fixed test bootstrap\n")
    launcher = module.PythonSqlClientObserveLauncher(
        **dict(departure_fixture.args, implementation_sha256=module.worker_installation_digest(root))
    )

    def start(thread):
        raise RuntimeError("thread start failed")

    monkeypatch.setattr(module.threading.Thread, "start", start)
    with pytest.raises(TdsLaunchUnknown) as caught:
        launcher.spawn(startup_deadline=10.0, operation_deadline=20.0, termination_timeout=1.0)
    retained = caught.value.launch
    assert retained.process is departure_fixture.process
    assert retained.handle is None
    sockets = [resource for resource in retained._resources._resources if resource.kind == "socket"]
    assert len(sockets) == 2 and all(resource.state == "OWNED" for resource in sockets)
    assert all(resource.value.fileno() >= 0 for resource in sockets)
    # Test teardown only; production keeps these unresolved originals.
    for resource in sockets:
        resource.value.close()


def test_caught_reentrant_release_during_startup_still_poisons_protocol(monkeypatch):
    from time import monotonic

    from dpone.adapters import mssql_tds_supervisor_process as module
    from tests.test_mssql_tds_supervisor_process import child, startup

    fixture = child.__wrapped__()
    held = next(fixture)
    read = module.read_worker_message

    def nested(*args, **kwargs):
        with pytest.raises(ValueError, match="release_invalid"):
            held.worker.send(b"{}", deadline=monotonic() + 1)
        return read(*args, **kwargs)

    monkeypatch.setattr(module, "read_worker_message", nested)
    try:
        with pytest.raises(ValueError, match="owner_invalid"):
            startup(held)
        assert not held.worker._started
        with pytest.raises(ValueError):
            held.worker.startup(deadline=monotonic() + 1)
        assert held.worker.terminate(deadline=monotonic() + 1).reaped
        held.worker.close()
    finally:
        try:
            next(fixture)
        except StopIteration:
            pass
