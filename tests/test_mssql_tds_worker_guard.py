"""Worker guards are verified with injected OS calls, never host process limits."""

from __future__ import annotations

import signal

import pytest

from dpone.adapters.mssql_tds_worker_guard import TdsWorkerGuardError, install_worker_guard


class Ops:
    def __init__(self):
        self.calls = []
        self.parents = [123, 123, 123]
        self.limits = (-1, -1)
        self.death = signal.SIGKILL

    def parent(self):
        self.calls.append("parent")
        return self.parents.pop(0)

    def set_death(self):
        self.calls.append("set_death")

    def get_death(self):
        self.calls.append("get_death")
        return self.death

    def get_limits(self):
        self.calls.append("get_limits")
        return self.limits

    def set_limits(self, limits):
        self.calls.append(("set_limits", limits))
        self.limits = limits


def test_effects_precede_success_and_preserve_expected_parent(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    actual = install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert (actual.soft_bytes, actual.hard_bytes) == (1000, 1000)
    assert ops.calls == [
        "parent",
        "set_death",
        "get_death",
        "parent",
        "get_limits",
        ("set_limits", (1000, 1000)),
        "get_limits",
        "parent",
    ]


@pytest.mark.parametrize(
    "limits,expected",
    [
        ((500, 900), (500, 900)),
        ((500, -1), (500, 1000)),
        ((-1, -1), (1000, 1000)),
        ((0, 0), (0, 0)),
        ((1500, 2000), (1000, 1000)),
    ],
)
def test_inherited_finite_limits_never_raised(monkeypatch, limits, expected):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    ops.limits = limits
    actual = install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert (actual.soft_bytes, actual.hard_bytes) == expected


@pytest.mark.parametrize("parents", [[999], [123, 999], [123, 123, 999]])
def test_reparenting_races_fail_closed(monkeypatch, parents):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    ops.parents = parents
    with pytest.raises(TdsWorkerGuardError, match="parent_changed"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)


@pytest.mark.parametrize(
    "key,value",
    [
        ("expected_parent_pid", True),
        ("expected_parent_pid", 0),
        ("expected_parent_pid", -1),
        ("expected_parent_pid", 2**31),
        ("max_address_space_bytes", True),
        ("max_address_space_bytes", 0),
        ("max_address_space_bytes", float("inf")),
        ("max_address_space_bytes", 2**63),
    ],
)
def test_argument_validation_has_no_os_effects(monkeypatch, key, value):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    args = {"expected_parent_pid": 123, "max_address_space_bytes": 1000}
    args[key] = value
    ops = Ops()
    with pytest.raises(ValueError):
        install_worker_guard(**args, _ops=ops)
    assert ops.calls == []


def test_nonlinux_has_no_os_effects(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "darwin")
    ops = Ops()
    with pytest.raises(TdsWorkerGuardError, match="platform_unsupported"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert ops.calls == []


@pytest.mark.parametrize("operation", ["parent", "set_death", "get_death", "get_limits", "set_limits"])
def test_syscall_failure_is_sanitized(monkeypatch, operation):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()

    def fail(*args):
        raise PermissionError("sensitive child context")

    setattr(ops, operation, fail)
    with pytest.raises(TdsWorkerGuardError, match="tds_worker_guard_os_failure") as caught:
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert str(caught.value) == "tds_worker_guard_os_failure"
    assert caught.value.__suppress_context__


def test_unverified_parent_death_signal_stops_before_limits(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    ops.death = signal.SIGTERM
    with pytest.raises(TdsWorkerGuardError, match="pdeathsig_unverified"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert "get_limits" not in ops.calls


@pytest.mark.parametrize("limits", [(200, 100), (-1, 100), (-2, -1), (True, 100), (0, 2**63), [100, 200], (100,)])
def test_malformed_inherited_limits_never_set(monkeypatch, limits):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    ops.limits = limits
    with pytest.raises(TdsWorkerGuardError, match="limits_invalid"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)
    assert not any(isinstance(c, tuple) and c[0] == "set_limits" for c in ops.calls)


def test_ineffective_setrlimit_is_not_success(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")
    ops = Ops()
    ops.set_limits = lambda limits: None
    with pytest.raises(TdsWorkerGuardError, match="limits_unverified"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000, _ops=ops)


@pytest.mark.parametrize("operation", [1, 2])
def test_real_prctl_wrapper_rejects_nonzero_return_without_os_mutation(monkeypatch, operation):
    from dpone.adapters.mssql_tds_worker_guard import _LinuxGuardOps

    class Function:
        def __call__(self, op, *args):
            return -1 if op.value == operation else 0

    class Libc:
        prctl = Function()

    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.ctypes.CDLL", lambda *a, **kw: Libc())
    ops = _LinuxGuardOps()
    with pytest.raises(TdsWorkerGuardError, match="pdeathsig_.*_failed"):
        if operation == 1:
            ops.set_death()
        else:
            ops.get_death()


def test_libc_loading_failure_is_sanitized(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.sys.platform", "linux")

    def fail(*args, **kwargs):
        raise OSError("private loader detail")

    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.ctypes.CDLL", fail)
    with pytest.raises(TdsWorkerGuardError, match="^tds_worker_guard_os_failure$"):
        install_worker_guard(expected_parent_pid=123, max_address_space_bytes=1000)


def test_module_import_does_not_load_libc_or_change_resource_limits(monkeypatch):
    import runpy

    import dpone.adapters.mssql_tds_worker_guard as module

    def forbidden(*args, **kwargs):
        raise AssertionError("import-time effect")

    monkeypatch.setattr(module.ctypes, "CDLL", forbidden)
    # Execute in an isolated namespace without replacing classes held by other tests.
    runpy.run_path(module.__file__)


def test_prctl_variadic_numeric_arguments_have_native_long_width(monkeypatch):
    import ctypes

    from dpone.adapters.mssql_tds_worker_guard import _LinuxGuardOps

    calls = []

    class Function:
        def __call__(self, option, arg2, *unused):
            assert type(option) is ctypes.c_int
            assert all(type(arg) is ctypes.c_ulong and arg.value == 0 for arg in unused)
            if option.value == 1:
                assert type(arg2) is ctypes.c_ulong
                assert arg2.value == signal.SIGKILL
            else:
                ctypes.cast(arg2, ctypes.POINTER(ctypes.c_int))[0] = int(signal.SIGKILL)
            calls.append(option.value)
            return 0

    class Libc:
        prctl = Function()

    monkeypatch.setattr("dpone.adapters.mssql_tds_worker_guard.ctypes.CDLL", lambda *a, **kw: Libc())
    ops = _LinuxGuardOps()
    ops.set_death()
    assert ops.get_death() == signal.SIGKILL
    assert calls == [1, 2]
