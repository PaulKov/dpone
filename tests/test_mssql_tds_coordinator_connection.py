"""Connection material and admitted binary checks do not leak or invent authority."""

from hashlib import sha256
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_tds_coordinator_connection as module
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsConnectionError,
    TdsConnectionMaterial,
    TdsConnectionProfile,
    TdsCoordinatorBuild,
    TdsCoordinatorConnection,
)


def material():
    return TdsConnectionMaterial("localhost", 1433, "db", "user;name", "private;}password")


@pytest.fixture
def driver(tmp_path, monkeypatch):
    path = tmp_path / "library"
    path.write_bytes(b"admitted fixture")
    pin = TdsBinaryPin(path, sha256(path.read_bytes()).hexdigest())
    build = TdsCoordinatorBuild(pin, pin, pin, pin)
    events = []
    cursor = SimpleNamespace(close=lambda: events.append("cursor-close"))
    conn = SimpleNamespace(timeout=None, cursor=lambda: cursor, close=lambda: events.append("connection-close"))

    def connect(text, **kwargs):
        events.append((text, kwargs))
        return conn

    fake = SimpleNamespace(connect=connect)
    monkeypatch.setattr(module, "_admit", lambda supplied: fake)
    return build, events, conn


def test_structured_material_is_redacted_and_odbc_values_escaped(driver):
    build, events, _ = driver
    assert "password" not in repr(material()) and "user;name" not in repr(material())
    factory = TdsCoordinatorConnection(build, TdsConnectionProfile.VERIFIED_TLS)
    connection = factory.connect(material(), deadline=monotonic() + 1)
    text, kwargs = events[0]
    assert "PWD={private;}}password}" in text and "UID={user;name}" in text
    assert "TrustServerCertificate=no" in text and "Encrypt=yes" in text
    assert (
        "MARS_Connection=no" in text
        and "ConnectRetryCount=0" in text
        and "RetryExec" not in text
        and "DSN=" not in text
    )
    assert kwargs["autocommit"] is True
    connection.close()
    assert events[-2:] == ["cursor-close", "connection-close"]
    with pytest.raises(TdsConnectionError):
        factory.connect(material(), deadline=monotonic() + 1)


def test_synthetic_certificate_exception_requires_named_profile(driver):
    build, events, _ = driver
    TdsCoordinatorConnection(build, TdsConnectionProfile.SYNTHETIC_LOCAL).connect(material(), deadline=monotonic() + 1)
    assert "TrustServerCertificate=yes" in events[0][0]


@pytest.mark.parametrize(
    "change",
    [
        {"host": "host;DSN=bad"},
        {"port": True},
        {"port": 65536},
        {"password": "\0"},
        {"password": "x" * 16385},
        {"database": "db\u0085"},
    ],
)
def test_invalid_connection_material_before_driver(change):
    from dataclasses import replace

    with pytest.raises(ValueError):
        replace(material(), **change)


def test_missing_or_wrong_expected_pin_never_becomes_admission(tmp_path):
    path = tmp_path / "binary"
    path.write_bytes(b"actual")
    with pytest.raises(ValueError):
        TdsBinaryPin(path, "")
    with pytest.raises(ValueError):
        module._verify(TdsBinaryPin(path, "a" * 64))
    assert module._verify(TdsBinaryPin(path, sha256(b"actual").hexdigest())) == path
    with pytest.raises(ValueError):
        TdsCoordinatorBuild(None, None, None, None)


def test_connect_error_hides_driver_secret_and_never_retries(driver, monkeypatch):
    build, events, _ = driver
    factory = TdsCoordinatorConnection(build, TdsConnectionProfile.VERIFIED_TLS)

    def fail(*args, **kwargs):
        raise RuntimeError("private;}password")

    monkeypatch.setattr(factory._module, "connect", fail)
    with pytest.raises(TdsConnectionError) as raised:
        factory.connect(material(), deadline=monotonic() + 1)
    assert "private" not in str(raised.value)
    with pytest.raises(TdsConnectionError, match="reuse"):
        factory.connect(material(), deadline=monotonic() + 1)


def test_close_attempts_connection_even_if_cursor_close_fails(driver, monkeypatch):
    build, events, _ = driver
    connection = TdsCoordinatorConnection(build, TdsConnectionProfile.VERIFIED_TLS).connect(
        material(), deadline=monotonic() + 1
    )
    monkeypatch.setattr(connection.cursor, "close", lambda: (_ for _ in ()).throw(RuntimeError("secret")))
    with pytest.raises(TdsConnectionError, match="close_unknown"):
        connection.close()
    assert events[-1] == "connection-close"
    with pytest.raises(TdsConnectionError, match="owner"):
        connection.close()


def test_unadmitted_platform_and_preimported_driver_fail_before_loading(tmp_path, monkeypatch):
    pin = TdsBinaryPin(tmp_path / "binary", "a" * 64)
    build = TdsCoordinatorBuild(pin, pin, pin, pin)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_verify", lambda *args: pytest.fail("must not load"))
    with pytest.raises(TdsConnectionError, match="admission_required"):
        module._admit(build)


def test_pure_admission_descriptor_never_loads_driver(driver, monkeypatch):
    build, _, _ = driver
    monkeypatch.setattr(module, "_admit", lambda *args: pytest.fail("descriptor loaded driver"))
    payload = module.encode_connection_admission(build, TdsConnectionProfile.VERIFIED_TLS)
    assert module.decode_connection_admission(payload) == (build, TdsConnectionProfile.VERIFIED_TLS)
    for bad in (b"{}", payload[:-1] + b',"profile":"duplicate"}', b" " * 16385):
        with pytest.raises(ValueError):
            module.decode_connection_admission(bad)


def test_expired_post_connect_cleanup_is_single_use(driver):
    build, events, _ = driver
    now = iter((0.0, 2.0))
    factory = TdsCoordinatorConnection(build, TdsConnectionProfile.VERIFIED_TLS, clock=lambda: next(now))
    with pytest.raises(TdsConnectionError):
        factory.connect(material(), deadline=1.0)
    assert factory.connection is not None
    assert events[-1] == "connection-close"
    with pytest.raises(TdsConnectionError):
        factory.connection.close()
    assert events.count("connection-close") == 1


@pytest.mark.parametrize("wrong_manager", [False, True])
def test_actual_binary_pins_and_loaded_manager_are_checked_before_material(tmp_path, monkeypatch, wrong_manager):
    pins = []
    for name in ("python", "pyodbc.so", "driver.so", "libodbc.so.2"):
        path = tmp_path / name
        path.write_bytes(name.encode())
        pins.append(TdsBinaryPin(path, sha256(path.read_bytes()).hexdigest()))
    build = TdsCoordinatorBuild(*pins)
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.sys, "executable", str(pins[0].path))
    monkeypatch.setattr(module.platform, "machine", lambda: "aarch64")
    monkeypatch.delitem(module.sys.modules, "pyodbc", raising=False)
    fake = SimpleNamespace(version="5.3.0", __file__=str(pins[1].path))
    monkeypatch.setattr(module.importlib.util, "find_spec", lambda name: SimpleNamespace(origin=str(pins[1].path)))
    monkeypatch.setattr(module.importlib, "import_module", lambda name: fake)
    loaded = []
    monkeypatch.setattr(module.ctypes, "CDLL", lambda path: loaded.append(path))
    original = Path.read_text

    def read(path, *args, **kwargs):
        if str(path) == "/proc/self/maps":
            return "123 /foreign/libodbc.so.2" if wrong_manager else "123 " + str(pins[3].path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    if wrong_manager:
        with pytest.raises(TdsConnectionError, match="admission_failed"):
            module._admit(build)
    else:
        assert module._admit(build) is fake
        assert fake.pooling is False and fake.native_uuid is True
        assert loaded == [str(pins[3].path), str(pins[2].path)]
