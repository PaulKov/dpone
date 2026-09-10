"""Credential composition must never replace another caller's module dependencies."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from dpone.runtime.connectors.api import appsflyer as facade
from dpone.runtime.connectors.api import appsflyer_connector as canonical


@pytest.mark.parametrize("connector", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_facade_keeps_canonical_dependencies_during_resolution(monkeypatch, connector, fail):
    originals = (canonical.get_env_code, canonical.get_default_manager, canonical.AppsflyerCredentials)

    def assert_unchanged():
        assert canonical.get_env_code is originals[0]
        assert canonical.get_default_manager is originals[1]
        assert canonical.AppsflyerCredentials is originals[2]

    class Manager:
        def get_secret(self, *, mount_point, path):
            assert_unchanged()
            assert mount_point == "sample"
            if path == "outer":
                nested = facade.AppsflyerCredentials.from_vault("inner", self)
                assert type(nested) is facade.AppsflyerCredentials
                assert nested.api_key == "synthetic-inner"
                assert_unchanged()
                if fail:
                    raise ValueError("synthetic failure")
            return {"endpoint": "https://example.invalid/api", "token": f"synthetic-{path}"}

    manager = Manager()

    def factory():
        assert_unchanged()
        return manager

    monkeypatch.setattr(facade, "get_env_code", lambda: "sample")
    monkeypatch.setattr(facade, "get_default_manager", factory)
    cls = facade.AppsflyerConnector if connector else facade.AppsflyerCredentials
    if fail:
        with pytest.raises(ValueError, match="synthetic failure"):
            cls.from_vault("outer")
    else:
        result = cls.from_vault("outer")
        assert type(result) is cls
        credentials = result.credentials if connector else result
        assert type(credentials) is facade.AppsflyerCredentials
        assert credentials.api_key == "synthetic-outer"
        assert credentials.endpoint == "https://example.invalid/api/raw-data/export/app"
    assert_unchanged()


def test_concurrent_canonical_load_is_isolated_from_facade(monkeypatch):
    entered, release = Event(), Event()
    canonical_credentials = canonical.AppsflyerCredentials

    class Manager:
        def __init__(self, expected_mount):
            self.expected_mount = expected_mount

        def get_secret(self, *, mount_point, path):
            assert mount_point == self.expected_mount
            if path == "facade":
                entered.set()
                assert release.wait(5), "canonical call did not complete"
            return {"endpoint": "https://example.invalid", "token": f"synthetic-{path}"}

    canonical_manager = Manager("canonical")
    monkeypatch.setattr(canonical, "get_env_code", lambda: "canonical")
    monkeypatch.setattr(canonical, "get_default_manager", lambda: canonical_manager)
    monkeypatch.setattr(facade, "get_env_code", lambda: "facade")
    monkeypatch.setattr(facade, "get_default_manager", lambda: Manager("facade"))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(facade.AppsflyerConnector.from_vault, "facade")
        try:
            assert entered.wait(5), "facade call did not start"
            result = canonical.AppsflyerConnector.from_vault("canonical")
            assert type(result) is canonical.AppsflyerConnector
            assert type(result.credentials) is canonical_credentials
            assert result.credentials.api_key == "synthetic-canonical"
        finally:
            release.set()
        facade_result = future.result(timeout=5)
    assert type(facade_result) is facade.AppsflyerConnector
    assert type(facade_result.credentials) is facade.AppsflyerCredentials
    assert facade_result.credentials.api_key == "synthetic-facade"
