"""Object storage must honor mount-backed runtime connection context."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.object_storage_access_models import ObjectStorageConnectionRef
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver


@dataclass(frozen=True)
class _Resolved:
    credentials: CredentialsConfig


class _Resolver:
    def __init__(self, mapping: dict[str, CredentialsConfig]) -> None:
        self._mapping = mapping

    def resolve(self, connection_ref: str) -> _Resolved:
        try:
            return _Resolved(credentials=self._mapping[connection_ref])
        except KeyError as exc:
            raise ValueError(f"missing {connection_ref}") from exc


class _Context:
    def __init__(self, mapping: dict[str, CredentialsConfig]) -> None:
        self.resolver = _Resolver(mapping)


def test_object_storage_prefers_runtime_connection_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = CredentialsConfig(
        username="AKIAEXAMPLE",
        password="secret",
        endpoint="https://storage.example",
        additional_params={"region_name": "ru-central1"},
    )

    class _Loader:
        def load(self, environ=None):  # noqa: ANN001
            del environ
            return _Context({"s3_dpone_stage_writer": expected})

    monkeypatch.setattr(
        "dpone.runtime.object_storage_connection_resolver.RuntimeConnectionContextLoader",
        _Loader,
    )

    def _boom(*_args: object, **_kwargs: object) -> CredentialsConfig:
        raise AssertionError("Airflow credentials must not be used when context is present")

    resolver = ObjectStorageConnectionResolver()
    monkeypatch.setattr(resolver._credentials_manager, "get_credentials", _boom)

    got = resolver.get_credentials(
        ObjectStorageConnectionRef(connection_type="airflow", connection_id="s3_dpone_stage_writer")
    )
    assert got.username == "AKIAEXAMPLE"
    assert got.endpoint == "https://storage.example"


def test_object_storage_falls_back_to_credentials_manager_without_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = CredentialsConfig(username="from-airflow", password="x")

    class _Loader:
        def load(self, environ=None):  # noqa: ANN001
            del environ
            return None

    monkeypatch.setattr(
        "dpone.runtime.object_storage_connection_resolver.RuntimeConnectionContextLoader",
        _Loader,
    )
    resolver = ObjectStorageConnectionResolver()
    monkeypatch.setattr(
        resolver._credentials_manager,
        "get_credentials",
        lambda *args, **kwargs: expected,
    )
    got = resolver.get_credentials(
        ObjectStorageConnectionRef(connection_type="airflow", connection_id="s3_dpone_stage_writer")
    )
    assert got.username == "from-airflow"
