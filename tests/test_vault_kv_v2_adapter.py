from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.adapters.vault_kv_v2 import (
    HvacKubernetesKvV2Reader,
    VaultKvV2ReadError,
    build_hvac_kubernetes_vault_kv_v2_reader,
)


class _KubernetesAuth:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.calls: list[dict[str, object]] = []

    def login(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))


class _KvV2:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def read_secret_version(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        return self.response


class _Client:
    def __init__(self, response: object, *, accepted: bool = True) -> None:
        self.kubernetes = _KubernetesAuth(accepted=accepted)
        self.kv = _KvV2(response)
        self.auth = SimpleNamespace(kubernetes=self.kubernetes)
        self.secrets = SimpleNamespace(kv=SimpleNamespace(v2=self.kv))
        self._accepted = accepted

    def is_authenticated(self) -> bool:
        return self._accepted


def _reader(tmp_path: Path, client: _Client) -> HvacKubernetesKvV2Reader:
    token = tmp_path / "token"
    token.write_text("signed-service-account-token", encoding="utf-8")

    def factory(**kwargs: Any) -> _Client:
        assert kwargs == {"url": "https://vault.test", "verify": True, "namespace": None}
        return client

    return HvacKubernetesKvV2Reader(
        address="https://vault.test",
        role="semantic-refresh",
        jwt_file=str(token),
        client_factory=factory,
    )


def test_reader_authenticates_and_returns_versioned_snapshot(tmp_path: Path) -> None:
    client = _Client(
        {
            "data": {
                "data": {"username": "runtime-user", "password": "runtime-password"},
                "metadata": {"version": 7},
            }
        }
    )

    snapshot = _reader(tmp_path, client).get_secret(
        mount_point="dpone-kv",
        path="semantic-refresh/runtime",
    )

    assert snapshot == {
        "username": "runtime-user",
        "password": "runtime-password",
        "_metadata": {"version": 7},
    }
    assert client.kubernetes.calls == [
        {
            "role": "semantic-refresh",
            "jwt": "signed-service-account-token",
            "mount_point": "k8s",
            "use_token": True,
        }
    ]
    assert client.kv.calls == [
        {
            "mount_point": "dpone-kv",
            "path": "semantic-refresh/runtime",
            "raise_on_deleted_version": True,
        }
    ]


@pytest.mark.parametrize(
    "response",
    [
        {"data": {"data": {"username": "runtime-user"}, "metadata": {}}},
        {"data": {"data": {"_metadata": "collision"}, "metadata": {"version": 1}}},
    ],
)
def test_reader_rejects_unversioned_or_reserved_payload(tmp_path: Path, response: object) -> None:
    with pytest.raises(VaultKvV2ReadError):
        _reader(tmp_path, _Client(response)).get_secret(
            mount_point="dpone-kv",
            path="semantic-refresh/runtime",
        )


def test_reader_rejects_failed_kubernetes_authentication(tmp_path: Path) -> None:
    client = _Client(
        {"data": {"data": {"username": "runtime-user"}, "metadata": {"version": 1}}},
        accepted=False,
    )
    with pytest.raises(VaultKvV2ReadError, match="not accepted"):
        _reader(tmp_path, client).get_secret(
            mount_point="dpone-kv",
            path="semantic-refresh/runtime",
        )


def test_factory_builds_only_kubernetes_versioned_reader() -> None:
    reader = build_hvac_kubernetes_vault_kv_v2_reader(
        {
            "vault": {
                "address": "https://vault.test",
                "verify": "/var/run/dpone/vault-ca.pem",
                "namespace": "data-platform",
                "auth": {
                    "method": "kubernetes",
                    "role": "semantic-refresh",
                    "mount_point": "k8s-prod",
                    "jwt_file": "/var/run/secrets/dpone/token",
                },
            }
        }
    )

    assert isinstance(reader, HvacKubernetesKvV2Reader)
    assert build_hvac_kubernetes_vault_kv_v2_reader({}) is None
    with pytest.raises(ValueError, match="Kubernetes"):
        build_hvac_kubernetes_vault_kv_v2_reader(
            {"vault": {"address": "https://vault.test", "auth": {"method": "approle"}}}
        )
