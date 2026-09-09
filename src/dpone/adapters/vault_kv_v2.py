"""Version-aware Vault KV v2 reader for protected runtime bindings.

The adapter keeps Vault SDK types out of the runtime contract.  It returns the
reserved ``_metadata.version`` envelope understood by
``BindingCredentialResolver`` so a KV v2 response without durable version
identity cannot be mistaken for an authenticated credential snapshot.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.vault_references import is_valid_vault_logical_path, is_valid_vault_mount


class VaultKvV2ReadError(RuntimeError):
    """Raised when Vault authentication or versioned read evidence is invalid."""


class HvacKubernetesKvV2Reader:
    """Read exact Vault KV v2 snapshots after Kubernetes authentication.

    Authentication is lazy so importing the adapter and constructing the
    application graph perform no network I/O.  A caller may inject
    ``client_factory`` for deterministic tests; production imports ``hvac``
    only when the first secret is requested.
    """

    def __init__(
        self,
        *,
        address: str,
        role: str,
        jwt_file: str,
        auth_mount_point: str = "k8s",
        verify: bool | str = True,
        namespace: str | None = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        if not address.startswith(("https://", "http://")):
            raise ValueError("Vault address must be an HTTP(S) URL")
        if not role.strip():
            raise ValueError("Vault Kubernetes role is required")
        if not is_valid_vault_mount(auth_mount_point):
            raise ValueError("Vault Kubernetes auth mount must be a logical mount")
        self._address = address
        self._role = role
        self._jwt_file = Path(jwt_file)
        self._auth_mount_point = auth_mount_point
        self._verify = verify
        self._namespace = namespace
        self._client_factory = client_factory
        self._client: Any | None = None

    def get_secret(self, *, mount_point: str, path: str) -> Mapping[str, Any]:
        """Return secret values plus a positive, backend-observed KV version."""

        if not is_valid_vault_mount(mount_point):
            raise ValueError("Vault KV mount must be a logical mount")
        if not is_valid_vault_logical_path(path):
            raise ValueError("Vault KV path must be a logical path")
        client = self._authenticated_client()
        try:
            response = client.secrets.kv.v2.read_secret_version(
                mount_point=mount_point,
                path=path,
                raise_on_deleted_version=True,
            )
            data = _mapping(_mapping(response).get("data"))
            values = dict(_mapping(data.get("data")))
            metadata = _mapping(data.get("metadata"))
            version = int(metadata.get("version") or 0)
        except Exception as exc:
            raise VaultKvV2ReadError("Vault KV v2 read did not return a valid versioned snapshot") from exc
        if version <= 0:
            raise VaultKvV2ReadError("Vault KV v2 response has no positive version")
        if "_metadata" in values:
            raise VaultKvV2ReadError("Vault secret uses the reserved _metadata field")
        return {**values, "_metadata": {"version": version}}

    def _authenticated_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            jwt = self._jwt_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise VaultKvV2ReadError("Vault Kubernetes service-account token is unavailable") from exc
        if not jwt:
            raise VaultKvV2ReadError("Vault Kubernetes service-account token is empty")
        factory = self._client_factory or _hvac_client_factory()
        try:
            client = factory(url=self._address, verify=self._verify, namespace=self._namespace)
            client.auth.kubernetes.login(
                role=self._role,
                jwt=jwt,
                mount_point=self._auth_mount_point,
                use_token=True,
            )
            authenticated = client.is_authenticated()
        except Exception as exc:
            raise VaultKvV2ReadError("Vault Kubernetes authentication failed") from exc
        if authenticated is not True:
            raise VaultKvV2ReadError("Vault Kubernetes authentication was not accepted")
        self._client = client
        return client


def build_hvac_kubernetes_vault_kv_v2_reader(
    credential_runtime: Mapping[str, Any],
) -> HvacKubernetesKvV2Reader | None:
    """Build the concrete reader from verified non-secret runtime policy."""

    vault = credential_runtime.get("vault")
    if vault is None:
        return None
    vault_policy = _mapping(vault)
    auth = _mapping(vault_policy.get("auth"))
    if str(auth.get("method") or "") != "kubernetes":
        raise ValueError("Vault runtime requires Kubernetes authentication")
    address = str(vault_policy.get("address") or "")
    role = str(auth.get("role") or "")
    jwt_file = str(auth.get("jwt_file") or "/var/run/secrets/kubernetes.io/serviceaccount/token")
    verify_value = vault_policy.get("verify", True)
    if not isinstance(verify_value, (bool, str)):
        raise ValueError("Vault TLS verification policy must be a boolean or CA path")
    return HvacKubernetesKvV2Reader(
        address=address,
        role=role,
        jwt_file=jwt_file,
        auth_mount_point=str(auth.get("mount_point") or "k8s"),
        verify=verify_value,
        namespace=str(vault_policy.get("namespace") or "") or None,
    )


def _hvac_client_factory() -> Callable[..., Any]:
    try:
        import hvac
    except ImportError as exc:  # pragma: no cover - optional deployment extra.
        raise VaultKvV2ReadError("Vault runtime dependency is not installed") from exc
    return hvac.Client


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise VaultKvV2ReadError("Vault response is not a mapping")
    return value


__all__ = [
    "HvacKubernetesKvV2Reader",
    "VaultKvV2ReadError",
    "build_hvac_kubernetes_vault_kv_v2_reader",
]
