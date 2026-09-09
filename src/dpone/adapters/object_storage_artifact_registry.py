"""Object-storage adapter for the immutable artifact registry port."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

from dpone.ports.artifact_registry import (
    ArtifactMetadata,
    ArtifactRegistryKeyError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReadLimitExceeded,
    ArtifactRegistryUnavailable,
    CreateResult,
    artifact_registry_scope_id,
    exact_artifact_registry_scope_id,
)
from dpone.storage import (
    ImmutableObjectStorageClient,
    ObjectStorageEndpointAuthority,
    ObjectStorageObject,
    ObjectStorageReadLimitExceeded,
    ObjectStorageUri,
    ObjectStorageWriteConflict,
)

_MUTABLE_COMPONENTS = frozenset({"current", "latest"})
_SAFE_ENDPOINT_PATH_PART = re.compile(r"^[A-Za-z0-9._~-]+$")


class ObjectStorageArtifactRegistry:
    """Join safe logical keys to one configured object-storage root."""

    def __init__(
        self,
        *,
        client: ImmutableObjectStorageClient,
        root: ObjectStorageUri,
    ) -> None:
        _validate_root(root)
        self._client = client
        self._root = root

    @property
    def root(self) -> ObjectStorageUri:
        return self._root

    @property
    def scope_id(self) -> str:
        """Return the historical root-only identity for compatible callers."""

        return object_storage_registry_scope_id(self._root)

    @property
    def authority_scope_id(self) -> str:
        """Return the endpoint-bound identity when the client exposes authority."""

        if not isinstance(self._client, ObjectStorageEndpointAuthority):
            return ""
        endpoint_authority = self._client.endpoint_authority
        if not isinstance(endpoint_authority, str):
            return ""
        return object_storage_registry_authority_scope_id(
            self._root,
            endpoint_authority=endpoint_authority,
        )

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        normalized = _validate_key(key)
        try:
            created = self._client.put_file_if_absent(source, self._uri(normalized))
        except ObjectStorageWriteConflict:
            return CreateResult(created=False, metadata=self.stat(normalized))
        except Exception as exc:  # noqa: BLE001 - vendor payload is hidden at this boundary.
            raise ArtifactRegistryUnavailable("artifact registry create failed") from exc
        return CreateResult(created=True, metadata=_metadata(normalized, created))

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        normalized = _validate_key(key)
        try:
            value = self._client.stat(self._uri(normalized))
        except FileNotFoundError as exc:
            raise ArtifactRegistryObjectNotFound("pinned artifact is not present") from exc
        except Exception as exc:  # noqa: BLE001 - vendor payload is hidden at this boundary.
            if _is_not_found(exc):
                raise ArtifactRegistryObjectNotFound("pinned artifact is not present") from exc
            raise ArtifactRegistryUnavailable("artifact registry stat failed") from exc
        return _metadata(normalized, value)

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        normalized = _validate_key(key)
        try:
            self._client.get_file_bounded(self._uri(normalized), destination, max_bytes=max_bytes)
        except FileNotFoundError as exc:
            raise ArtifactRegistryObjectNotFound("pinned artifact is not present") from exc
        except ObjectStorageReadLimitExceeded as exc:
            raise ArtifactRegistryReadLimitExceeded("pinned artifact body exceeds declared metadata") from exc
        except Exception as exc:  # noqa: BLE001 - vendor payload is hidden at this boundary.
            if _is_not_found(exc):
                raise ArtifactRegistryObjectNotFound("pinned artifact is not present") from exc
            raise ArtifactRegistryUnavailable("artifact registry download failed") from exc

    def _uri(self, key: PurePosixPath) -> ObjectStorageUri:
        return self._root.child(key.as_posix())


def parse_artifact_registry_root(value: str) -> ObjectStorageUri:
    """Parse and validate a registry root before an optional SDK is constructed."""

    try:
        root = ObjectStorageUri.parse(value)
    except ValueError as exc:
        raise ArtifactRegistryKeyError("artifact registry URI is invalid or unsafe") from exc
    _validate_root(root)
    return root


def object_storage_registry_scope_id(root: ObjectStorageUri) -> str:
    """Return the historical root-only compatibility fingerprint."""

    _validate_root(root)
    return artifact_registry_scope_id(
        kind="object_storage",
        attributes={
            "provider": root.provider_name,
            "account": root.account,
            "bucket": root.bucket,
            "root": root.key,
        },
    )


def object_storage_registry_authority_scope_id(
    root: ObjectStorageUri,
    *,
    endpoint_authority: str,
) -> str:
    """Fingerprint one exact registry, including its observed endpoint authority."""

    _validate_root(root)
    return exact_artifact_registry_scope_id(
        kind="object_storage",
        attributes={
            "provider": root.provider_name,
            "endpoint_authority": canonical_object_storage_endpoint_authority(endpoint_authority),
            "account": root.account,
            "bucket": root.bucket,
            "root": root.key,
        },
    )


def canonical_object_storage_endpoint_authority(value: str) -> str:
    """Normalize a credential-free endpoint origin for exact registry identity."""

    if not value or value != value.strip() or any(char.isspace() for char in value):
        raise ArtifactRegistryKeyError("artifact registry endpoint authority is invalid or unsafe")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ArtifactRegistryKeyError("artifact registry endpoint authority is invalid or unsafe") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https", "local"}:
        raise ArtifactRegistryKeyError("artifact registry endpoint scheme is unsupported")
    if (
        not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ArtifactRegistryKeyError("artifact registry endpoint authority is invalid or unsafe")
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    if port is not None and (scheme, port) not in {("http", 80), ("https", 443)}:
        host = f"{host}:{port}"
    path = _canonical_endpoint_path(parsed.path)
    return urlunsplit((scheme, host, path, "", ""))


def _canonical_endpoint_path(value: str) -> str:
    if value in {"", "/"}:
        return ""
    if "\\" in value or not value.startswith("/"):
        raise ArtifactRegistryKeyError("artifact registry endpoint authority is invalid or unsafe")
    parts = tuple(part for part in value.split("/") if part)
    if not parts or any(part in {".", ".."} or not _SAFE_ENDPOINT_PATH_PART.fullmatch(part) for part in parts):
        raise ArtifactRegistryKeyError("artifact registry endpoint authority is invalid or unsafe")
    return "/" + "/".join(parts)


def _validate_root(root: ObjectStorageUri) -> None:
    if not root.bucket.strip():
        raise ArtifactRegistryKeyError("artifact registry root requires a bucket or container")
    for authority in (root.bucket, root.account):
        if authority is not None and (
            authority != authority.strip()
            or any(char.isspace() for char in authority)
            or any(char in authority for char in "/\\@?#")
        ):
            raise ArtifactRegistryKeyError("artifact registry authority is invalid or unsafe")
    if not root.key:
        raise ArtifactRegistryKeyError("artifact registry root path must not be empty")
    logical = PurePosixPath(root.key)
    if "\\" in root.key or logical.as_posix() != root.key:
        raise ArtifactRegistryKeyError("artifact registry root must be a canonical relative POSIX path")
    _validate_parts(logical, label="artifact registry root")


def _validate_key(key: PurePosixPath) -> PurePosixPath:
    raw = key.as_posix()
    if not raw or raw == "." or "\\" in raw or "?" in raw or "#" in raw:
        raise ArtifactRegistryKeyError("artifact registry key must be a normalized relative POSIX path")
    _validate_parts(key, label="artifact registry key")
    return key


def _validate_parts(value: PurePosixPath, *, label: str) -> None:
    if value.is_absolute() or any(part in {"", ".", ".."} for part in value.parts):
        raise ArtifactRegistryKeyError(f"{label} must be a normalized relative POSIX path")
    if any(part.lower() in _MUTABLE_COMPONENTS for part in value.parts):
        raise ArtifactRegistryKeyError(f"{label} must not contain current or latest")


def _metadata(key: PurePosixPath, value: ObjectStorageObject) -> ArtifactMetadata:
    advertised = value.sha256 or None
    if advertised and not advertised.startswith("sha256:") and len(advertised) == 64:
        advertised = "sha256:" + advertised
    return ArtifactMetadata(key=key, size_bytes=value.size_bytes, advertised_sha256=advertised)


def _is_not_found(exc: BaseException) -> bool:
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, dict):
            status = status or metadata.get("HTTPStatusCode")
    return status in {404, "404", "NotFound", "NoSuchKey"}


__all__ = [
    "ObjectStorageArtifactRegistry",
    "canonical_object_storage_endpoint_authority",
    "object_storage_registry_authority_scope_id",
    "object_storage_registry_scope_id",
    "parse_artifact_registry_root",
]
