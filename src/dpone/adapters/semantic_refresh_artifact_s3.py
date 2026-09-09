"""Create-only version-pinned S3 artifact store for semantic refresh V2."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timezone
from hashlib import sha256 as hashlib_sha256
from typing import Any

from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreUnavailable,
)
from dpone.ports.semantic_refresh_s3_policy import SemanticRefreshS3ArtifactStorePolicy
from dpone.storage.checksum import is_conditional_create_conflict

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_UTC = timezone.utc  # noqa: UP017 - datetime.UTC is absent from the supported Python 3.10 API


class S3CreateOnlyArtifactStore:
    """Use conditional PUT, independent SHA metadata, KMS, and exact VersionId."""

    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        operation_prefix: str,
        policy: SemanticRefreshS3ArtifactStorePolicy,
        encryption_algorithm: str = "aws:kms",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        for value, field_name in (
            (bucket, "bucket"),
            (operation_prefix, "operation_prefix"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty protected value")
        if encryption_algorithm != "aws:kms":
            raise ValueError("semantic-refresh V2 artifacts require aws:kms encryption")
        if policy.require_object_lock is not True:
            raise ValueError("semantic-refresh V2 artifacts require provider Object Lock")
        normalized = operation_prefix.strip("/") + "/"
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("operation_prefix is invalid")
        if bucket != policy.bucket_or_container_authority_id:
            raise ValueError("S3 bucket differs from protected authority")
        if normalized != policy.artifact_prefix + "/":
            raise ValueError("S3 operation prefix differs from protected authority")
        observed_endpoint = _client_endpoint_authority_id(client)
        if _normalized_endpoint(observed_endpoint) != _normalized_endpoint(policy.endpoint_authority_id):
            raise ValueError("S3 endpoint differs from protected authority")
        self._client = client
        self._bucket = bucket
        self._operation_prefix = normalized
        self._kms_key_id = policy.kms_key_authority_id
        self._encryption_algorithm = encryption_algorithm
        self._policy = policy
        self._clock = clock or (lambda: datetime.now(_UTC))

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
    ) -> ArtifactObjectRef:
        """Conditionally create one final key and require a provider VersionId."""

        self._assert_key(key)
        self._assert_create_inputs(
            content=content,
            sha256=sha256,
            encryption_scope=encryption_scope,
            retention_until=retention_until,
        )
        self._assert_bucket_capabilities()
        metadata = {
            "dpone-sha256": sha256,
            "dpone-encryption-scope": encryption_scope,
            "dpone-retention-until": retention_until,
            "dpone-capability-evidence-sha256": self._policy.capability_evidence_sha256,
            "dpone-encryption-policy-sha256": self._policy.encryption_policy_sha256,
            "dpone-retention-policy-id": self._policy.retention_policy_id,
            "dpone-retention-policy-sha256": self._policy.retention_policy_sha256,
            "dpone-retention-days": str(self._policy.retention_days),
            "dpone-retention-issued-at": self._policy.retention_issued_at,
            "dpone-writer-scope": self._policy.writer_scope,
        }
        request: dict[str, object] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": content,
            "IfNoneMatch": "*",
            "Metadata": metadata,
            "ServerSideEncryption": self._encryption_algorithm,
        }
        request["SSEKMSKeyId"] = self._kms_key_id
        if self._policy.require_object_lock:
            request["ObjectLockMode"] = self._policy.object_lock_mode
            request["ObjectLockRetainUntilDate"] = retention_datetime(retention_until)
        try:
            response = self._client.put_object(**request)
        except Exception as exc:
            if is_conditional_create_conflict(exc):
                raise ArtifactCreateConflict(f"create-only artifact exists: {key}") from exc
            raise ArtifactStoreUnavailable(f"artifact create acknowledgement unavailable: {key}") from exc
        version = response.get("VersionId")
        if not isinstance(version, str) or not version:
            raise ArtifactStoreUnavailable("S3 create did not return a version-pinned identity")
        observed = self._head_ref(key=key, version=version)
        if (
            observed.size_bytes != len(content)
            or observed.sha256 != sha256
            or observed.encryption_scope != encryption_scope
            or observed.retention_until != retention_until
        ):
            raise ArtifactStoreUnavailable("S3 exact-version create metadata differs")
        self._assert_unambiguous_version(key, version)
        return observed

    def head(self, *, key: str) -> ArtifactObjectRef | None:
        """Read create-only metadata; latest is safe only under overwrite-deny policy."""

        self._assert_key(key)
        self._assert_bucket_capabilities()
        try:
            observed = self._head_ref(key=key, version=None)
            self._assert_unambiguous_version(key, observed.version)
            return observed
        except Exception as exc:
            if isinstance(exc, ArtifactStoreUnavailable):
                raise
            status = _status(exc)
            if status in {404, "404", "NoSuchKey", "NotFound"}:
                return None
            raise ArtifactStoreUnavailable(f"artifact HEAD unavailable: {key}") from exc

    def read_version(self, *, key: str, version: str) -> bytes:
        """Read the exact manifest-pinned version, never latest/list discovery."""

        self._assert_key(key)
        if not isinstance(version, str) or not version:
            raise ValueError("version must be non-empty")
        self._assert_bucket_capabilities()
        try:
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=key,
                VersionId=version,
            )
            if response.get("VersionId", version) != version:
                raise ArtifactStoreUnavailable("S3 exact-version read returned another VersionId")
            body = response["Body"]
            try:
                content = bytes(body.read())
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
            observed = self._head_ref(key=key, version=version)
            if observed.size_bytes != len(content) or observed.sha256 != (
                "sha256:" + hashlib_sha256(content).hexdigest()
            ):
                raise ArtifactStoreUnavailable("S3 exact-version content metadata differs")
            self._assert_unambiguous_version(key, version)
            return content
        except ArtifactStoreUnavailable:
            raise
        except Exception as exc:
            raise ArtifactStoreUnavailable(f"exact artifact version unavailable: {key}@{version}") from exc

    def _assert_bucket_capabilities(self) -> None:
        self._assert_retention_unexpired()
        if self._policy.conditional_create_authorized is not True:
            raise ArtifactStoreUnavailable("S3 conditional create capability is not authorized")
        try:
            versioning = self._client.get_bucket_versioning(Bucket=self._bucket)
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 bucket versioning capability is unavailable") from exc
        if versioning.get("Status") != "Enabled":
            raise ArtifactStoreUnavailable("S3 bucket versioning is not Enabled")
        if not self._policy.require_object_lock:
            return
        try:
            raw = self._client.get_object_lock_configuration(Bucket=self._bucket)
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 object lock capability is unavailable") from exc
        configuration = raw.get("ObjectLockConfiguration")
        if not isinstance(configuration, dict) or configuration.get("ObjectLockEnabled") != "Enabled":
            raise ArtifactStoreUnavailable("S3 object lock is not Enabled")

    def _head_ref(self, *, key: str, version: str | None) -> ArtifactObjectRef:
        request: dict[str, object] = {"Bucket": self._bucket, "Key": key}
        if version is not None:
            request["VersionId"] = version
        response = self._client.head_object(**request)
        metadata = response.get("Metadata")
        observed_version = response.get("VersionId")
        if not isinstance(metadata, dict) or not isinstance(observed_version, str) or not observed_version:
            raise ArtifactStoreUnavailable("S3 HEAD omitted protected metadata or VersionId")
        if version is not None and observed_version != version:
            raise ArtifactStoreUnavailable("S3 exact-version HEAD returned another VersionId")
        if response.get("ServerSideEncryption") != self._encryption_algorithm:
            raise ArtifactStoreUnavailable("S3 object encryption authority differs")
        if response.get("SSEKMSKeyId") != self._kms_key_id:
            raise ArtifactStoreUnavailable("S3 object encryption authority differs")
        expected_metadata = {
            "dpone-capability-evidence-sha256": self._policy.capability_evidence_sha256,
            "dpone-encryption-policy-sha256": self._policy.encryption_policy_sha256,
            "dpone-retention-policy-id": self._policy.retention_policy_id,
            "dpone-retention-policy-sha256": self._policy.retention_policy_sha256,
            "dpone-retention-days": str(self._policy.retention_days),
            "dpone-retention-issued-at": self._policy.retention_issued_at,
            "dpone-writer-scope": self._policy.writer_scope,
        }
        if any(metadata.get(key) != value for key, value in expected_metadata.items()):
            raise ArtifactStoreUnavailable("S3 protected object metadata differs")
        retention_until = str(metadata.get("dpone-retention-until", ""))
        if self._policy.require_object_lock:
            if response.get("ObjectLockMode") != self._policy.object_lock_mode:
                raise ArtifactStoreUnavailable("S3 object lock mode differs")
            if _retention_text(response.get("ObjectLockRetainUntilDate")) != _retention_text(retention_until):
                raise ArtifactStoreUnavailable("S3 object retention authority differs")
        size = response.get("ContentLength")
        digest = metadata.get("dpone-sha256")
        encryption_scope = metadata.get("dpone-encryption-scope")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or _DIGEST_RE.fullmatch(digest) is None
            or not isinstance(encryption_scope, str)
            or not encryption_scope
            or not retention_until
        ):
            raise ArtifactStoreUnavailable("S3 HEAD protected metadata is invalid")
        return ArtifactObjectRef(
            key=key,
            version=observed_version,
            size_bytes=size,
            sha256=digest,
            encryption_scope=encryption_scope,
            retention_until=retention_until,
        )

    def _assert_unambiguous_version(self, key: str, version: str) -> None:
        try:
            response = self._client.list_object_versions(
                Bucket=self._bucket,
                Prefix=key,
            )
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 version-history capability is unavailable") from exc
        if response.get("IsTruncated") is True:
            raise ArtifactStoreUnavailable("S3 exact-key version history is truncated")
        versions = tuple(
            item for item in response.get("Versions", ()) if isinstance(item, dict) and item.get("Key") == key
        )
        delete_markers = tuple(
            item for item in response.get("DeleteMarkers", ()) if isinstance(item, dict) and item.get("Key") == key
        )
        if (
            len(versions) != 1
            or delete_markers
            or versions[0].get("VersionId") != version
            or versions[0].get("IsLatest") is not True
        ):
            raise ArtifactStoreUnavailable("S3 create-only version history is ambiguous")

    def _assert_create_inputs(
        self,
        *,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
    ) -> None:
        if not isinstance(content, bytes) or not content:
            raise ValueError("S3 artifact content must be non-empty bytes")
        if len(content) > self._policy.max_artifact_bytes:
            raise ValueError("S3 artifact content exceeds the protected byte budget")
        if _DIGEST_RE.fullmatch(sha256) is None:
            raise ValueError("S3 artifact sha256 is invalid")
        if sha256 != "sha256:" + hashlib_sha256(content).hexdigest():
            raise ValueError("S3 artifact sha256 differs from content")
        if encryption_scope != self._policy.writer_scope:
            raise ValueError("S3 encryption/writer scope differs from protected authority")
        if retention_until != self._policy.retention_until:
            raise ValueError("S3 retention_until differs from protected authority")
        self._assert_retention_unexpired()

    def _assert_retention_unexpired(self) -> None:
        """Revalidate immutable retention authority without shortening replay time."""

        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("S3 retention clock must return a timezone-aware datetime")
        retention = retention_datetime(self._policy.retention_until)
        if retention <= now:
            raise ArtifactStoreUnavailable("S3 protected retention has expired")

    def _assert_key(self, key: str) -> None:
        if not isinstance(key, str) or not key.startswith(self._operation_prefix):
            raise ValueError("artifact key is outside the protected operation prefix")
        if ".." in key.split("/"):
            raise ValueError("artifact key is invalid")


def _status(exc: BaseException) -> object:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, dict):
            return metadata.get("HTTPStatusCode")
        error = response.get("Error")
        if isinstance(error, dict):
            return error.get("Code")
    return getattr(exc, "status_code", None) or getattr(exc, "code", None)


def _client_endpoint_authority_id(client: Any) -> str:
    meta = getattr(client, "meta", None)
    endpoint = getattr(meta, "endpoint_url", None)
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("S3 client endpoint authority is unavailable")
    return endpoint


def _normalized_endpoint(value: str) -> str:
    return value.rstrip("/")


def _retention_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        try:
            return retention_datetime(value).isoformat().replace("+00:00", "Z")
        except ValueError:
            return ""
    return ""


def retention_datetime(value: str) -> datetime:
    """Parse one required timezone-aware retention boundary."""

    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError("S3 retention_until must be timezone-aware ISO-8601") from exc
    if result.tzinfo is None:
        raise ValueError("S3 retention_until must be timezone-aware ISO-8601")
    return result


__all__ = [
    "S3CreateOnlyArtifactStore",
]
