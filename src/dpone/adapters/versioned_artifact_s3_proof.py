"""S3 authority and response proof without network calls or body ownership."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256 as hashlib_sha256

from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef, ArtifactStoreUnavailable
from dpone.ports.versioned_artifact_store import S3VersionedArtifactPolicy

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_UTC = timezone.utc  # noqa: UP017 - supported Python 3.10


class S3ArtifactProof:
    """Validate protected coordinates and exact provider observations."""

    def __init__(
        self,
        *,
        endpoint_authority_id: str,
        bucket: str,
        operation_prefix: str,
        policy: S3VersionedArtifactPolicy,
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
        observed_endpoint = endpoint_authority_id
        if _normalized_endpoint(observed_endpoint) != _normalized_endpoint(policy.endpoint_authority_id):
            raise ValueError("S3 endpoint differs from protected authority")
        self._operation_prefix = normalized
        self._kms_key_id = policy.kms_key_authority_id
        self._encryption_algorithm = encryption_algorithm
        self._policy = policy
        self._clock = clock or (lambda: datetime.now(_UTC))

    def object_ref(self, response: Mapping[str, object], *, key: str, version: str | None) -> ArtifactObjectRef:
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

    def assert_complete_version_history(self, response: Mapping[str, object], *, key: str, version: str) -> None:
        """Reject unknown history entries before filtering exact-key membership."""
        if response.get("IsTruncated") is not False:
            raise ArtifactStoreUnavailable("S3 version history has no complete observation")
        for collection in ("Versions", "DeleteMarkers"):
            for entry in _entries(response, collection):
                if (
                    not isinstance(entry, dict)
                    or not isinstance(entry.get("Key"), str)
                    or not entry["Key"]
                    or not isinstance(entry.get("VersionId"), str)
                    or not entry["VersionId"]
                    or type(entry.get("IsLatest")) is not bool
                ):
                    raise ArtifactStoreUnavailable("S3 version history contains an unknown entry")
        self.assert_version_history(response, key=key, version=version)

    def assert_version_history(self, response: Mapping[str, object], *, key: str, version: str) -> None:
        if response.get("IsTruncated") is True:
            raise ArtifactStoreUnavailable("S3 exact-key version history is truncated")
        versions = tuple(
            item for item in _entries(response, "Versions") if isinstance(item, dict) and item.get("Key") == key
        )
        delete_markers = tuple(
            item for item in _entries(response, "DeleteMarkers") if isinstance(item, dict) and item.get("Key") == key
        )
        if (
            len(versions) != 1
            or delete_markers
            or versions[0].get("VersionId") != version
            or versions[0].get("IsLatest") is not True
        ):
            raise ArtifactStoreUnavailable("S3 create-only version history is ambiguous")

    def assert_create_inputs(
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
        self.assert_retention_unexpired()

    def assert_retention_unexpired(self) -> None:
        """Revalidate immutable retention authority without shortening replay time."""

        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("S3 retention clock must return a timezone-aware datetime")
        retention = retention_datetime(self._policy.retention_until)
        if retention <= now:
            raise ArtifactStoreUnavailable("S3 protected retention has expired")

    def assert_key(self, key: str) -> None:
        if not isinstance(key, str) or not key.startswith(self._operation_prefix):
            raise ValueError("artifact key is outside the protected operation prefix")
        if ".." in key.split("/"):
            raise ValueError("artifact key is invalid")


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


def _entries(response: Mapping[str, object], field: str) -> Sequence[object]:
    value = response.get(field, ())
    if not isinstance(value, (tuple, list)):
        raise ArtifactStoreUnavailable("S3 version history is malformed")
    return value
