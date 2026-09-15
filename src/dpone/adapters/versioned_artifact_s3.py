"""Shared S3 I/O operations and an explicitly budgeted native-facing adapter.

The operations object also serves the historical API with no added budget. Only
VersionedS3ArtifactStore implements the bounded port; native callers must use it.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import datetime
from hashlib import sha256 as hashlib_sha256
from typing import Any, cast

from dpone.adapters.versioned_artifact_s3_proof import S3ArtifactProof, retention_datetime
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreUnavailable,
)
from dpone.ports.versioned_artifact_store import (
    S3ObjectBody,
    S3VersionedArtifactPolicy,
    S3VersionedObjectClient,
    VersionedArtifactIoBudget,
)
from dpone.storage.checksum import is_conditional_create_conflict


class S3ArtifactOperations:
    """One shared algorithm implementation for native and compatibility adapters.

    An omitted budget is reserved for the legacy adapter. It intentionally does
    not establish native bounded-read or observed-GET-version qualification.
    """

    def __init__(
        self,
        *,
        client: S3VersionedObjectClient,
        bucket: str,
        operation_prefix: str,
        policy: S3VersionedArtifactPolicy,
        encryption_algorithm: str,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
    ) -> None:
        self._proof = S3ArtifactProof(
            endpoint_authority_id=_client_endpoint_authority_id(client),
            bucket=bucket,
            operation_prefix=operation_prefix,
            policy=policy,
            encryption_algorithm=encryption_algorithm,
            clock=clock,
        )
        self._client = client
        self._bucket = bucket
        self._policy = policy
        self._kms_key_id = policy.kms_key_authority_id
        self._encryption_algorithm = encryption_algorithm
        self._monotonic_clock = monotonic_clock

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
        budget: VersionedArtifactIoBudget | None = None,
    ) -> ArtifactObjectRef:
        """Conditionally create one final key and require a provider VersionId."""

        self._check(budget)
        if budget is not None and len(content) > budget.max_bytes:
            raise ValueError("S3 content exceeds operation byte budget")
        self._proof.assert_key(key)
        self._proof.assert_create_inputs(
            content=content,
            sha256=sha256,
            encryption_scope=encryption_scope,
            retention_until=retention_until,
        )
        self._assert_bucket_capabilities(budget)
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
            response = self._call(self._client.put_object, budget, **request)
        except Exception as exc:
            if is_conditional_create_conflict(exc):
                raise ArtifactCreateConflict(f"create-only artifact exists: {key}") from exc
            raise ArtifactStoreUnavailable(f"artifact create acknowledgement unavailable: {key}") from exc
        version = response.get("VersionId")
        if not isinstance(version, str) or not version:
            raise ArtifactStoreUnavailable("S3 create did not return a version-pinned identity")
        observed = self._head_ref(key=key, version=version, budget=budget)
        if (
            observed.size_bytes != len(content)
            or observed.sha256 != sha256
            or observed.encryption_scope != encryption_scope
            or observed.retention_until != retention_until
        ):
            raise ArtifactStoreUnavailable("S3 exact-version create metadata differs")
        self._assert_unambiguous_version(key, version, budget)
        return observed

    def head(self, *, key: str, budget: VersionedArtifactIoBudget | None = None) -> ArtifactObjectRef | None:
        """Read create-only metadata; latest is safe only under overwrite-deny policy."""

        self._check(budget)
        self._proof.assert_key(key)
        self._assert_bucket_capabilities(budget)
        try:
            observed = self._head_ref(key=key, version=None, budget=budget)
            self._assert_unambiguous_version(key, observed.version, budget)
            return observed
        except Exception as exc:
            if isinstance(exc, ArtifactStoreUnavailable):
                raise
            status = _status(exc)
            if status in {404, "404", "NoSuchKey", "NotFound"}:
                self._check(budget)
                return None
            raise ArtifactStoreUnavailable(f"artifact HEAD unavailable: {key}") from exc

    def read_version(self, *, key: str, version: str, budget: VersionedArtifactIoBudget | None = None) -> bytes:
        """Read a pinned version; own its body before any response rejection."""
        self._check(budget)
        self._proof.assert_key(key)
        if not isinstance(version, str) or not version:
            raise ValueError("version must be non-empty")
        self._assert_bucket_capabilities(budget)
        try:
            self._check(budget)
            response = self._client.get_object(Bucket=self._bucket, Key=key, VersionId=version)
            body = cast(S3ObjectBody, response["Body"])
            try:
                self._check(budget)
                observed_version = response.get("VersionId", version) if budget is None else response.get("VersionId")
                if observed_version != version:
                    raise ArtifactStoreUnavailable("S3 exact-version read returned another VersionId")
                if budget is None:
                    content = bytes(cast(Any, body).read())
                else:
                    size = response.get("ContentLength")
                    if type(size) is not int or size < 0 or size > budget.max_bytes:
                        raise ArtifactStoreUnavailable(
                            "S3 GET content length exceeds operation byte budget or is invalid"
                        )
                    content = self._read_bounded(body, budget)
                    if len(content) != size:
                        raise ArtifactStoreUnavailable("S3 GET content length differs from complete bytes")
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
                elif budget is not None:
                    raise ArtifactStoreUnavailable("S3 response body has no close capability")
            self._check(budget)
            observed = self._head_ref(key=key, version=version, budget=budget)
            if (
                observed.size_bytes != len(content)
                or observed.sha256 != "sha256:" + hashlib_sha256(content).hexdigest()
            ):
                raise ArtifactStoreUnavailable("S3 exact-version content metadata differs")
            self._assert_unambiguous_version(key, version, budget)
            return content
        except ArtifactStoreUnavailable:
            raise
        except Exception as exc:
            raise ArtifactStoreUnavailable(f"exact artifact version unavailable: {key}@{version}") from exc

    def _read_bounded(self, body: S3ObjectBody, budget: VersionedArtifactIoBudget) -> bytes:
        chunks = bytearray()
        maximum = min(budget.max_bytes, self._policy.max_artifact_bytes)
        while True:
            self._check(budget)
            requested = min(budget.chunk_bytes, maximum - len(chunks) + 1)
            chunk = body.read(requested)
            self._check(budget)
            if type(chunk) is not bytes or len(chunk) > requested:
                raise ArtifactStoreUnavailable("S3 response body violated bounded byte read")
            if not chunk:
                return bytes(chunks)
            if len(chunk) > maximum - len(chunks):
                raise ArtifactStoreUnavailable("S3 response exceeds operation byte budget")
            chunks.extend(chunk)

    def _assert_bucket_capabilities(self, budget: VersionedArtifactIoBudget | None) -> None:
        self._proof.assert_retention_unexpired()
        if self._policy.conditional_create_authorized is not True:
            raise ArtifactStoreUnavailable("S3 conditional create capability is not authorized")
        try:
            versioning = self._call(self._client.get_bucket_versioning, budget, Bucket=self._bucket)
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 bucket versioning capability is unavailable") from exc
        if versioning.get("Status") != "Enabled":
            raise ArtifactStoreUnavailable("S3 bucket versioning is not Enabled")
        if not self._policy.require_object_lock:
            return
        try:
            raw = self._call(self._client.get_object_lock_configuration, budget, Bucket=self._bucket)
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 object lock capability is unavailable") from exc
        configuration = raw.get("ObjectLockConfiguration")
        if not isinstance(configuration, dict) or configuration.get("ObjectLockEnabled") != "Enabled":
            raise ArtifactStoreUnavailable("S3 object lock is not Enabled")

    def _head_ref(
        self, *, key: str, version: str | None, budget: VersionedArtifactIoBudget | None
    ) -> ArtifactObjectRef:
        request: dict[str, object] = {"Bucket": self._bucket, "Key": key}
        if version is not None:
            request["VersionId"] = version
        response = self._call(self._client.head_object, budget, **request)
        observed = self._proof.object_ref(response, key=key, version=version)
        if budget is not None and observed.size_bytes > min(budget.max_bytes, self._policy.max_artifact_bytes):
            raise ArtifactStoreUnavailable("S3 HEAD size exceeds operation byte budget")
        if budget is not None and (
            observed.encryption_scope != self._policy.writer_scope
            or observed.retention_until != self._policy.retention_until
        ):
            raise ArtifactStoreUnavailable("S3 object writer/retention authority differs from policy")
        return observed

    def _assert_unambiguous_version(self, key: str, version: str, budget: VersionedArtifactIoBudget | None) -> None:
        try:
            response = self._call(self._client.list_object_versions, budget, Bucket=self._bucket, Prefix=key)
        except Exception as exc:
            raise ArtifactStoreUnavailable("S3 version-history capability is unavailable") from exc
        if budget is None:
            self._proof.assert_version_history(response, key=key, version=version)
        else:
            self._proof.assert_complete_version_history(response, key=key, version=version)
        self._check(budget)

    def _call(
        self, method: Callable[..., Mapping[str, object]], budget: VersionedArtifactIoBudget | None, **request: object
    ) -> Mapping[str, object]:
        self._check(budget)
        response = method(**request)
        self._check(budget)
        return response

    def _check(self, budget: VersionedArtifactIoBudget | None) -> None:
        if budget is not None:
            now = self._monotonic_clock()
            if type(now) not in (int, float) or not math.isfinite(now) or now >= budget.deadline_monotonic:
                raise ArtifactStoreUnavailable("S3 operation deadline expired or clock is invalid")


class VersionedS3ArtifactStore:
    """Bounded native API; clocks/client are supplied by authenticated composition."""

    def __init__(
        self,
        *,
        client: S3VersionedObjectClient,
        bucket: str,
        operation_prefix: str,
        policy: S3VersionedArtifactPolicy,
        encryption_algorithm: str = "aws:kms",
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
    ) -> None:
        self._operations = S3ArtifactOperations(
            client=client,
            bucket=bucket,
            operation_prefix=operation_prefix,
            policy=policy,
            encryption_algorithm=encryption_algorithm,
            clock=clock,
            monotonic_clock=monotonic_clock,
        )

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
        budget: VersionedArtifactIoBudget,
    ) -> ArtifactObjectRef:
        return self._operations.create(
            key=key,
            content=content,
            sha256=sha256,
            encryption_scope=encryption_scope,
            retention_until=retention_until,
            budget=_require_budget(budget),
        )

    def head(self, *, key: str, budget: VersionedArtifactIoBudget) -> ArtifactObjectRef | None:
        return self._operations.head(key=key, budget=_require_budget(budget))

    def read_version(self, *, key: str, version: str, budget: VersionedArtifactIoBudget) -> bytes:
        return self._operations.read_version(key=key, version=version, budget=_require_budget(budget))


def _require_budget(value: VersionedArtifactIoBudget) -> VersionedArtifactIoBudget:
    if type(value) is not VersionedArtifactIoBudget:
        raise TypeError("native versioned artifact I/O requires an explicit budget")
    value.__post_init__()
    return value


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
