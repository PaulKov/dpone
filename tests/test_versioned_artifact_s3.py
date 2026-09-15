"""Bounded shared S3 operations using injected clients, clocks and streaming bodies."""

from __future__ import annotations

from collections.abc import Callable
from io import BytesIO

import pytest

from dpone.adapters.versioned_artifact_s3 import VersionedS3ArtifactStore
from dpone.ports.semantic_refresh_artifact_store import ArtifactCreateConflict, ArtifactStoreUnavailable
from dpone.ports.versioned_artifact_store import VersionedArtifactIoBudget
from tests.test_semantic_refresh_artifact_s3 import _PREFIX, _Client, _clock, _policy


@pytest.mark.parametrize(
    "values",
    [
        (True, 1, 10.0),
        (0, 1, 10.0),
        (8, 9, 10.0),
        (8, False, 10.0),
        (8, 1, float("inf")),
        (8, 1, float("nan")),
        (8, 1, 10),
    ],
)
def test_budget_rejects_unbounded_or_coerced_values(values: tuple[object, object, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        VersionedArtifactIoBudget(*values)  # type: ignore[arg-type]


class RecordingBody(BytesIO):
    def __init__(self, content: bytes, tick: Callable[[str], None]) -> None:
        super().__init__(content)
        self.sizes: list[int] = []
        self.closed_count = 0
        self.tick = tick
        self.read_error: BaseException | None = None
        self.close_error = False
        self.invalid_chunk: object = None

    def read(self, size: int = -1) -> bytes:
        self.sizes.append(size)
        self.tick("read")
        if self.read_error is not None:
            raise self.read_error
        if self.invalid_chunk is not None:
            return self.invalid_chunk  # type: ignore[return-value]
        # Exercise short reads, independent of requested chunk size.
        return super().read(min(size, 2))

    def close(self) -> None:
        self.closed_count += 1
        super().close()
        self.tick("close")
        if self.close_error:
            raise OSError("close failed")


class Client(_Client):
    def __init__(self, content: bytes = b"payload") -> None:
        super().__init__()
        self.now = 0.0
        self.expire_on: str | None = None
        self.calls: list[str] = []
        self.body = RecordingBody(content, self.tick)
        self.response_version: str | None = "version-1"
        self.content_length: object = 7
        self.history: dict[str, object] | None = None
        self.head_overrides: dict[str, object] = {}
        self.put_error = False

    def tick(self, stage: str) -> None:
        self.calls.append(stage)
        if self.expire_on == stage:
            self.now = 10.0

    def get_bucket_versioning(self, **kwargs: object) -> dict[str, str]:
        self.tick("versioning")
        return super().get_bucket_versioning(**kwargs)

    def get_object_lock_configuration(self, **kwargs: object) -> dict[str, object]:
        self.tick("lock")
        return super().get_object_lock_configuration(**kwargs)

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["VersionId"] == "version-1"
        self.tick("get")
        response: dict[str, object] = {"Body": self.body, "ContentLength": self.content_length}
        if self.response_version is not None:
            response["VersionId"] = self.response_version
        return response

    def head_object(self, **kwargs: object) -> dict[str, object]:
        self.tick("head")
        response = super().head_object(**kwargs)
        response.update(self.head_overrides)
        return response

    def list_object_versions(self, **kwargs: object) -> dict[str, object]:
        self.tick("history")
        return self.history if self.history is not None else super().list_object_versions(**kwargs)

    def put_object(self, **kwargs: object) -> dict[str, str]:
        self.tick("put")
        if self.put_error:
            raise OSError("lost acknowledgement")
        return super().put_object(**kwargs)


def store(client: Client) -> VersionedS3ArtifactStore:
    return VersionedS3ArtifactStore(
        client=client,
        bucket="dpone-semantic-refresh",
        operation_prefix=_PREFIX,
        policy=_policy(),
        clock=_clock,
        monotonic_clock=lambda: client.now,
    )


def read(client: Client, *, maximum: int = 7) -> bytes:
    return store(client).read_version(
        key=f"{_PREFIX}/manifest.json",
        version="version-1",
        budget=VersionedArtifactIoBudget(maximum, min(maximum, 3), 10.0),
    )


def test_read_exact_byte_cap_checks_eof_and_closes_once() -> None:
    client = Client()
    assert read(client) == b"payload"
    assert client.body.sizes == [3, 3, 3, 2, 1]
    assert client.body.closed_count == 1
    assert client.calls[-3:] == ["close", "head", "history"]


@pytest.mark.parametrize("content", [b"payloa", b"payload!", b"PAYLOAD"])
def test_incomplete_excess_or_different_bytes_fail_and_close(content: bytes) -> None:
    client = Client(content)
    with pytest.raises(ArtifactStoreUnavailable):
        read(client)
    assert client.body.closed_count == 1


@pytest.mark.parametrize("version", [None, "wrong-version"])
def test_native_get_requires_observed_version_and_closes_before_read(version: str | None) -> None:
    client = Client()
    client.response_version = version
    with pytest.raises(ArtifactStoreUnavailable, match="VersionId"):
        read(client)
    assert client.body.sizes == []
    assert client.body.closed_count == 1


@pytest.mark.parametrize("length", [None, True, -1, 8, "7"])
def test_invalid_get_length_fails_before_read(length: object) -> None:
    client = Client()
    client.content_length = length
    with pytest.raises(ArtifactStoreUnavailable, match="length"):
        read(client)
    assert client.body.sizes == []
    assert client.body.closed_count == 1


@pytest.mark.parametrize("stage", ["versioning", "lock", "get", "read", "close", "head", "history"])
def test_same_absolute_deadline_covers_all_io_and_cleanup(stage: str) -> None:
    client = Client()
    client.expire_on = stage
    with pytest.raises(ArtifactStoreUnavailable):
        read(client)
    assert client.now == 10.0
    assert client.calls[-1] == ("close" if stage in {"get", "read"} else stage)
    assert client.body.closed_count == (0 if stage in {"versioning", "lock"} else 1)


def test_expired_budget_prevents_all_provider_calls() -> None:
    client = Client()
    client.now = 10.0
    with pytest.raises(ArtifactStoreUnavailable, match="deadline"):
        read(client)
    assert client.calls == []


@pytest.mark.parametrize("chunk", [bytearray(b"a"), "a", b"too much data", 1])
def test_body_must_honor_positive_bounded_bytes_read(chunk: object) -> None:
    client = Client()
    client.body.invalid_chunk = chunk
    with pytest.raises(ArtifactStoreUnavailable, match="bounded"):
        read(client)
    assert client.body.closed_count == 1


@pytest.mark.parametrize("failure", [OSError("broken framing"), KeyboardInterrupt()])
def test_read_errors_and_interrupts_close_without_success(failure: BaseException) -> None:
    client = Client()
    client.body.read_error = failure
    with pytest.raises(ArtifactStoreUnavailable if isinstance(failure, OSError) else KeyboardInterrupt):
        read(client)
    assert client.body.closed_count == 1


def test_close_failure_cannot_report_success() -> None:
    client = Client()
    client.body.close_error = True
    with pytest.raises(ArtifactStoreUnavailable):
        read(client)
    assert "head" not in client.calls


@pytest.mark.parametrize("scenario", ["conflict", "lost_ack", "late_ack", "bad_metadata", "two_versions"])
def test_put_or_proof_failure_never_retries_mutation(scenario: str) -> None:
    client = Client()
    client.conflict = scenario == "conflict"
    client.put_error = scenario == "lost_ack"
    client.expire_on = "put" if scenario == "late_ack" else None
    client.encryption = "AES256" if scenario == "bad_metadata" else "aws:kms"
    client.extra_version = scenario == "two_versions"
    with pytest.raises(ArtifactCreateConflict if scenario == "conflict" else ArtifactStoreUnavailable):
        store(client).create(
            key=f"{_PREFIX}/manifest.json",
            content=b"payload",
            sha256=client.sha256,
            encryption_scope="business-sensitive",
            retention_until=client.retention_until,
            budget=VersionedArtifactIoBudget(7, 3, 10.0),
        )
    assert client.calls.count("put") == 1


def test_create_and_head_retain_exact_existing_artifact_reference() -> None:
    client = Client()
    bounded = store(client)
    budget = VersionedArtifactIoBudget(7, 3, 10.0)
    reference = bounded.create(
        key=f"{_PREFIX}/manifest.json",
        content=b"payload",
        sha256=client.sha256,
        encryption_scope="business-sensitive",
        retention_until=client.retention_until,
        budget=budget,
    )
    assert bounded.head(key=reference.key, budget=budget) == reference
    assert client.created is not None and client.created["IfNoneMatch"] == "*"


@pytest.mark.parametrize("method", ["head", "read_version", "create"])
def test_native_methods_reject_absent_budget_before_provider_calls(method: str) -> None:
    client = Client()
    arguments: dict[str, object] = {"key": f"{_PREFIX}/manifest.json", "budget": None}
    if method == "read_version":
        arguments["version"] = "version-1"
    elif method == "create":
        arguments.update(
            content=b"payload",
            sha256=client.sha256,
            encryption_scope="business-sensitive",
            retention_until=client.retention_until,
        )
    with pytest.raises(TypeError, match="explicit budget"):
        getattr(store(client), method)(**arguments)
    assert client.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("ContentLength", 8),
        ("ContentLength", True),
        ("VersionId", "wrong"),
        ("ServerSideEncryption", "AES256"),
        ("SSEKMSKeyId", "another-key"),
        ("ObjectLockMode", "COMPLIANCE"),
    ],
)
def test_native_head_rejects_size_version_or_encryption_drift(field: str, value: object) -> None:
    client = Client()
    client.head_overrides[field] = value
    with pytest.raises(ArtifactStoreUnavailable):
        read(client)
    assert client.body.closed_count == 1


@pytest.mark.parametrize("scenario", ["truncated", "two", "deleted", "not_latest", "none", "malformed_flag"])
def test_native_history_requires_unambiguous_complete_observation(scenario: str) -> None:
    client = Client()
    key = f"{_PREFIX}/manifest.json"
    entry = {"Key": key, "VersionId": "version-1", "IsLatest": scenario != "not_latest"}
    client.history = {
        "Versions": [] if scenario == "none" else [entry],
        "IsTruncated": "false" if scenario == "malformed_flag" else scenario == "truncated",
        "DeleteMarkers": [{"Key": key}] if scenario == "deleted" else [],
    }
    if scenario == "two":
        client.history["Versions"] = [entry, dict(entry)]
    with pytest.raises(ArtifactStoreUnavailable):
        read(client)
    assert client.calls.count("history") == 1


def test_native_read_retention_must_equal_authenticated_policy() -> None:
    client = Client()
    client.retention_until = "2026-08-16T00:00:00Z"
    client.object_lock_retention = client.retention_until
    with pytest.raises(ArtifactStoreUnavailable, match="retention"):
        read(client)


def test_policy_protocol_legacy_import_and_pickle_identity() -> None:
    import pickle

    from dpone.ports.semantic_refresh_s3_policy import SemanticRefreshS3ArtifactStorePolicy
    from dpone.ports.versioned_artifact_store import S3VersionedArtifactPolicy

    assert SemanticRefreshS3ArtifactStorePolicy is S3VersionedArtifactPolicy
    assert pickle.loads(pickle.dumps(S3VersionedArtifactPolicy)) is S3VersionedArtifactPolicy


@pytest.mark.parametrize("collection", ["Versions", "DeleteMarkers"])
@pytest.mark.parametrize(
    "malformed",
    [
        None,
        {},
        {"VersionId": "other", "IsLatest": False},
        {"Key": "neighbor", "VersionId": None, "IsLatest": False},
        {"Key": "neighbor", "VersionId": "other", "IsLatest": "false"},
    ],
)
def test_native_history_does_not_filter_away_unknown_entries(collection: str, malformed: object) -> None:
    client = Client()
    key = f"{_PREFIX}/manifest.json"
    entry = {"Key": key, "VersionId": "version-1", "IsLatest": True}
    client.history = {"IsTruncated": False, "Versions": [entry], "DeleteMarkers": []}
    client.history[collection] = [entry, malformed] if collection == "Versions" else [malformed]
    with pytest.raises(ArtifactStoreUnavailable, match="history"):
        read(client)
    assert client.body.closed_count == 1
