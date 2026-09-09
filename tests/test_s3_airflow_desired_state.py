from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

import pytest

from dpone.adapters.s3_airflow_desired_state import (
    S3AirflowDesiredStateStore,
    parse_s3_desired_state_uri,
)
from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.ports.airflow_desired_state import (
    DesiredStateConditionalWriteConflict,
    DesiredStateReadStatus,
    DesiredStateReadUnavailable,
    DesiredStateWriteContention,
    DesiredStateWriteUncertain,
)


class _ClientError(RuntimeError):
    def __init__(self, status: int, code: str = "") -> None:
        self.response = {
            "ResponseMetadata": {"HTTPStatusCode": status},
            "Error": {"Code": code},
        }


class _ReadClient:
    def __init__(self, response: dict[str, object] | Exception) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []
        self.meta = SimpleNamespace(endpoint_url="https://storage.yandexcloud.net")

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class _WriteClient:
    def __init__(self, response: dict[str, object] | Exception) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []
        self.meta = SimpleNamespace(endpoint_url="https://storage.yandexcloud.net")

    def put_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _store(client: object) -> S3AirflowDesiredStateStore:
    return S3AirflowDesiredStateStore(
        client=client,
        uri=parse_s3_desired_state_uri("s3://bucket/control/dev/desired.json"),
        certified_endpoint_url="https://storage.yandexcloud.net",
    )


def test_read_is_bounded_and_preserves_opaque_etag() -> None:
    body = BytesIO(b'{"schema":"value"}')
    client = _ReadClient({"Body": body, "ETag": '"multipart-etag-7"'})

    result = _store(client).read(max_bytes=1024)

    assert result.status is DesiredStateReadStatus.PRESENT
    assert result.body == b'{"schema":"value"}'
    assert result.revision == DesiredStateRevision('"multipart-etag-7"')
    assert client.calls == [{"Bucket": "bucket", "Key": "control/dev/desired.json"}]
    assert body.closed


def test_conditional_read_returns_unchanged_and_absent() -> None:
    revision = DesiredStateRevision('"etag"')
    unchanged = _ReadClient(_ClientError(304, "NotModified"))
    absent = _ReadClient(_ClientError(404, "NoSuchKey"))

    assert _store(unchanged).read(max_bytes=100, if_changed_from=revision).status is (DesiredStateReadStatus.UNCHANGED)
    assert unchanged.calls[0]["IfNoneMatch"] == '"etag"'
    assert _store(absent).read(max_bytes=100).status is DesiredStateReadStatus.ABSENT


def test_oversized_or_malformed_read_fails_closed() -> None:
    with pytest.raises(DesiredStateReadUnavailable, match="read limit"):
        _store(_ReadClient({"Body": BytesIO(b"1234"), "ETag": '"etag"'})).read(max_bytes=3)
    with pytest.raises(DesiredStateReadUnavailable, match="read failed"):
        _store(_ReadClient(RuntimeError("credential payload"))).read(max_bytes=3)


def test_create_and_replace_use_only_conditional_puts() -> None:
    create_client = _WriteClient({"ETag": '"created"'})
    replace_client = _WriteClient({"ETag": '"replaced"'})

    created = _store(create_client).create_if_absent(b"{}")
    replaced = _store(replace_client).replace_if_revision(DesiredStateRevision('"old"'), b"{}")

    assert created.revision.value == '"created"'
    assert create_client.calls[0]["IfNoneMatch"] == "*"
    assert "IfMatch" not in create_client.calls[0]
    assert replaced.revision.value == '"replaced"'
    assert replace_client.calls[0]["IfMatch"] == '"old"'
    assert "IfNoneMatch" not in replace_client.calls[0]


def test_unknown_or_mismatched_endpoint_is_rejected_before_write() -> None:
    unknown = _WriteClient({"ETag": '"unused"'})
    unknown.meta.endpoint_url = "https://storage.example.test"

    with pytest.raises(ValueError, match="not certified"):
        S3AirflowDesiredStateStore(
            client=unknown,
            uri=parse_s3_desired_state_uri("s3://bucket/control/dev/desired.json"),
            certified_endpoint_url="https://storage.example.test",
        )
    assert unknown.calls == []

    mismatched = _WriteClient({"ETag": '"unused"'})
    with pytest.raises(ValueError, match="does not match"):
        S3AirflowDesiredStateStore(
            client=mismatched,
            uri=parse_s3_desired_state_uri("s3://bucket/control/dev/desired.json"),
            certified_endpoint_url="https://s3.amazonaws.com",
        )
    assert mismatched.calls == []


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (_ClientError(412, "PreconditionFailed"), DesiredStateConditionalWriteConflict),
        (_ClientError(409, "ConditionalRequestConflict"), DesiredStateWriteContention),
        (TimeoutError("timed out"), DesiredStateWriteUncertain),
    ],
)
def test_write_failures_are_classified_without_vendor_payload(
    exception: Exception,
    expected: type[Exception],
) -> None:
    with pytest.raises(expected):
        _store(_WriteClient(exception)).create_if_absent(b"{}")


@pytest.mark.parametrize(
    "response",
    [
        {},
        RuntimeError("unknown SDK failure with credential payload"),
    ],
)
def test_unknown_post_dispatch_outcome_is_always_uncertain(
    response: dict[str, object] | Exception,
) -> None:
    with pytest.raises(DesiredStateWriteUncertain):
        _store(_WriteClient(response)).create_if_absent(b"{}")


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket/control/dev/desired.json",
        "s3://bucket",
        "s3://bucket/control/dev/",
        "s3://bucket/control/../desired.json",
        "s3://user:pass@bucket/control/dev/desired.json",
    ],
)
def test_desired_state_uri_rejects_wrong_provider_and_unsafe_paths(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_desired_state_uri(uri)
