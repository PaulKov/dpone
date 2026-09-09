"""S3 conditional-object adapter for Airflow desired state."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.ports.airflow_desired_state import (
    DesiredStateConditionalWriteConflict,
    DesiredStatePortError,
    DesiredStateReadResult,
    DesiredStateReadUnavailable,
    DesiredStateWriteContention,
    DesiredStateWriteResult,
    DesiredStateWriteUncertain,
)
from dpone.storage.models import ObjectStorageProvider, ObjectStorageUri


class S3AirflowDesiredStateStore:
    """Read and conditionally replace one allowlisted S3 object."""

    def __init__(
        self,
        *,
        client: Any,
        uri: ObjectStorageUri,
        certified_endpoint_url: str,
    ) -> None:
        _validate_uri(uri)
        _validate_conditional_write_endpoint(
            configured=certified_endpoint_url,
            actual=_client_endpoint_url(client),
        )
        self._client = client
        self._uri = uri

    def read(
        self,
        *,
        max_bytes: int,
        if_changed_from: DesiredStateRevision | None = None,
    ) -> DesiredStateReadResult:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        kwargs: dict[str, object] = {
            "Bucket": self._uri.bucket,
            "Key": self._uri.key,
        }
        if if_changed_from is not None:
            kwargs["IfNoneMatch"] = if_changed_from.value
        try:
            response = self._client.get_object(**kwargs)
            revision = _revision(response)
            body = response["Body"]
            try:
                payload = body.read(max_bytes + 1)
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
        except Exception as exc:  # noqa: BLE001 - optional SDK details stay private.
            status, code = _error_identity(exc)
            if status == 304 or code == "NotModified":
                if if_changed_from is None:
                    raise DesiredStateReadUnavailable("unexpected conditional read result") from exc
                return DesiredStateReadResult.unchanged(if_changed_from)
            if status == 404 or code in {"NoSuchKey", "NotFound"}:
                return DesiredStateReadResult.absent()
            raise DesiredStateReadUnavailable("desired-state read failed") from exc
        if not isinstance(payload, bytes):
            raise DesiredStateReadUnavailable("desired-state body is not binary")
        if len(payload) > max_bytes:
            raise DesiredStateReadUnavailable("desired-state body exceeds the configured read limit")
        return DesiredStateReadResult.present(payload, revision)

    def create_if_absent(self, body: bytes) -> DesiredStateWriteResult:
        return self._put(body=body, if_none_match="*")

    def replace_if_revision(
        self,
        expected_revision: DesiredStateRevision,
        body: bytes,
    ) -> DesiredStateWriteResult:
        return self._put(body=body, if_match=expected_revision.value)

    def _put(
        self,
        *,
        body: bytes,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> DesiredStateWriteResult:
        if not isinstance(body, bytes) or not body:
            raise ValueError("desired-state body must be non-empty bytes")
        if (if_match is None) == (if_none_match is None):
            raise ValueError("exactly one conditional write precondition is required")
        kwargs: dict[str, object] = {
            "Bucket": self._uri.bucket,
            "Key": self._uri.key,
            "Body": body,
            "ContentType": "application/json",
        }
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        else:
            kwargs["IfNoneMatch"] = if_none_match
        try:
            response = self._client.put_object(**kwargs)
        except Exception as exc:  # noqa: BLE001 - optional SDK details stay private.
            status, _ = _error_identity(exc)
            if status == 412:
                raise DesiredStateConditionalWriteConflict("conditional desired-state write lost") from exc
            if status == 409:
                raise DesiredStateWriteContention("conditional desired-state write contended") from exc
            raise DesiredStateWriteUncertain("conditional desired-state write outcome is uncertain") from exc
        try:
            revision = _revision(response)
        except DesiredStatePortError as exc:
            raise DesiredStateWriteUncertain("conditional desired-state write revision is unavailable") from exc
        return DesiredStateWriteResult(revision)


def parse_s3_desired_state_uri(value: str) -> ObjectStorageUri:
    """Parse one credential-free S3 object URI."""

    try:
        uri = ObjectStorageUri.parse(value)
    except ValueError as exc:
        raise ValueError("desired-state URI is invalid") from exc
    _validate_uri(uri)
    return uri


def _validate_uri(uri: ObjectStorageUri) -> None:
    if uri.provider is not ObjectStorageProvider.S3:
        raise ValueError("S3 desired-state adapter requires an s3:// URI")
    if (
        not uri.bucket
        or not uri.key
        or uri.key.endswith("/")
        or "\\" in uri.key
        or any(part in {"", ".", ".."} for part in uri.key.split("/"))
    ):
        raise ValueError("desired-state URI must identify one canonical S3 object")


def _revision(response: object) -> DesiredStateRevision:
    if not isinstance(response, dict):
        raise DesiredStatePortError("object storage response is invalid")
    value = response.get("ETag")
    if not isinstance(value, str) or not value:
        raise DesiredStatePortError("object storage response omitted the opaque revision")
    return DesiredStateRevision(value)


def _error_identity(exc: BaseException) -> tuple[int | None, str]:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None, ""
    metadata = response.get("ResponseMetadata")
    error = response.get("Error")
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    code = error.get("Code") if isinstance(error, dict) else ""
    return (int(status) if isinstance(status, int) else None, str(code or ""))


def _client_endpoint_url(client: Any) -> str:
    meta = getattr(client, "meta", None)
    value = getattr(meta, "endpoint_url", None)
    if not isinstance(value, str) or not value:
        raise ValueError("S3 conditional-write endpoint authority is unavailable")
    return value


def _validate_conditional_write_endpoint(*, configured: str, actual: str) -> None:
    configured_authority = _certified_endpoint_authority(configured)
    actual_authority = _endpoint_authority(actual)
    if actual_authority != configured_authority:
        raise ValueError("actual S3 endpoint does not match the certified endpoint")


def _certified_endpoint_authority(value: str) -> str:
    authority = _endpoint_authority(value)
    host = urlparse(value).hostname or ""
    aws = host == "s3.amazonaws.com" or (host.startswith("s3.") and host.endswith(".amazonaws.com"))
    if host != "storage.yandexcloud.net" and not aws:
        raise ValueError("S3 conditional writes are not certified for this endpoint")
    return authority


def _endpoint_authority(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("S3 conditional-write endpoint must be a credential-free HTTPS URL")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("S3 conditional-write endpoint must contain only an authority")
    port = "" if parsed.port is None else f":{parsed.port}"
    return f"{parsed.hostname.lower()}{port}"


__all__ = ["S3AirflowDesiredStateStore", "parse_s3_desired_state_uri"]
