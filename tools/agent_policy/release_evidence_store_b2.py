"""Append-only release evidence store adapters (in-memory + Backblaze B2)."""

from __future__ import annotations

import base64
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

_TRANSIENT_HTTP = frozenset({408, 429, 500, 502, 503, 504})


class EvidenceStore(Protocol):
    """Minimal append/list port used by the lease service."""

    def list_receipts(self, release_identity_id: str) -> list[dict[str, Any]]:
        """Return stream receipts ordered by ascending sequence."""

    def append_receipt(
        self,
        envelope: dict[str, Any],
        *,
        retention_days: int,
    ) -> dict[str, Any]:
        """Append one immutable receipt and return store metadata."""


def object_key_for(*, release_identity_id: str, sequence: int, receipt_id: str) -> str:
    """Deterministic object key under the release stream prefix."""

    digest = receipt_id.removeprefix("sha256:")
    return f"streams/{release_identity_id}/{sequence:010d}-{digest}.json"


@dataclass
class InMemoryEvidenceStore:
    """Test double with CAS-friendly ordered appends."""

    _streams: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def list_receipts(self, release_identity_id: str) -> list[dict[str, Any]]:
        return list(self._streams.get(release_identity_id, []))

    def append_receipt(
        self,
        envelope: dict[str, Any],
        *,
        retention_days: int,
    ) -> dict[str, Any]:
        del retention_days
        release_id = str(envelope["stream"]["release_identity_id"])
        sequence = int(envelope["stream"]["sequence"])
        current = self._streams.setdefault(release_id, [])
        if sequence != len(current):
            raise ValueError(f"SEQUENCE_CAS_FAILED expected={len(current)} got={sequence}")
        if sequence == 0:
            if envelope["stream"]["previous"] != "GENESIS":
                raise ValueError("GENESIS_REQUIRED")
        else:
            if envelope["stream"]["previous"] != current[-1]["receipt_id"]:
                raise ValueError("PREVIOUS_CAS_FAILED")
        current.append(dict(envelope))
        return {
            "object_key": object_key_for(
                release_identity_id=release_id,
                sequence=sequence,
                receipt_id=str(envelope["receipt_id"]),
            ),
            "status": "APPENDED",
        }


@dataclass(frozen=True)
class B2Credentials:
    key_id: str
    application_key: str
    bucket_id: str
    bucket_name: str


class BackblazeB2EvidenceStore:
    """B2 Object Lock store using native upload + list APIs."""

    def __init__(self, credentials: B2Credentials, *, opener: Any = None) -> None:
        self._credentials = credentials
        self._opener = opener or urllib.request.urlopen
        self._api_url = ""
        self._auth_token = ""
        self._authorize()

    def list_receipts(self, release_identity_id: str) -> list[dict[str, Any]]:
        prefix = f"streams/{release_identity_id}/"
        names: list[tuple[int, str, str]] = []
        start: str | None = None
        while True:
            body: dict[str, Any] = {
                "bucketId": self._credentials.bucket_id,
                "prefix": prefix,
                "maxFileCount": 1000,
            }
            if start is not None:
                body["startFileName"] = start
            payload = self._json_post(f"{self._api_url}/b2api/v2/b2_list_file_names", body)
            for item in payload.get("files") or []:
                name = str(item["fileName"])
                sequence = int(name.rsplit("/", 1)[-1].split("-", 1)[0])
                names.append((sequence, name, str(item["fileId"])))
            next_name = payload.get("nextFileName")
            if not next_name:
                break
            start = str(next_name)
        names.sort(key=lambda row: row[0])
        receipts: list[dict[str, Any]] = []
        for _, file_name, file_id in names:
            raw = self._download_file_by_id(file_id)
            receipts.append(json.loads(raw.decode("utf-8")))
            del file_name
        return receipts

    def append_receipt(
        self,
        envelope: dict[str, Any],
        *,
        retention_days: int,
    ) -> dict[str, Any]:
        release_id = str(envelope["stream"]["release_identity_id"])
        sequence = int(envelope["stream"]["sequence"])
        key = object_key_for(
            release_identity_id=release_id,
            sequence=sequence,
            receipt_id=str(envelope["receipt_id"]),
        )
        body = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        retain_until = int(
            (
                datetime.now(timezone.utc) + timedelta(days=max(1, retention_days))  # noqa: UP017
            ).timestamp()
            * 1000
        )
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                upload = self._json_post(
                    f"{self._api_url}/b2api/v2/b2_get_upload_url",
                    {"bucketId": self._credentials.bucket_id},
                )
                headers = {
                    "Authorization": str(upload["authorizationToken"]),
                    "X-Bz-File-Name": urllib.parse.quote(key),
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                    "X-Bz-Content-Sha1": hashlib.sha1(body).hexdigest(),
                    "X-Bz-File-Retention-Mode": "compliance",
                    "X-Bz-File-Retention-Retain-Until-Timestamp": str(retain_until),
                }
                request = urllib.request.Request(str(upload["uploadUrl"]), data=body, method="POST", headers=headers)
                with self._opener(request) as response:
                    result = json.load(response)
                return {
                    "object_key": key,
                    "file_id": result.get("fileId"),
                    "status": "APPENDED",
                    "file_retention": result.get("fileRetention"),
                }
            except urllib.error.HTTPError as exc:
                last_error = exc
                if int(exc.code) not in _TRANSIENT_HTTP or attempt == 4:
                    detail = exc.read().decode("utf-8", errors="replace")
                    raise RuntimeError(f"B2_HTTP_{exc.code}:{detail}") from exc
                time.sleep(0.5 * (2**attempt))
            except RuntimeError as exc:
                last_error = exc
                if "B2_HTTP_" not in str(exc) or attempt == 4:
                    raise
                code = int(str(exc).split("B2_HTTP_", 1)[1].split(":", 1)[0])
                if code not in _TRANSIENT_HTTP:
                    raise
                time.sleep(0.5 * (2**attempt))
        assert last_error is not None
        raise last_error

    def _authorize(self) -> None:
        token = base64.b64encode(f"{self._credentials.key_id}:{self._credentials.application_key}".encode()).decode()
        request = urllib.request.Request(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            headers={"Authorization": f"Basic {token}"},
        )
        with self._opener(request) as response:
            payload = json.load(response)
        self._api_url = str(payload["apiUrl"])
        self._auth_token = str(payload["authorizationToken"])
        allowed = payload.get("allowed") or {}
        if allowed.get("bucketId") not in (None, self._credentials.bucket_id):
            raise ValueError("B2_BUCKET_MISMATCH")

    def _json_post(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": self._auth_token,
                "Content-Type": "application/json",
            },
        )
        try:
            with self._opener(request) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"B2_HTTP_{exc.code}:{detail}") from exc

    def _download_file_by_id(self, file_id: str) -> bytes:
        request = urllib.request.Request(
            f"{self._api_url}/b2api/v2/b2_download_file_by_id?fileId={urllib.parse.quote(file_id)}",
            headers={"Authorization": self._auth_token},
        )
        with self._opener(request) as response:
            return response.read()
