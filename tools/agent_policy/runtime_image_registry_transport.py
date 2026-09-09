"""Authenticated GHCR transport for digest-bound runtime-image promotion."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import sleep
from typing import Any

CANONICAL_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
STABLE_SEMVER = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
SAFE_ACTOR = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\[\]-]{0,127}$")
SAFE_TAG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
IMAGE = re.compile(
    r"^ghcr\.io/(?P<repository>[a-z0-9]+(?:[._-][a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*)$"
)
RETRYABLE_STATUS = frozenset({408, 425, 429, *range(500, 600)})
SUPPORTED_MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
ACCEPT_MANIFESTS = ", ".join(SUPPORTED_MANIFEST_MEDIA_TYPES)
MAX_SOURCE_MANIFEST_BYTES = 4_194_304
INSPECT = ("docker", "buildx", "imagetools", "inspect")
CREATE = ("docker", "buildx", "imagetools", "create")


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes = b""


@dataclass(frozen=True)
class RegistryLookup:
    status_code: int | None
    headers: dict[str, str]
    authenticated: bool
    error_code: str | None = None
    version: str | None = None


@dataclass(frozen=True)
class RegistryCommand:
    ok: bool
    error_code: str | None = None


def _default_requester(method: str, url: str, headers: dict[str, str]) -> HttpResponse:
    request = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310 - fixed HTTPS GHCR host
            return HttpResponse(
                status_code=response.status,
                headers=dict(response.headers.items()),
                body=response.read(MAX_SOURCE_MANIFEST_BYTES + 1),
            )
    except urllib.error.HTTPError as exc:
        return HttpResponse(
            exc.code,
            dict(exc.headers.items()),
            exc.read(MAX_SOURCE_MANIFEST_BYTES + 1),
        )


def _default_runner(command: tuple[str, ...]) -> Any:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )


def _headers(values: Mapping[str, str]) -> dict[str, str]:
    return {str(key).lower(): str(value).strip() for key, value in values.items()}


class AuthenticatedOciRegistry:
    """GHCR read/write capability with bounded retries and safe command vectors."""

    def __init__(
        self,
        *,
        image: str,
        actor: str,
        token: str,
        requester: Callable[[str, str, dict[str, str]], HttpResponse] = _default_requester,
        runner: Callable[[tuple[str, ...]], Any] = _default_runner,
        sleeper: Callable[[float], None] = sleep,
        max_attempts: int = 3,
        retry_after_cap: float = 5.0,
    ) -> None:
        match = IMAGE.fullmatch(image)
        if match is None or SAFE_ACTOR.fullmatch(actor) is None or not token:
            raise ValueError("registry configuration is invalid")
        if not 1 <= max_attempts <= 5 or not 0 <= retry_after_cap <= 30:
            raise ValueError("registry retry configuration is invalid")
        self.image = image
        self._repository = match.group("repository")
        self._actor = actor
        self._token = token
        self._requester = requester
        self._runner = runner
        self._sleeper = sleeper
        self._max_attempts = max_attempts
        self._retry_after_cap = retry_after_cap
        self._bearer: str | None = None

    def _reference_tag(self, reference: str) -> str | None:
        prefix = f"{self.image}:"
        tag = reference.removeprefix(prefix)
        return tag if reference.startswith(prefix) and SAFE_TAG.fullmatch(tag) else None

    def _retry_delay(self, response: HttpResponse | None, attempt: int) -> float:
        retry_after = _headers(response.headers).get("retry-after", "") if response else ""
        try:
            requested = float(retry_after)
        except ValueError:
            requested = 0.25 * (2**attempt)
        return min(max(requested, 0.0), self._retry_after_cap)

    def _request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
    ) -> tuple[HttpResponse | None, str | None]:
        response: HttpResponse | None = None
        for attempt in range(self._max_attempts):
            transport_failed = False
            try:
                response = self._requester(method, url, headers)
            except (OSError, TimeoutError, urllib.error.URLError):
                transport_failed = True
            retryable = transport_failed or (response is not None and response.status_code in RETRYABLE_STATUS)
            if not retryable:
                return response, None
            if attempt + 1 == self._max_attempts:
                code = "REGISTRY_TRANSPORT_ERROR" if transport_failed else "REGISTRY_RETRY_EXHAUSTED"
                return response, code
            self._sleeper(self._retry_delay(response, attempt))
        raise AssertionError("bounded retry must execute")

    def _registry_token(self) -> tuple[str | None, str | None]:
        if self._bearer is not None:
            return self._bearer, None
        scope = f"repository:{self._repository}:pull,push"
        query = urllib.parse.urlencode({"service": "ghcr.io", "scope": scope})
        basic = base64.b64encode(f"{self._actor}:{self._token}".encode()).decode("ascii")
        response, error = self._request(
            "GET",
            f"https://ghcr.io/token?{query}",
            {"Accept": "application/json", "Authorization": f"Basic {basic}"},
        )
        if error == "REGISTRY_TRANSPORT_ERROR":
            return None, error
        if error is not None or response is None or response.status_code != 200 or len(response.body) > 65_536:
            return None, "REGISTRY_AUTH_FAILED"
        try:
            payload = json.loads(response.body)
            bearer = payload["token"]
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return None, "REGISTRY_AUTH_FAILED"
        if not isinstance(bearer, str) or not bearer or len(bearer) > 8192 or any(char.isspace() for char in bearer):
            return None, "REGISTRY_AUTH_FAILED"
        self._bearer = bearer
        return bearer, None

    def _inspect_version(self, digest: str) -> tuple[str | None, str | None]:
        command = (*INSPECT, f"{self.image}@{digest}", "--format", "{{json .Image.Config.Labels}}")
        try:
            result = self._runner(command)
        except (OSError, subprocess.SubprocessError):
            return None, "VERSION_METADATA_UNAVAILABLE"
        if result.returncode != 0 or len(result.stdout) > 4096:
            return None, "VERSION_METADATA_UNAVAILABLE"
        try:
            labels = json.loads(result.stdout)
            version = labels["org.opencontainers.image.version"]
        except (KeyError, TypeError, json.JSONDecodeError):
            return None, "VERSION_METADATA_INVALID"
        if not isinstance(version, str) or STABLE_SEMVER.fullmatch(version) is None:
            return None, "VERSION_METADATA_INVALID"
        return version, None

    def lookup(self, reference: str, *, include_version: bool = False) -> RegistryLookup:
        tag = self._reference_tag(reference)
        if tag is None:
            return RegistryLookup(None, {}, False, "REGISTRY_REFERENCE_INVALID")
        bearer, auth_error = self._registry_token()
        if auth_error is not None or bearer is None:
            return RegistryLookup(None, {}, False, auth_error or "REGISTRY_AUTH_FAILED")
        encoded_tag = urllib.parse.quote(tag, safe="._-")
        response, error = self._request(
            "HEAD",
            f"https://ghcr.io/v2/{self._repository}/manifests/{encoded_tag}",
            {"Accept": ACCEPT_MANIFESTS, "Authorization": f"Bearer {bearer}"},
        )
        if response is None:
            return RegistryLookup(None, {}, True, error or "REGISTRY_TRANSPORT_ERROR")
        normalized = _headers(response.headers)
        if error is not None:
            return RegistryLookup(response.status_code, normalized, True, error)
        version: str | None = None
        if response.status_code == 200 and include_version:
            digest = normalized.get("docker-content-digest", "")
            if CANONICAL_DIGEST.fullmatch(digest):
                version, error = self._inspect_version(digest)
        return RegistryLookup(response.status_code, normalized, True, error, version)

    def _verify_source_manifest(self, digest: str) -> str | None:
        bearer, auth_error = self._registry_token()
        if auth_error is not None or bearer is None:
            return auth_error or "REGISTRY_AUTH_FAILED"
        response, error = self._request(
            "GET",
            f"https://ghcr.io/v2/{self._repository}/manifests/{digest}",
            {"Accept": ACCEPT_MANIFESTS, "Authorization": f"Bearer {bearer}"},
        )
        if error is not None:
            return error
        if response is None or response.status_code != 200:
            return "SOURCE_MANIFEST_LOOKUP_FAILED"
        headers = _headers(response.headers)
        if headers.get("docker-content-digest") != digest:
            return "SOURCE_MANIFEST_DIGEST_MISMATCH"
        media_type = headers.get("content-type", "").partition(";")[0].strip()
        if media_type not in SUPPORTED_MANIFEST_MEDIA_TYPES:
            return "SOURCE_MANIFEST_MEDIA_TYPE_UNSUPPORTED"
        if not response.body or len(response.body) > MAX_SOURCE_MANIFEST_BYTES:
            return "SOURCE_MANIFEST_SIZE_INVALID"
        observed_digest = f"sha256:{hashlib.sha256(response.body).hexdigest()}"
        return None if observed_digest == digest else "SOURCE_MANIFEST_DIGEST_MISMATCH"

    def create_alias(self, reference: str, digest: str) -> RegistryCommand:
        if self._reference_tag(reference) is None or CANONICAL_DIGEST.fullmatch(digest) is None:
            return RegistryCommand(False, "IMAGETOOLS_INPUT_INVALID")
        if error := self._verify_source_manifest(digest):
            return RegistryCommand(False, error)
        command = (
            *CREATE,
            "--prefer-index=false",
            "--tag",
            reference,
            f"{self.image}@{digest}",
        )
        try:
            result = self._runner(command)
        except (OSError, subprocess.SubprocessError):
            return RegistryCommand(False, "IMAGETOOLS_CREATE_FAILED")
        if result.returncode != 0:
            return RegistryCommand(False, "IMAGETOOLS_CREATE_FAILED")
        return RegistryCommand(True)
