"""Redirect-aware, budget-metered artifact archive transport."""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import urlparse

from dpone.ports.ci_shadow_reconciliation import ArtifactHttpResponse
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class ArtifactTransportError(ValueError):
    """Artifact transport is unavailable, ambiguous, unsafe, or incomplete."""


def fetch_artifact_archive(
    initial_url: str,
    *,
    budget: RequestBudget,
    request: Callable[[str, float, int], ArtifactHttpResponse],
) -> bytes:
    """Fetch archive bytes while separately counting initial and redirect dispatches."""

    url = _safe_url(initial_url)
    is_redirect = False
    while True:
        response = budget.dispatch_response(
            "redirect_requests" if is_redirect else "artifact_download_requests",
            lambda timeout: _request_once(request, url, timeout, budget.remaining_response_bytes),
            response_class="artifact_download_body_bytes",
        )
        if response.status == 200:
            return response.body
        if response.status not in _REDIRECT_STATUSES:
            raise ArtifactTransportError("artifact transport returned an unexpected HTTP status")
        location = response.headers.get("Location") or response.headers.get("location")
        if not isinstance(location, str):
            raise ArtifactTransportError("artifact redirect has no location")
        url = _safe_url(location)
        is_redirect = True


def _request_once(
    request: Callable[[str, float, int], ArtifactHttpResponse], url: str, timeout: float, maximum: int
) -> tuple[ArtifactHttpResponse, bytes]:
    if maximum < 1:
        raise ArtifactTransportError("artifact response budget is exhausted")
    response = request(url, timeout, maximum)
    if not isinstance(response, ArtifactHttpResponse) or len(response.body) > maximum:
        raise ArtifactTransportError("artifact response exceeds the bounded read")
    return response, response.body


def _safe_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        raise ArtifactTransportError("artifact URL is unsafe")
    return value


__all__ = ["ArtifactHttpResponse", "ArtifactTransportError", "fetch_artifact_archive"]
