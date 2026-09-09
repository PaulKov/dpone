from __future__ import annotations

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_capacity_transport import (
    ArtifactHttpResponse,
    ArtifactTransportError,
    fetch_artifact_archive,
)
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget


def test_artifact_transport_counts_initial_and_each_redirect_before_returning_archive() -> None:
    calls: list[str] = []
    responses = iter(
        [
            ArtifactHttpResponse(302, {"Location": "https://storage.example/archive"}, b"redirect"),
            ArtifactHttpResponse(200, {}, b"archive"),
        ]
    )

    def request(url: str, timeout: float, maximum: int) -> ArtifactHttpResponse:
        calls.append(url)
        assert timeout > 0
        assert maximum > 0
        return next(responses)

    budget = RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)
    archive = fetch_artifact_archive("https://api.github.com/artifact", budget=budget, request=request)

    assert archive == b"archive"
    assert calls == ["https://api.github.com/artifact", "https://storage.example/archive"]
    counters = budget.counters()
    assert counters["artifact_download_requests"] == 1
    assert counters["redirect_requests"] == 1
    assert counters["artifact_download_body_bytes"] == len(b"redirectarchive")


@pytest.mark.parametrize(
    "response",
    [
        ArtifactHttpResponse(302, {}, b""),
        ArtifactHttpResponse(302, {"Location": "http://unsafe.example"}, b""),
        ArtifactHttpResponse(500, {}, b""),
    ],
)
def test_artifact_transport_fails_closed_on_missing_unsafe_or_nonredirect_response(
    response: ArtifactHttpResponse,
) -> None:
    budget = RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)

    with pytest.raises(ArtifactTransportError):
        fetch_artifact_archive("https://api.github.com/artifact", budget=budget, request=lambda *_: response)
