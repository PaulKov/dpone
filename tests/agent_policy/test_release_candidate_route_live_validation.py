"""Manual PostgreSQL to MSSQL route authority remains fail-closed."""

from __future__ import annotations

import copy

import pytest
from tests.agent_policy import _release_candidate_evidence_helpers as helpers
from tests.agent_policy._release_candidate_route_live_helpers import (
    valid_route_live_manifest,
    valid_route_live_provider_binding,
)
from tools.route_live_certification.contract import REQUIRED_SUITE_IDS

route_validation = helpers.load_module(
    "dpone_release_candidate_route_live_validation_tests",
    "tools/agent_policy/release_candidate_route_live_validation.py",
)


def _sources() -> dict[str, dict]:
    manifest = valid_route_live_manifest(
        required_suite_ids=REQUIRED_SUITE_IDS,
        commit_sha=helpers.COMMIT_SHA,
    )
    return {
        "postgres_mssql_route_certification": manifest,
        "postgres_mssql_route_artifact_binding": valid_route_live_provider_binding(
            manifest,
            manifest_raw=_raw(manifest),
            commit_sha=helpers.COMMIT_SHA,
        ),
    }


def _raw(payload: dict) -> bytes:
    return helpers.canonical(payload)


def test_route_live_manifest_and_provider_artifact_are_manual_authority() -> None:
    payloads = _sources()
    manifest = payloads["postgres_mssql_route_certification"]
    binding = payloads["postgres_mssql_route_artifact_binding"]
    related = {"postgres_mssql_route_certification": _raw(manifest)}

    manifest_result = route_validation.validate_manifest(
        manifest,
        commit_sha=helpers.COMMIT_SHA,
    )
    binding_result = route_validation.validate_provider_binding(
        binding,
        commit_sha=helpers.COMMIT_SHA,
        manifest_raw=related["postgres_mssql_route_certification"],
    )

    assert manifest_result["status"] == binding_result["status"] == "PASS"
    assert manifest_result["suite_count"] == len(REQUIRED_SUITE_IDS)
    assert binding_result["artifact_id"] == binding["artifact_id"]


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.update(status="passed_partial"),
        lambda payload: payload.update(release_ready=False),
        lambda payload: payload["required_suite_ids"].pop(),
        lambda payload: payload["suites"][0].update(case_count=2),
        lambda payload: payload["suites"][0]["junit"].update(skipped=1),
        lambda payload: payload.update(manifest_sha256="9" * 64),
    ),
)
def test_manual_authority_rejects_tampered_route_manifest(mutation) -> None:
    payload = copy.deepcopy(_sources()["postgres_mssql_route_certification"])
    mutation(payload)

    with pytest.raises(ValueError):
        route_validation.validate_manifest(
            payload,
            commit_sha=helpers.COMMIT_SHA,
        )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.update(artifact_id=0),
        lambda payload: payload.update(artifact_digest="9" * 64),
        lambda payload: payload.update(artifact_name="unbound"),
        lambda payload: payload.update(workflow_run_attempt=3),
        lambda payload: payload.update(manifest_content_sha256="8" * 64),
        lambda payload: payload.update(manifest_sha256="7" * 64),
    ),
)
def test_manual_authority_rejects_tampered_provider_binding(mutation) -> None:
    payloads = _sources()
    binding = copy.deepcopy(payloads["postgres_mssql_route_artifact_binding"])
    mutation(binding)

    with pytest.raises(ValueError):
        route_validation.validate_provider_binding(
            binding,
            commit_sha=helpers.COMMIT_SHA,
            manifest_raw=_raw(payloads["postgres_mssql_route_certification"]),
        )
