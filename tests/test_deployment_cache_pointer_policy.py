"""Strict pointer admission and diagnostic identities have distinct contracts."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from dpone.runtime import deployment_cache_recovery_policy as policy
from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner
from tests.airflow_cache_promotion_test_support import load_json, write_cache_fixture, write_json


@pytest.fixture
def promoted_cache(tmp_path: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    fixture = write_cache_fixture(tmp_path)
    DeploymentCacheMaterializer(tmp_path).promote(fixture.deployment, environment="dev")
    return tmp_path, load_json(tmp_path / "current-pointer.json"), load_json(fixture.deployment / "deployment.json")


@pytest.mark.parametrize("environment", [None, "dev"])
def test_matching_promoted_pointer_and_current_are_accepted(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], environment: str | None
) -> None:
    _, pointer, current = promoted_cache
    result = policy.assess_pointer(pointer, expected_environment=environment)
    assert result.violation is None
    assert (result.deployment_id, result.release_id) == (current["deployment_id"], current["release_ref"])
    assert (
        policy.assess_current_identity(
            pointer, current, physical_deployment_id=current["deployment_id"], expected_environment=environment
        )
        == current["deployment_id"]
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema", None, "current pointer schema is invalid"),
        ("schema", "dpone.current-pointer.v2", "current pointer schema is invalid"),
        ("promoted_by", None, "current pointer promoted_by is required"),
        ("promoted_by", "", "current pointer promoted_by is required"),
        ("promoted_by", 12, "current pointer promoted_by is required"),
        ("promoted_at", None, "current pointer promoted_at must be an offset-aware date-time"),
        ("promoted_at", "2026-09-11T00:00:00", "current pointer promoted_at must be an offset-aware date-time"),
        ("activation_id", "not-a-uuid", "current pointer activation_id is invalid"),
        ("previous_deployment_id", "sha256:" + "A" * 64, "current pointer previous_deployment_id is invalid"),
        (
            "workspace_authority_connection_ref",
            "UPPER",
            "current pointer workspace authority connection_ref is invalid",
        ),
    ],
    ids=[
        "schema-missing",
        "schema-invalid",
        "actor-missing",
        "actor-empty",
        "actor-type",
        "date-missing",
        "date-naive",
        "activation-invalid",
        "previous-invalid",
        "workspace-invalid",
    ],
)
def test_invalid_authorization_retains_diagnostic_ids_but_refuses_strict_current(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], field: str, value: object, message: str
) -> None:
    _, pointer, current = promoted_cache
    if value is None:
        pointer.pop(field)
    else:
        pointer[field] = value
    result = policy.assess_pointer(pointer, expected_environment="dev")
    assert result.violation is not None
    assert (result.violation.code, result.violation.message) == ("DPONE_CURRENT_POINTER_INVALID", message)
    assert (result.deployment_id, result.release_id) == (current["deployment_id"], current["release_ref"])
    assert (
        policy.assess_current_identity(
            pointer, current, physical_deployment_id=current["deployment_id"], expected_environment="dev"
        )
        is None
    )


@pytest.mark.parametrize("field", ["deployment_id", "release_id"])
@pytest.mark.parametrize("value", [None, "", "sha256:" + "A" * 64, "sha256:abc", 23])
def test_diagnostic_id_validity_is_independent_for_each_digest(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], field: str, value: object
) -> None:
    _, pointer, current = promoted_cache
    if value is None:
        pointer.pop(field)
    else:
        pointer[field] = value
    result = policy.assess_pointer(pointer)
    assert result.violation is not None
    assert result.violation.message == f"current pointer {field} must be a canonical lowercase sha256 digest"
    assert result.deployment_id == (None if field == "deployment_id" else current["deployment_id"])
    assert result.release_id == (None if field == "release_id" else current["release_ref"])
    assert policy.pointer_identity(pointer, environment="dev") == (result.deployment_id, result.release_id)
    assert policy.assess_current_identity(pointer, current, physical_deployment_id=current["deployment_id"]) is None


@pytest.mark.parametrize("invalid_schema", [False, True])
def test_requested_environment_preserves_violation_precedence_and_hides_diagnostic_ids(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], invalid_schema: bool
) -> None:
    _, pointer, current = promoted_cache
    pointer.pop("promoted_by")
    if invalid_schema:
        pointer["schema"] = "invalid"
    result = policy.assess_pointer(pointer, expected_environment="prod")
    assert (result.deployment_id, result.release_id) == (None, None)
    assert result.violation is not None
    assert (result.violation.code, result.violation.message) == (
        ("DPONE_CURRENT_POINTER_INVALID", "current pointer schema is invalid")
        if invalid_schema
        else ("DPONE_CURRENT_POINTER_ENVIRONMENT_MISMATCH", "current pointer does not match requested environment")
    )
    assert (
        policy.assess_current_identity(
            pointer, current, physical_deployment_id=current["deployment_id"], expected_environment="prod"
        )
        is None
    )


@pytest.mark.parametrize("environment", [None, "", 0, []])
def test_diagnostic_assessment_uses_raw_environment_without_inventing_a_default(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], environment: object
) -> None:
    _, pointer, current = promoted_cache
    pointer["environment"] = environment
    result = policy.assess_pointer(pointer)
    assert result.violation is not None
    assert result.violation.message == "current pointer environment is required"
    assert (result.deployment_id, result.release_id) == (current["deployment_id"], current["release_ref"])
    requested = policy.assess_pointer(pointer, expected_environment="dev")
    assert (requested.deployment_id, requested.release_id) == (None, None)
    assert policy.pointer_identity(pointer, environment="dev") == (None, None)
    assert policy.assess_current_identity(pointer, current, physical_deployment_id=current["deployment_id"]) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("deployment_id", "sha256:" + "f" * 64),
        ("deployment_id", None),
        ("release_ref", "sha256:" + "f" * 64),
        ("release_ref", None),
        ("environment", "prod"),
        ("environment", None),
    ],
)
def test_current_metadata_must_match_each_pointer_identity(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], field: str, value: object
) -> None:
    _, pointer, current = promoted_cache
    physical_id = pointer["deployment_id"]
    current[field] = value
    assert policy.assess_current_identity(pointer, current, physical_deployment_id=physical_id) is None


@pytest.mark.parametrize("physical_id", [None, "", "sha256:" + "f" * 64])
def test_physical_identity_is_required_and_must_match_current(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], physical_id: str | None
) -> None:
    _, pointer, current = promoted_cache
    assert policy.assess_current_identity(pointer, current, physical_deployment_id=physical_id) is None


def test_matching_physical_and_current_ids_cannot_override_a_different_pointer(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]],
) -> None:
    _, pointer, current = promoted_cache
    current["deployment_id"] = "sha256:" + "f" * 64
    assert policy.assess_current_identity(pointer, current, physical_deployment_id=current["deployment_id"]) is None


def test_current_metadata_keeps_existing_string_coercion(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]],
) -> None:
    class MetadataText:
        def __init__(self, text: str) -> None:
            self.text = text

        def __str__(self) -> str:
            return self.text

    _, pointer, current = promoted_cache
    metadata = {field: MetadataText(current[field]) for field in ("deployment_id", "release_ref", "environment")}
    assert (
        policy.assess_current_identity(pointer, metadata, physical_deployment_id=pointer["deployment_id"])
        == pointer["deployment_id"]
    )


def test_absence_does_not_fabricate_a_pointer_violation_or_identity() -> None:
    result = policy.assess_pointer(None, expected_environment="dev")
    assert (result.violation, result.deployment_id, result.release_id) == (None, None, None)
    assert tuple(field.name for field in fields(result)) == ("violation", "deployment_id", "release_id")
    assert policy.pointer_identity(None, environment="dev") == (None, None)


def test_policy_is_read_only_and_does_not_open_files(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, pointer, current = promoted_cache
    originals = deepcopy((pointer, current))

    def refuse_io(*args: object, **kwargs: object) -> None:
        pytest.fail("pointer assessment must not open files")

    with monkeypatch.context() as guard:
        guard.setattr("builtins.open", refuse_io)
        guard.setattr("os.open", refuse_io)
        guard.setattr(Path, "open", refuse_io)
        assert policy.assess_pointer(pointer).violation is None
        assert (
            policy.assess_current_identity(pointer, current, physical_deployment_id=current["deployment_id"])
            == current["deployment_id"]
        )
    assert (pointer, current) == originals


def test_recovery_keeps_original_schema_then_environment_issue_order(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]],
) -> None:
    cache, pointer, _ = promoted_cache
    pointer["schema"] = "invalid"
    pointer["environment"] = "prod"
    pointer_path = cache / "current-pointer.json"
    write_json(pointer_path, pointer)
    plan = DeploymentCacheRecoveryPlanner(cache).plan(environment="dev")
    assert [(issue.code, issue.message, issue.path) for issue in plan.issues] == [
        ("DPONE_CURRENT_POINTER_INVALID", "current pointer schema is invalid", pointer_path.as_posix()),
        (
            "DPONE_CURRENT_POINTER_ENVIRONMENT_MISMATCH",
            "current pointer does not match requested environment",
            pointer_path.as_posix(),
        ),
    ]
    assert plan.current_deployment_id is None


def test_recovery_uses_canonical_ids_despite_missing_authorization(
    promoted_cache: tuple[Path, dict[str, Any], dict[str, Any]],
) -> None:
    cache, pointer, current = promoted_cache
    pointer.pop("promoted_by")
    write_json(cache / "current-pointer.json", pointer)
    plan = DeploymentCacheRecoveryPlanner(cache).plan(environment="dev")
    assert plan.current_deployment_id == current["deployment_id"]
    assert plan.preferred_repair_deployment_id == current["deployment_id"]
    assert plan.status == "repairable"
    assert (plan.issues[0].code, plan.issues[0].message) == (
        "DPONE_CURRENT_POINTER_INVALID",
        "current pointer promoted_by is required",
    )
