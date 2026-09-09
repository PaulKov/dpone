"""Workspace runtime authority is exact, immutable and workload-neutral."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _authority() -> DbtWorkspaceRuntimeAuthority:
    return DbtWorkspaceRuntimeAuthority.build(
        environment="prod",
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        release_sha256=_digest("c"),
        deployment_sha256=_digest("d"),
        binding_set_sha256=_digest("e"),
        connection_registry_sha256=_digest("f"),
        credential_runtime_sha256=_digest("1"),
    )


def test_authority_subject_binds_every_shared_verified_descriptor() -> None:
    authority = _authority()

    assert authority.authority_subject_sha256.startswith("sha256:")
    assert authority.release_id == _digest("a")
    assert authority.deployment_id == _digest("b")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("environment", "PROD"),
        ("release_sha256", "not-a-digest"),
        ("authority_subject_sha256", "sha256:" + "9" * 64),
    ],
)
def test_invalid_or_tampered_authority_is_rejected(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="workspace runtime"):
        replace(_authority(), **{field: value})


def test_workload_plan_identity_is_intentionally_absent() -> None:
    assert not hasattr(_authority(), "init_fetch_plan_sha256")
