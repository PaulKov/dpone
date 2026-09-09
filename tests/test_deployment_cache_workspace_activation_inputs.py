"""Sealed cache inputs bind source and runtime authority without storing secrets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.app.deployment_cache_workspace_activation_inputs import (
    DeploymentCacheWorkspaceActivationInputs,
)
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationError

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64


def _bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _descriptor(payload: bytes, name: str) -> dict[str, object]:
    return {
        "artifact_ref": f"cache://runtime-connection-contexts/sha256-{'9' * 64}/{name}.json",
        "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }


class _SourceReader:
    def __init__(self) -> None:
        self.calls: list[tuple[Path, str]] = []
        self.result = SimpleNamespace(release_id=RELEASE_ID)

    def read(self, root: Path, *, expected_release_id: str):
        self.calls.append((root, expected_release_id))
        return self.result


class _Resolver:
    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.calls: list[str] = []

    def resolve(self, connection_ref: str):
        self.calls.append(connection_ref)
        return SimpleNamespace(generation=self.generation, connection_ref=connection_ref)


class _ResolverFactory:
    def __init__(self) -> None:
        self.calls = []
        self.resolvers: list[_Resolver] = []

    def build(self, **values):
        self.calls.append(values)
        resolver = _Resolver(len(self.calls))
        self.resolvers.append(resolver)
        return resolver


def _projection(tmp_path: Path):
    cache_root = tmp_path / ".dpone-cache"
    root = cache_root / "activations" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    release_root = cache_root / "releases" / RELEASE_ID.replace(":", "-")
    root.mkdir(parents=True)
    release_root.mkdir(parents=True)
    snapshots = {
        "binding_set": {"schema": "dpone.binding-set.v1", "environment": "prod", "bindings": {}},
        "connection_registry": {
            "schema": "dpone.connection-registry.v1",
            "environment": "prod",
            "connections": {},
        },
        "credential_runtime": {
            "schema": "dpone.credential-runtime.v1",
            "environment": "prod",
        },
    }
    filenames = {
        "binding_set": "binding-set.json",
        "connection_registry": "connection-registry.ref",
        "credential_runtime": "credential-runtime.ref",
    }
    descriptors = {}
    for name, payload in snapshots.items():
        content = _bytes(payload)
        (root / filenames[name]).write_bytes(content)
        descriptors[name] = _descriptor(content, name.replace("_", "-"))
    deployment = {
        "schema": "dpone.deployment-set.v2",
        "environment": "prod",
        "release_ref": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "binding_set_ref": canonical_fingerprint(snapshots["binding_set"]),
        "connection_registry_ref": canonical_fingerprint(snapshots["connection_registry"]),
        "credential_runtime_ref": canonical_fingerprint(snapshots["credential_runtime"]),
        **descriptors,
    }
    index = {
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "release": {"sha256": "sha256:" + "c" * 64},
        "deployment": {"sha256": "sha256:" + "d" * 64},
        **descriptors,
    }
    (root / "deployment.json").write_bytes(_bytes(deployment))
    (root / "airflow-index.json").write_bytes(_bytes(index))
    return cache_root, root, release_root, deployment, index


def test_inputs_load_complete_release_and_exact_runtime_authority(tmp_path: Path) -> None:
    cache_root, root, release_root, _, _ = _projection(tmp_path)
    sources, factory = _SourceReader(), _ResolverFactory()
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache_root,
        source_reader=sources,  # type: ignore[arg-type]
        resolver_factory=factory,
    )

    assert inputs.load_sources(projection_root=root, release_id=RELEASE_ID) is sources.result
    authority = inputs.load_runtime_authority(
        projection_root=root,
        environment="prod",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
    )
    resolved = inputs.resolve_connection(authority, "warehouse")

    assert sources.calls == [(release_root, RELEASE_ID)]
    assert authority.release_sha256 == "sha256:" + "c" * 64
    assert resolved.generation == 1  # type: ignore[attr-defined]
    assert "password" not in repr(authority)


def test_exact_replay_keeps_first_resolver_capability(tmp_path: Path) -> None:
    cache_root, root, _, _, _ = _projection(tmp_path)
    factory = _ResolverFactory()
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache_root,
        source_reader=_SourceReader(),  # type: ignore[arg-type]
        resolver_factory=factory,
    )
    coordinates = dict(
        projection_root=root,
        environment="prod",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
    )

    first = inputs.load_runtime_authority(**coordinates)
    second = inputs.load_runtime_authority(**coordinates)

    assert first == second
    assert inputs.resolve_connection(second, "warehouse").generation == 1  # type: ignore[attr-defined]
    assert len(factory.calls) == 2


def test_changed_snapshot_or_foreign_coordinates_fail_before_resolver(tmp_path: Path) -> None:
    cache_root, root, _, _, _ = _projection(tmp_path)
    factory = _ResolverFactory()
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache_root,
        source_reader=_SourceReader(),  # type: ignore[arg-type]
        resolver_factory=factory,
    )
    (root / "binding-set.json").write_text('{"changed":true}', encoding="utf-8")

    with pytest.raises(DbtWorkspaceActivationError, match="runtime_context"):
        inputs.load_runtime_authority(
            projection_root=root,
            environment="prod",
            release_id=RELEASE_ID,
            deployment_id=DEPLOYMENT_ID,
        )
    with pytest.raises(DbtWorkspaceActivationError, match="projection_root"):
        inputs.load_sources(projection_root=tmp_path, release_id=RELEASE_ID)
    assert factory.calls == []


def test_authority_must_be_loaded_before_connection_resolution(tmp_path: Path) -> None:
    cache_root, _, _, _, _ = _projection(tmp_path)
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache_root,
        source_reader=_SourceReader(),  # type: ignore[arg-type]
        resolver_factory=_ResolverFactory(),
    )
    from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority

    authority = DbtWorkspaceRuntimeAuthority.build(
        environment="prod",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        release_sha256="sha256:" + "c" * 64,
        deployment_sha256="sha256:" + "d" * 64,
        binding_set_sha256="sha256:" + "e" * 64,
        connection_registry_sha256="sha256:" + "f" * 64,
        credential_runtime_sha256="sha256:" + "1" * 64,
    )

    with pytest.raises(DbtWorkspaceActivationError, match="runtime_context_not_loaded"):
        inputs.resolve_connection(authority, "warehouse")
