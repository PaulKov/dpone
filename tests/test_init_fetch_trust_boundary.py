from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from dpone.ports.artifact_registry import ArtifactMetadata, CreateResult
from dpone.runtime.artifact_delivery import (
    InitFetchAttestationVerifier,
    InitFetchError,
    InitFetchExecutor,
    InitFetchPlan,
    RuntimeArtifactRef,
)

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_RELEASE_DIR = "sha256-" + "a" * 64
_ARTIFACT_REF = f"cache://releases/{_RELEASE_DIR}/packs/orders.airflow-pack.json"


@dataclass
class RecordingRegistry:
    bodies: dict[PurePosixPath, bytes]
    calls: list[tuple[str, PurePosixPath]] = field(default_factory=list)

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        raise AssertionError("init-fetch must not write to the registry")

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        self.calls.append(("stat", key))
        body = self.bodies[key]
        return ArtifactMetadata(key=key, size_bytes=len(body))

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        self.calls.append(("download", key))
        body = self.bodies[key]
        if len(body) > max_bytes:
            raise AssertionError("test registry body exceeds caller limit")
        destination.write_bytes(body)


@dataclass
class RecordingVerifier:
    plan: InitFetchPlan | None = None
    staged_artifacts: tuple[Any, ...] = ()
    staged_payloads: tuple[bytes, ...] = ()
    reject: bool = False

    def verify(self, *, plan: InitFetchPlan, staged_artifacts: tuple[Any, ...]) -> None:
        self.plan = plan
        self.staged_artifacts = staged_artifacts
        assert all(item.path.is_file() for item in staged_artifacts)
        self.staged_payloads = tuple(item.path.read_bytes() for item in staged_artifacts)
        if self.reject:
            raise RuntimeError("untrusted signer detail")


def _artifact(
    body: bytes = b'{"id":"orders"}\n',
    *,
    artifact_id: str = "orders",
    artifact_ref: str = _ARTIFACT_REF,
) -> RuntimeArtifactRef:
    return RuntimeArtifactRef(
        id=artifact_id,
        artifact_ref=artifact_ref,
        sha256="sha256:" + hashlib.sha256(body).hexdigest(),
        bytes=len(body),
    )


def _tampered_artifact(**changes: object) -> RuntimeArtifactRef:
    artifact = _artifact()
    for name, value in changes.items():
        object.__setattr__(artifact, name, value)
    return artifact


def _plan(
    *artifacts: RuntimeArtifactRef,
    attestations: str = "optional",
) -> InitFetchPlan:
    return InitFetchPlan(
        release_id=_RELEASE_ID,
        deployment_id=_DEPLOYMENT_ID,
        artifact_registry_ref="dpone-prod-artifacts",
        artifacts=artifacts or (_artifact(),),
        identity={
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        verify={"checksums": "required", "attestations": attestations},
    )


@pytest.mark.parametrize(
    "plan_kwargs",
    (
        {"release_id": "sha256:bad"},
        {"deployment_id": "sha256:bad"},
        {"artifact_registry_ref": "registry/current"},
        {"verify": {"checksums": "optional", "attestations": "optional"}},
        {"artifacts": (_tampered_artifact(sha256="sha256:bad"),)},
        {
            "artifacts": (
                _tampered_artifact(
                    artifact_ref=(f"cache://releases/{_RELEASE_DIR}/packs/../outside.airflow-pack.json")
                ),
            )
        },
        {
            "artifacts": (
                _artifact(),
                _artifact(
                    b"second",
                    artifact_ref=f"cache://releases/{_RELEASE_DIR}/packs/second.airflow-pack.json",
                ),
            )
        },
        {
            "artifacts": (
                _artifact(),
                _artifact(b"second", artifact_id="second"),
            )
        },
        {
            "artifacts": (
                _artifact(artifact_ref=("cache://releases/sha256-" + "c" * 64 + "/packs/orders.airflow-pack.json")),
            )
        },
    ),
)
def test_direct_init_fetch_plan_construction_rejects_invalid_contract(
    plan_kwargs: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "release_id": _RELEASE_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "artifact_registry_ref": "dpone-prod-artifacts",
        "artifacts": (_artifact(),),
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "verify": {"checksums": "required", "attestations": "optional"},
    }
    values.update(plan_kwargs)

    with pytest.raises(ValueError):
        InitFetchPlan(**values)  # type: ignore[arg-type]


def test_init_fetch_plan_defensively_normalizes_mutable_inputs() -> None:
    identity = {
        "method": "kubernetes_workload_identity",
        "service_account": "dpone-runtime",
    }
    verify = {"checksums": "required", "attestations": "optional"}
    artifacts = [_artifact()]

    plan = InitFetchPlan(
        release_id=_RELEASE_ID,
        deployment_id=_DEPLOYMENT_ID,
        artifact_registry_ref="dpone-prod-artifacts",
        artifacts=artifacts,  # type: ignore[arg-type]
        identity=identity,
        verify=verify,
    )
    identity["method"] = "mutated"
    verify["checksums"] = "mutated"
    artifacts.clear()

    assert plan.identity["method"] == "kubernetes_workload_identity"
    assert plan.verify["checksums"] == "required"
    assert plan.artifacts == (_artifact(),)
    with pytest.raises(TypeError):
        plan.identity["method"] = "mutated"  # type: ignore[index]
    with pytest.raises(TypeError):
        plan.verify["checksums"] = "mutated"  # type: ignore[index]


def test_executor_revalidates_plan_before_registry_or_destination_io(tmp_path: Path) -> None:
    plan = _plan()
    object.__setattr__(plan, "release_id", "sha256:tampered")
    registry = RecordingRegistry({_key(_artifact()): b'{"id":"orders"}\n'})
    destination = tmp_path / "missing-parent" / "runtime-artifacts"

    with pytest.raises(ValueError, match="release_id"):
        InitFetchExecutor(registry=registry, destination_root=destination).execute(plan)

    assert registry.calls == []
    assert not destination.parent.exists()


@pytest.mark.parametrize(
    ("executor_kwargs", "expected_code"),
    (
        ({"max_artifact_count": 1}, "DPONE_INIT_FETCH_ARTIFACT_LIMIT_EXCEEDED"),
        ({"max_total_bytes": 1}, "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED"),
    ),
)
def test_executor_preflights_aggregate_limits_before_registry_reads(
    tmp_path: Path,
    executor_kwargs: dict[str, int],
    expected_code: str,
) -> None:
    first = _artifact()
    second = _artifact(
        b"second",
        artifact_id="second",
        artifact_ref=f"cache://releases/{_RELEASE_DIR}/packs/second.airflow-pack.json",
    )
    registry = RecordingRegistry({_key(first): b'{"id":"orders"}\n', _key(second): b"second"})

    with pytest.raises(InitFetchError) as exc:
        InitFetchExecutor(
            registry=registry,
            destination_root=tmp_path / "runtime-artifacts",
            **executor_kwargs,
        ).execute(_plan(first, second))

    assert exc.value.code == expected_code
    assert registry.calls == []
    assert not (tmp_path / "runtime-artifacts").exists()


def test_executor_stages_path_backed_artifacts_incrementally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.init_fetch_execution as execution

    first_body = b"first"
    second_body = b"second"
    first = _artifact(first_body)
    second = _artifact(
        second_body,
        artifact_id="second",
        artifact_ref=f"cache://releases/{_RELEASE_DIR}/packs/second.airflow-pack.json",
    )
    registry = RecordingRegistry({_key(first): first_body, _key(second): second_body})
    actual_materialize = execution.materialize_immutable_local_tree
    publication_sources: list[object] = []

    def capture_sources(*args: Any, **kwargs: Any) -> str:
        publication_sources.extend(args[1].values())
        return actual_materialize(*args, **kwargs)

    monkeypatch.setattr(execution, "materialize_immutable_local_tree", capture_sources)

    InitFetchExecutor(
        registry=registry,
        destination_root=tmp_path / "runtime-artifacts",
    ).execute(_plan(first, second))

    assert publication_sources
    assert all(isinstance(source, Path) for source in publication_sources)
    assert [name for name, _ in registry.calls] == ["stat", "download", "stat", "download"]


def test_required_attestation_receives_exact_plan_and_verified_staged_artifacts(
    tmp_path: Path,
) -> None:
    body = b'{"id":"orders"}\n'
    artifact = _artifact(body)
    plan = _plan(artifact, attestations="required_for_prod")
    verifier = RecordingVerifier()

    result = InitFetchExecutor(
        registry=RecordingRegistry({_key(artifact): body}),
        destination_root=tmp_path / "runtime-artifacts",
        attestation_verifier=verifier,
    ).execute(plan)

    assert result.passed is True
    assert verifier.plan == plan
    assert tuple(item.artifact for item in verifier.staged_artifacts) == plan.artifacts
    assert verifier.staged_payloads == (body,)
    assert all(not item.path.exists() for item in verifier.staged_artifacts)


@pytest.mark.parametrize("verifier", (None, RecordingVerifier(reject=True)))
def test_required_attestation_fails_closed_and_cleans_staging(
    tmp_path: Path,
    verifier: InitFetchAttestationVerifier | None,
) -> None:
    body = b'{"id":"orders"}\n'
    artifact = _artifact(body)
    destination = tmp_path / "runtime-artifacts"

    with pytest.raises(InitFetchError) as exc:
        InitFetchExecutor(
            registry=RecordingRegistry({_key(artifact): body}),
            destination_root=destination,
            attestation_verifier=verifier,
        ).execute(_plan(artifact, attestations="required_for_prod"))

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_REQUIRED"
    assert not destination.exists()
    if isinstance(verifier, RecordingVerifier):
        assert verifier.staged_artifacts
        assert all(not item.path.exists() for item in verifier.staged_artifacts)


def test_failed_fetch_leaves_no_partial_output_and_retry_is_deterministic(
    tmp_path: Path,
) -> None:
    expected = b'{"id":"orders"}\n'
    artifact = _artifact(expected)
    destination = tmp_path / "runtime-artifacts"
    registry = RecordingRegistry({_key(artifact): b"x" * len(expected)})
    executor = InitFetchExecutor(registry=registry, destination_root=destination)

    with pytest.raises(InitFetchError) as exc:
        executor.execute(_plan(artifact))

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert not destination.exists()

    registry.bodies[_key(artifact)] = expected
    first = executor.execute(_plan(artifact)).to_dict()
    second = executor.execute(_plan(artifact)).to_dict()

    assert first == second
    assert (destination / "releases" / _RELEASE_DIR / "packs" / "orders.airflow-pack.json").read_bytes() == expected


def test_failed_publication_preserves_existing_destination_without_partial_output(
    tmp_path: Path,
) -> None:
    body = b'{"id":"orders"}\n'
    artifact = _artifact(body)
    destination = tmp_path / "runtime-artifacts"
    destination.mkdir()
    sentinel = destination / "existing.txt"
    sentinel.write_bytes(b"existing")

    with pytest.raises(InitFetchError) as exc:
        InitFetchExecutor(
            registry=RecordingRegistry({_key(artifact): body}),
            destination_root=destination,
        ).execute(_plan(artifact))

    assert exc.value.code == "DPONE_INIT_FETCH_DESTINATION_CONFLICT"
    assert sentinel.read_bytes() == b"existing"
    assert tuple(destination.rglob("*")) == (sentinel,)


def test_local_registry_resolve_preserves_existing_file_validation(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import LocalArtifactRegistry

    artifact = _artifact()
    path = tmp_path / "registry" / _key(artifact)
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"id":"orders"}\n')
    registry = LocalArtifactRegistry(tmp_path / "registry")

    assert registry.resolve(artifact.artifact_ref) == path

    path.unlink()
    with pytest.raises(InitFetchError) as exc:
        registry.resolve(artifact.artifact_ref)

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_NOT_FOUND"


def _key(artifact: RuntimeArtifactRef) -> PurePosixPath:
    return PurePosixPath(artifact.artifact_ref.removeprefix("cache://"))
