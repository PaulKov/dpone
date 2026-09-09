from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest

from dpone.contracts.runtime_connection import RuntimeConnectionAuthorityError
from dpone.runtime.credentials.runtime_context import (
    RUNTIME_CONNECTION_CONTEXT_ENV,
    RUNTIME_INIT_FETCH_PLAN_B64_ENV,
    RUNTIME_INIT_FETCH_PLAN_SHA256_ENV,
    RuntimeConnectionContextLoader,
)
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimeWorkloadPackRef,
    canonical_runtime_init_fetch_plan_bytes,
)

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64
_CONTEXT_DIR = "sha256-" + "d" * 64


def test_loader_reads_only_exact_artifacts_from_pinned_plan(tmp_path: Path) -> None:
    environ, context_root = _runtime_context(tmp_path)

    context = RuntimeConnectionContextLoader(
        vault_reader_factory=lambda _: None,
    ).load(environ)

    assert context is not None
    assert context.environment == "prod"
    assert context.release_id == _RELEASE_ID
    assert context.deployment_id == _DEPLOYMENT_ID
    assert context.init_fetch_plan_sha256 == environ[RUNTIME_INIT_FETCH_PLAN_SHA256_ENV]
    assert context.authority_subject_sha256 is not None
    assert context.authority_subject_sha256.startswith("sha256:")
    assert context.binding_set["bindings"]["source-main"]["connection_ref"] == "source-registry"
    assert context_root.is_dir()


def test_shared_context_subject_is_stable_across_workload_specific_plans(tmp_path: Path) -> None:
    environ, _ = _runtime_context(tmp_path)
    first = RuntimeConnectionContextLoader(vault_reader_factory=lambda _: None).load(environ)
    encoded = base64.b64decode(environ[RUNTIME_INIT_FETCH_PLAN_B64_ENV])
    payload = json.loads(encoded)
    payload["workload_pack"]["sha256"] = "sha256:" + "9" * 64
    replacement = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    changed_environ = {
        **environ,
        RUNTIME_INIT_FETCH_PLAN_B64_ENV: base64.b64encode(replacement).decode(),
        RUNTIME_INIT_FETCH_PLAN_SHA256_ENV: _digest(replacement),
    }
    second = RuntimeConnectionContextLoader(vault_reader_factory=lambda _: None).load(changed_environ)

    assert first is not None and second is not None
    assert first.init_fetch_plan_sha256 != second.init_fetch_plan_sha256
    assert first.authority_subject_sha256 == second.authority_subject_sha256


def test_loader_rejects_context_without_pinned_plan(tmp_path: Path) -> None:
    _, context_root = _runtime_context(tmp_path)

    with pytest.raises(RuntimeConnectionAuthorityError) as exc:
        RuntimeConnectionContextLoader().load({RUNTIME_CONNECTION_CONTEXT_ENV: str(context_root)})

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_UNVERIFIED"


def test_loader_rejects_tampered_or_symlinked_context_artifact(
    tmp_path: Path,
) -> None:
    environ, context_root = _runtime_context(tmp_path)
    binding_path = context_root / "binding-set.json"
    original = binding_path.read_bytes()
    binding_path.write_bytes(original + b" ")

    with pytest.raises(RuntimeConnectionAuthorityError) as exc:
        RuntimeConnectionContextLoader(
            vault_reader_factory=lambda _: None,
        ).load(environ)

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID"

    binding_path.unlink()
    outside = tmp_path / "outside.json"
    outside.write_bytes(original)
    binding_path.symlink_to(outside)
    with pytest.raises(RuntimeConnectionAuthorityError) as symlink_exc:
        RuntimeConnectionContextLoader(
            vault_reader_factory=lambda _: None,
        ).load(environ)

    assert symlink_exc.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID"


def test_loader_rejects_required_vault_backend_missing_from_credential_runtime(
    tmp_path: Path,
) -> None:
    environ, _ = _runtime_context(
        tmp_path,
        source_credentials={
            "resolver": "vault_kv",
            "mount": "kv",
            "kv_version": 2,
            "path": "dpone/prod/credentials/source-main",
            "fields": {"username": "username", "password": "password"},
            "version_policy": "latest",
            "resolution_scope": "workload_start",
        },
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as exc:
        RuntimeConnectionContextLoader(vault_reader_factory=lambda _: None).load(environ)

    assert exc.value.code == "DPONE_RUNTIME_CREDENTIAL_BACKEND_UNAVAILABLE"
    assert "dpone/prod/credentials" not in str(exc.value)


def test_loader_rejects_nested_runtime_context_schema_violation(
    tmp_path: Path,
) -> None:
    environ, context_root = _runtime_context(tmp_path)
    binding_path = context_root / "binding-set.json"
    invalid = json.loads(binding_path.read_text(encoding="utf-8"))
    invalid["bindings"]["source-main"]["unexpected"] = "must-not-be-accepted"
    content = json.dumps(invalid, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    binding_path.write_bytes(content)
    environ = _replace_plan_descriptor(
        environ,
        name="binding_set",
        content=content,
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as exc:
        RuntimeConnectionContextLoader(vault_reader_factory=lambda _: None).load(environ)

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_INVALID"
    assert "must-not-be-accepted" not in str(exc.value)


def _runtime_context(
    tmp_path: Path,
    *,
    source_credentials: dict[str, object] | None = None,
) -> tuple[dict[str, str], Path]:
    context_root = tmp_path / "payload" / "runtime-connection-contexts" / _CONTEXT_DIR
    context_root.mkdir(parents=True)
    payloads = {
        "binding_set": {
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {
                "source-main": {"connection_ref": "source-registry"},
                "sink-main": {"connection_ref": "sink-registry"},
            },
            "runtime": {},
        },
        "connection_registry": {
            "schema": "dpone.connection-registry.v1",
            "environment": "prod",
            "connections": {
                "source-registry": {
                    "type": "postgres",
                    "connection": {},
                    "credentials": source_credentials or _env_credentials("DPONE_SOURCE"),
                },
                "sink-registry": {
                    "type": "clickhouse",
                    "connection": {},
                    "credentials": _env_credentials("DPONE_SINK"),
                },
            },
        },
        "credential_runtime": {
            "schema": "dpone.credential-runtime.v1",
            "environment": "prod",
        },
    }
    descriptors: dict[str, RuntimeArtifactDescriptor] = {}
    for name, payload in payloads.items():
        filename = name.replace("_", "-") + ".json"
        content = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        (context_root / filename).write_bytes(content)
        descriptors[name] = RuntimeArtifactDescriptor(
            artifact_ref=(f"cache://runtime-connection-contexts/{_CONTEXT_DIR}/{filename}"),
            sha256=_digest(content),
            bytes=len(content),
        )
    plan = RuntimeInitFetchPlan(
        environment="prod",
        trust_tier="non_production",
        release_id=_RELEASE_ID,
        deployment_id=_DEPLOYMENT_ID,
        runtime_image_ref=f"registry.example/dpone/runtime@{_IMAGE_DIGEST}",
        runtime_image_digest=_IMAGE_DIGEST,
        artifact_registry_ref="dpone-artifacts",
        registry_config_ref={
            "kind": "kubernetes_config_map",
            "name": "dpone-artifacts",
            "key": "registry.json",
            "sha256": "sha256:" + "1" * 64,
        },
        trust_policy_ref=None,
        identity={
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        release=_artifact(
            f"cache://releases/{_RELEASE_ID.replace(':', '-')}/release-set.json",
            "2",
        ),
        deployment=_artifact(
            f"cache://deployments/prod/{_DEPLOYMENT_ID.replace(':', '-')}/deployment.json",
            "3",
        ),
        binding_set=descriptors["binding_set"],
        connection_registry=descriptors["connection_registry"],
        credential_runtime=descriptors["credential_runtime"],
        workload_pack=RuntimeWorkloadPackRef(
            id="orders",
            artifact_ref=(f"cache://releases/{_RELEASE_ID.replace(':', '-')}/packs/orders.json"),
            sha256="sha256:" + "4" * 64,
            bytes=128,
            pack_fingerprint="sha256:" + "5" * 64,
        ),
        execution=RuntimeExecutionSelection(
            kind="runtime",
            selector="orders",
        ),
        verify={
            "checksums": "required",
            "attestations": "optional",
        },
    )
    encoded = canonical_runtime_init_fetch_plan_bytes(plan)
    return (
        {
            RUNTIME_CONNECTION_CONTEXT_ENV: str(context_root),
            RUNTIME_INIT_FETCH_PLAN_B64_ENV: base64.b64encode(encoded).decode(),
            RUNTIME_INIT_FETCH_PLAN_SHA256_ENV: _digest(encoded),
        },
        context_root,
    )


def _artifact(artifact_ref: str, digest_character: str) -> RuntimeArtifactDescriptor:
    return RuntimeArtifactDescriptor(
        artifact_ref=artifact_ref,
        sha256="sha256:" + digest_character * 64,
        bytes=128,
    )


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _env_credentials(prefix: str) -> dict[str, object]:
    return {
        "resolver": "env_var",
        "support": "development_only",
        "fields": {
            "host": f"{prefix}_HOST",
            "username": f"{prefix}_USERNAME",
        },
    }


def _replace_plan_descriptor(
    environ: dict[str, str],
    *,
    name: str,
    content: bytes,
) -> dict[str, str]:
    encoded = base64.b64decode(environ[RUNTIME_INIT_FETCH_PLAN_B64_ENV])
    payload = json.loads(encoded)
    payload[name]["sha256"] = _digest(content)
    payload[name]["bytes"] = len(content)
    replacement = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return {
        **environ,
        RUNTIME_INIT_FETCH_PLAN_B64_ENV: base64.b64encode(replacement).decode(),
        RUNTIME_INIT_FETCH_PLAN_SHA256_ENV: _digest(replacement),
    }
