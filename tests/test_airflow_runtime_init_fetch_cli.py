from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import logging
import shlex
import tarfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.commands import airflow_runtime_delivery_cmd
from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestationVerification,
    AirflowArtifactObservedSubject,
)
from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    deployment_id,
    release_id,
    requires_release_set_v2_for_runtime_payloads,
)
from dpone.contracts.dbt_execution_pack import DbtInvocationTarget
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import (
    DbtExecutionPack,
    DbtProfileSpec,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    dbt_runtime_payload_reference,
    dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.runtime_artifact_attestation import (
    RuntimeArtifactAttestationSubject,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.artifact_registry import ArtifactMetadata
from dpone.readiness.airflow_runtime_init_fetch import (
    DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV,
    PLAN_B64_ENV,
    PLAN_SHA256_ENV,
    AirflowRuntimeInitFetchService,
    RuntimeRegistryConfiguration,
    WorkloadIdentityRegistryFactory,
)
from dpone.runtime import runtime_init_fetch_receipts as runtime_init_fetch_receipts_module
from dpone.runtime import runtime_init_fetch_service as runtime_init_fetch_service_module
from dpone.runtime import runtime_init_fetch_storage as runtime_init_fetch_storage_module
from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimePayloadDescriptor,
    RuntimeWorkloadPackRef,
    canonical_runtime_init_fetch_plan_bytes,
)
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_ready import parse_ready_manifest, ready_manifest_bytes
from dpone.runtime.runtime_init_fetch_ready_builder import build_runtime_fetch_ready
from dpone.runtime.runtime_init_fetch_ready_models import ReadyArtifact
from dpone.runtime.runtime_init_fetch_service import RuntimeInitFetchExecutor
from dpone.runtime.verified_pack_launcher import (
    RUNTIME_CONNECTION_CONTEXT_ENV,
    VerifiedPackLauncher,
)
from dpone.services.airflow_artifact_attestation_consumer import (
    AirflowArtifactAttestationRejected,
)
from dpone.services.airflow_artifact_attestation_registry import (
    AirflowArtifactAttestationRegistryError,
)

_IMAGE_DIGEST = "sha256:" + "d" * 64
_IMAGE_REF = f"registry.example/dpone-runtime@{_IMAGE_DIGEST}"
_DBT_TOOLCHAIN_DIGEST = DBT_SQLSERVER_1_12_CERTIFIED.sha256


@pytest.mark.parametrize(
    ("release_schema", "has_payloads", "trust_tier", "expected"),
    [
        ("dpone.release-set.v1", True, "production", True),
        ("dpone.release-set.v1", True, "non_production", False),
        ("dpone.release-set.v1", False, "production", False),
        ("dpone.release-set.v2", True, "production", False),
    ],
)
def test_runtime_payload_authority_policy_is_shared_across_build_and_runtime(
    release_schema: str, has_payloads: bool, trust_tier: str, expected: bool
) -> None:
    assert (
        requires_release_set_v2_for_runtime_payloads(
            release_schema=release_schema,
            has_runtime_payloads=has_payloads,
            trust_tier=trust_tier,
        )
        is expected
    )


def test_runtime_receipt_authority_uses_release_inventory_not_plan_selection() -> None:
    bundle = _bundle(trust_tier="production")
    release = json.loads(bundle.objects[_key(bundle.plan.release.artifact_ref)])
    release["artifacts"]["runtime_payloads"] = [
        _runtime_payload_entry(
            item_id="dbt_project",
            kind="dbt_project_bundle",
            path="runtime/dbt/project.tar.gz",
            payload=b"not-selected-by-plan",
            media_type="application/vnd.dpone.dbt-project-bundle+gzip",
        )
    ]
    release["release_id"] = ""
    release["release_id"] = release_id(release)
    release_bytes = _json_bytes(release)

    deployment = json.loads(bundle.objects[_key(bundle.plan.deployment.artifact_ref)])
    deployment["release_ref"] = release["release_id"]
    deployment["deployment_id"] = ""
    deployment["deployment_id"] = deployment_id(deployment)
    deployment_bytes = _json_bytes(deployment)

    release_dir = str(release["release_id"]).replace(":", "-")
    deployment_dir = str(deployment["deployment_id"]).replace(":", "-")
    plan = replace(
        bundle.plan,
        release_id=str(release["release_id"]),
        deployment_id=str(deployment["deployment_id"]),
        release=RuntimeArtifactDescriptor(
            artifact_ref=f"cache://releases/{release_dir}/release-set.json",
            sha256=_sha(release_bytes),
            bytes=len(release_bytes),
        ),
        deployment=RuntimeArtifactDescriptor(
            artifact_ref=f"cache://deployments/prod/{deployment_dir}/deployment.json",
            sha256=_sha(deployment_bytes),
            bytes=len(deployment_bytes),
        ),
        workload_pack=replace(
            bundle.plan.workload_pack,
            artifact_ref=(f"cache://releases/{release_dir}/packs/{bundle.plan.workload_pack.id}.airflow-pack.json"),
        ),
        runtime_payloads=(),
    )
    payloads = {
        plan.release.artifact_ref: release_bytes,
        plan.deployment.artifact_ref: deployment_bytes,
        plan.workload_pack.artifact_ref: bundle.objects[_key(bundle.plan.workload_pack.artifact_ref)],
        **{
            descriptor.artifact_ref: bundle.objects[_key(descriptor.artifact_ref)]
            for descriptor in (plan.binding_set, plan.connection_registry, plan.credential_runtime)
        },
    }

    with pytest.raises(InitFetchError) as caught:
        runtime_init_fetch_receipts_module.validate_runtime_receipts(plan, payloads)

    assert caught.value.code == "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED"
    assert plan.runtime_payloads == ()


def test_init_fetch_bootstraps_scoped_dbt_spool_before_plan_decode(
    tmp_path: Path,
) -> None:
    bootstrap = tmp_path / "evidence"
    bootstrap.mkdir()
    service = AirflowRuntimeInitFetchService(
        dev_evidence_bootstrap_root=bootstrap,
    )

    with pytest.raises(InitFetchError):
        service.init_fetch(
            {
                DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV: str(bootstrap),
                PLAN_B64_ENV: "",
                PLAN_SHA256_ENV: "",
            }
        )

    spool = bootstrap / "dbt-spool"
    assert spool.is_dir()
    assert not spool.is_symlink()


class RecordingRegistry:
    def __init__(self, objects: dict[PurePosixPath, bytes]) -> None:
        self.objects = objects
        self.calls: list[tuple[str, PurePosixPath]] = []

    @property
    def authority_scope_id(self) -> str:
        return "sha256:" + "2" * 64

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        self.calls.append(("stat", key))
        body = self.objects[key]
        return ArtifactMetadata(
            key=key,
            size_bytes=len(body),
            advertised_sha256=_sha(body),
        )

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        self.calls.append(("download", key))
        body = self.objects[key]
        assert len(body) <= max_bytes
        destination.write_bytes(body)


@dataclass
class RecordingRegistryFactory:
    registry: RecordingRegistry
    configurations: list[RuntimeRegistryConfiguration]

    def build(self, configuration: RuntimeRegistryConfiguration) -> RecordingRegistry:
        self.configurations.append(configuration)
        return self.registry


@dataclass
class RecordingReleaseAttestationVerifier:
    calls: list[RuntimeArtifactAttestationSubject]

    def verify(self, *, subject: RuntimeArtifactAttestationSubject) -> None:
        self.calls.append(subject)


@dataclass
class RecordingDeploymentAttestationVerifier:
    policy_sha256: str
    calls: list[AirflowArtifactObservedSubject]

    def verify(self, subject: AirflowArtifactObservedSubject) -> AirflowArtifactAttestationVerification:
        self.calls.append(subject)
        return AirflowArtifactAttestationVerification(
            decision="verified",
            code="DPONE_ARTIFACT_ATTESTATION_VERIFIED",
            message="verified test artifact attestation",
            attestation_id="sha256:" + "a" * 64,
            policy_fingerprint="sha256:" + "b" * 64,
            public_key_id="test-key",
            public_key_sha256="sha256:" + "c" * 64,
            verifier_version="3.0.4",
            verified_at="2026-07-29T10:00:00Z",
        )


@dataclass
class RejectedAttestationVerifier:
    code: str
    policy_sha256: str

    def verify(self, subject: AirflowArtifactObservedSubject) -> AirflowArtifactAttestationVerification:
        del subject
        raise AirflowArtifactAttestationRejected(
            AirflowArtifactAttestationVerification(
                decision="invalid",
                code=self.code,
                message="artifact source is not allowed by policy",
                attestation_id="sha256:" + "a" * 64,
                policy_fingerprint="sha256:" + "b" * 64,
                public_key_id=None,
                public_key_sha256=None,
                verifier_version="3.0.4",
                verified_at="2026-07-29T10:00:00Z",
            )
        )


@dataclass
class RegistryFailingAttestationVerifier:
    code: str
    policy_sha256: str

    def verify(self, subject: AirflowArtifactObservedSubject) -> AirflowArtifactAttestationVerification:
        del subject
        raise AirflowArtifactAttestationRegistryError(
            self.code,
            "redacted registry failure",
        )


def test_runtime_init_fetch_plan_rejects_hash_and_noncanonical_bytes_before_io() -> None:
    bundle = _bundle()
    encoded = base64.b64encode(bundle.plan_bytes + b"\n").decode("ascii")
    expected = _sha(bundle.plan_bytes + b"\n")

    with pytest.raises(InitFetchError) as exc:
        decode_runtime_init_fetch_plan(encoded, expected)

    assert exc.value.code == "DPONE_INIT_FETCH_PLAN_NON_CANONICAL"


def test_runtime_init_fetch_plan_v3_round_trip_preserves_hook_ownership() -> None:
    bundle = _bundle(
        execution_scope="process",
        process_selector="dbo.orders",
        hook_execution="inline",
    )

    plan, actual_sha256 = decode_runtime_init_fetch_plan(
        bundle.environment[PLAN_B64_ENV],
        bundle.environment[PLAN_SHA256_ENV],
    )

    assert plan.to_dict()["schema"] == "dpone.airflow-runtime-init-fetch-plan.v3"
    assert plan.execution.scope == "process"
    assert plan.execution.process_selector == "dbo.orders"
    assert plan.execution.hook_execution == "inline"
    assert plan.runtime_payloads == ()
    assert actual_sha256 == bundle.plan_sha256


def test_runtime_init_fetch_end_to_end_publishes_ready_and_prepares_shell_free_command(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)
    factory = RecordingRegistryFactory(registry, [])
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    artifact_root = tmp_path / "artifacts"
    worktree_root = tmp_path / "worktree"
    service = AirflowRuntimeInitFetchService(
        registry_factory=factory,
        registry_config_path=registry_path,
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    )

    ready = service.init_fetch(bundle.environment)
    command = service.prepare_pack_exec(bundle.environment)
    first_calls = list(registry.calls)
    repeated_ready = service.init_fetch(bundle.environment)

    assert ready["schema"] == "dpone.runtime-fetch-ready.v1"
    assert (
        GitOpsSchemaValidator().validate(
            dict(ready),
            expected_kind="dpone.runtime-fetch-ready.v1",
        )
        == ()
    )
    assert repeated_ready == ready
    assert ready["release_id"] == bundle.plan.release_id
    assert ready["plan_sha256"] == bundle.plan_sha256
    assert registry.calls == [
        call
        for descriptor in bundle.plan.artifacts
        for call in (
            ("stat", _key(descriptor.artifact_ref)),
            ("download", _key(descriptor.artifact_ref)),
        )
    ]
    assert registry.calls == first_calls
    assert command.argv == (
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
        "--selector",
        "load_orders",
    )
    assert command.env == {
        "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1",
        RUNTIME_CONNECTION_CONTEXT_ENV: (
            artifact_root / "payload" / _key(bundle.plan.binding_set.artifact_ref).parent
        ).as_posix(),
    }
    assert command.working_directory == worktree_root.absolute()
    assert command.publish_xcom is True
    assert (worktree_root / "runtime" / "manifest.json").read_text(encoding="utf-8") == "kind: dpone.batch.v1\n"
    assert factory.configurations == [
        RuntimeRegistryConfiguration(
            logical_ref="dpone-prod-artifacts",
            registry_uri="s3://dpone-artifacts/releases",
            access_mode="workload_identity",
        )
    ]


def test_runtime_init_fetch_extracts_and_reverifies_pinned_dbt_project_bundle(
    tmp_path: Path,
) -> None:
    project = tmp_path / "source-project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    (project / "models" / "orders.sql").write_text("select 1\n", encoding="utf-8")
    dbt_runtime = _dbt_runtime_fixture(project)
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
        dbt_runtime=dbt_runtime,
    )
    artifact_root = tmp_path / "artifacts"
    worktree_root = tmp_path / "worktree"
    executor = RuntimeInitFetchExecutor(
        registry=RecordingRegistry(bundle.objects),
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    )

    executor.execute(bundle.plan, plan_sha256=bundle.plan_sha256)
    command = VerifiedPackLauncher(
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert command.argv == (
        "dpone",
        "dbt",
        "execute-pack",
        "runtime/dbt-execution-pack.json",
        "--format",
        "json",
    )
    assert command.exit_code_policy == "child"
    assert command.publish_xcom is True
    assert (worktree_root / "dbt-project" / "dbt_project.yml").read_text(encoding="utf-8") == "name: analytics\n"
    assert (worktree_root / "dbt-project" / "models" / "orders.sql").read_text(encoding="utf-8") == "select 1\n"

    (worktree_root / "dbt-project" / "models" / "orders.sql").write_text(
        "select 2\n",
        encoding="utf-8",
    )
    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=artifact_root,
            worktree_root=worktree_root,
        ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)
    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize(
    ("mismatch", "expected_code"),
    (
        ("project_bundle", "DPONE_DBT_PROJECT_BUNDLE_INVALID"),
        ("manifest", "DPONE_DBT_SELECTION_DRIFT"),
        ("selection", "DPONE_DBT_SELECTION_DRIFT"),
        ("toolchain", "DPONE_DBT_SELECTION_DRIFT"),
        ("workload", "DPONE_DBT_SELECTION_DRIFT"),
    ),
)
def test_launcher_rejects_dbt_release_and_execution_pack_identity_mismatch(
    mismatch: str,
    expected_code: str,
    tmp_path: Path,
) -> None:
    project = tmp_path / "source-project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
        dbt_runtime=_dbt_runtime_fixture(project, mismatch=mismatch),
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == expected_code
    assert str(exc.value) == "verified dbt runtime identity is inconsistent"
    assert "must_not_leak" not in str(exc.value)


def test_launcher_rejects_dbt_execute_pack_without_runtime_payload_identity(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_DBT_SELECTION_DRIFT"


def test_runtime_init_fetch_rejects_pack_mutated_after_fingerprint_claim(
    tmp_path: Path,
) -> None:
    bundle = _bundle(mutate_pack_after_sign=True)
    registry = RecordingRegistry(bundle.objects)

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=registry,
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert not (tmp_path / "artifacts" / "runtime-fetch-ready.json").exists()
    assert not (tmp_path / "worktree").exists()


def test_runtime_init_fetch_accepts_kubernetes_configmap_projection_symlinks(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    mount = tmp_path / "artifact-registry"
    versioned = mount / "..2026_07_21_12_00_00.123456789"
    versioned.mkdir(parents=True)
    (versioned / "registry.json").write_bytes(bundle.registry_config_bytes)
    (mount / "..data").symlink_to(versioned.name, target_is_directory=True)
    registry_path = mount / "registry.json"
    registry_path.symlink_to("..data/registry.json")
    assert registry_path.is_symlink()

    service = AirflowRuntimeInitFetchService(
        registry_factory=RecordingRegistryFactory(RecordingRegistry(bundle.objects), []),
        registry_config_path=registry_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    ready = service.init_fetch(bundle.environment)

    assert ready["schema"] == "dpone.runtime-fetch-ready.v1"
    assert ready["plan_sha256"] == bundle.plan_sha256


def test_runtime_init_fetch_rejects_registry_config_symlink_escape(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    outside = tmp_path / "escaped-registry.json"
    outside.write_bytes(bundle.registry_config_bytes)
    mount = tmp_path / "artifact-registry"
    mount.mkdir()
    registry_path = mount / "registry.json"
    registry_path.symlink_to(outside)

    service = AirflowRuntimeInitFetchService(
        registry_factory=RecordingRegistryFactory(RecordingRegistry(bundle.objects), []),
        registry_config_path=registry_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.init_fetch(bundle.environment)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID"
    assert "mount directory" in str(exc.value)


def test_runtime_ready_parser_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    bundle = _bundle()
    ready, _ = _publish_ready_from_init_view(tmp_path, bundle)
    payload = json.dumps(ready, separators=(",", ":"), sort_keys=True).encode("utf-8")
    duplicate = payload.replace(
        b'"plan_sha256":',
        f'"plan_sha256":"{bundle.plan_sha256}","plan_sha256":'.encode(),
        1,
    )

    with pytest.raises(InitFetchError) as exc:
        parse_ready_manifest(
            duplicate,
            plan=bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


def test_runtime_init_fetch_retry_after_ready_publication_failure_reuses_same_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    artifact_root = tmp_path / "artifacts"
    worktree_root = tmp_path / "worktree"
    service = AirflowRuntimeInitFetchService(
        registry_factory=RecordingRegistryFactory(registry, []),
        registry_config_path=registry_path,
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    )
    actual_write_ready = runtime_init_fetch_service_module.write_ready_last
    attempts = 0

    def fail_once(path: Path, payload: bytes) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise InitFetchError(
                "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
                "injected ready publication failure",
            )
        actual_write_ready(path, payload)

    monkeypatch.setattr(
        runtime_init_fetch_service_module,
        "write_ready_last",
        fail_once,
    )

    with pytest.raises(InitFetchError):
        service.init_fetch(bundle.environment)

    assert worktree_root.is_dir()
    assert tuple(worktree_root.iterdir()) == ()

    ready = service.init_fetch(bundle.environment)

    assert ready["schema"] == "dpone.runtime-fetch-ready.v1"
    assert (worktree_root / "runtime" / "manifest.json").is_file()


def test_runtime_init_fetch_preserves_worktree_after_ambiguous_ready_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    artifact_root = tmp_path / "artifacts"
    worktree_root = tmp_path / "worktree"
    service = AirflowRuntimeInitFetchService(
        registry_factory=RecordingRegistryFactory(registry, []),
        registry_config_path=registry_path,
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    )
    actual_write_ready = runtime_init_fetch_service_module.write_ready_last
    actual_fsync = runtime_init_fetch_storage_module.os.fsync
    attempts = 0

    def fail_directory_fsync_once(path: Path, payload: bytes) -> None:
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            actual_write_ready(path, payload)
            return
        fsync_calls = 0

        def fail_after_link(descriptor: int) -> None:
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("injected directory fsync failure")
            actual_fsync(descriptor)

        with monkeypatch.context() as context:
            context.setattr(runtime_init_fetch_storage_module.os, "fsync", fail_after_link)
            actual_write_ready(path, payload)

    monkeypatch.setattr(
        runtime_init_fetch_service_module,
        "write_ready_last",
        fail_directory_fsync_once,
    )

    with pytest.raises(InitFetchError) as exc:
        service.init_fetch(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert (artifact_root / "runtime-fetch-ready.json").is_file()
    assert (worktree_root / "runtime" / "manifest.json").is_file()

    ready = service.init_fetch(bundle.environment)

    assert ready["schema"] == "dpone.runtime-fetch-ready.v1"
    assert (worktree_root / "runtime" / "manifest.json").is_file()


def test_production_without_verifier_fails_before_registry_factory_or_registry_io(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_tier="production")
    registry = RecordingRegistry(bundle.objects)
    factory = RecordingRegistryFactory(registry, [])
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    trust_path = tmp_path / "policy.json"
    trust_path.write_bytes(bundle.trust_policy_bytes)
    service = AirflowRuntimeInitFetchService(
        registry_factory=factory,
        registry_config_path=registry_path,
        trust_policy_path=trust_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.init_fetch(bundle.environment)

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_REQUIRED"
    assert factory.configurations == []
    assert registry.calls == []
    assert not (tmp_path / "artifacts").exists()


def test_runtime_init_fetch_rejects_multiple_attestation_authorities_before_io(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_tier="production")
    registry = RecordingRegistry(bundle.objects)

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=registry,
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
            attestation_verifier=RecordingReleaseAttestationVerifier([]),
            deployment_attestation_verifier=RecordingDeploymentAttestationVerifier(
                policy_sha256=_sha(bundle.trust_policy_bytes),
                calls=[],
            ),
            trusted_attestation_required=True,
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT"
    assert registry.calls == []
    assert not (tmp_path / "artifacts").exists()


def test_production_deployment_policy_uses_deployment_verifier_and_v2_ready(
    tmp_path: Path,
) -> None:
    bundle = _deployment_policy_bundle()
    registry = RecordingRegistry(bundle.objects)
    factory = RecordingRegistryFactory(registry, [])
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    trust_path = tmp_path / "policy.json"
    trust_path.write_bytes(bundle.trust_policy_bytes)
    verifier = RecordingDeploymentAttestationVerifier(
        policy_sha256=_sha(bundle.trust_policy_bytes),
        calls=[],
    )
    service = AirflowRuntimeInitFetchService(
        registry_factory=factory,
        deployment_attestation_verifier=verifier,
        registry_config_path=registry_path,
        trust_policy_path=trust_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    ready = service.init_fetch(bundle.environment)

    assert ready["schema"] == "dpone.runtime-fetch-ready.v2"
    assert ready["verification"]["attestations"] == "passed"
    evidence = ready["verification"]["artifact_attestation"]
    assert evidence["subject_kind"] == "airflow_deployment"
    assert evidence["backend"] == "cosign_public_key"
    assert evidence["unobserved_claims"] == ["airflow_index_sha256"]
    assert verifier.calls == [_observed_subject(bundle)]


def test_runtime_init_fetch_preserves_stable_attestation_rejection_code(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_tier="production")

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=RecordingRegistry(bundle.objects),
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
            deployment_attestation_verifier=RejectedAttestationVerifier(
                "DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED",
                _sha(bundle.trust_policy_bytes),
            ),
            trusted_attestation_required=True,
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED"


@pytest.mark.parametrize(
    "code",
    [
        "DPONE_ARTIFACT_ATTESTATION_NOT_FOUND",
        "DPONE_ARTIFACT_ATTESTATION_INVALID",
    ],
)
def test_runtime_init_fetch_preserves_stable_attestation_registry_code(
    tmp_path: Path,
    code: str,
) -> None:
    bundle = _bundle(trust_tier="production")

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=RecordingRegistry(bundle.objects),
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
            deployment_attestation_verifier=RegistryFailingAttestationVerifier(
                code,
                _sha(bundle.trust_policy_bytes),
            ),
            trusted_attestation_required=True,
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == code


def test_non_production_runtime_ready_records_concrete_attestation_evidence() -> None:
    bundle = _bundle()
    published = {
        artifact.artifact_ref: ReadyArtifact(
            artifact_ref=artifact.artifact_ref,
            locator=f"payload/{index}.json",
            sha256=artifact.sha256,
            bytes=artifact.bytes,
        )
        for index, artifact in enumerate(bundle.plan.artifacts)
    }

    ready = build_runtime_fetch_ready(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        published=published,
        attestation_required=True,
        attestation_status="passed",
        attestation_verification=RecordingDeploymentAttestationVerifier(
            policy_sha256=_sha(bundle.trust_policy_bytes),
            calls=[],
        ).verify(
            _observed_subject(bundle),
        ),
        attestation_subject=_observed_subject(bundle),
        runtime_payload_sha256="sha256:" + "e" * 64,
        verified_pack_fingerprint=bundle.plan.workload_pack.pack_fingerprint,
    )

    assert ready.schema == "dpone.runtime-fetch-ready.v2"
    assert ready.trust_tier == "non_production"
    assert (
        GitOpsSchemaValidator().validate(
            ready.to_dict(),
            expected_kind="dpone.runtime-fetch-ready.v2",
        )
        == ()
    )
    assert (
        parse_ready_manifest(
            ready_manifest_bytes(ready),
            plan=bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=True,
            deployment_attestation_required=True,
        )
        == ready
    )


def test_trust_policy_snapshot_mismatch_uses_trust_specific_error(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_tier="production")
    registry = RecordingRegistry(bundle.objects)
    factory = RecordingRegistryFactory(registry, [])
    registry_path = tmp_path / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    trust_path = tmp_path / "policy.json"
    trust_path.write_bytes(b"{}")
    service = AirflowRuntimeInitFetchService(
        registry_factory=factory,
        registry_config_path=registry_path,
        trust_policy_path=trust_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.init_fetch(bundle.environment)

    assert exc.value.code == "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH"
    assert factory.configurations == []
    assert registry.calls == []


@pytest.mark.parametrize(
    ("policy_attestations", "expected_requirement", "expected_status"),
    (
        ("optional", "optional", "not_required"),
        ("required_for_prod", "required", "passed"),
    ),
)
def test_init_filesystem_view_records_effective_attestation_decision(
    tmp_path: Path,
    policy_attestations: str,
    expected_requirement: str,
    expected_status: str,
) -> None:
    bundle = _bundle(trust_policy_attestations=policy_attestations)

    ready, verifier = _publish_ready_from_init_view(
        tmp_path / policy_attestations,
        bundle,
    )

    assert ready["verification"]["effective_attestation_requirement"] == expected_requirement
    assert ready["verification"]["attestations"] == expected_status
    if expected_requirement == "required":
        assert len(verifier.calls) == 1
        assert verifier.calls[0].release_id == bundle.plan.release_id
        assert verifier.calls[0].subject_sha256 == bundle.plan.release.sha256
    else:
        assert verifier.calls == []


@pytest.mark.parametrize("policy_attestations", ("optional", "required_for_prod"))
def test_base_filesystem_view_uses_ready_without_init_only_configuration(
    tmp_path: Path,
    policy_attestations: str,
) -> None:
    root = tmp_path / policy_attestations
    bundle = _bundle(trust_policy_attestations=policy_attestations)
    _publish_ready_from_init_view(root, bundle)
    base_view = root / "base-view"
    base_view.mkdir()
    registry_path = base_view / "registry.json"
    trust_path = base_view / "policy.json"
    service = AirflowRuntimeInitFetchService(
        registry_config_path=registry_path,
        trust_policy_path=trust_path,
        artifact_root=root / "artifacts",
        worktree_root=root / "worktree",
    )

    command = service.prepare_pack_exec(bundle.environment)

    assert not registry_path.exists()
    assert not trust_path.exists()
    assert command.argv[:3] == ("dpone", "run", "runtime/manifest.json")


@pytest.mark.parametrize("mutation", ["changed_manifest", "extra_file"])
def test_base_launcher_rejects_worktree_changed_after_ready(
    tmp_path: Path,
    mutation: str,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    worktree_root = tmp_path / "worktree"
    if mutation == "changed_manifest":
        (worktree_root / "runtime" / "manifest.json").write_text(
            "kind: attacker.manifest.v1\n",
            encoding="utf-8",
        )
    else:
        (worktree_root / "runtime" / "unreviewed.sql").write_text(
            "SELECT password FROM credentials",
            encoding="utf-8",
        )
    service = AirflowRuntimeInitFetchService(
        artifact_root=tmp_path / "artifacts",
        worktree_root=worktree_root,
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_init_reuse_rejects_a_changed_trusted_attestation_decision_before_registry_io(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)
    artifact_root = tmp_path / "artifacts"
    worktree_root = tmp_path / "worktree"
    RuntimeInitFetchExecutor(
        registry=registry,
        artifact_root=artifact_root,
        worktree_root=worktree_root,
    ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)
    completed_calls = list(registry.calls)

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=registry,
            artifact_root=artifact_root,
            worktree_root=worktree_root,
            attestation_verifier=RecordingReleaseAttestationVerifier([]),
            trusted_attestation_required=True,
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"
    assert registry.calls == completed_calls


def test_per_artifact_limit_fails_before_registry_or_destination_io(tmp_path: Path) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=registry,
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
            max_artifact_bytes=1,
        ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_TOO_LARGE"
    assert registry.calls == []
    assert not (tmp_path / "artifacts").exists()


def test_launcher_refuses_pack_tampering_after_ready_publication(tmp_path: Path) -> None:
    bundle = _bundle()
    registry = RecordingRegistry(bundle.objects)
    RuntimeInitFetchExecutor(
        registry=registry,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).execute(bundle.plan, plan_sha256=bundle.plan_sha256)
    pack_path = tmp_path / "artifacts" / "payload" / _key(bundle.plan.workload_pack.artifact_ref).as_posix()
    pack_path.write_bytes(b"x" * bundle.plan.workload_pack.bytes)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_launcher_refuses_runtime_connection_context_tampering_after_ready_publication(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    registry_path = tmp_path / "artifacts" / "payload" / _key(bundle.plan.connection_registry.artifact_ref).as_posix()
    registry_path.write_bytes(b"x" * bundle.plan.connection_registry.bytes)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_launcher_preserves_valid_strict_v2_pre_hook_command(tmp_path: Path) -> None:
    bundle = _bundle(
        execution_kind="pre_hook",
        hook_name="refresh_orders",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "hooks",
        "execute",
        "runtime/manifest.json",
        "--phase",
        "pre_hook",
        "--hook-id",
        "refresh_orders",
    )
    assert command.env == {}
    assert command.exit_code_policy == "child"
    assert command.publish_xcom is False


def test_launcher_v2_uses_unique_structured_generated_pre_hook_command(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        execution_kind="pre_hook",
        hook_runtime_process_selector="dbo.orders",
        hook_name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "hooks",
        "execute",
        "runtime/manifest.json",
        "--phase",
        "pre_hook",
        "--hook-id",
        "refresh_orders",
        "--selector",
        "dbo.orders",
    )
    assert command.exit_code_policy == "child"


def test_launcher_v3_selects_exact_process_runtime_command(tmp_path: Path) -> None:
    bundle = _bundle(
        execution_scope="process",
        process_selector="dbo.orders",
        hook_execution="inline",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
        "--selector",
        "dbo.orders",
    )
    assert command.env["DPONE_RUNTIME_CONNECTION_CONTEXT"]
    assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS" not in command.env


def test_launcher_v3_process_scope_selects_default_process_explicitly(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        execution_scope="process",
        hook_execution="inline",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
    )
    assert bundle.plan.execution.scope == "process"
    assert bundle.plan.execution.process_selector is None


def test_launcher_v3_workload_scope_runs_without_process_selector(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        execution_scope="workload",
        hook_execution="externalized",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
    )
    assert command.env["DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS"] == "1"


def test_launcher_v3_executes_structured_selector_scoped_pre_hook(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        execution_kind="pre_hook",
        execution_scope="process",
        process_selector="dbo.orders",
        hook_execution="externalized",
        hook_name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert command.argv == (
        "dpone",
        "hooks",
        "execute",
        "runtime/manifest.json",
        "--phase",
        "pre_hook",
        "--hook-id",
        "refresh_orders",
        "--selector",
        "dbo.orders",
    )
    assert command.exit_code_policy == "child"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("hook_id", "other_hook"),
        ("process_selector", "dbo.customers"),
        (
            "argv",
            [
                "dpone",
                "hooks",
                "execute",
                "runtime/manifest.json",
                "--phase",
                "pre_hook",
                "--hook-id",
                "refresh_orders",
                "--selector",
                "dbo.customers",
            ],
        ),
    ),
)
def test_launcher_v3_rejects_tampered_structured_pre_hook(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    bundle = _bundle(
        execution_kind="pre_hook",
        execution_scope="process",
        process_selector="dbo.orders",
        hook_execution="externalized",
        hook_name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        hook_runtime_overrides={field: value},
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_launcher_accepts_transitional_default_bootstrap_command_key(tmp_path: Path) -> None:
    """Published packs keyed commands as ``__default__``; plans use workload id."""

    bundle = _bundle(bootstrap_command_key="__default__")
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
        attestation_required=False,
    )

    assert bundle.plan.execution.selector == "load_orders"
    assert command.argv == (
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
    )


def test_launcher_accepts_only_the_fixed_dbt_execute_pack_shape(tmp_path: Path) -> None:
    project = tmp_path / "source-project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
        dbt_runtime=_dbt_runtime_fixture(project),
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    command = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)

    assert command.argv == (
        "dpone",
        "dbt",
        "execute-pack",
        "runtime/dbt-execution-pack.json",
        "--format",
        "json",
    )


@pytest.mark.parametrize(
    "runtime_argv",
    (
        [
            "dpone",
            "hooks",
            "execute",
            "runtime/manifest.json",
            "--phase",
            "pre_hook",
            "--hook-id",
            "refresh_orders",
        ],
        [
            "dpone",
            "run",
            "runtime/manifest.json",
            "--format",
            "json",
            "--selector",
            "other_workload",
        ],
        [
            "dpone",
            "run",
            "runtime/manifest.json",
            "--format",
            "json",
            "--phase",
            "pre_hook",
        ],
    ),
    ids=("hook-kind-confusion", "wrong-selector", "unknown-control-flag"),
)
def test_launcher_rejects_noncanonical_runtime_command_grammar(
    tmp_path: Path,
    runtime_argv: list[str],
) -> None:
    bundle = _bundle(runtime_argv=runtime_argv)
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize(
    "hook_command",
    (
        ("dpone hooks execute runtime/manifest.json --phase pre_hook --hook-id refresh_orders --phase pre_hook"),
        (
            "dpone hooks execute runtime/manifest.json --phase pre_hook "
            "--hook-id refresh_orders --hook-id refresh_orders"
        ),
        ("dpone hooks execute runtime/manifest.json --phase pre_hook --hook-id another_hook"),
        ("dpone hooks execute runtime/manifest.json --phase pre_hook --hook-id refresh_orders --selector load_orders"),
        "dpone run runtime/manifest.json --format json --selector load_orders",
    ),
    ids=(
        "duplicate-phase",
        "duplicate-hook-id",
        "wrong-hook-id",
        "unknown-control-flag",
        "runtime-kind-confusion",
    ),
)
def test_launcher_rejects_noncanonical_pre_hook_command_grammar(
    tmp_path: Path,
    hook_command: str,
) -> None:
    bundle = _bundle(
        execution_kind="pre_hook",
        hook_name="refresh_orders",
        hook_command=hook_command,
    )
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize(
    "runtime_environment",
    (
        {},
        {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "0"},
        {
            "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1",
            "DPONE_UNEXPECTED": "1",
        },
    ),
    ids=("missing", "wrong-value", "unknown-key"),
)
def test_launcher_requires_exact_runtime_environment_contract(
    tmp_path: Path,
    runtime_environment: dict[str, str],
) -> None:
    bundle = _bundle(runtime_environment=runtime_environment)
    _publish_ready_from_init_view(tmp_path, bundle)

    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
            attestation_required=False,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_runtime_pack_exec_rejects_kind_confusion_before_workload_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "hooks",
            "execute",
            "runtime/manifest.json",
            "--phase",
            "pre_hook",
            "--hook-id",
            "refresh_orders",
        ]
    )
    _publish_ready_from_init_view(tmp_path, bundle)
    service = AirflowRuntimeInitFetchService(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    class RuntimePackExecService:
        def prepare_pack_exec(self) -> object:
            return service.prepare_pack_exec(bundle.environment)

    execute_calls: list[object] = []

    def record_execute(command: object, **kwargs: object) -> int:
        execute_calls.append((command, kwargs))
        return 0

    monkeypatch.setattr(
        airflow_runtime_delivery_cmd,
        "AirflowRuntimeInitFetchService",
        RuntimePackExecService,
    )
    monkeypatch.setattr(
        airflow_runtime_delivery_cmd,
        "execute_verified_pack_command",
        record_execute,
    )

    code = airflow_runtime_delivery_cmd.cmd_airflow_runtime_pack_exec(
        argparse.Namespace(),
        ctx=object(),
        logger=logging.getLogger("test.runtime-pack-exec.command-grammar"),
    )

    assert code == 4
    assert execute_calls == []


@pytest.mark.parametrize("receipt_name", ("release", "deployment"))
@pytest.mark.parametrize(
    "invalid_member",
    (
        b'"created_at":null,"created_at":null',
        b'"created_at":NaN',
        b'"created_at":Infinity',
        b'"created_at":1e999',
        b'"created_at":' + (b"[" * 100_000) + b"0" + (b"]" * 100_000),
    ),
    ids=("duplicate-key", "nan", "infinity", "exponent-overflow", "recursive"),
)
def test_runtime_init_fetch_rejects_non_strict_receipt_json_without_ready(
    tmp_path: Path,
    receipt_name: str,
    invalid_member: bytes,
) -> None:
    bundle = _bundle()
    payload = _receipt_payload(bundle, receipt_name)
    invalid_bundle = _with_receipt_payload(
        bundle,
        receipt_name,
        b"{" + invalid_member + b"," + payload[1:],
    )
    artifact_root = tmp_path / "artifacts"

    with pytest.raises(InitFetchError) as exc:
        RuntimeInitFetchExecutor(
            registry=RecordingRegistry(invalid_bundle.objects),
            artifact_root=artifact_root,
            worktree_root=tmp_path / "worktree",
        ).execute(
            invalid_bundle.plan,
            plan_sha256=invalid_bundle.plan_sha256,
        )

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert not (artifact_root / "runtime-fetch-ready.json").exists()


@pytest.mark.parametrize("decision", (None, "", "required_for_prod", True))
def test_launcher_rejects_missing_or_malformed_effective_attestation_decision(
    tmp_path: Path,
    decision: object | None,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    if decision is None:
        ready["verification"].pop("effective_attestation_requirement", None)
    else:
        ready["verification"]["effective_attestation_requirement"] = decision
    ready_path.write_bytes(_json_bytes(ready))
    service = AirflowRuntimeInitFetchService(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


def test_launcher_rejects_missing_attestation_decision_binding(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    ready["verification"].pop("attestation_decision_sha256")
    ready_path.write_bytes(_json_bytes(ready))
    service = AirflowRuntimeInitFetchService(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


@pytest.mark.parametrize("container", (None, "artifacts", "verification"))
def test_launcher_rejects_unknown_ready_manifest_fields(
    tmp_path: Path,
    container: str | None,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    target = ready if container is None else ready[container]
    target["unexpected"] = "must-not-pass"
    ready_path.write_bytes(_json_bytes(ready))

    with pytest.raises(InitFetchError) as exc:
        AirflowRuntimeInitFetchService(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


def test_launcher_rejects_ready_decision_bound_to_a_different_plan(
    tmp_path: Path,
) -> None:
    bundle = _bundle()
    _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    ready["plan_sha256"] = "sha256:" + "f" * 64
    ready["verification"]["effective_attestation_requirement"] = "required"
    ready["verification"]["attestations"] = "passed"
    ready_path.write_bytes(_json_bytes(ready))
    service = AirflowRuntimeInitFetchService(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


def test_launcher_rejects_tampered_policy_strengthened_ready_decision(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_policy_attestations="required_for_prod")
    _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    ready["verification"]["effective_attestation_requirement"] = "optional"
    ready["verification"]["attestations"] = "not_required"
    ready_path.write_bytes(_json_bytes(ready))
    base_view = tmp_path / "base-view"
    base_view.mkdir()
    service = AirflowRuntimeInitFetchService(
        registry_config_path=base_view / "registry.json",
        trust_policy_path=base_view / "policy.json",
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"


def test_production_launcher_rejects_ready_attestation_downgrade(
    tmp_path: Path,
) -> None:
    bundle = _bundle(trust_tier="production")
    _, verifier = _publish_ready_from_init_view(tmp_path, bundle)
    ready_path = tmp_path / "artifacts" / "runtime-fetch-ready.json"
    ready = json.loads(ready_path.read_text(encoding="utf-8"))
    ready["verification"]["effective_attestation_requirement"] = "optional"
    ready["verification"]["attestations"] = "not_required"
    ready_path.write_bytes(_json_bytes(ready))
    base_view = tmp_path / "base-view"
    base_view.mkdir()
    service = AirflowRuntimeInitFetchService(
        registry_config_path=base_view / "registry.json",
        trust_policy_path=base_view / "policy.json",
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError) as exc:
        service.prepare_pack_exec(bundle.environment)

    assert exc.value.code == "DPONE_RUNTIME_FETCH_READY_INVALID"
    assert len(verifier.calls) == 1
    assert verifier.calls[0].release_id == bundle.plan.release_id
    assert verifier.calls[0].subject_sha256 == bundle.plan.release.sha256


def test_registry_factory_normalizes_parser_failure_without_uri_disclosure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = RuntimeRegistryConfiguration(
        logical_ref="prod-registry",
        registry_uri="s3://private-bucket/secret-prefix",
        access_mode="workload_identity",
    )

    def fail_build(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise ValueError("s3://private-bucket/secret-prefix?token=secret")

    monkeypatch.setattr(
        "dpone.readiness.airflow_runtime_init_fetch.ArtifactRegistryOptions.build",
        fail_build,
    )

    with pytest.raises(InitFetchError) as exc:
        WorkloadIdentityRegistryFactory().build(configuration)

    assert exc.value.code == "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID"
    assert "private-bucket" not in str(exc.value)
    assert "secret" not in str(exc.value)


def test_parallel_init_fetch_services_with_isolated_roots_do_not_cross_write(
    tmp_path: Path,
) -> None:
    bundle = _bundle()

    def execute(label: str) -> tuple[dict[str, Any], Path, Path]:
        root = tmp_path / label
        root.mkdir()
        registry_path = root / "registry.json"
        registry_path.write_bytes(bundle.registry_config_bytes)
        artifact_root = root / "artifacts"
        worktree_root = root / "worktree"
        service = AirflowRuntimeInitFetchService(
            registry_factory=RecordingRegistryFactory(RecordingRegistry(dict(bundle.objects)), []),
            registry_config_path=registry_path,
            artifact_root=artifact_root,
            worktree_root=worktree_root,
        )
        return dict(service.init_fetch(bundle.environment)), artifact_root, worktree_root

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(execute, ("first", "second")))

    for ready, artifact_root, worktree_root in results:
        assert ready["deployment_id"] == bundle.plan.deployment_id
        assert (artifact_root / "runtime-fetch-ready.json").is_file()
        assert (worktree_root / "runtime" / "manifest.json").read_text(encoding="utf-8") == ("kind: dpone.batch.v1\n")
    first_artifact_root = results[0][1]
    second_artifact_root = results[1][1]
    assert first_artifact_root != second_artifact_root
    assert not (first_artifact_root / "second").exists()
    assert not (second_artifact_root / "first").exists()


def test_runtime_pack_exec_failure_never_starts_workload_or_changes_directory(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class MissingReadyService:
        def prepare_pack_exec(self) -> object:
            raise InitFetchError(
                "DPONE_RUNTIME_FETCH_READY_INVALID",
                "runtime ready manifest is unavailable",
            )

    def unexpected_call(*args: object, **kwargs: object) -> int:
        del args, kwargs
        raise AssertionError("workload execution must not start before verified ready")

    monkeypatch.setattr(
        airflow_runtime_delivery_cmd,
        "AirflowRuntimeInitFetchService",
        MissingReadyService,
    )
    monkeypatch.setattr(
        airflow_runtime_delivery_cmd,
        "execute_verified_pack_command",
        unexpected_call,
    )
    logger = logging.getLogger("test.runtime-pack-exec")

    code = airflow_runtime_delivery_cmd.cmd_airflow_runtime_pack_exec(
        argparse.Namespace(),
        ctx=object(),
        logger=logger,
    )

    assert code == 4
    assert "DPONE_RUNTIME_FETCH_READY_INVALID" in caplog.text


@pytest.mark.parametrize("error_number", [2, 13, 28])
def test_runtime_pack_exec_cli_reports_safe_preparation_os_diagnostic(
    error_number: int,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class UnavailableService:
        def prepare_pack_exec(self) -> object:
            raise OSError(error_number, "private-message", "/private-workload-path")

    monkeypatch.setattr(airflow_runtime_delivery_cmd, "AirflowRuntimeInitFetchService", UnavailableService)
    code = airflow_runtime_delivery_cmd.cmd_airflow_runtime_pack_exec(
        argparse.Namespace(), ctx=object(), logger=logging.getLogger("test.runtime-pack-exec.os-error")
    )

    assert code == 5
    assert "DPONE_RUNTIME_PACK_EXEC_FAILED" in caplog.text
    assert "stage=prepare_verified_command" in caplog.text
    assert "service_path=verified_inputs" in caplog.text
    assert f"exception_type={type(OSError(error_number, 'unused')).__name__}" in caplog.text
    assert f"errno={error_number}" in caplog.text
    assert "private-message" not in caplog.text
    assert "private-workload-path" not in caplog.text
    assert capsys.readouterr().out == ""


def test_runtime_pack_exec_help_describes_hook_and_runtime_service_files() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    command = airflow_runtime_delivery_cmd.register_runtime_pack_exec_parser(subparsers)

    help_text = command.format_help()

    assert "/var/lib/dpone/run" in help_text
    assert "/airflow/xcom/return.json" in help_text
    assert "separate hooks use no XCom path" in help_text


def test_runtime_pack_exec_writes_non_empty_xcom_return_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shell-free runtime-pack-exec must publish return.json for the KPO sidecar."""

    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    run_dir = tmp_path / "run"
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    evidence = {
        "status": "SUCCESS",
        "extracted_rows": 430,
        "loaded_rows": 430,
        "duration_seconds": 1.25,
    }
    command = VerifiedPackCommand(
        argv=("dpone", "run", "runtime/manifest.json", "--format", "json"),
        env={"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        working_directory=workdir,
    )

    def fake_run(argv: list[str], **kwargs: object) -> object:
        del argv
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(json.dumps(evidence).encode("utf-8"))
        return argparse.Namespace(returncode=0)

    monkeypatch.setattr(pack_exec.subprocess, "run", fake_run)

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=run_dir,
        xcom_return_path=xcom_path,
    )

    assert code == 0
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    assert payload.get("kind") == "gitops.airflow_xcom_summary"
    assert payload.get("status") == "passed"
    assert (run_dir / "runtime-evidence.json").is_file()


def test_runtime_pack_exec_removes_ambient_hook_skip_for_inline_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    command = VerifiedPackCommand(
        argv=("dpone", "run", "runtime/manifest.json", "--format", "json"),
        env={},
        working_directory=workdir,
    )
    monkeypatch.setenv("DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS", "1")

    def fake_run(argv: list[str], **kwargs: object) -> object:
        del argv
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS" not in environment
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(b'{"status":"SUCCESS"}')
        return argparse.Namespace(returncode=0)

    monkeypatch.setattr(pack_exec.subprocess, "run", fake_run)

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=tmp_path / "run",
        xcom_return_path=xcom_path,
    )

    assert code == 0


def test_runtime_pack_exec_captures_dbt_failure_in_xcom_without_retry_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    run_dir = tmp_path / "run"
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    command = VerifiedPackCommand(
        argv=(
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ),
        env={"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        working_directory=workdir,
        exit_code_policy="child",
    )

    def fake_run(argv: list[str], **kwargs: object) -> object:
        assert argv[:3] == ["dpone", "dbt", "execute-pack"]
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(
            json.dumps(
                {
                    "schema": "dpone.dbt-execution-evidence.v1",
                    "status": "failed",
                    "code": "DPONE_DBT_EXECUTION_FAILED",
                }
            ).encode()
        )
        return argparse.Namespace(returncode=1)

    monkeypatch.setattr(pack_exec.subprocess, "run", fake_run)

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=run_dir,
        xcom_return_path=xcom_path,
    )

    assert code == 1
    assert json.loads(xcom_path.read_text(encoding="utf-8"))["status"] == "failed"


def test_runtime_pack_exec_propagates_pre_hook_failure_to_airflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    command = VerifiedPackCommand(
        argv=(
            "dpone",
            "hooks",
            "execute",
            "runtime/manifest.json",
            "--phase",
            "pre_hook",
            "--hook-id",
            "refresh_orders",
        ),
        env={},
        working_directory=workdir,
        exit_code_policy="child",
    )

    def fake_run(argv: list[str], **kwargs: object) -> object:
        assert argv[:3] == ["dpone", "hooks", "execute"]
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(b'{"status":"failed","error_code":"DPONE_HOOK_FAILED"}')
        return argparse.Namespace(returncode=17)

    monkeypatch.setattr(pack_exec.subprocess, "run", fake_run)

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=tmp_path / "run",
        xcom_return_path=xcom_path,
    )

    assert code == 17
    assert json.loads(xcom_path.read_text(encoding="utf-8"))["status"] == "failed"


def test_runtime_pack_exec_emits_outcome_marker_with_error_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Base container logs must surface start/outcome markers for KPO get_logs."""

    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    run_dir = tmp_path / "run"
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    evidence = {
        "status": "error",
        "error_code": "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED",
        "errors": [
            "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED: Canonical connection_ref "
            "execution requires a verified runtime connection context."
        ],
    }
    command = VerifiedPackCommand(
        argv=("dpone", "run", "runtime/manifest.json", "--format", "json"),
        env={"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        working_directory=workdir,
    )

    def fake_run(argv: list[str], **kwargs: object) -> object:
        del argv
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(json.dumps(evidence).encode("utf-8"))
        return argparse.Namespace(returncode=1)

    monkeypatch.setattr(pack_exec.subprocess, "run", fake_run)

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=run_dir,
        xcom_return_path=xcom_path,
    )

    assert code == 0
    err = capsys.readouterr().err
    assert "===DPONE_RUNTIME_PACK_EXEC_START===" in err
    assert "===DPONE_RUNTIME_PACK_EXEC_OUTCOME===" in err
    assert "status=failed" in err
    assert "error_code=DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED" in err
    assert "child_returncode=1" in err
    xcom = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert xcom["status"] == "failed"
    assert xcom["blockers"], xcom
    assert any(
        "CONNECTION_CONTEXT_REQUIRED" in str(item.get("code", "")) or "connection_ref" in str(item.get("message", ""))
        for item in xcom["blockers"]
    )


@dataclass(frozen=True)
class _Bundle:
    plan: RuntimeInitFetchPlan
    plan_bytes: bytes
    plan_sha256: str
    environment: dict[str, str]
    objects: dict[PurePosixPath, bytes]
    registry_config_bytes: bytes
    trust_policy_bytes: bytes


@dataclass(frozen=True)
class _DbtRuntimeFixture:
    workflow_id: str
    project_archive: bytes
    manifest: bytes
    selection_lock: bytes
    execution_pack: bytes


def _bundle(
    *,
    trust_tier: str = "non_production",
    trust_policy_attestations: str | None = None,
    mutate_pack_after_sign: bool = False,
    execution_kind: str = "runtime",
    execution_scope: str | None = None,
    process_selector: str | None = None,
    hook_execution: str | None = None,
    hook_name: str | None = None,
    hook_id: str | None = None,
    hook_runtime_process_selector: str | None = None,
    hook_runtime_overrides: dict[str, object] | None = None,
    hook_command: str | None = None,
    runtime_argv: list[str] | None = None,
    runtime_environment: dict[str, str] | None = None,
    bootstrap_command_key: str | None = None,
    dbt_project_archive: bytes | None = None,
    dbt_runtime: _DbtRuntimeFixture | None = None,
    dbt_wire_contract: str | None = None,
    dbt_release_mutator: Callable[[dict[str, Any], dict[str, bytes]], None] | None = None,
) -> _Bundle:
    archive = _archive(
        execution_pack=(dbt_runtime.execution_pack if dbt_runtime is not None else None),
    )
    workload_id = f"dbt__{dbt_runtime.workflow_id}" if dbt_runtime is not None else "load_orders"
    command_key = (
        (process_selector or "__default_process__" if execution_scope == "process" else workload_id)
        if bootstrap_command_key is None
        else bootstrap_command_key
    )
    selected_runtime_argv = runtime_argv or [
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
        *(
            ()
            if command_key in {"__default__", "__default_process__"}
            else (
                "--selector",
                command_key,
            )
        ),
    ]
    selected_runtime_environment = (
        {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"} if runtime_environment is None else runtime_environment
    )
    selected_hook_process_selector = (
        hook_runtime_process_selector if hook_runtime_process_selector is not None else process_selector
    )
    declared_process_selector = (
        selected_hook_process_selector
        if selected_hook_process_selector is not None
        else (None if command_key in {"__default__", "__default_process__"} else workload_id)
    )
    process_plan_key = declared_process_selector or "__default__"
    selected_hook_id = hook_id or hook_name or ""
    hook_argv = (
        "dpone",
        "hooks",
        "execute",
        "runtime/manifest.json",
        "--phase",
        "pre_hook",
        "--hook-id",
        selected_hook_id,
        *(("--selector", selected_hook_process_selector) if selected_hook_process_selector is not None else ()),
    )
    steps = (
        [
            {
                "name": hook_name,
                "phase": "pre_hook",
                "command": (hook_command if hook_command is not None else shlex.join(hook_argv)),
                "runtime_command": {
                    "schema": "dpone.airflow-pre-hook-command.v1",
                    "hook_id": selected_hook_id,
                    "process_selector": selected_hook_process_selector,
                    "argv": list(hook_argv),
                    **(hook_runtime_overrides or {}),
                },
            }
        ]
        if hook_name is not None
        else []
    )
    pack = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": workload_id},
        "steps": steps,
        "process_plans": {
            process_plan_key: {
                "selector": declared_process_selector,
                "dag_node": {
                    "visibility": ("inline" if hook_execution == "inline" else "task"),
                },
                "steps": steps,
            }
        },
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {
                command_key: {
                    "argv": selected_runtime_argv,
                    "env": selected_runtime_environment,
                },
                **(
                    {}
                    if command_key == "__workload__"
                    else {
                        "__workload__": {
                            "argv": [
                                "dpone",
                                "run",
                                "runtime/manifest.json",
                                "--format",
                                "json",
                            ],
                            "env": selected_runtime_environment,
                        }
                    }
                ),
            },
        },
        "runtime_payload": {
            "schema": "dpone.airflow-runtime-payload.v1",
            "archive": {
                "encoding": "base64",
                "format": "tar+gzip",
                "sha256": _sha(archive),
                "bytes": len(archive),
                "data": base64.b64encode(archive).decode("ascii"),
            },
        },
    }
    pack_fingerprint = compute_pack_fingerprint(pack)
    pack["pack_fingerprint"] = pack_fingerprint
    if mutate_pack_after_sign:
        pack["steps"] = [{"name": "mutated-after-signing"}]
    pack_bytes = _json_bytes(pack)
    runtime_payload_bodies: dict[str, bytes] = {}
    runtime_payload_entries: list[dict[str, Any]] = []
    if dbt_runtime is not None:
        runtime_payload_bodies = {
            "runtime/dbt/project.tar.gz": dbt_runtime.project_archive,
            "runtime/dbt/manifest.json": dbt_runtime.manifest,
            f"runtime/dbt/{dbt_runtime.workflow_id}.selection-lock.json": dbt_runtime.selection_lock,
        }
        runtime_payload_entries = [
            _runtime_payload_entry(
                item_id="dbt_project",
                kind="dbt_project_bundle",
                path="runtime/dbt/project.tar.gz",
                payload=dbt_runtime.project_archive,
                media_type="application/vnd.dpone.dbt-project-bundle+gzip",
            ),
            _runtime_payload_entry(
                item_id="dbt_manifest",
                kind="dbt_manifest",
                path="runtime/dbt/manifest.json",
                payload=dbt_runtime.manifest,
                media_type="application/vnd.dbt.manifest+json",
            ),
            _runtime_payload_entry(
                item_id=f"dbt_selection_{dbt_runtime.workflow_id}",
                kind="dbt_selection_lock",
                path=f"runtime/dbt/{dbt_runtime.workflow_id}.selection-lock.json",
                payload=dbt_runtime.selection_lock,
                media_type="application/vnd.dpone.dbt-selection-lock+json",
            ),
        ]
    elif dbt_project_archive is not None:
        runtime_payload_bodies = {
            "runtime/dbt/project-bundle.tar.gz": dbt_project_archive,
        }
        runtime_payload_entries = [
            _runtime_payload_entry(
                item_id="dbt-project",
                kind="dbt_project_bundle",
                path="runtime/dbt/project-bundle.tar.gz",
                payload=dbt_project_archive,
                media_type="application/vnd.dpone.dbt-project+tar+gzip",
            )
        ]
    if dbt_runtime is not None and dbt_wire_contract == DBT_RUNTIME_WIRE_V2:
        ids = dbt_runtime_payload_trio(
            workflow_id=dbt_runtime.workflow_id,
            project_sha256=_sha(dbt_runtime.project_archive),
            manifest_sha256=_sha(dbt_runtime.manifest),
            selection_lock_payload=dbt_runtime.selection_lock,
            wire_contract=dbt_wire_contract,
        )
        runtime_payload_entries = []
        runtime_payload_bodies = {}
        for item_id, payload in zip(
            ids, (dbt_runtime.project_archive, dbt_runtime.manifest, dbt_runtime.selection_lock), strict=True
        ):
            reference = dbt_runtime_payload_reference(item_id, wire_contract=dbt_wire_contract)
            runtime_payload_entries.append(reference.descriptor(payload))
            runtime_payload_bodies[reference.path] = payload
    if runtime_payload_entries:
        pack["runtime_payload_ids"] = [str(item["id"]) for item in runtime_payload_entries]
        pack.pop("pack_fingerprint", None)
        pack_fingerprint = compute_pack_fingerprint(pack)
        pack["pack_fingerprint"] = pack_fingerprint
        pack_bytes = _json_bytes(pack)
    workload_descriptor: dict[str, Any] = {
        "id": workload_id,
        "path": f"packs/{workload_id}.airflow-pack.json",
        "sha256": _sha(pack_bytes),
    }
    if runtime_payload_entries:
        workload_descriptor["runtime_payload_ids"] = [str(item["id"]) for item in runtime_payload_entries]
    release = {
        "schema": ("dpone.release-set.v2" if runtime_payload_entries else "dpone.release-set.v1"),
        "release_id": "",
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [workload_descriptor],
            **({"runtime_payloads": runtime_payload_entries} if runtime_payload_entries else {}),
        },
    }
    if dbt_wire_contract is not None:
        release["producer"] = {"dpone_version": "0.74.28", "wire_contract": dbt_wire_contract}
    if dbt_release_mutator is not None:
        dbt_release_mutator(release, runtime_payload_bodies)
    release["release_id"] = release_id(release)
    release_bytes = _json_bytes(release)
    registry_config_bytes = _json_bytes(
        {
            "schema": "dpone.artifact-registry-runtime-config.v1",
            "registries": {
                "dpone-prod-artifacts": {
                    "registry_uri": "s3://dpone-artifacts/releases",
                    "access": {"mode": "workload_identity"},
                }
            },
        }
    )
    registry_config_ref = _config_ref("registry", _sha(registry_config_bytes))
    trust_policy_bytes = _json_bytes(
        {
            "schema": "dpone.runtime-artifact-trust-policy.v1",
            "trust_tier": trust_tier,
            "attestations": (
                trust_policy_attestations
                if trust_policy_attestations is not None
                else ("required_for_prod" if trust_tier == "production" else "optional")
            ),
        }
    )
    trust_policy_ref = (
        _config_ref("trust", _sha(trust_policy_bytes))
        if trust_tier == "production" or trust_policy_attestations is not None
        else None
    )
    verify = {
        "checksums": "required",
        "attestations": ("required_for_prod" if trust_tier == "production" else "optional"),
    }
    identity = {
        "method": "kubernetes_workload_identity",
        "service_account": "dpone-runtime",
        "namespace": "airflow-example",
    }
    environment_name = "prod" if trust_tier == "production" else "dev"
    binding_set = {
        "schema": "dpone.binding-set.v1",
        "environment": environment_name,
        "bindings": {"source-main": {"connection_ref": "source-main"}},
    }
    connection_registry = {
        "schema": "dpone.connection-registry.v1",
        "environment": environment_name,
        "connections": {
            "source-main": {
                "type": "mssql",
                "connection": {"host": "db.internal"},
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "secret",
                    "path": "teams/example/source-main",
                },
            }
        },
    }
    credential_runtime = {
        "schema": "dpone.credential-runtime.v1",
        "environment": environment_name,
        "vault": {
            "address": "https://vault.internal",
            "auth": {"method": "kubernetes", "role": "dpone-runtime"},
        },
    }
    runtime_connection_payloads = {
        "binding_set": _json_bytes(binding_set),
        "connection_registry": _json_bytes(connection_registry),
        "credential_runtime": _json_bytes(credential_runtime),
    }
    runtime_connection_context_id = canonical_fingerprint(
        {name: _sha(payload) for name, payload in runtime_connection_payloads.items()}
    ).replace(":", "-")
    runtime_connection_root = f"cache://runtime-connection-contexts/{runtime_connection_context_id}"
    runtime_connection_descriptors = {
        name: RuntimeArtifactDescriptor(
            artifact_ref=f"{runtime_connection_root}/{name.replace('_', '-')}.json",
            sha256=_sha(payload),
            bytes=len(payload),
        )
        for name, payload in runtime_connection_payloads.items()
    }
    delivery = {
        "mode": "init_fetch",
        "trust_tier": trust_tier,
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": identity,
        "registry_config_ref": registry_config_ref,
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": verify,
    }
    if trust_policy_ref is not None:
        delivery["trust_policy_ref"] = trust_policy_ref
    deployment: dict[str, Any] = {
        "schema": "dpone.deployment-set.v2",
        "deployment_id": "",
        "environment": environment_name,
        "release_ref": release["release_id"],
        "binding_set_ref": canonical_fingerprint(binding_set),
        "connection_registry_ref": canonical_fingerprint(connection_registry),
        "credential_runtime_ref": canonical_fingerprint(credential_runtime),
        **{name: descriptor.to_dict() for name, descriptor in runtime_connection_descriptors.items()},
        "runtime_image_ref": _IMAGE_REF,
        "runtime_image_digest": _IMAGE_DIGEST,
        "runtime_artifact_delivery": delivery,
        "workloads": [
            {
                "id": workload_id,
                "sha256": _sha(pack_bytes),
                "pack_fingerprint": pack_fingerprint,
            }
        ],
    }
    deployment["deployment_id"] = deployment_id(deployment)
    deployment_bytes = _json_bytes(deployment)
    release_dir = str(release["release_id"]).replace(":", "-")
    deployment_dir = str(deployment["deployment_id"]).replace(":", "-")
    runtime_payloads = tuple(
        RuntimePayloadDescriptor(
            id=str(item["id"]),
            kind=str(item["kind"]),
            artifact_ref=f"cache://releases/{release_dir}/{item['path']}",
            sha256=str(item["sha256"]),
            bytes=int(item["bytes"]),
            media_type=str(item["media_type"]),
        )
        for item in runtime_payload_entries
    )
    plan = RuntimeInitFetchPlan(
        environment=str(deployment["environment"]),
        trust_tier=trust_tier,
        release_id=str(release["release_id"]),
        deployment_id=str(deployment["deployment_id"]),
        runtime_image_ref=_IMAGE_REF,
        runtime_image_digest=_IMAGE_DIGEST,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=registry_config_ref,
        trust_policy_ref=trust_policy_ref,
        identity=identity,
        release=RuntimeArtifactDescriptor(
            artifact_ref=f"cache://releases/{release_dir}/release-set.json",
            sha256=_sha(release_bytes),
            bytes=len(release_bytes),
        ),
        deployment=RuntimeArtifactDescriptor(
            artifact_ref=f"cache://deployments/{deployment['environment']}/{deployment_dir}/deployment.json",
            sha256=_sha(deployment_bytes),
            bytes=len(deployment_bytes),
        ),
        binding_set=runtime_connection_descriptors["binding_set"],
        connection_registry=runtime_connection_descriptors["connection_registry"],
        credential_runtime=runtime_connection_descriptors["credential_runtime"],
        workload_pack=RuntimeWorkloadPackRef(
            id=workload_id,
            artifact_ref=f"cache://releases/{release_dir}/packs/{workload_id}.airflow-pack.json",
            sha256=_sha(pack_bytes),
            bytes=len(pack_bytes),
            pack_fingerprint=pack_fingerprint,
        ),
        execution=RuntimeExecutionSelection(
            kind=execution_kind,
            selector=workload_id,
            scope=execution_scope,
            process_selector=process_selector,
            hook_name=hook_name,
            hook_execution=hook_execution,
        ),
        verify=verify,
        runtime_payloads=runtime_payloads,
    )
    plan_bytes = canonical_runtime_init_fetch_plan_bytes(plan)
    plan_sha256 = _sha(plan_bytes)
    return _Bundle(
        plan=plan,
        plan_bytes=plan_bytes,
        plan_sha256=plan_sha256,
        environment={
            PLAN_B64_ENV: base64.b64encode(plan_bytes).decode("ascii"),
            PLAN_SHA256_ENV: plan_sha256,
        },
        objects={
            _key(plan.release.artifact_ref): release_bytes,
            _key(plan.deployment.artifact_ref): deployment_bytes,
            _key(plan.workload_pack.artifact_ref): pack_bytes,
            **{
                _key(descriptor.artifact_ref): runtime_connection_payloads[name]
                for name, descriptor in runtime_connection_descriptors.items()
            },
            **{
                _key(descriptor.artifact_ref): runtime_payload_bodies[
                    descriptor.artifact_ref.removeprefix(f"cache://releases/{release_dir}/")
                ]
                for descriptor in runtime_payloads
            },
        },
        registry_config_bytes=registry_config_bytes,
        trust_policy_bytes=trust_policy_bytes,
    )


def _deployment_policy_bundle() -> _Bundle:
    bundle = _bundle(trust_tier="production")
    policy = {
        "schema": "dpone.airflow-deployment-trust-policy.v1",
        "trust_tier": "production",
        "attestations": "required_for_prod",
        "backend": "cosign_public_key_v1",
        "trusted_public_keys": {
            "test-key": {
                "file": "cosign.pub",
                "sha256": "sha256:" + "1" * 64,
            }
        },
        "cosign": {
            "minimum_version": "3.0.4",
            "maximum_version_exclusive": "4.0.0",
            "timeout_seconds": 10,
        },
        "allowed_environments": ["prod"],
        "allowed_artifact_registry_refs": ["dpone-prod-artifacts"],
        "allowed_registry_scope_ids": ["sha256:" + "2" * 64],
        "allowed_source_projects": ["platform/example-workloads"],
        "allowed_source_refs": ["refs/heads/master"],
        "revoked_attestation_ids": [],
        "revoked_public_key_ids": [],
    }
    trust_policy_bytes = json.dumps(
        policy,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    trust_policy_ref = _config_ref("trust", _sha(trust_policy_bytes))
    deployment = json.loads(bundle.objects[_key(bundle.plan.deployment.artifact_ref)])
    deployment["runtime_artifact_delivery"]["trust_policy_ref"] = trust_policy_ref
    deployment["deployment_id"] = ""
    deployment["deployment_id"] = deployment_id(deployment)
    deployment_bytes = _json_bytes(deployment)
    deployment_dir = str(deployment["deployment_id"]).replace(":", "-")
    deployment_descriptor = RuntimeArtifactDescriptor(
        artifact_ref=f"cache://deployments/prod/{deployment_dir}/deployment.json",
        sha256=_sha(deployment_bytes),
        bytes=len(deployment_bytes),
    )
    plan = replace(
        bundle.plan,
        deployment_id=str(deployment["deployment_id"]),
        deployment=deployment_descriptor,
        trust_policy_ref=trust_policy_ref,
    )
    plan_bytes = canonical_runtime_init_fetch_plan_bytes(plan)
    plan_sha256 = _sha(plan_bytes)
    return replace(
        bundle,
        plan=plan,
        plan_bytes=plan_bytes,
        plan_sha256=plan_sha256,
        environment={
            PLAN_B64_ENV: base64.b64encode(plan_bytes).decode("ascii"),
            PLAN_SHA256_ENV: plan_sha256,
        },
        objects={
            **{key: value for key, value in bundle.objects.items() if key != _key(bundle.plan.deployment.artifact_ref)},
            _key(deployment_descriptor.artifact_ref): deployment_bytes,
        },
        trust_policy_bytes=trust_policy_bytes,
    )


def _observed_subject(bundle: _Bundle) -> AirflowArtifactObservedSubject:
    return AirflowArtifactObservedSubject(
        release_id=bundle.plan.release_id,
        deployment_id=bundle.plan.deployment_id,
        environment=bundle.plan.environment,
        artifact_registry_ref=bundle.plan.artifact_registry_ref,
        registry_scope_id="sha256:" + "2" * 64,
        release_set_sha256=bundle.plan.release.sha256,
        deployment_sha256=bundle.plan.deployment.sha256,
        runtime_image_digest=bundle.plan.runtime_image_digest,
        airflow_index_sha256=None,
    )


def _dbt_runtime_fixture(
    project: Path,
    *,
    mismatch: str | None = None,
    execution_pack_v2: bool = False,
) -> _DbtRuntimeFixture:
    workflow_id = "orders_publish"
    project_bundle = build_dbt_project_bundle(project)
    manifest = _json_bytes(
        {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "dbt_version": "1.12.3",
            },
            "nodes": {},
        }
    )
    invocation = DbtInvocationContext.canonical()
    graph_contract_sha256 = _sha(b"dbt-graph-contract")
    release_lock = DbtSelectionLock.build(
        manifest_sha256=_sha(manifest),
        toolchain_sha256=(_sha(b"other-toolchain-must_not_leak") if mismatch == "toolchain" else _DBT_TOOLCHAIN_DIGEST),
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=graph_contract_sha256,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("orders",),
        selected_graph_unique_ids=("model.analytics.orders",),
        expected_run_result_unique_ids=("model.analytics.orders",),
        publish_model_unique_ids=("model.analytics.orders",),
    )
    execution_lock = DbtSelectionLock.build(
        manifest_sha256=(
            _sha(b"other-manifest-must_not_leak") if mismatch == "manifest" else release_lock.manifest_sha256
        ),
        toolchain_sha256=_DBT_TOOLCHAIN_DIGEST,
        invocation_context_sha256=release_lock.invocation_context_sha256,
        graph_contract_sha256=release_lock.graph_contract_sha256,
        graph_policy_id=release_lock.graph_policy_id,
        graph_policy_sha256=release_lock.graph_policy_sha256,
        selectors=(("other_must_not_leak",) if mismatch == "selection" else release_lock.selectors),
        selected_graph_unique_ids=release_lock.selected_graph_unique_ids,
        expected_run_result_unique_ids=release_lock.expected_run_result_unique_ids,
        publish_model_unique_ids=release_lock.publish_model_unique_ids,
    )
    build_pack = DbtExecutionPack.build_v2 if execution_pack_v2 else DbtExecutionPack.build
    execution_pack = build_pack(
        **({"invocation_target": DbtInvocationTarget("analytics", "base")} if execution_pack_v2 else {}),
        workflow_id=("other_must_not_leak" if mismatch == "workload" else workflow_id),
        project_bundle_sha256=(
            _sha(b"other-project-must_not_leak")
            if mismatch == "project_bundle"
            else project_bundle.bundle.archive_sha256
        ),
        project_subdir="dbt-project",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="analytics",
            target_name="runtime",
            connection_ref="source-main",
            adapter_type="sqlserver",
            database="analytics",
            schema="mart",
            threads=2,
        ),
        selection_lock=execution_lock,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(600),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=600,
    )
    return _DbtRuntimeFixture(
        workflow_id=workflow_id,
        project_archive=project_bundle.archive,
        manifest=manifest,
        selection_lock=_json_bytes(release_lock.to_dict()),
        execution_pack=_json_bytes(execution_pack.to_dict()),
    )


def _runtime_payload_entry(
    *,
    item_id: str,
    kind: str,
    path: str,
    payload: bytes,
    media_type: str,
) -> dict[str, Any]:
    return {
        "id": item_id,
        "kind": kind,
        "path": path,
        "sha256": _sha(payload),
        "bytes": len(payload),
        "media_type": media_type,
    }


def _receipt_payload(bundle: _Bundle, receipt_name: str) -> bytes:
    if receipt_name == "release":
        artifact_ref = bundle.plan.release.artifact_ref
    elif receipt_name == "deployment":
        artifact_ref = bundle.plan.deployment.artifact_ref
    else:
        raise AssertionError(f"unsupported receipt name: {receipt_name}")
    return bundle.objects[_key(artifact_ref)]


def _with_receipt_payload(
    bundle: _Bundle,
    receipt_name: str,
    payload: bytes,
) -> _Bundle:
    if receipt_name == "release":
        descriptor = replace(
            bundle.plan.release,
            sha256=_sha(payload),
            bytes=len(payload),
        )
        plan = replace(bundle.plan, release=descriptor)
    elif receipt_name == "deployment":
        descriptor = replace(
            bundle.plan.deployment,
            sha256=_sha(payload),
            bytes=len(payload),
        )
        plan = replace(bundle.plan, deployment=descriptor)
    else:
        raise AssertionError(f"unsupported receipt name: {receipt_name}")
    plan_bytes = canonical_runtime_init_fetch_plan_bytes(plan)
    plan_sha256 = _sha(plan_bytes)
    objects = dict(bundle.objects)
    objects[_key(descriptor.artifact_ref)] = payload
    return replace(
        bundle,
        plan=plan,
        plan_bytes=plan_bytes,
        plan_sha256=plan_sha256,
        environment={
            PLAN_B64_ENV: base64.b64encode(plan_bytes).decode("ascii"),
            PLAN_SHA256_ENV: plan_sha256,
        },
        objects=objects,
    )


def _publish_ready_from_init_view(
    root: Path,
    bundle: _Bundle,
) -> tuple[dict[str, Any], RecordingReleaseAttestationVerifier]:
    init_view = root / "init-view"
    init_view.mkdir(parents=True)
    registry_path = init_view / "registry.json"
    registry_path.write_bytes(bundle.registry_config_bytes)
    trust_path = init_view / "policy.json"
    if bundle.plan.trust_policy_ref is not None:
        trust_path.write_bytes(bundle.trust_policy_bytes)
    verifier = RecordingReleaseAttestationVerifier([])
    service = AirflowRuntimeInitFetchService(
        registry_factory=RecordingRegistryFactory(RecordingRegistry(bundle.objects), []),
        attestation_verifier=verifier,
        registry_config_path=registry_path,
        trust_policy_path=trust_path,
        artifact_root=root / "artifacts",
        worktree_root=root / "worktree",
    )
    return dict(service.init_fetch(bundle.environment)), verifier


def _archive(*, execution_pack: bytes | None = None) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as bundle:
            body = b"kind: dpone.batch.v1\n"
            info = tarfile.TarInfo("runtime/manifest.json")
            info.size = len(body)
            info.mtime = 0
            info.mode = 0o600
            bundle.addfile(info, io.BytesIO(body))
            execution_pack = execution_pack or b'{"schema":"dpone.dbt-execution-pack.v1"}\n'
            pack_info = tarfile.TarInfo("runtime/dbt-execution-pack.json")
            pack_info.size = len(execution_pack)
            pack_info.mtime = 0
            pack_info.mode = 0o600
            bundle.addfile(pack_info, io.BytesIO(execution_pack))
    return raw.getvalue()


def _config_ref(label: str, digest: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-{label}-config",
        "key": f"{label}.json",
        "sha256": digest,
    }


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _key(artifact_ref: str) -> PurePosixPath:
    return PurePosixPath(artifact_ref.removeprefix("cache://"))


def test_runtime_pack_exec_tees_child_stderr_live(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Long-running workloads must stream stderr before the process exits."""

    import sys

    from dpone.readiness import airflow_runtime_pack_exec as pack_exec
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

    workdir = tmp_path / "worktree"
    workdir.mkdir()
    run_dir = tmp_path / "run"
    xcom_path = tmp_path / "airflow" / "xcom" / "return.json"
    script = workdir / "emit.py"
    evidence_json = json.dumps({"status": "SUCCESS"})
    script.write_text(
        f"import sys\nprint('LIVE_STDERR_LINE_FROM_CHILD', file=sys.stderr, flush=True)\nprint({evidence_json!r})\n",
        encoding="utf-8",
    )
    command = VerifiedPackCommand(
        argv=(sys.executable, str(script)),
        env={},
        working_directory=workdir,
    )

    code = pack_exec.execute_verified_pack_command(
        command,
        run_output_dir=run_dir,
        xcom_return_path=xcom_path,
    )

    assert code == 0
    err = capsys.readouterr().err
    assert "===DPONE_RUNTIME_PACK_EXEC_START===" in err
    assert "LIVE_STDERR_LINE_FROM_CHILD" in err
    assert "===DPONE_RUNTIME_PACK_EXEC_OUTCOME===" in err
    assert "LIVE_STDERR_LINE_FROM_CHILD" in (run_dir / "runtime-stderr.log").read_text(encoding="utf-8")
