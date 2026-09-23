"""Remote development delivery stays behind injected authority."""

from __future__ import annotations

import json
import shutil
import traceback
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pytest

from dpone.app.release_composition import build_release_composition_service
from dpone.contracts.development_delivery_authority import DEVELOPMENT_COMPOSITION_PROFILE
from dpone.contracts.development_target_admission import DevelopmentTargetAdmission
from dpone.contracts.release_composition import ReleaseCompositionRequest
from dpone.readiness.airflow_artifact_delivery import (
    ArtifactRegistryOptions,
    materialize_command_result,
    publish_command_result,
)
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.readiness.airflow_deployment_projection_errors import AirflowDeploymentProjectionError
from dpone.readiness.airflow_desired_state_authority import AirflowDesiredStateAuthority
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
)
from dpone.runtime.airflow_artifact_delivery_models import MaterializeRequest, PublishRequest
from dpone.runtime.airflow_artifact_publication import prepare_publication
from tests.dbt_compact_wire_v2_helpers import IMAGE, SIDECAR, prepare_projects, workspace_service
from tests.test_airflow_credential_projection_delivery import write_native_environment
from tests.test_airflow_remote_artifact_delivery import _registry
from tests.test_dbt_airflow_release_e2e import _config_map_ref, _write_environment
from tests.test_dbt_compact_wire_v2 import _development_authority
from tests.test_release_composition_ordinary import ordinary_root


@pytest.mark.parametrize("composed", [False, True])
def test_remote_development_delivery_requires_injected_authority(tmp_path: Path, composed: bool) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    registry = _registry(tmp_path)
    publish_request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied_publish:
        AirflowArtifactPublisher(registry=registry).publish(publish_request)
    assert denied_publish.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"

    with pytest.raises(AirflowArtifactDeliveryError) as denied_stale_state:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            clock=_clock,
        ).publish(publish_request)
    assert denied_stale_state.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (tmp_path / "registry" / "releases").exists()

    published = AirflowArtifactPublisher(
        registry=registry,
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(publish_request)
    assert published.status == "published"

    materialize_request = MaterializeRequest(
        cache_root=tmp_path / "remote-cache",
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
    )
    with pytest.raises(AirflowArtifactDeliveryError) as denied_materialize:
        AirflowArtifactMaterializer(registry=registry).materialize(materialize_request)
    assert denied_materialize.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (materialize_request.cache_root / "releases").exists()

    installed = AirflowArtifactMaterializer(
        registry=registry,
        development_admission=_admission(authority, "materialize", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).materialize(materialize_request)
    assert installed.projection_verified


def test_development_release_rejects_production_target_projection(tmp_path: Path) -> None:
    authority = _development_authority()
    workspace = tmp_path / "workspace"
    prepare_projects(workspace)
    compiled = tmp_path / "compiled"
    assert (
        workspace_service(tmp_path / "profiles", development_authority=authority)
        .compile(workspace, output_dir=compiled)
        .passed
    )
    native = materialize_compact_pack_release(
        pack_root=compiled,
        cache_root=tmp_path / ".dpone-cache",
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert native.passed
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError, match="DEV-only releases require"):
        AirflowDeploymentProjectionService(root=tmp_path).materialize(
            release_id=native.release_id,
            environment="prod",
            trust_tier="production",
            runtime_image_ref=IMAGE,
            runtime_image_digest=IMAGE.split("@")[-1],
            artifact_registry_ref="synthetic-artifacts",
            registry_config_ref=_config_map_ref("registry", "1"),
            trust_policy_ref=_config_map_ref("policy", "2"),
            airflow_bundle_ref="git:" + "d" * 40,
        )


@pytest.mark.parametrize("mismatch", ["target", "operation"])
def test_unknown_target_or_operation_rejects_before_registry_write(tmp_path: Path, mismatch: str) -> None:
    authority = _development_authority()
    target_environment = "qa" if mismatch == "target" else "development"
    cache, release_id, deployment_id = _development_projection(
        tmp_path,
        composed=False,
        target_environment=target_environment,
    )
    registry = _registry(tmp_path)
    request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment=target_environment,
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )
    admission = _admission(
        authority,
        "materialize" if mismatch == "operation" else "publish",
        release_id,
        deployment_id,
        environment="development",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=admission,
            development_admission_verifier=_admission_verifier(authority),
            clock=_clock,
        ).publish(request)

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (tmp_path / "registry" / "releases").exists()


@pytest.mark.parametrize("changed_state", ["policy", "revocation"])
def test_post_issuance_target_state_change_rejects_before_registry_write(
    tmp_path: Path,
    changed_state: str,
) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=False)
    registry = _registry(tmp_path)
    verifier = _admission_verifier(
        authority,
        target_policy_sha256="sha256:" + ("7" if changed_state == "policy" else "6") * 64,
        current_revocation_epoch=authority.revocation_epoch + int(changed_state == "revocation"),
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            development_admission_verifier=verifier,
            clock=_clock,
        ).publish(
            PublishRequest(
                cache_root=cache,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
                publication_mode="exact",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (tmp_path / "registry" / "releases").exists()


def test_publish_rechecks_revocation_after_preparation_before_first_write(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    registry = _registry(tmp_path)
    verifier = _admission_verifier(authority, revoke_after_successes=2)

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            development_admission_verifier=verifier,
            clock=_clock,
        ).publish(
            PublishRequest(
                cache_root=cache,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
                publication_mode="exact",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert verifier.calls == 2
    assert not (tmp_path / "registry" / "releases").exists()


@pytest.mark.parametrize("publication_mode", ["exact", "compatible"])
def test_publish_rechecks_grant_time_after_preparation_before_first_write(
    tmp_path: Path,
    publication_mode: Literal["exact", "compatible"],
) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    registry = _registry(tmp_path)
    clock = _ExpiringClock(expire_after_calls=3)

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            development_admission_verifier=_admission_verifier(authority),
            clock=clock,
        ).publish(
            PublishRequest(
                cache_root=cache,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
                publication_mode=publication_mode,
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert clock.calls >= 4
    assert not (tmp_path / "registry" / "releases").exists()


@pytest.mark.parametrize("publication_mode", ["exact", "compatible"])
def test_publish_rechecks_grant_time_after_final_target_verification(
    tmp_path: Path,
    publication_mode: Literal["exact", "compatible"],
) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    registry = _registry(tmp_path)
    clock = _MutableClock()
    verifier = _ClockAdvancingVerifier(
        _admission_verifier(authority),
        clock=clock,
        advance_on_call=6,
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            development_admission_verifier=verifier,
            clock=clock,
        ).publish(
            PublishRequest(
                cache_root=cache,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
                publication_mode=publication_mode,
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert verifier.calls == 6
    assert not (tmp_path / "registry" / "releases").exists()
    assert not (tmp_path / "registry" / "deployments").exists()


def test_publish_closes_external_target_verifier_failure(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=False)
    registry = _registry(tmp_path)

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactPublisher(
            registry=registry,
            development_admission=_admission(authority, "publish", release_id, deployment_id),
            development_admission_verifier=_UnavailableVerifier(),
            clock=_clock,
        ).publish(
            PublishRequest(
                cache_root=cache,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
                publication_mode="exact",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert denied.value.__cause__ is None
    assert denied.value.__context__ is None
    assert "protected-policy-location-must-not-escape" not in str(denied.value)
    assert "protected-policy-location-must-not-escape" not in "".join(
        traceback.format_exception(denied.type, denied.value, denied.tb)
    )
    assert not (tmp_path / "registry" / "releases").exists()


def test_materialize_rechecks_revocation_immediately_before_install(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(
        registry=registry,
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(
        PublishRequest(
            cache_root=cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="development",
            artifact_registry_ref="synthetic-artifacts",
            publication_mode="exact",
        )
    )
    destination = tmp_path / "revoked-cache"
    verifier = _admission_verifier(authority, revoke_after_successes=2)

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactMaterializer(
            registry=registry,
            development_admission=_admission(authority, "materialize", release_id, deployment_id),
            development_admission_verifier=verifier,
            clock=_clock,
        ).materialize(
            MaterializeRequest(
                cache_root=destination,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert verifier.calls == 2
    assert not (destination / "releases").exists()
    assert not (destination / "deployments").exists()


def test_materialize_rechecks_grant_time_after_final_target_verification(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(
        registry=registry,
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(
        PublishRequest(
            cache_root=cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="development",
            artifact_registry_ref="synthetic-artifacts",
            publication_mode="exact",
        )
    )
    destination = tmp_path / "expired-cache"
    clock = _MutableClock()
    verifier = _ClockAdvancingVerifier(
        _admission_verifier(authority),
        clock=clock,
        advance_on_call=3,
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactMaterializer(
            registry=registry,
            development_admission=_admission(authority, "materialize", release_id, deployment_id),
            development_admission_verifier=verifier,
            clock=clock,
        ).materialize(
            MaterializeRequest(
                cache_root=destination,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert verifier.calls == 3
    assert not (destination / "releases").exists()
    assert not (destination / "deployments").exists()


def test_materialize_closes_external_target_verifier_failure(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=False)
    registry = _registry(tmp_path)
    AirflowArtifactPublisher(
        registry=registry,
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(
        PublishRequest(
            cache_root=cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="development",
            artifact_registry_ref="synthetic-artifacts",
            publication_mode="exact",
        )
    )
    destination = tmp_path / "unavailable-cache"

    with pytest.raises(AirflowArtifactDeliveryError) as denied:
        AirflowArtifactMaterializer(
            registry=registry,
            development_admission=_admission(authority, "materialize", release_id, deployment_id),
            development_admission_verifier=_UnavailableVerifier(),
            clock=_clock,
        ).materialize(
            MaterializeRequest(
                cache_root=destination,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
            )
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert denied.value.__cause__ is None
    assert denied.value.__context__ is None
    assert "protected-policy-location-must-not-escape" not in str(denied.value)
    assert "protected-policy-location-must-not-escape" not in "".join(
        traceback.format_exception(denied.type, denied.value, denied.tb)
    )
    assert not (destination / "releases").exists()
    assert not (destination / "deployments").exists()


def test_legacy_delivery_authority_keyword_is_accepted_but_cannot_self_authorize(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=False)
    registry = _registry(tmp_path)
    publish_request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )

    with pytest.raises(AirflowArtifactDeliveryError) as denied_prepare:
        prepare_publication(publish_request, development_authority=authority)
    assert denied_prepare.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"

    with pytest.raises(AirflowArtifactDeliveryError) as denied_publish:
        AirflowArtifactPublisher(
            registry=registry,
            development_authority=authority,
        ).publish(publish_request)
    assert denied_publish.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (tmp_path / "registry" / "releases").exists()

    AirflowArtifactPublisher(
        registry=registry,
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(publish_request)
    destination = tmp_path / "legacy-authority-cache"

    with pytest.raises(AirflowArtifactDeliveryError) as denied_materialize:
        AirflowArtifactMaterializer(
            registry=registry,
            development_authority=authority,
        ).materialize(
            MaterializeRequest(
                cache_root=destination,
                release_id=release_id,
                deployment_id=deployment_id,
                environment="development",
                artifact_registry_ref="synthetic-artifacts",
            )
        )

    assert denied_materialize.value.code == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"
    assert not (destination / "releases").exists()
    assert not (destination / "deployments").exists()


def test_public_cli_cannot_self_authorize_development_delivery(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=True)
    options = ArtifactRegistryOptions(
        registry_uri="s3://synthetic-artifacts/airflow",
        local_registry_root=str(tmp_path / "registry"),
    )
    request = PublishRequest(
        cache_root=cache,
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
        publication_mode="exact",
    )
    AirflowArtifactPublisher(
        registry=options.build(),
        development_admission=_admission(authority, "publish", release_id, deployment_id),
        development_admission_verifier=_admission_verifier(authority),
        clock=_clock,
    ).publish(request)
    published = publish_command_result(
        cache_root=str(cache),
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
        registry_options=options,
        publication_mode="exact",
    )
    assert not published.passed
    assert published.errors[0]["code"] == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"

    materialized = materialize_command_result(
        cache_root=str(tmp_path / "cli-cache"),
        release_id=release_id,
        deployment_id=deployment_id,
        environment="development",
        artifact_registry_ref="synthetic-artifacts",
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
        registry_options=options,
    )
    assert not materialized.passed
    assert materialized.errors[0]["code"] == "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED"


def _development_projection(
    tmp_path: Path,
    *,
    composed: bool,
    target_environment: str = "development",
) -> tuple[Path, str, str]:
    authority = _development_authority()
    workspace = tmp_path / "workspace"
    prepare_projects(workspace)
    compiled = tmp_path / "compiled"
    assert (
        workspace_service(
            tmp_path / "profiles",
            development_authority=authority,
        )
        .compile(workspace, output_dir=compiled)
        .passed
    )
    native = materialize_compact_pack_release(
        pack_root=compiled,
        cache_root=tmp_path / "native-cache",
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert native.passed
    source = Path(native.release_dir)
    if composed:
        ordinary = ordinary_root(tmp_path)
        service = build_release_composition_service(development_authority=authority)
        inventory = service.inventory(ordinary, xcom_sidecar_image=SIDECAR)
        output = tmp_path / "composed"
        report = service.compose(
            ReleaseCompositionRequest(
                native_root=source,
                expected_release_id=native.release_id,
                standalone_root=ordinary,
                expected_inventory_sha256=inventory["inventory_sha256"],
                output_dir=output,
                xcom_sidecar_image=SIDECAR,
                profile=DEVELOPMENT_COMPOSITION_PROFILE,
            )
        )
        assert report.passed
        source = output
    cache = tmp_path / ".dpone-cache"
    installed = materialize_compact_pack_release(
        pack_root=source,
        cache_root=cache,
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert installed.passed, installed.blockers
    release = json.loads(Path(installed.release_dir, "release-set.json").read_bytes())
    projection_authority = _write_target_environment(tmp_path, target_environment, ordinary=composed)
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release["release_id"],
        environment=target_environment,
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        runtime_authority_ref={
            "kind": "kubernetes_secret",
            "name": "dpone-runtime-authority",
            "key": "authority.json",
        },
        airflow_bundle_ref="git:" + "d" * 40,
        desired_state_authority=projection_authority,
    )
    assert projection.deployment["schema"] == "dpone.deployment-set.v6"
    return cache, release["release_id"], projection.deployment["deployment_id"]


def _write_target_environment(root: Path, environment: str, *, ordinary: bool) -> AirflowDesiredStateAuthority:
    authority = write_native_environment(root, ordinary=ordinary)
    if environment == "prod":
        return authority
    source = root / "environments" / "prod"
    target = root / "environments" / environment
    shutil.copytree(source, target)
    for path in target.iterdir():
        path.write_text(path.read_text(encoding="utf-8").replace("prod", environment), encoding="utf-8")
    registry = root / "platform" / "connection-registries"
    body = (registry / "prod.yaml").read_text(encoding="utf-8").replace("prod", environment)
    (registry / f"{environment}.yaml").write_text(body, encoding="utf-8")
    return replace(authority, environment=environment)


def _admission(
    authority,
    operation,
    release_id: str,
    deployment_id: str,
    *,
    environment: str = "development",
) -> DevelopmentTargetAdmission:
    return DevelopmentTargetAdmission(
        authority=authority,
        operation=operation,
        release_id=release_id,
        deployment_id=deployment_id,
        target_environment=environment,
        target_trust_tier="non_production",
        target_policy_sha256="sha256:" + "6" * 64,
        checked_at=datetime(2026, 9, 17, 12, 0, tzinfo=UTC),
        current_revocation_epoch=authority.revocation_epoch,
    )


def _clock() -> datetime:
    return datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


class _ExpiringClock:
    def __init__(self, *, expire_after_calls: int) -> None:
        self._expire_after_calls = expire_after_calls
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        if self.calls > self._expire_after_calls:
            return _clock() + timedelta(hours=2)
        return _clock()


class _MutableClock:
    def __init__(self) -> None:
        self._now = _clock()

    def __call__(self) -> datetime:
        return self._now

    def expire(self) -> None:
        self._now += timedelta(hours=2)


class _ClockAdvancingVerifier:
    def __init__(self, delegate, *, clock: _MutableClock, advance_on_call: int) -> None:
        self._delegate = delegate
        self._clock = clock
        self._advance_on_call = advance_on_call
        self.calls = 0

    def require_current(self, admission: DevelopmentTargetAdmission, *, now: datetime) -> None:
        self.calls += 1
        self._delegate.require_current(admission, now=now)
        if self.calls == self._advance_on_call:
            self._clock.expire()


class _UnavailableVerifier:
    def require_current(self, admission: DevelopmentTargetAdmission, *, now: datetime) -> None:
        raise OSError("protected-policy-location-must-not-escape")


class _CurrentTargetVerifier:
    def __init__(
        self,
        *,
        target_policy_sha256: str,
        current_revocation_epoch: int,
        revoke_after_successes: int | None = None,
    ) -> None:
        self._target_policy_sha256 = target_policy_sha256
        self._current_revocation_epoch = current_revocation_epoch
        self._revoke_after_successes = revoke_after_successes
        self.calls = 0

    def require_current(self, admission: DevelopmentTargetAdmission, *, now: datetime) -> None:
        admission.require_current_target(
            target_policy_sha256=self._target_policy_sha256,
            current_revocation_epoch=self._current_revocation_epoch,
            now=now,
        )
        self.calls += 1
        if self.calls == self._revoke_after_successes:
            self._current_revocation_epoch += 1


def _admission_verifier(
    authority,
    *,
    target_policy_sha256: str = "sha256:" + "6" * 64,
    current_revocation_epoch: int | None = None,
    revoke_after_successes: int | None = None,
) -> _CurrentTargetVerifier:
    return _CurrentTargetVerifier(
        target_policy_sha256=target_policy_sha256,
        current_revocation_epoch=(
            authority.revocation_epoch if current_revocation_epoch is None else current_revocation_epoch
        ),
        revoke_after_successes=revoke_after_successes,
    )
