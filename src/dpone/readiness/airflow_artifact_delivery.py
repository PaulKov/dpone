"""CLI-facing composition for pinned Airflow artifact delivery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
    object_storage_registry_scope_id,
    parse_artifact_registry_root,
)
from dpone.readiness.airflow_deployment_attestation_verifier import (
    optional_airflow_deployment_attestation_verifier,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.runtime.airflow_artifact_delivery import (
    DEFAULT_MAX_OBJECT_BYTES,
    DEFAULT_MAX_TOTAL_BYTES,
    AirflowArtifactDeliveryError,
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
    MaterializeRequest,
    PublishRequest,
    canonical_digest_or_none,
    is_safe_artifact_delivery_name,
    prepare_publication,
    validate_materialization_target,
)
from dpone.runtime.object_storage_access import ObjectStorageConnectionRef, ObjectStorageConnectionResolver
from dpone.storage import (
    AzureBlobObjectStorageClient,
    GCSObjectStorageClient,
    LocalObjectStorageClient,
    ObjectStorageProvider,
    ObjectStorageUri,
    S3ObjectStorageClient,
)

_DEPENDENCY_CODES = frozenset(
    {
        "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
        "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
    }
)
_SECURITY_CODES = frozenset(
    {
        "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
        "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH",
        "DPONE_CACHE_PATH_ESCAPE",
        "DPONE_EXACT_PUBLICATION_PROJECTION_REQUIRED",
    }
)
_INPUT_CODES = frozenset(
    {
        "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
        "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
        "DPONE_ARTIFACT_REGISTRY_URI_UNPINNED",
        "DPONE_RELEASE_ID_INVALID",
        "DPONE_DEPLOYMENT_ID_INVALID",
    }
)


@dataclass(frozen=True, slots=True)
class ArtifactRegistryOptions:
    registry_uri: str
    local_registry_root: str | None = None
    identity_mode: str | None = None
    connection_type: str | None = None
    connection_id: str | None = None

    @property
    def scope_id(self) -> str:
        """Return the historical root-only v1 identity."""

        return object_storage_registry_scope_id(parse_artifact_registry_root(self.registry_uri))

    @property
    def legacy_scope_id(self) -> str:
        """Alias naming the historical identity explicitly for new callers."""

        return self.scope_id

    def _validate(self) -> None:
        modes = sum(value is not None for value in (self.local_registry_root, self.identity_mode, self.connection_id))
        if modes != 1:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "exactly one artifact registry access mode is required",
            )
        if self.local_registry_root is not None and not self.local_registry_root.strip():
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "local registry root must not be empty",
            )
        if self.connection_id is None and self.connection_type is not None:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "credential resolver options require --connection-id",
            )
        if self.connection_type not in {None, "airflow", "env", "vault"}:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "artifact registry credentials require a logical resolver",
            )
        if self.connection_id is not None and not is_safe_artifact_delivery_name(self.connection_id):
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "artifact registry connection_id must be a bounded logical name",
            )

    def build(self) -> ObjectStorageArtifactRegistry:
        self._validate()
        root = parse_artifact_registry_root(self.registry_uri)
        if self.local_registry_root is not None:
            client = LocalObjectStorageClient(self.local_registry_root)
        elif self.identity_mode == "workload_identity":
            client = _workload_identity_client(root)
        elif self.connection_id is not None:
            try:
                ref = ObjectStorageConnectionRef(
                    connection_type=self.connection_type or "vault",
                    connection_id=self.connection_id,
                )
            except ValueError as exc:
                raise AirflowArtifactDeliveryError(
                    "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                    "logical object-storage credential reference is invalid",
                ) from exc
            try:
                client = ObjectStorageConnectionResolver().build_client(ref=ref, uri=root)
            except ImportError as exc:
                raise AirflowArtifactDeliveryError(
                    "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
                    "the selected object-storage SDK is not installed",
                ) from exc
            except Exception as exc:  # noqa: BLE001 - provider/Vault details are redacted.
                raise AirflowArtifactDeliveryError(
                    "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                    "artifact registry credential dependency is unavailable",
                ) from exc
        else:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "exactly one artifact registry access mode is required",
            )
        return ObjectStorageArtifactRegistry(client=client, root=root)


def publish_command_result(
    *,
    cache_root: str,
    release_id: str,
    deployment_id: str,
    environment: str,
    artifact_registry_ref: str,
    max_object_bytes: int,
    max_total_bytes: int,
    registry_options: ArtifactRegistryOptions,
    attestation_bundle_path: str | None = None,
    expected_registry_scope_id: str | None = None,
    publication_mode: str = "compatible",
) -> SelfServiceResult:
    try:
        request = PublishRequest(
            cache_root=Path(cache_root),
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            artifact_registry_ref=artifact_registry_ref,
            max_object_bytes=max_object_bytes,
            max_total_bytes=max_total_bytes,
            attestation_bundle_path=(Path(attestation_bundle_path) if attestation_bundle_path is not None else None),
            registry_scope_id=expected_registry_scope_id,
            publication_mode=publication_mode,
        )
        prepare_publication(request)
        report = AirflowArtifactPublisher(registry=registry_options.build()).publish(request)
        return SelfServiceResult(passed=True, details=report.to_dict(), exit_code=0)
    except Exception as exc:  # noqa: BLE001 - optional SDK and credential details are redacted.
        return _error_result(
            exc,
            stage="airflow_artifact_publish",
            details={
                **_failure_identity(
                    schema=(
                        "dpone.airflow-artifact-publish.v2"
                        if publication_mode == "exact"
                        else "dpone.airflow-artifact-publish.v1"
                    ),
                    release_id=release_id,
                    deployment_id=deployment_id,
                    environment=environment,
                    artifact_registry_ref=artifact_registry_ref,
                ),
                "created_objects": 0,
                "existing_equal_objects": 0,
                "published_release": False,
                "published_deployment": False,
                **(
                    {
                        "verified_objects": 0,
                        "publication_commitment": None,
                    }
                    if publication_mode == "exact"
                    else {}
                ),
            },
        )


def materialize_command_result(
    *,
    cache_root: str,
    release_id: str,
    deployment_id: str,
    environment: str,
    artifact_registry_ref: str,
    max_object_bytes: int,
    max_total_bytes: int,
    registry_options: ArtifactRegistryOptions,
    trust_policy_path: str = "/etc/dpone/artifact-trust/policy.json",
    trust_key_root: str = "/etc/dpone/artifact-trust",
) -> SelfServiceResult:
    try:
        request = MaterializeRequest(
            cache_root=Path(cache_root),
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            artifact_registry_ref=artifact_registry_ref,
            max_object_bytes=max_object_bytes,
            max_total_bytes=max_total_bytes,
        )
        validate_materialization_target(request)
        registry = registry_options.build()
        policy_path = Path(trust_policy_path)
        verifier = optional_airflow_deployment_attestation_verifier(
            registry=registry,
            policy_path=policy_path,
            key_root=Path(trust_key_root),
        )
        report = AirflowArtifactMaterializer(
            registry=registry,
            deployment_attestation_verifier=verifier,
        ).materialize(request)
        return SelfServiceResult(passed=True, details=report.to_dict(), exit_code=0)
    except Exception as exc:  # noqa: BLE001 - optional SDK and credential details are redacted.
        return _error_result(
            exc,
            stage="airflow_cache_materialize",
            details={
                **_failure_identity(
                    schema="dpone.airflow-cache-materialize.v1",
                    release_id=release_id,
                    deployment_id=deployment_id,
                    environment=environment,
                    artifact_registry_ref=artifact_registry_ref,
                ),
                "downloaded_objects": 0,
                "downloaded_bytes": 0,
                "local_release_state": "not_installed",
                "local_deployment_state": "not_installed",
                "projection_verified": False,
                "activated": False,
            },
        )


def _workload_identity_client(root: ObjectStorageUri):
    try:
        if root.provider == ObjectStorageProvider.S3:
            return S3ObjectStorageClient()
        if root.provider == ObjectStorageProvider.GCS:
            return GCSObjectStorageClient()
        if root.provider == ObjectStorageProvider.AZURE_BLOB:
            if root.account is None:
                raise AirflowArtifactDeliveryError(
                    "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                    "Azure workload identity requires azure://<account>/<container>/<root>",
                )
            from azure.identity import WorkloadIdentityCredential

            return AzureBlobObjectStorageClient(
                account_url=f"https://{root.account}.blob.core.windows.net",
                credential=WorkloadIdentityCredential(),
            )
    except AirflowArtifactDeliveryError:
        raise
    except ImportError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
            "the selected workload-identity object-storage SDK is not installed",
        ) from exc
    except Exception as exc:  # noqa: BLE001 - provider discovery details are redacted.
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
            "workload-identity object-storage client is unavailable",
        ) from exc
    raise AirflowArtifactDeliveryError(
        "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
        "the selected workload-identity object-storage provider is unsupported",
    )


def _error_result(exc: BaseException, *, stage: str, details: dict[str, object]) -> SelfServiceResult:
    if isinstance(exc, AirflowArtifactDeliveryError):
        code, message = exc.code, str(exc)
    elif isinstance(exc, ValueError):
        code, message = "DPONE_ARTIFACT_REGISTRY_URI_UNPINNED", "artifact registry configuration is invalid"
    elif isinstance(exc, ImportError):
        code, message = (
            "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
            "the selected object-storage SDK is not installed",
        )
    else:
        code, message = "DPONE_INTERNAL_AIRFLOW_ARTIFACT_DELIVERY_FAILED", "artifact delivery failed unexpectedly"
    _merge_failure_progress(exc, details)
    error = dpone_error(
        code,
        message,
        stage=stage,
        fixes=[manual_fix("review_artifact_registry_delivery", command=f"dpone airflow {_command_name(stage)} --help")],
    )
    return SelfServiceResult(passed=False, errors=(error,), details=details, exit_code=_exit_code(code))


def _merge_failure_progress(exc: BaseException, details: dict[str, object]) -> None:
    if not isinstance(exc, AirflowArtifactDeliveryError):
        return
    for key in (
        "created_objects",
        "existing_equal_objects",
        "verified_objects",
        "downloaded_objects",
        "downloaded_bytes",
    ):
        value = exc.details.get(key)
        if key in details and isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            details[key] = value
    for key in ("published_release", "published_deployment"):
        value = exc.details.get(key)
        if key in details and isinstance(value, bool):
            details[key] = value
    for key in ("local_release_state", "local_deployment_state"):
        value = exc.details.get(key)
        if key in details and value in {"created", "no_op", "not_installed"}:
            details[key] = value


def _failure_identity(
    *,
    schema: str,
    release_id: str,
    deployment_id: str,
    environment: str,
    artifact_registry_ref: str,
) -> dict[str, object]:
    return {
        "schema": schema,
        "status": "failed",
        "release_id": canonical_digest_or_none(release_id),
        "deployment_id": canonical_digest_or_none(deployment_id),
        "environment": _safe_logical_value(environment),
        "artifact_registry_ref": _safe_logical_value(artifact_registry_ref),
    }


def _safe_logical_value(value: str) -> str | None:
    return value if is_safe_artifact_delivery_name(value) else None


def _command_name(stage: str) -> str:
    return "publish" if stage == "airflow_artifact_publish" else "cache-materialize"


def _exit_code(code: str) -> int:
    if code in _INPUT_CODES:
        return 2
    if code in _DEPENDENCY_CODES:
        return 3
    if code in _SECURITY_CODES:
        return 4
    if code.startswith("DPONE_INTERNAL_"):
        return 5
    return 1


__all__ = [
    "ArtifactRegistryOptions",
    "DEFAULT_MAX_OBJECT_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "materialize_command_result",
    "publish_command_result",
]
