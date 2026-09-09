"""Resolve object storage clients through dpone credentials providers."""

from __future__ import annotations

from dpone.runtime.credentials.config import CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.manager import CredentialsManager
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader
from dpone.runtime.object_storage_access_models import ObjectStorageConnectionRef, text
from dpone.storage import (
    AzureBlobObjectStorageClient,
    GCSObjectStorageClient,
    ImmutableObjectStorageClient,
    ObjectStorageProvider,
    ObjectStorageUri,
    S3ObjectStorageClient,
)


class ObjectStorageConnectionResolver:
    """Resolve object storage credentials through the standard dpone providers."""

    def __init__(self, credentials_manager: CredentialsManager | None = None) -> None:
        self._credentials_manager = credentials_manager or CredentialsManager()

    def get_credentials(self, ref: ObjectStorageConnectionRef) -> CredentialsConfig:
        # Strict init_fetch pods mount Airflow Connection URIs and pin
        # DPONE_RUNTIME_CONNECTION_CONTEXT. Object-storage fast paths historically
        # called CredentialsManager(airflow) directly, which fails closed in the
        # runtime image (no Airflow SDK, no AIRFLOW_CONN_* env). Prefer the
        # verified mount/context resolver whenever the context is present.
        context_credentials = _credentials_from_runtime_context(ref.connection_id)
        if context_credentials is not None:
            return context_credentials
        return self._credentials_manager.get_credentials(
            ref.connection_id,
            source=CredentialsSource(ref.connection_type),
            mount_point=ref.vault_mount_point or "",
            path=ref.vault_path or "",
        )

    def build_client(self, *, ref: ObjectStorageConnectionRef, uri: ObjectStorageUri) -> ImmutableObjectStorageClient:
        credentials = self.get_credentials(ref)
        params = credentials.additional_params or {}
        if uri.provider == ObjectStorageProvider.S3:
            access_key = credentials.username or text(params.get("aws_access_key_id"))
            secret_key = credentials.password or text(params.get("aws_secret_access_key"))
            if not access_key or not secret_key:
                raise ValueError("S3 logical credential reference requires explicit access key and secret key")
            return S3ObjectStorageClient(
                endpoint_url=credentials.endpoint or text(params.get("endpoint_url")),
                region_name=text(params.get("region_name") or params.get("region")),
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=credentials.token or text(params.get("aws_session_token")),
            )
        if uri.provider == ObjectStorageProvider.GCS:
            return _gcs_client(credentials)
        if uri.provider == ObjectStorageProvider.AZURE_BLOB:
            return AzureBlobObjectStorageClient(connection_string=credentials.password or credentials.token)
        raise ValueError(f"Unsupported object storage provider: {uri.provider}")


def _credentials_from_runtime_context(connection_ref: str) -> CredentialsConfig | None:
    context = RuntimeConnectionContextLoader().load()
    if context is None:
        return None
    resolved = context.resolver.resolve(connection_ref)
    credentials = getattr(resolved, "credentials", None)
    if not isinstance(credentials, CredentialsConfig):
        raise TypeError("runtime connection resolver returned an unsupported object")
    return credentials


def _gcs_client(credentials: CredentialsConfig) -> GCSObjectStorageClient:
    if credentials.service_account_info:
        from google.oauth2.service_account import Credentials

        resolved = Credentials.from_service_account_info(credentials.service_account_info)
    elif credentials.service_account_key_file:
        from google.oauth2.service_account import Credentials

        resolved = Credentials.from_service_account_file(credentials.service_account_key_file)
    else:
        raise ValueError("GCS logical credential reference requires explicit service account credentials")
    from google.cloud.storage import Client

    project_id = credentials.project_id or getattr(resolved, "project_id", None)
    return GCSObjectStorageClient(client=Client(project=project_id, credentials=resolved))
