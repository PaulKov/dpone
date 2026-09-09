from __future__ import annotations

import pytest

from dpone.runtime.errors import (
    LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE,
    LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE,
    LegacyRuntimeDefaultsDisabledError,
)
from dpone.runtime.support.gcs import (
    create_gcs_client,
    get_env_code,
    get_gcs_bucket_name,
    get_gcs_hmac_credentials,
)


def test_runtime_gcs_support_reexports_get_env_code() -> None:
    assert callable(get_env_code)


def test_get_gcs_bucket_name_fails_closed_without_explicit_bucket() -> None:
    with pytest.raises(LegacyRuntimeDefaultsDisabledError) as exc:
        get_gcs_bucket_name("analytics", "prod")

    assert exc.value.code == LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE
    assert LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE in str(exc.value)


def test_get_gcs_hmac_credentials_fails_closed_without_explicit_coordinates() -> None:
    with pytest.raises(LegacyRuntimeDefaultsDisabledError) as exc:
        get_gcs_hmac_credentials()

    assert exc.value.code == LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE
    assert LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE in str(exc.value)


def test_get_gcs_hmac_credentials_rejects_auto_generated_vault_path() -> None:
    with pytest.raises(LegacyRuntimeDefaultsDisabledError) as exc:
        get_gcs_hmac_credentials(auto_generate_vault_path=True)

    assert exc.value.code == LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE
    assert "Automatic GCS HMAC Vault path generation is disabled" in str(exc.value)


def test_get_gcs_hmac_credentials_accepts_explicit_pair() -> None:
    assert get_gcs_hmac_credentials(hmac_key="key", hmac_secret="secret") == ("key", "secret")


def test_create_gcs_client_fails_closed_without_explicit_vault_path() -> None:
    with pytest.raises(LegacyRuntimeDefaultsDisabledError) as exc:
        create_gcs_client(env_code="prod")

    assert exc.value.code == LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE
    assert "Explicit vault_path is required" in str(exc.value)
