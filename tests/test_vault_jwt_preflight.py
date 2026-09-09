from __future__ import annotations

import io
import json
from urllib import error

import pytest
from tools.ci.vault_jwt_preflight import (
    decode_selected_jwt_claims,
    resolve_vault_jwt_preflight_config,
    run_vault_jwt_preflight,
)


def _jwt(payload: dict[str, object]) -> str:
    import base64

    header = {"alg": "none", "typ": "JWT"}

    def encode(value: dict[str, object]) -> str:
        raw = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{encode(header)}.{encode(payload)}.signature"


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


def test_resolve_vault_jwt_preflight_config_uses_dev_role() -> None:
    config = resolve_vault_jwt_preflight_config(
        {
            "VAULT_ADDR": "https://vault.example.com",
            "VAULT_AUTH_METHOD": "jwt",
            "VAULT_ID_TOKEN": "jwt-token",
            "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
        }
    )

    assert config.auth_path == "jwt"
    assert config.role == "dpone-viewer-dev"
    assert config.env_code == "dev"
    assert config.jwt_source == "VAULT_ID_TOKEN"


def test_resolve_vault_jwt_preflight_config_uses_prod_role() -> None:
    config = resolve_vault_jwt_preflight_config(
        {
            "VAULT_SERVER_URL": "https://vault.example.com",
            "VAULT_AUTH_METHOD": "jwt",
            "VAULT_ID_TOKEN": "jwt-token",
            "ENV_CODE": "prod",
            "VAULT_AUTH_ROLE_PROD": "dpone-viewer-prod",
            "VAULT_AUTH_PATH": "jwt_v2",
        }
    )

    assert config.auth_path == "jwt_v2"
    assert config.role == "dpone-viewer-prod"
    assert config.env_code == "prod"


def test_resolve_vault_jwt_preflight_config_prefers_explicit_role() -> None:
    config = resolve_vault_jwt_preflight_config(
        {
            "VAULT_ADDR": "https://vault.example.com",
            "VAULT_AUTH_METHOD": "jwt",
            "VAULT_ID_TOKEN": "jwt-token",
            "VAULT_AUTH_ROLE": "dpone-explicit-role",
            "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
        }
    )

    assert config.role == "dpone-explicit-role"


def test_decode_selected_jwt_claims_filters_unrelated_claims() -> None:
    claims = decode_selected_jwt_claims(
        _jwt(
            {
                "iss": "gitlab",
                "aud": "vault",
                "project_path": "data-platform/data-platform-dpone",
                "ref": "develop",
                "custom_claim": "ignored",
            }
        )
    )

    assert claims == {
        "iss": "gitlab",
        "aud": "vault",
        "project_path": "data-platform/data-platform-dpone",
        "ref": "develop",
    }


def test_run_vault_jwt_preflight_reformats_403_without_leaking_token() -> None:
    token = _jwt(
        {
            "iss": "https://gitlab.example.com",
            "aud": ["vault"],
            "sub": "job_123",
            "project_path": "data-platform/data-platform-dpone",
            "ref": "develop",
            "ref_type": "branch",
            "ref_protected": True,
            "pipeline_source": "schedule",
            "secret": "should-not-appear",
        }
    )
    env = {
        "VAULT_ADDR": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_AUTH_PATH": "jwt_v2",
        "VAULT_ID_TOKEN": token,
        "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
    }

    def fake_urlopen(req, context=None):
        del context
        raise error.HTTPError(
            req.full_url,
            403,
            "Forbidden",
            hdrs=None,
            fp=io.BytesIO(b'{"errors":["permission denied"]}'),
        )

    with pytest.raises(RuntimeError) as exc_info:
        run_vault_jwt_preflight(env, urlopen=fake_urlopen)

    message = str(exc_info.value)
    assert "auth_path=jwt_v2" in message
    assert "role=dpone-viewer-dev" in message
    assert "env_code=dev" in message
    assert "namespace_present=no" in message
    assert "jwt_source=VAULT_ID_TOKEN" in message
    assert "permission denied" in message
    assert '"aud": ["vault"]' in message
    assert '"project_path": "data-platform/data-platform-dpone"' in message
    assert '"pipeline_source": "schedule"' in message
    assert "secret" not in message
    assert token not in message


def test_run_vault_jwt_preflight_succeeds_and_returns_config() -> None:
    env = {
        "VAULT_ADDR": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_ID_TOKEN": _jwt({"aud": "vault"}),
        "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
        "VAULT_NAMESPACE": "example_travel",
    }

    config = run_vault_jwt_preflight(env, urlopen=lambda req, context=None: _FakeResponse())

    assert config.role == "dpone-viewer-dev"
    assert config.namespace == "example_travel"
