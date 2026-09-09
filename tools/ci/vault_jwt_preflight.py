from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib import error, request

FALSEY = {"0", "false", "no", "off"}
JWT_CLAIMS_TO_REPORT = (
    "iss",
    "aud",
    "sub",
    "project_path",
    "ref",
    "ref_type",
    "ref_protected",
    "pipeline_source",
)


@dataclass(frozen=True, slots=True)
class VaultJWTPreflightConfig:
    addr: str
    auth_path: str
    role: str
    env_code: str
    namespace: str | None
    verify: bool | str
    jwt: str
    jwt_source: str

    @property
    def login_path(self) -> str:
        return f"/v1/auth/{self.auth_path}/login"


def _env_value(source: Mapping[str, str] | None, name: str) -> str:
    return str((source or os.environ).get(name, "")).strip()


def _resolve_addr(source: Mapping[str, str] | None = None) -> str:
    return _env_value(source, "VAULT_ADDR") or _env_value(source, "VAULT_SERVER_URL")


def resolve_vault_auth_role(source: Mapping[str, str] | None = None) -> str:
    explicit_role = _env_value(source, "VAULT_AUTH_ROLE")
    if explicit_role:
        return explicit_role

    env_code = _env_value(source, "ENV_CODE") or "dev"
    if env_code == "prod":
        return _env_value(source, "VAULT_AUTH_ROLE_PROD")
    return _env_value(source, "VAULT_AUTH_ROLE_DEV")


def _resolve_auth_path(source: Mapping[str, str] | None = None) -> str:
    return _env_value(source, "VAULT_PATH") or _env_value(source, "VAULT_AUTH_PATH") or "jwt"


def _resolve_verify(source: Mapping[str, str] | None = None) -> bool | str:
    if _env_value(source, "VAULT_SKIP_VERIFY").lower() in FALSEY:
        pass
    elif _env_value(source, "VAULT_SKIP_VERIFY"):
        return False

    cacert = _env_value(source, "VAULT_CACERT")
    if cacert:
        return cacert

    verify = _env_value(source, "VAULT_VERIFY")
    if verify:
        return verify.lower() not in FALSEY
    return True


def _load_jwt_from_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError as exc:
        raise RuntimeError(f"Vault JWT auth is configured but JWT file '{path}' could not be read: {exc}") from exc


def _resolve_jwt(source: Mapping[str, str] | None = None) -> tuple[str, str]:
    jwt = _env_value(source, "VAULT_JWT")
    if jwt:
        return ("VAULT_JWT", jwt)

    jwt_env_var_name = _env_value(source, "VAULT_JWT_ENV_VAR")
    if jwt_env_var_name:
        jwt = _env_value(source, jwt_env_var_name)
        if jwt:
            return (f"VAULT_JWT_ENV_VAR:{jwt_env_var_name}", jwt)

    jwt = _env_value(source, "VAULT_ID_TOKEN")
    if jwt:
        return ("VAULT_ID_TOKEN", jwt)

    jwt_file = _env_value(source, "VAULT_JWT_FILE")
    if jwt_file:
        return ("VAULT_JWT_FILE", _load_jwt_from_file(jwt_file))

    raise RuntimeError(
        "Vault JWT auth is configured but no JWT source was found. "
        "Set VAULT_JWT, VAULT_JWT_ENV_VAR, VAULT_ID_TOKEN, or VAULT_JWT_FILE."
    )


def resolve_vault_jwt_preflight_config(source: Mapping[str, str] | None = None) -> VaultJWTPreflightConfig:
    addr = _resolve_addr(source)
    if not addr:
        raise RuntimeError("Vault JWT auth is configured but VAULT_ADDR or VAULT_SERVER_URL is missing.")

    env_code = _env_value(source, "ENV_CODE") or "dev"
    role = resolve_vault_auth_role(source)
    if not role:
        raise RuntimeError(
            "Vault JWT auth is configured but no role was resolved. "
            "Set VAULT_AUTH_ROLE or VAULT_AUTH_ROLE_DEV/VAULT_AUTH_ROLE_PROD."
        )

    jwt_source, jwt = _resolve_jwt(source)
    return VaultJWTPreflightConfig(
        addr=addr.rstrip("/"),
        auth_path=_resolve_auth_path(source).strip("/"),
        role=role,
        env_code=env_code,
        namespace=_env_value(source, "VAULT_NAMESPACE") or None,
        verify=_resolve_verify(source),
        jwt=jwt,
        jwt_source=jwt_source,
    )


def decode_selected_jwt_claims(jwt: str) -> dict[str, Any]:
    parts = jwt.split(".")
    if len(parts) < 2:
        return {}

    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        parsed = json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return {}

    if not isinstance(parsed, dict):
        return {}
    return {key: parsed[key] for key in JWT_CLAIMS_TO_REPORT if key in parsed}


def _build_ssl_context(source: Mapping[str, str] | None, verify: bool | str) -> ssl.SSLContext:
    try:
        if verify is False:
            return ssl._create_unverified_context()

        context = ssl.create_default_context()
        if isinstance(verify, str):
            context.load_verify_locations(cafile=verify)

        cacert_bytes = _env_value(source, "VAULT_CACERT_BYTES")
        if cacert_bytes:
            context.load_verify_locations(cadata=cacert_bytes)
        return context
    except (OSError, ssl.SSLError) as exc:
        raise RuntimeError(f"Vault JWT auth is configured but TLS verify settings are invalid: {exc}") from exc


def _format_claims_for_error(claims: dict[str, Any]) -> str:
    if not claims:
        return "{}"
    return json.dumps(claims, ensure_ascii=True, sort_keys=True)


def _raise_preflight_failure(
    *,
    config: VaultJWTPreflightConfig,
    claims: dict[str, Any],
    reason: str,
) -> None:
    raise RuntimeError(
        "Vault JWT auth preflight failed: "
        f"reason={reason}; auth_path={config.auth_path}; role={config.role}; env_code={config.env_code}; "
        f"namespace_present={'yes' if config.namespace else 'no'}; jwt_source={config.jwt_source}; "
        f"claims={_format_claims_for_error(claims)}. "
        "Check VAULT_AUTH_ROLE_DEV/VAULT_AUTH_ROLE_PROD, VAULT_JWT_AUDIENCE, VAULT_AUTH_PATH, "
        "VAULT_NAMESPACE, and Vault JWT role claim bindings."
    )


def run_vault_jwt_preflight(
    source: Mapping[str, str] | None = None,
    *,
    urlopen=request.urlopen,
) -> VaultJWTPreflightConfig:
    config = resolve_vault_jwt_preflight_config(source)
    claims = decode_selected_jwt_claims(config.jwt)

    headers = {"Content-Type": "application/json"}
    if config.namespace:
        headers["X-Vault-Namespace"] = config.namespace

    payload = json.dumps({"role": config.role, "jwt": config.jwt}).encode("utf-8")
    req = request.Request(
        f"{config.addr}{config.login_path}",
        data=payload,
        headers=headers,
        method="POST",
    )
    context = _build_ssl_context(source, config.verify)

    try:
        with urlopen(req, context=context):
            return config
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            parsed = {}
        errors = parsed.get("errors") if isinstance(parsed, dict) else None
        if isinstance(errors, list):
            reason = "; ".join(str(item) for item in errors if item)
        elif errors:
            reason = str(errors)
        elif body:
            reason = body[:300]
        else:
            reason = f"HTTP {exc.code}"
        _raise_preflight_failure(config=config, claims=claims, reason=f"HTTP {exc.code}: {reason}")
    except error.URLError as exc:
        _raise_preflight_failure(config=config, claims=claims, reason=f"request error: {exc.reason}")


def _build_success_message(config: VaultJWTPreflightConfig) -> str:
    claims = decode_selected_jwt_claims(config.jwt)
    return (
        "Vault JWT auth preflight ok: "
        f"auth_path={config.auth_path}; role={config.role}; env_code={config.env_code}; "
        f"namespace_present={'yes' if config.namespace else 'no'}; jwt_source={config.jwt_source}; "
        f"claims={_format_claims_for_error(claims)}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Vault JWT auth wiring without exposing secrets.")
    parser.parse_args(argv)
    try:
        config = run_vault_jwt_preflight()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(_build_success_message(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
