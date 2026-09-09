"""Parse-safe exact cache activation identity for provider runtime tasks."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

AIRFLOW_DEPLOYMENT_IDENTITY_ENV = "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY"
AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA = "dpone.airflow-deployment-identity.v1"
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def deployment_identity_from_context(context: Mapping[str, Any]) -> dict[str, str] | None:
    release_id = context.get("release_id")
    deployment_id = context.get("deployment_id")
    activation_id = context.get("_activation_id")
    if activation_id is None:
        return None
    identity = {
        "schema": AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "activation_id": activation_id,
    }
    error = deployment_identity_error(identity)
    if error:
        raise ValueError(f"DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_INVALID: {error}")
    return {key: str(value) for key, value in identity.items()}


def deployment_identity_error(value: object) -> str:
    if not isinstance(value, Mapping):
        return "deployment_identity must be an object"
    expected = {"schema", "release_id", "deployment_id", "activation_id"}
    if set(value) != expected:
        return "deployment_identity fields are invalid"
    if value.get("schema") != AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA:
        return "deployment_identity.schema is invalid"
    if any(_DIGEST.fullmatch(str(value.get(field) or "")) is None for field in ("release_id", "deployment_id")):
        return "deployment_identity release_id and deployment_id must be canonical sha256 digests"
    if _UUID_V4.fullmatch(str(value.get("activation_id") or "")) is None:
        return "deployment_identity.activation_id must be a canonical UUID v4"
    return ""


def serialize_deployment_identity(identity: Mapping[str, Any]) -> str:
    error = deployment_identity_error(identity)
    if error:
        raise ValueError(f"DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_INVALID: {error}")
    return json.dumps(dict(identity), ensure_ascii=True, separators=(",", ":"), sort_keys=True)


__all__ = [
    "AIRFLOW_DEPLOYMENT_IDENTITY_ENV",
    "AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA",
    "deployment_identity_error",
    "deployment_identity_from_context",
    "serialize_deployment_identity",
]
