"""Dependency-free contract for exact Airflow loader acknowledgements."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

LOADER_ACK_SCHEMA_V2 = "dpone.airflow_loader_ack.v2"
MAX_LOADER_ACK_BYTES = 64 * 1024
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_DAG_ID = re.compile(r"^[A-Za-z0-9_.~-]+$")
_ERROR_CODE = re.compile(r"^DPONE_[A-Z0-9_]+$")
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_FIELDS = frozenset(
    {
        "schema",
        "release_id",
        "deployment_id",
        "airflow_index_sha256",
        "activation_id",
        "loaded_dag_ids",
        "skipped_dag_ids",
        "error_codes",
        "fatal",
        "acknowledged_at",
    }
)


class AirflowLoaderAckError(ValueError):
    """One loader ACK violates its closed contract."""


@dataclass(frozen=True, slots=True)
class AirflowLoaderAck:
    release_id: str
    deployment_id: str
    airflow_index_sha256: str
    activation_id: str
    loaded_dag_ids: tuple[str, ...]
    skipped_dag_ids: tuple[str, ...]
    error_codes: tuple[str, ...]
    fatal: bool
    acknowledged_at: str

    @property
    def acknowledged_dag_ids(self) -> tuple[str, ...]:
        return tuple(sorted((*self.loaded_dag_ids, *self.skipped_dag_ids)))

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": LOADER_ACK_SCHEMA_V2,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "activation_id": self.activation_id,
            "loaded_dag_ids": list(self.loaded_dag_ids),
            "skipped_dag_ids": list(self.skipped_dag_ids),
            "error_codes": list(self.error_codes),
            "fatal": self.fatal,
            "acknowledged_at": self.acknowledged_at,
        }


def parse_airflow_loader_ack(raw: bytes) -> AirflowLoaderAck:
    """Parse strict bounded UTF-8 JSON and enforce canonical set ordering."""

    if len(raw) > MAX_LOADER_ACK_BYTES:
        raise AirflowLoaderAckError("loader acknowledgement exceeds 64 KiB")
    try:
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise AirflowLoaderAckError("loader acknowledgement must be strict UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != _FIELDS or payload.get("schema") != LOADER_ACK_SCHEMA_V2:
        raise AirflowLoaderAckError("loader acknowledgement is incomplete")
    loaded = _string_array(payload.get("loaded_dag_ids"), "loaded_dag_ids", _DAG_ID)
    skipped = _string_array(payload.get("skipped_dag_ids"), "skipped_dag_ids", _DAG_ID)
    errors = _string_array(payload.get("error_codes"), "error_codes", _ERROR_CODE)
    if set(loaded) & set(skipped):
        raise AirflowLoaderAckError("loaded and skipped DAG ids must be disjoint")
    for field in ("release_id", "deployment_id", "airflow_index_sha256"):
        if not isinstance(payload.get(field), str) or _DIGEST.fullmatch(payload[field]) is None:
            raise AirflowLoaderAckError(f"loader acknowledgement {field} is invalid")
    activation_id = payload.get("activation_id")
    if not isinstance(activation_id, str) or _UUID_V4.fullmatch(activation_id) is None:
        raise AirflowLoaderAckError("loader acknowledgement activation_id is invalid")
    fatal = payload.get("fatal")
    if not isinstance(fatal, bool):
        raise AirflowLoaderAckError("loader acknowledgement fatal field is invalid")
    acknowledged_at = payload.get("acknowledged_at")
    if not isinstance(acknowledged_at, str) or not _aware_datetime(acknowledged_at):
        raise AirflowLoaderAckError("loader acknowledgement timestamp is invalid")
    return AirflowLoaderAck(
        release_id=payload["release_id"],
        deployment_id=payload["deployment_id"],
        airflow_index_sha256=payload["airflow_index_sha256"],
        activation_id=activation_id,
        loaded_dag_ids=loaded,
        skipped_dag_ids=skipped,
        error_codes=errors,
        fatal=fatal,
        acknowledged_at=acknowledged_at,
    )


def _string_array(value: object, field: str, pattern: re.Pattern[str]) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or pattern.fullmatch(item) is None for item in value
    ):
        raise AirflowLoaderAckError(f"loader acknowledgement {field} is invalid")
    if value != sorted(set(value)):
        raise AirflowLoaderAckError(f"loader acknowledgement {field} must be sorted and unique")
    return tuple(value)


def _aware_datetime(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("loader acknowledgement contains duplicate fields")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("loader acknowledgement contains a non-finite number")


__all__ = [
    "AirflowLoaderAck",
    "AirflowLoaderAckError",
    "LOADER_ACK_SCHEMA_V2",
    "MAX_LOADER_ACK_BYTES",
    "parse_airflow_loader_ack",
]
