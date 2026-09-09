"""Bounded local acknowledgement for one completed Airflow DAG parse."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone_airflow_pack.deployment_index import LoadReport
from dpone_airflow_pack.deployment_index_contract import infer_cache_root

LOADER_ACK_SCHEMA_V1 = "dpone.airflow_loader_ack.v1"
LOADER_ACK_SCHEMA_V2 = "dpone.airflow_loader_ack.v2"
LOADER_ACK_SCHEMA = LOADER_ACK_SCHEMA_V2
_MAX_ACK_BYTES = 64 * 1024
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_DAG_ID = re.compile(r"^[A-Za-z0-9_.~-]+$")
_ERROR_CODE = re.compile(r"^DPONE_[A-Z0-9_]+$")
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_ACK_FIELDS = frozenset(
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
_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class LoaderAcknowledgementError(RuntimeError):
    """Raised when a loader acknowledgement cannot be safely persisted."""


@dataclass(frozen=True)
class LoaderAcknowledgement:
    """Identity and result of one local deployment-index parse."""

    release_id: str
    deployment_id: str
    airflow_index_sha256: str
    activation_id: str
    loaded_dag_ids: tuple[str, ...]
    skipped_dag_ids: tuple[str, ...]
    error_codes: tuple[str, ...]
    fatal: bool
    acknowledged_at: str
    schema: str

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self)
        for field in ("loaded_dag_ids", "skipped_dag_ids", "error_codes"):
            payload[field] = list(payload[field])
        return payload


@dataclass(frozen=True)
class AcknowledgedDagLoad:
    """One exact DAG parse result and its durably persisted acknowledgement."""

    report: LoadReport
    acknowledgement: LoaderAcknowledgement


def parse_loader_ack_json(raw: bytes) -> LoaderAcknowledgement:
    """Parse one closed ACK v2 contract without importing the full runtime."""

    if len(raw) > _MAX_ACK_BYTES:
        raise LoaderAcknowledgementError("loader acknowledgement exceeds 64 KiB")
    try:
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise LoaderAcknowledgementError("loader acknowledgement must be strict UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != _ACK_FIELDS:
        raise LoaderAcknowledgementError("loader acknowledgement is incomplete")
    if payload.get("schema") != LOADER_ACK_SCHEMA_V2 or not isinstance(payload.get("fatal"), bool):
        raise LoaderAcknowledgementError("loader acknowledgement schema or fatal field is invalid")
    loaded = _strict_string_array(payload.get("loaded_dag_ids"), field="loaded_dag_ids", pattern=_DAG_ID)
    skipped = _strict_string_array(payload.get("skipped_dag_ids"), field="skipped_dag_ids", pattern=_DAG_ID)
    error_codes = _strict_string_array(payload.get("error_codes"), field="error_codes", pattern=_ERROR_CODE)
    if set(loaded) & set(skipped):
        raise LoaderAcknowledgementError("loader acknowledgement loaded and skipped DAG ids must be disjoint")
    acknowledged_at = payload.get("acknowledged_at")
    if not isinstance(acknowledged_at, str):
        raise LoaderAcknowledgementError("loader acknowledgement timestamp is invalid")
    try:
        timestamp = datetime.fromisoformat(acknowledged_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LoaderAcknowledgementError("loader acknowledgement timestamp is invalid") from exc
    if timestamp.tzinfo is None:
        raise LoaderAcknowledgementError("loader acknowledgement timestamp must include a timezone")
    for field in ("release_id", "deployment_id", "airflow_index_sha256"):
        value = payload.get(field)
        if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
            raise LoaderAcknowledgementError(f"loader acknowledgement {field} is invalid")
    activation_id = payload.get("activation_id")
    if not isinstance(activation_id, str) or _UUID_V4.fullmatch(activation_id) is None:
        raise LoaderAcknowledgementError("loader acknowledgement activation_id is invalid")
    return LoaderAcknowledgement(
        release_id=payload["release_id"],
        deployment_id=payload["deployment_id"],
        airflow_index_sha256=payload["airflow_index_sha256"],
        activation_id=activation_id,
        loaded_dag_ids=loaded,
        skipped_dag_ids=skipped,
        error_codes=error_codes,
        fatal=payload["fatal"],
        acknowledged_at=acknowledged_at,
        schema=LOADER_ACK_SCHEMA_V2,
    )


def write_dpone_loader_ack(
    report: LoadReport,
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = None,
) -> LoaderAcknowledgement:
    """Atomically acknowledge a completed local parse without metadata I/O.

    ``ack_root`` separates the writable acknowledgement channel from a
    read-only cache mount. Omitting it preserves the legacy
    ``<cache>/status`` destination.
    """

    if not report.release_id or not _DIGEST.fullmatch(report.release_id):
        raise LoaderAcknowledgementError("loader report release_id is invalid")
    if not report.deployment_id or not _DIGEST.fullmatch(report.deployment_id):
        raise LoaderAcknowledgementError("loader report deployment_id is invalid")
    if not report.airflow_index_sha256 or not _DIGEST.fullmatch(report.airflow_index_sha256):
        raise LoaderAcknowledgementError("loader report airflow_index_sha256 is invalid")
    activation_id = report.activation_id
    if activation_id is None:
        raise LoaderAcknowledgementError("loader report activation_id is required for v2 acknowledgement")
    if _UUID_V4.fullmatch(activation_id) is None:
        raise LoaderAcknowledgementError("loader report activation_id is invalid")
    lexical_index = Path(index_path).absolute()
    cache_root = infer_cache_root(lexical_index)
    if report.cache_root != cache_root.as_posix():
        raise LoaderAcknowledgementError("loader report belongs to a different cache root")
    destination = Path(ack_path).absolute()
    write_root, directory_name = _ack_destination(
        cache_root=cache_root,
        destination=destination,
        ack_root=ack_root,
    )
    acknowledgement = LoaderAcknowledgement(
        release_id=report.release_id,
        deployment_id=report.deployment_id,
        airflow_index_sha256=report.airflow_index_sha256,
        activation_id=activation_id,
        loaded_dag_ids=_validated_dag_ids(report.loaded),
        skipped_dag_ids=_validated_dag_ids(tuple(item.get("dag_id", "") for item in report.skipped)),
        error_codes=_validated_error_codes(report.errors),
        fatal=report.fatal,
        acknowledged_at=datetime.now(tz=timezone.utc).isoformat(),  # noqa: UP017
        schema=LOADER_ACK_SCHEMA_V2,
    )
    _write_ack(
        write_root,
        directory_name=directory_name,
        filename=destination.name,
        acknowledgement=acknowledgement,
    )
    return acknowledgement


def _ack_destination(
    *,
    cache_root: Path,
    destination: Path,
    ack_root: str | Path | None,
) -> tuple[Path, str | None]:
    if destination.name in {"", ".", ".."}:
        raise LoaderAcknowledgementError("loader acknowledgement filename is invalid")
    if ack_root is None:
        if destination.parent != cache_root / "status":
            raise LoaderAcknowledgementError("loader acknowledgement must stay under the cache status directory")
        return cache_root, "status"
    external_root = Path(ack_root).absolute()
    if destination.parent != external_root:
        raise LoaderAcknowledgementError(
            "loader acknowledgement must stay directly under the configured acknowledgement root"
        )
    try:
        resolved_cache = cache_root.resolve(strict=True)
        resolved_external = external_root.resolve(strict=True)
    except OSError as exc:
        raise LoaderAcknowledgementError("loader acknowledgement root could not be resolved safely") from exc
    if (
        resolved_external == resolved_cache
        or resolved_external.is_relative_to(resolved_cache)
        or resolved_cache.is_relative_to(resolved_external)
    ):
        raise LoaderAcknowledgementError("loader acknowledgement root must be separate from the cache root")
    return external_root, None


def _validated_dag_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(sorted(set(values)))
    if any(not _DAG_ID.fullmatch(value) for value in normalized):
        raise LoaderAcknowledgementError("loader report contains an invalid DAG id")
    return normalized


def _validated_error_codes(errors: tuple[dict[str, str], ...]) -> tuple[str, ...]:
    codes = tuple(sorted({str(item.get("code") or "DPONE_AIRFLOW_DAG_SPEC_INVALID") for item in errors}))
    if any(not _ERROR_CODE.fullmatch(code) for code in codes):
        raise LoaderAcknowledgementError("loader report contains an invalid error code")
    return codes


def _strict_string_array(value: object, *, field: str, pattern: re.Pattern[str]) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or pattern.fullmatch(item) is None for item in value
    ):
        raise LoaderAcknowledgementError(f"loader acknowledgement {field} is invalid")
    if len(value) != len(set(value)):
        raise LoaderAcknowledgementError(f"loader acknowledgement {field} contains duplicates")
    if value != sorted(value):
        raise LoaderAcknowledgementError(f"loader acknowledgement {field} must use canonical sorted order")
    return tuple(value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("loader acknowledgement contains duplicate fields")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("loader acknowledgement contains a non-finite number")


def _write_ack(
    write_root: Path,
    *,
    directory_name: str | None,
    filename: str,
    acknowledgement: LoaderAcknowledgement,
) -> None:
    serialized = (
        json.dumps(
            acknowledgement.to_jsonable(),
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    )
    if len(serialized.encode("utf-8")) > _MAX_ACK_BYTES:
        raise LoaderAcknowledgementError("loader acknowledgement exceeds 64 KiB")
    root_descriptor: int | None = None
    destination_descriptor: int | None = None
    temporary_name = f".{filename}.{uuid4().hex}.tmp"
    try:
        root_descriptor = os.open(write_root, _DIRECTORY_FLAGS)
        if directory_name is None:
            destination_descriptor = root_descriptor
        else:
            try:
                os.mkdir(directory_name, mode=0o750, dir_fd=root_descriptor)
            except FileExistsError:
                pass
            destination_descriptor = os.open(
                directory_name,
                _DIRECTORY_FLAGS,
                dir_fd=root_descriptor,
            )
        temporary_descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o640,
            dir_fd=destination_descriptor,
        )
        try:
            os.fchmod(temporary_descriptor, 0o640)
            content = serialized.encode("utf-8")
            written = 0
            while written < len(content):
                chunk_size = os.write(temporary_descriptor, content[written:])
                if chunk_size <= 0:
                    raise OSError("loader acknowledgement write made no progress")
                written += chunk_size
            os.fsync(temporary_descriptor)
        finally:
            os.close(temporary_descriptor)
        os.replace(
            temporary_name,
            filename,
            src_dir_fd=destination_descriptor,
            dst_dir_fd=destination_descriptor,
        )
        os.fsync(destination_descriptor)
    except OSError as exc:
        if destination_descriptor is not None:
            try:
                os.unlink(temporary_name, dir_fd=destination_descriptor)
            except OSError:
                pass
        raise LoaderAcknowledgementError("loader acknowledgement could not be persisted") from exc
    finally:
        if destination_descriptor is not None and destination_descriptor != root_descriptor:
            os.close(destination_descriptor)
        if root_descriptor is not None:
            os.close(root_descriptor)


__all__ = [
    "AcknowledgedDagLoad",
    "LOADER_ACK_SCHEMA",
    "LOADER_ACK_SCHEMA_V1",
    "LOADER_ACK_SCHEMA_V2",
    "LoaderAcknowledgement",
    "LoaderAcknowledgementError",
    "parse_loader_ack_json",
    "write_dpone_loader_ack",
]
