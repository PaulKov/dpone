from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dpone.runtime.object_storage_access_models import ObjectStorageConnectionRef
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver
from dpone.runtime.object_storage_retention import (
    ObjectStorageBudgetProbe,
    ObjectStorageLifecycleAdvisor,
    ObjectStorageRetentionPolicy,
    ObjectStorageSweeper,
)
from dpone.runtime.storage_policy import parse_byte_size
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


@dataclass(frozen=True, slots=True)
class ObjectStorageOpsResult:
    code: int
    payload: dict[str, object]


class ObjectStorageOpsService:
    """Ops use case for object-storage staging retention and budget governance."""

    def budget(self, **options: Any) -> ObjectStorageOpsResult:
        policy = _policy(options)
        result = ObjectStorageBudgetProbe(_client(options)).check(
            ObjectStorageUri.parse(str(options["uri_prefix"])).prefix(),
            policy=policy,
            expected_run_bytes=parse_byte_size(options.get("expected_run_bytes", "0")),
        )
        return ObjectStorageOpsResult(code=1 if result.blockers else 0, payload=result.to_dict())

    def cleanup(self, **options: Any) -> ObjectStorageOpsResult:
        report = ObjectStorageSweeper(_client(options)).sweep(
            ObjectStorageUri.parse(str(options["uri_prefix"])).prefix(),
            policy=_policy(options),
            now=_time_arg(options.get("now")),
            dry_run=options.get("mode") == "dry-run",
        )
        return ObjectStorageOpsResult(code=1 if report.blockers else 0, payload=report.to_dict())

    def lifecycle_render(self, **options: Any) -> ObjectStorageOpsResult:
        payload = ObjectStorageLifecycleAdvisor().render(
            ObjectStorageUri.parse(str(options["uri_prefix"])).prefix(),
            policy=_policy(options),
        )
        return ObjectStorageOpsResult(code=0, payload=payload)

    def lifecycle_verify(self, **options: Any) -> ObjectStorageOpsResult:
        payload = ObjectStorageLifecycleAdvisor().verify(
            ObjectStorageUri.parse(str(options["uri_prefix"])).prefix(),
            policy=_policy(options),
            existing_rules=(),
        )
        return ObjectStorageOpsResult(code=1 if payload.get("blockers") else 0, payload=payload)


def _policy(options: dict[str, Any]) -> ObjectStorageRetentionPolicy:
    return ObjectStorageRetentionPolicy.from_options(options)


def _client(options: dict[str, Any]) -> Any:
    local_root_dir = options.get("local_root_dir")
    if local_root_dir:
        return LocalObjectStorageClient(str(local_root_dir))
    connection_id = options.get("connection_id")
    if not connection_id:
        raise SystemExit("--connection-id or --local-root-dir is required")
    return ObjectStorageConnectionResolver().build_client(
        ref=ObjectStorageConnectionRef(
            connection_type=str(options.get("connection_type") or "env"),
            connection_id=str(connection_id),
        ),
        uri=ObjectStorageUri.parse(str(options["uri_prefix"])),
    )


def _time_arg(value: object | None) -> datetime | None:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None


__all__ = ["ObjectStorageOpsResult", "ObjectStorageOpsService"]
