"""Private, restart-safe receipt for an original prepared-recovery plan."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.contracts.clickhouse_cluster_publication import digest_payload
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import authority_from_mapping
from dpone.runtime.sinks.clickhouse_prepared_recovery import PreparedRecoveryPlan

_SCHEMA = "dpone.clickhouse.prepared-recovery-plan.v1"
_MAX_BYTES = 1024 * 1024


def write_plan(plan: PreparedRecoveryPlan, path: Path) -> None:
    """Create an owner-only receipt, never overwrite or follow a prior path."""
    payload = {
        "schema": _SCHEMA,
        "cluster": plan.cluster,
        "record": asdict(plan.record),
        "record_digest": plan.record.payload_sha256,
        "authority_version": plan.authority_version,
        "operation_started_at": plan.operation_started_at.isoformat(),
        "token": plan.token,
        "query_digest": plan.query_digest,
        "plan_digest": plan.plan_digest,
    }
    content = (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    if len(content) > _MAX_BYTES:
        raise ValueError("recovery plan is too large")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def read_plan(path: Path) -> PreparedRecoveryPlan:
    """Read only an owner-private, exact-schema artifact."""
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
            or metadata.st_size > _MAX_BYTES
        ):
            raise ValueError("recovery plan must be a private regular file")
        content = source.read(_MAX_BYTES + 1)
    if len(content) > _MAX_BYTES:
        raise ValueError("recovery plan is too large")
    raw: Any = json.loads(content.decode("utf-8"))
    if not isinstance(raw, dict) or raw.get("schema") != _SCHEMA:
        raise ValueError("recovery plan schema is invalid")
    record = authority_from_mapping(dict(raw["record"]))
    started_at = datetime.fromisoformat(raw["operation_started_at"])
    if started_at.tzinfo is None or raw["record_digest"] != record.payload_sha256:
        raise ValueError("recovery plan record or timestamp changed")
    plan = PreparedRecoveryPlan(
        cluster=raw["cluster"],
        record=record,
        authority_version=raw["authority_version"],
        operation_started_at=started_at,
        token=raw["token"],
        query_digest=raw["query_digest"],
        plan_digest=raw["plan_digest"],
    )
    expected = digest_payload(
        {
            "record": record.payload_sha256,
            "version": plan.authority_version,
            "inventory": record.inventory_digest,
            "started_at": started_at.astimezone(UTC).isoformat(),
            "token": plan.token,
            "query_digest": plan.query_digest,
        }
    )
    if expected != plan.plan_digest:
        raise ValueError("recovery plan digest changed")
    return plan
