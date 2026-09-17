"""Pure identity and state contracts for ClickHouse full-refresh publication."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from dpone._compat import StrEnum

SCHEMA_VERSION = "dpone.clickhouse.full-refresh-publication.v1"
_MARKER_SUFFIX = "__dpone_full_refresh_publication"
_MAX_IDENTIFIER_LENGTH = 255


class ClickHouseFullRefreshPublicationError(RuntimeError):
    """A full-refresh publication cannot proceed or be reconciled safely."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}:{detail}")


class PublicationState(StrEnum):
    """Catalog states that authorize retry decisions."""

    PENDING = "pending"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class FullRefreshPublicationMarker:
    """Immutable publication intent stored in a target-local table comment."""

    operation_id: str
    plan_sha256: str
    database: str
    target: str
    candidate: str
    predecessor_uuid: str | None
    desired_uuid: str
    staged_rows: int
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def create(
        cls,
        *,
        operation_id: str,
        database: str,
        target: str,
        candidate: str,
        predecessor_uuid: str | None,
        desired_uuid: str,
        staged_rows: int,
    ) -> FullRefreshPublicationMarker:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "operation_id": operation_id,
            "database": database,
            "target": target,
            "candidate": candidate,
            "predecessor_uuid": predecessor_uuid,
            "desired_uuid": desired_uuid,
            "staged_rows": staged_rows,
        }
        return cls(
            operation_id=operation_id,
            plan_sha256=_sha256(payload),
            database=database,
            target=target,
            candidate=candidate,
            predecessor_uuid=predecessor_uuid,
            desired_uuid=desired_uuid,
            staged_rows=staged_rows,
        )

    @classmethod
    def from_json(cls, raw: str) -> FullRefreshPublicationMarker:
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "comment is not valid JSON"
            ) from exc
        expected = {
            "schema_version",
            "operation_id",
            "plan_sha256",
            "database",
            "target",
            "candidate",
            "predecessor_uuid",
            "desired_uuid",
            "staged_rows",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "comment fields do not match v1"
            )
        marker = cls(**payload)
        marker.validate()
        return marker

    def validate(self) -> None:
        strings = (self.operation_id, self.plan_sha256, self.database, self.target, self.candidate, self.desired_uuid)
        if self.schema_version != SCHEMA_VERSION or not all(isinstance(value, str) and value for value in strings):
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "required identity is missing"
            )
        if self.predecessor_uuid is not None and not isinstance(self.predecessor_uuid, str):
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "predecessor UUID is invalid"
            )
        if isinstance(self.staged_rows, bool) or not isinstance(self.staged_rows, int) or self.staged_rows < 0:
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "staged row count is invalid"
            )
        unsigned = asdict(self)
        digest = unsigned.pop("plan_sha256")
        if digest != _sha256(unsigned):
            raise ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "plan digest mismatch"
            )

    def to_json(self) -> str:
        self.validate()
        return json.dumps(asdict(self), ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


def publication_operation_id(*, run_id: str, database: str, target: str) -> str:
    """Derive a retry-stable operation ID without worker try or load identity."""

    if not run_id:
        raise ClickHouseFullRefreshPublicationError(
            "DPONE_CLICKHOUSE_FULL_REFRESH_IDENTITY_REQUIRED", "runtime run_id is empty"
        )
    return _sha256({"version": 1, "run_id": run_id, "database": database, "target": target})


def publication_marker_name(target: str) -> str:
    """Return one bounded marker name for a physical target."""

    digest = hashlib.sha256(target.encode("utf-8")).hexdigest()[:16]
    maximum_prefix = _MAX_IDENTIFIER_LENGTH - len(_MARKER_SUFFIX) - len(digest) - 2
    return f"{target[:maximum_prefix]}{_MARKER_SUFFIX}__{digest}"


def classify_publication(
    marker: FullRefreshPublicationMarker,
    *,
    target_uuid: str | None,
    candidate_uuid: str | None,
) -> PublicationState:
    """Classify only exact before/after UUID mappings."""

    if marker.predecessor_uuid is None:
        if target_uuid is None and candidate_uuid == marker.desired_uuid:
            return PublicationState.PENDING
        if target_uuid == marker.desired_uuid and candidate_uuid is None:
            return PublicationState.COMMITTED
        return PublicationState.UNKNOWN
    if (target_uuid, candidate_uuid) == (marker.predecessor_uuid, marker.desired_uuid):
        return PublicationState.PENDING
    if (target_uuid, candidate_uuid) == (marker.desired_uuid, marker.predecessor_uuid):
        return PublicationState.COMMITTED
    return PublicationState.UNKNOWN


def _sha256(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = [
    "SCHEMA_VERSION",
    "ClickHouseFullRefreshPublicationError",
    "FullRefreshPublicationMarker",
    "PublicationState",
    "classify_publication",
    "publication_marker_name",
    "publication_operation_id",
]
