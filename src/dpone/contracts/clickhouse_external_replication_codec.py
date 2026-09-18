"""Private JSON codec helpers for external ClickHouse authority."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

from dpone.contracts.clickhouse_external_replication_identity import ExternalContractError


def decode_member(value: Any) -> Any:
    from dpone.contracts.clickhouse_external_replication import (
        ExternalMemberRecord,
        ExternalMemberStageState,
        MemberPublicationState,
        PhysicalGeneration,
    )

    require_exact_fields(value, ExternalMemberRecord)
    value = dict(value)
    value["stage_state"] = ExternalMemberStageState(value["stage_state"])
    value["publication_state"] = MemberPublicationState(value["publication_state"])
    for name in ("predecessor", "candidate"):
        value[name] = None if value[name] is None else decode_dataclass(PhysicalGeneration, value[name])
    return ExternalMemberRecord(**value)


def decode_dataclass(kind: type[Any], value: Any) -> Any:
    require_exact_fields(value, kind)
    return kind(**value)


def require_exact_fields(value: Any, kind: type[Any]) -> None:
    if not isinstance(value, dict) or set(value) != {item.name for item in fields(kind)}:
        raise ExternalContractError("AUTHORITY_INVALID", f"{kind.__name__} fields differ")


def member_evidence(member: Any) -> dict[str, Any]:
    def generation(value: Any | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return {
            "uuid": value.uuid,
            "schema_digest": value.schema_digest,
            "content_digest": value.content_digest,
            "row_count": value.row_count,
        }

    return {
        "member_id": member.member_id,
        "stage_state": member.stage_state.value,
        "publication_state": member.publication_state.value,
        "cleanup_complete": member.cleanup_complete,
        "predecessor": generation(member.predecessor),
        "candidate": generation(member.candidate),
    }


__all__ = ["decode_dataclass", "decode_member", "member_evidence", "require_exact_fields"]
