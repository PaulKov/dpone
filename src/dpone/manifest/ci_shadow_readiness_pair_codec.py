"""Canonical, bounded records for the private readiness-pair transaction."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import cast

SCHEMA = "dpone.readiness-report-transaction.v1"
IDENTITY_SCHEMA = "dpone.readiness-report-identity.v1"
PHASES = frozenset({"PREPARED", "JSON_REPLACED", "PAIR_REPLACED", "COMMITTED"})
MAX_LEAF_BYTES = 128
MAX_REPORT_KIND_BYTES = 128


@dataclass(frozen=True, slots=True)
class PairMember:
    """One desired public leaf and its durable recovery backup, if any."""

    name: str
    digest: str
    staged: str
    desired_device: int
    desired_inode: int
    backup: str | None
    backup_digest: str | None


def identity(
    kind: str,
    subject: str,
    json_name: str,
    markdown_name: str,
    json_bytes: bytes,
    markdown_bytes: bytes,
    *,
    directory_device: int,
    directory_inode: int,
) -> dict[str, object]:
    """Build a receipt bound to the opened output directory, not just its path."""

    return {
        "directory": {"device": directory_device, "inode": directory_inode},
        "json": {"digest": digest(json_bytes), "name": json_name},
        "markdown": {"digest": digest(markdown_bytes), "name": markdown_name},
        "report_kind": kind,
        "subject_commit_sha": subject,
    }


def journal_record(
    identity_value: dict[str, object], json_member: PairMember, markdown_member: PairMember, state: str
) -> dict[str, object]:
    if state not in PHASES:
        raise ValueError("invalid state")
    return {
        "identity": identity_value,
        "json": member_value(json_member),
        "markdown": member_value(markdown_member),
        "schema": SCHEMA,
        "state": state,
        "version": 1,
    }


def identity_receipt(identity_value: dict[str, object], *, outputs: dict[str, tuple[int, int]]) -> bytes:
    return encode({"identity": identity_value, "outputs": outputs, "schema": IDENTITY_SCHEMA, "version": 1})


def decode_identity_receipt(content: bytes) -> tuple[dict[str, object], dict[str, tuple[int, int]]]:
    value = json.loads(content)
    if (
        not isinstance(value, dict)
        or set(value) != {"identity", "outputs", "schema", "version"}
        or encode(value) != content
        or value["schema"] != IDENTITY_SCHEMA
        or value["version"] != 1
    ):
        raise ValueError("invalid identity receipt")
    validate_identity(value["identity"])
    outputs = value["outputs"]
    if not isinstance(outputs, dict) or set(outputs) != {"json", "markdown"}:
        raise ValueError("invalid output receipt")
    parsed: dict[str, tuple[int, int]] = {}
    for role in ("json", "markdown"):
        item = outputs[role]
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not all(isinstance(part, int) and part >= 0 for part in item)
        ):
            raise ValueError("invalid output receipt")
        parsed[role] = (item[0], item[1])
    return cast(dict[str, object], value["identity"]), parsed


def decode_journal(content: bytes) -> dict[str, object]:
    value = json.loads(content)
    if (
        not isinstance(value, dict)
        or set(value) != {"identity", "json", "markdown", "schema", "state", "version"}
        or encode(value) != content
        or value["schema"] != SCHEMA
        or value["version"] != 1
        or value["state"] not in PHASES
    ):
        raise ValueError("invalid journal")
    validate_identity(value["identity"])
    member(value["json"])
    member(value["markdown"])
    return value


def member(value: object) -> PairMember:
    if not isinstance(value, dict) or set(value) != {
        "backup",
        "backup_digest",
        "desired_device",
        "desired_inode",
        "digest",
        "name",
        "staged",
    }:
        raise ValueError("invalid member")
    name, value_digest, staged, desired_device, desired_inode, backup, backup_digest = (
        value["name"],
        value["digest"],
        value["staged"],
        value["desired_device"],
        value["desired_inode"],
        value["backup"],
        value["backup_digest"],
    )
    if not all(isinstance(item, str) for item in (name, value_digest, staged)):
        raise ValueError("invalid member")
    if not all(isinstance(item, int) and item >= 0 for item in (desired_device, desired_inode)):
        raise ValueError("invalid member identity")
    leaf(name)
    leaf(staged)
    digest_syntax(value_digest)
    if (backup is None) != (backup_digest is None):
        raise ValueError("invalid backup")
    if backup is not None:
        if not isinstance(backup, str) or not isinstance(backup_digest, str):
            raise ValueError("invalid backup")
        leaf(backup)
        digest_syntax(backup_digest)
    return PairMember(name, value_digest, staged, desired_device, desired_inode, backup, backup_digest)


def validate_inputs(
    json_name: str,
    markdown_name: str,
    json_bytes: bytes,
    markdown_bytes: bytes,
    kind: str,
    subject: str,
    *,
    max_bytes: int,
) -> None:
    leaf(json_name)
    leaf(markdown_name)
    if (
        json_name == markdown_name
        or not all(isinstance(value, bytes) and len(value) <= max_bytes for value in (json_bytes, markdown_bytes))
        or not isinstance(kind, str)
        or not kind
        or len(kind.encode("utf-8")) > MAX_REPORT_KIND_BYTES
        or not is_sha(subject)
    ):
        raise ValueError("invalid pair inputs")


def member_value(item: PairMember) -> dict[str, object]:
    return {
        "backup": item.backup,
        "backup_digest": item.backup_digest,
        "desired_device": item.desired_device,
        "desired_inode": item.desired_inode,
        "digest": item.digest,
        "name": item.name,
        "staged": item.staged,
    }


def validate_identity(value: object) -> None:
    if (
        not isinstance(value, dict)
        or set(value) != {"directory", "json", "markdown", "report_kind", "subject_commit_sha"}
        or not isinstance(value["report_kind"], str)
        or not value["report_kind"]
        or not is_sha(value["subject_commit_sha"])
    ):
        raise ValueError("invalid identity")
    directory = value["directory"]
    if (
        not isinstance(directory, dict)
        or set(directory) != {"device", "inode"}
        or not all(isinstance(directory[key], int) and directory[key] >= 0 for key in directory)
    ):
        raise ValueError("invalid directory identity")
    for role in ("json", "markdown"):
        entry = value[role]
        if not isinstance(entry, dict) or set(entry) != {"digest", "name"}:
            raise ValueError("invalid identity member")
        if not isinstance(entry["name"], str) or not isinstance(entry["digest"], str):
            raise ValueError("invalid identity member")
        leaf(entry["name"])
        digest_syntax(entry["digest"])


def leaf(name: str) -> None:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name or "\x00" in name:
        raise ValueError("invalid leaf")
    if len(name.encode("utf-8")) > MAX_LEAF_BYTES:
        raise ValueError("leaf exceeds readiness-pair bound")


def is_sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 40 and all(char in "0123456789abcdef" for char in value)


def digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def digest_syntax(value: str) -> str:
    if len(value) != 71 or not value.startswith("sha256:") or any(char not in "0123456789abcdef" for char in value[7:]):
        raise ValueError("invalid digest")
    return value


def encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
