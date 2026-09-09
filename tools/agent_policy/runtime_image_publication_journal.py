"""Validate publication receipts and durably journal alias transitions."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import uuid4

JOURNAL_SCHEMA = "dpone.runtime-image-publication-journal.v1"
MAX_JOURNAL_BYTES = 32_768
CANONICAL_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
STABLE_SEMVER = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
REFERENCE = re.compile(
    r"^ghcr\.io/[a-z0-9]+(?:[._/-][a-z0-9]+)*:"
    r"[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$"
)
ROLE_ORDER = ("version", "sha", "latest")
DECISIONS = frozenset({"CREATED", "NOOP_SAME", "ADVANCED", "PRESERVED_NEWER", "BLOCKED"})
WRITE_DECISIONS = frozenset({"CREATED", "ADVANCED"})


class PublicationJournalError(OSError):
    """Bounded publication-journal failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def journal_path_for(receipt_path: Path) -> Path:
    """Derive the attempt-scoped repair journal next to the final receipt."""

    suffix = receipt_path.suffix
    stem = receipt_path.name[: -len(suffix)] if suffix else receipt_path.name
    return receipt_path.with_name(f"{stem}.journal{suffix}")


def _journal_error() -> PublicationJournalError:
    return PublicationJournalError("PUBLICATION_JOURNAL_INVALID")


def _digest(value: Any) -> str:
    if not isinstance(value, str) or CANONICAL_DIGEST.fullmatch(value) is None:
        raise _journal_error()
    return value


def _observation(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        raise _journal_error()
    state = value.get("state")
    if state == "ABSENT" and value.keys() == {"state"}:
        return {"state": state}
    if state == "ERROR" and value.keys() == {"state", "error_code"}:
        code = value["error_code"]
        if isinstance(code, str) and SAFE_CODE.fullmatch(code):
            return {"state": state, "error_code": code}
    if state == "PRESENT" and value.keys() in ({"state", "digest"}, {"state", "digest", "version"}):
        result = {"state": state, "digest": _digest(value["digest"])}
        if "version" in value:
            version = value["version"]
            if not isinstance(version, str) or STABLE_SEMVER.fullmatch(version) is None:
                raise _journal_error()
            result["version"] = version
        return result
    raise _journal_error()


def _transition(value: Any, *, expected_role: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise _journal_error()
    required = {"role", "reference", "before", "decision", "after", "status"}
    optional = {"blocker_code"}
    if not required <= value.keys() or not value.keys() <= required | optional:
        raise _journal_error()
    role = value["role"]
    reference = value["reference"]
    decision = value["decision"]
    status = value["status"]
    if (
        role != expected_role
        or not isinstance(reference, str)
        or REFERENCE.fullmatch(reference) is None
        or decision not in DECISIONS
        or status not in {"PASS", "FAIL"}
        or (status == "FAIL") != ("blocker_code" in value)
    ):
        raise _journal_error()
    result = {
        "role": role,
        "reference": reference,
        "before": _observation(value["before"]),
        "decision": decision,
        "after": _observation(value["after"]),
        "status": status,
    }
    if "blocker_code" in value:
        blocker = value["blocker_code"]
        if not isinstance(blocker, str) or SAFE_CODE.fullmatch(blocker) is None:
            raise _journal_error()
        result["blocker_code"] = blocker
    return result


def _sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_bytes(descriptor: int, content: bytes) -> None:
    offset = 0
    while offset < len(content):
        written = os.write(descriptor, content[offset:])
        if written <= 0:
            raise PublicationJournalError("PUBLICATION_JOURNAL_WRITE_FAILED")
        offset += written


class PublicationJournal:
    """Fsync each bounded repair snapshot before promotion may continue."""

    def __init__(
        self,
        *,
        path: Path,
        candidate_digest: str,
        certification_sha256: str,
    ) -> None:
        if path.exists() or path.is_symlink():
            raise PublicationJournalError("PUBLICATION_JOURNAL_EXISTS")
        self.path = path
        self._created = False
        self._snapshot: dict[str, Any] = {
            "schema": JOURNAL_SCHEMA,
            "state": "IN_PROGRESS",
            "candidate_digest": _digest(candidate_digest),
            "certification_sha256": _digest(certification_sha256),
            "pending_mutation": None,
            "transitions": [],
            "terminal": None,
        }

    def _persist(self, snapshot: dict[str, Any]) -> None:
        content = (json.dumps(snapshot, indent=2, sort_keys=True) + "\n").encode()
        if not content or len(content) > MAX_JOURNAL_BYTES:
            raise PublicationJournalError("PUBLICATION_JOURNAL_TOO_LARGE")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            _write_bytes(descriptor, content)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            if self._created:
                if self.path.is_symlink() or not self.path.is_file():
                    raise PublicationJournalError("PUBLICATION_JOURNAL_INVALID")
                os.replace(temporary, self.path)
            else:
                os.link(temporary, self.path, follow_symlinks=False)
                self._created = True
            _sync_directory(self.path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def _commit(self, snapshot: dict[str, Any]) -> None:
        self._persist(snapshot)
        self._snapshot = snapshot

    def prepare_mutation(
        self,
        *,
        role: str,
        reference: str,
        expected_digest: str,
        before: dict[str, Any],
        decision: str,
    ) -> None:
        """Persist repair intent before invoking a non-transactional alias write."""

        expected_role = ROLE_ORDER[len(self._snapshot["transitions"])]
        if (
            self._snapshot["state"] == "COMPLETE"
            or self._snapshot["pending_mutation"] is not None
            or role != expected_role
            or not isinstance(reference, str)
            or REFERENCE.fullmatch(reference) is None
            or decision not in WRITE_DECISIONS
        ):
            raise _journal_error()
        pending = {
            "role": role,
            "reference": reference,
            "expected_digest": _digest(expected_digest),
            "before": _observation(before),
            "decision": decision,
        }
        snapshot = deepcopy(self._snapshot)
        snapshot.update(state="MUTATION_PENDING", pending_mutation=pending)
        self._commit(snapshot)

    def record_transition(self, transition: dict[str, Any]) -> None:
        """Persist the observed transition before the next alias is inspected."""

        expected_role = ROLE_ORDER[len(self._snapshot["transitions"])]
        normalized = _transition(transition, expected_role=expected_role)
        pending = self._snapshot["pending_mutation"]
        if normalized["decision"] in WRITE_DECISIONS:
            if (
                type(pending) is not dict
                or pending["role"] != normalized["role"]
                or pending["reference"] != normalized["reference"]
                or pending["decision"] != normalized["decision"]
            ):
                raise _journal_error()
        elif pending is not None and (
            normalized["decision"] != "BLOCKED"
            or normalized["status"] != "FAIL"
            or pending["role"] != normalized["role"]
            or pending["reference"] != normalized["reference"]
        ):
            raise _journal_error()
        snapshot = deepcopy(self._snapshot)
        snapshot["state"] = "IN_PROGRESS"
        snapshot["pending_mutation"] = None
        snapshot["transitions"].append(normalized)
        self._commit(snapshot)

    def complete(self, receipt: Any) -> None:
        """Fsync terminal state before a canonical receipt may be exposed."""

        payload = receipt.to_payload()
        if (
            self._snapshot["state"] != "IN_PROGRESS"
            or self._snapshot["pending_mutation"] is not None
            or payload.get("candidate_digest") != self._snapshot["candidate_digest"]
            or payload.get("certification_sha256") != self._snapshot["certification_sha256"]
            or payload.get("transitions") != self._snapshot["transitions"]
            or receipt.status not in {"PASS", "FAIL"}
            or receipt.outcome not in {"PUBLISHED", "BLOCKED", "PARTIAL_ALIAS_PENDING"}
        ):
            raise _journal_error()
        snapshot = deepcopy(self._snapshot)
        snapshot["state"] = "COMPLETE"
        snapshot["terminal"] = {"status": receipt.status, "outcome": receipt.outcome}
        self._commit(snapshot)


def publication_from_completed_journal(
    payload: Any,
    *,
    candidate_digest: str,
    certification_sha256: str,
    policy: Any,
) -> dict[str, Any]:
    """Validate terminal journal state and reconstruct publication evidence."""

    fields = {
        "schema",
        "state",
        "candidate_digest",
        "certification_sha256",
        "pending_mutation",
        "transitions",
        "terminal",
    }
    if type(payload) is not dict or payload.keys() != fields:
        raise _journal_error()
    if payload["state"] != "COMPLETE":
        raise PublicationJournalError("PUBLICATION_REPAIR_REQUIRED")
    terminal = payload["terminal"]
    if (
        payload["schema"] != JOURNAL_SCHEMA
        or payload["pending_mutation"] is not None
        or payload["candidate_digest"] != _digest(candidate_digest)
        or payload["certification_sha256"] != _digest(certification_sha256)
        or type(terminal) is not dict
        or terminal.keys() != {"status", "outcome"}
    ):
        raise _journal_error()
    transitions = payload["transitions"]
    if not isinstance(transitions, list) or not 1 <= len(transitions) <= len(ROLE_ORDER):
        raise _journal_error()
    normalized = [
        _transition(transition, expected_role=ROLE_ORDER[index]) for index, transition in enumerate(transitions)
    ]
    return {
        "schema": policy.PUBLICATION_SCHEMA,
        "status": terminal["status"],
        "outcome": terminal["outcome"],
        "candidate_digest": candidate_digest,
        "certification_sha256": certification_sha256,
        "transitions": normalized,
    }


def _validate_observation(policy: Any, value: Any) -> dict[str, Any]:
    state = value.get("state") if type(value) is dict else None
    optional = ("version",) if state == "PRESENT" else ()
    if state == "ERROR":
        result = policy._strict(value, ("state", "error_code"))
        policy._text(result["error_code"], policy.SAFE_CODE)
    elif state == "PRESENT":
        result = policy._strict(value, ("state", "digest"), optional)
        policy._text(result["digest"], policy.CANONICAL_DIGEST)
        if "version" in result:
            policy._version(result["version"])
    elif state == "ABSENT":
        result = policy._strict(value, ("state",))
    else:
        raise policy.ContractError("PUBLICATION_OBSERVATION_INVALID")
    return result


def validate_publication(
    payload: Any,
    *,
    policy: Any,
    trusted_latest_certification: Any = None,
) -> Any:
    """Validate truthful per-alias publication evidence."""

    root = policy._strict(
        payload,
        ("schema", "status", "outcome", "candidate_digest", "certification_sha256", "transitions"),
    )
    if root["schema"] != policy.PUBLICATION_SCHEMA:
        raise policy.ContractError("PUBLICATION_SCHEMA_INVALID")
    digest = policy._text(root["candidate_digest"], policy.CANONICAL_DIGEST)
    policy._text(root["certification_sha256"], policy.CANONICAL_DIGEST)
    transitions = root["transitions"]
    if not isinstance(transitions, list) or not 1 <= len(transitions) <= 3:
        raise policy.ContractError("PUBLICATION_TRANSITIONS_INVALID")
    roles: list[str] = []
    for item in transitions:
        transition = policy._strict(
            item,
            ("role", "reference", "before", "decision", "after", "status"),
            ("blocker_code",),
        )
        role, decision, status = transition["role"], transition["decision"], transition["status"]
        if role not in ROLE_ORDER or decision not in {item.value for item in policy.AliasDecision}:
            raise policy.ContractError("PUBLICATION_TRANSITION_INVALID")
        policy._text(transition["reference"])
        _validate_observation(policy, transition["before"])
        after = _validate_observation(policy, transition["after"])
        if status not in ("PASS", "FAIL") or (status == "FAIL") != ("blocker_code" in transition):
            raise policy.ContractError("PUBLICATION_TRANSITION_INVALID")
        if "blocker_code" in transition:
            policy._text(transition["blocker_code"], policy.SAFE_CODE)
        roles.append(role)
        if status == "PASS" and (decision == "BLOCKED" or after.get("state") != "PRESENT"):
            raise policy.ContractError("PUBLICATION_FALSE_PASS")
    if roles != list(ROLE_ORDER[: len(roles)]):
        raise policy.ContractError("PUBLICATION_ORDER_INVALID")
    try:
        image, version = transitions[0]["reference"].rsplit(":", 1)
    except ValueError:
        raise policy.ContractError("PUBLICATION_REFERENCE_INVALID") from None
    policy._text(image, policy.IMAGE_NAME)
    policy._version(version)
    if (
        len(transitions) > 1
        and re.fullmatch(rf"{re.escape(image)}:sha-[0-9a-f]{{12}}", transitions[1]["reference"]) is None
    ):
        raise policy.ContractError("PUBLICATION_REFERENCE_INVALID")
    if len(transitions) > 2 and transitions[2]["reference"] != f"{image}:latest":
        raise policy.ContractError("PUBLICATION_REFERENCE_INVALID")
    policy._validate_publication_semantics(
        root,
        digest=digest,
        image=image,
        version=version,
        roles=roles,
        trusted_latest_certification=trusted_latest_certification,
    )
    return policy.PublicationReceipt(deepcopy(root), str(root["status"]), str(root["outcome"]))
