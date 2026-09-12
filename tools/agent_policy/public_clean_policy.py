"""Injected lexical privacy policy and scan-wide resource accounting.

Findings contain input identities solely for private receipts. Callers must render
only opaque ordinals and codes publicly; lexical absence never authorizes a commit.
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from tools.agent_policy import tenant_hygiene as limits
from tools.agent_policy.public_clean_limits import (
    MAX_FINDINGS,
    MAX_POLICY_ENTRIES,
    MAX_PRIVATE_JSON_BYTES,
    MAX_TICKET_POSITIONS,
)
from tools.agent_policy.public_clean_receipts import GateError, digest, strict_keys

_EMAIL = re.compile(r"(?<![\w.!#$%&'*+/=?^`{|}~-])[\w.!#$%&'*+/=?^`{|}~-]+@[\w-]+(?:\.[\w-]+)+")
_HOST = re.compile(r"(?<![\w@-])(?:[\w-]+\.)+[A-Za-z][A-Za-z0-9-]*(?![\w-])")
# Bare dotted tokens are ambiguous with source filenames. URL and explicit DNS
# contexts are always checked; this conservative TLD set covers bare DNS signals.
_DNS_SUFFIXES = frozenset(
    "com org net edu gov mil int io dev app cloud ai co ru uk de fr us ca au local internal test example invalid".split()
)
_URL_HOST = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://(?:[^\s/@]+@)?(\[[^\]\s]+\]|[^\s/:?#<>\"']+)")
_DNS_CONTEXT = re.compile(r"(?i)\b(?:host(?:name)?|server|endpoint|domain)[\"']?\s*[:=]\s*[\"']?([\w.-]+)")
_IP = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
_IPV6 = re.compile(r"(?i)(?<![\w:.])[0-9a-f:.]*:[0-9a-f:.]+(?![\w:.])")
_PRIVATE = tuple(
    ipaddress.ip_network(value)
    for value in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "fc00::/7",
        "fe80::/10",
        "::1/128",
    )
)
_RULES = (
    (
        "CREDENTIAL_SHAPE",
        re.compile(r"(?i)(?:password|passwd|secret|token|api[_-]?key)[\"']?\s*[:=]\s*[\"']?[^\s\"',;}]{4,}"),
    ),
    (
        "CREDENTIAL_SHAPE",
        re.compile(r"-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----|\b(?:gh[pousr]_[A-Za-z0-9]{20,}|AKIA[A-Z0-9]{16})\b"),
    ),
    ("CREDENTIAL_SHAPE", re.compile(r"(?<![a-zA-Z0-9+.-])[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/:]+:[^\s/@]+@")),
    (
        "LOCAL_PATH",
        re.compile(
            r"(?<![\w:])/(?:Users|home|private|tmp|var|opt|mnt|Volumes|workspace|etc|srv|root)/[^\s\"'<>]+|(?<![\w])[A-Za-z]:[\\/][^\s\"'<>]+|\\\\[^\s\\]+\\[^\s]+"
        ),
    ),
    ("TICKET_IDENTIFIER", re.compile(r"\b[A-Z][A-Z0-9]{1,15}-[0-9]+\b")),
    (
        "DATABASE_IDENTIFIER",
        re.compile(
            r"(?i)\b(?:database|schema|table|db)(?:[_-]?name)?\s*[:=]\s*[\"'\[]?[\w.-]+|\b(?:from|join|into|update|use|create\s+(?:table|schema|database))\s+[\"'\[]?[\w]+\.[\w]+"
        ),
    ),
    (
        "CLOUD_IDENTIFIER",
        re.compile(
            r"(?i)\b(?:project(?:[_-]?id)?|namespace|subscription[_-]?id|tenant[_-]?id)[\"']?\s*[:=]\s*[\"']?[\w.-]+"
        ),
    ),
    (
        "APPLICATION_IDENTIFIER",
        re.compile(
            r"(?i)\b(?:application[_-]?id|app[_-]?id|bundle[_-]?(?:id|identifier)|package[_-]?name)[\"']?\s*[:=]\s*[\"']?[\w.-]+"
        ),
    ),
)


def _fold(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKC", value).casefold()
        if unicodedata.category(char)[0] not in {"P", "Z"} and unicodedata.category(char) != "Cf"
    )


@dataclass(frozen=True)
class Policy:
    """Validated private policy; never serialize this to public output."""

    digest: str
    protected_terms: tuple[str, ...] = field(repr=False)
    protected_ticket_prefixes: tuple[str, ...] = field(repr=False)
    allowed_hosts: tuple[str, ...] = field(repr=False)
    allowed_identities: tuple[str, ...] = field(repr=False)
    reviewers: tuple[str, ...] = field(repr=False)


def load_policy(payload: dict[str, Any]) -> Policy:
    """Validate a closed schema without silently repairing private policy."""
    keys = {"protected_terms", "protected_ticket_prefixes", "allowed_hosts", "allowed_identities", "reviewers"}
    strict_keys(payload, keys | {"schema"})
    if payload["schema"] != "dpone.public-clean-policy.v1":
        raise GateError("POLICY_INVALID")
    parsed: dict[str, tuple[str, ...]] = {}
    for key in sorted(keys):
        values = payload[key]
        if not isinstance(values, list) or len(values) > MAX_POLICY_ENTRIES:
            raise GateError("POLICY_INVALID")
        if any(
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or any(unicodedata.category(char)[0] == "C" for char in value)
            for value in values
        ):
            raise GateError("POLICY_INVALID")
        if len(set(values)) != len(values):
            raise GateError("POLICY_INVALID")
        parsed[key] = tuple(values)
    if any(not parsed[key] for key in ("protected_terms", "allowed_hosts", "allowed_identities", "reviewers")):
        raise GateError("POLICY_INVALID")
    if any(not _fold(value) for key in ("protected_terms", "protected_ticket_prefixes") for value in parsed[key]):
        raise GateError("POLICY_INVALID")
    if any(_EMAIL.fullmatch(value) is None for value in parsed["allowed_identities"]):
        raise GateError("POLICY_INVALID")
    if any(_HOST.fullmatch(value) is None or value != value.lower() for value in parsed["allowed_hosts"]):
        raise GateError("POLICY_INVALID")
    return Policy(digest(payload), **parsed)


@dataclass
class Budget:
    """One budget shared by acquisition, expansion and lexical inspection."""

    items: int = 0
    bytes: int = 0
    archive_members: int = 0
    archive_bytes: int = 0
    archives: int = 0
    deadline: float = field(default_factory=lambda: time.monotonic() + 900)

    def tick(self) -> None:
        if time.monotonic() > self.deadline:
            raise GateError("SCAN_TIMEOUT")

    def charge(self, size: int) -> None:
        self.tick()
        self.items += 1
        self.bytes += size
        if size < 0 or size > limits.MAX_SOURCE_BLOB_BYTES or self.bytes > limits.MAX_SOURCE_AGGREGATE_BYTES:
            raise GateError("SOURCE_BYTES_LIMIT")

    def member(self, size: int) -> None:
        self.tick()
        self.archive_members += 1
        self.archive_bytes += size
        if (
            size < 0
            or size > limits.MAX_ARCHIVE_MEMBER_BYTES
            or self.archive_members > limits.MAX_ARCHIVE_MEMBERS
            or self.archive_bytes > limits.MAX_ARCHIVE_UNCOMPRESSED_BYTES
        ):
            raise GateError("ARCHIVE_EXPANSION_LIMIT")


@dataclass(frozen=True)
class Finding:
    code: str
    label: str
    offset: int
    content_digest: str


class Scanner:
    """Evaluate protected and generic rules; no findings are waived here."""

    def __init__(self, policy: Policy, budget: Budget) -> None:
        self.policy, self.budget = policy, budget
        self.findings: list[Finding] = []
        self._protected = [
            (
                code,
                re.compile(
                    "|".join(
                        re.escape(value) for value in sorted({_fold(term) for term in terms}, key=len, reverse=True)
                    )
                ),
            )
            for code, terms in (("PROTECTED_TERM", policy.protected_terms),)
            if terms
        ]

        prefixes = sorted({_fold(term) for term in policy.protected_ticket_prefixes}, key=len, reverse=True)
        self._tickets = re.compile("(?:" + "|".join(map(re.escape, prefixes)) + r")\d+") if prefixes else None

    def _scan_tickets(self, label: str, text: str, normalized: str, identity: str) -> None:
        if self._tickets is None:
            return
        positions: set[int] = set()
        for match in self._tickets.finditer(normalized):
            if len(positions) >= MAX_TICKET_POSITIONS:
                raise GateError("FINDINGS_LIMIT")
            positions.add(match.start())
        unfolded = unicodedata.normalize("NFKC", text).casefold()
        offset = 0
        for index, char in enumerate(unfolded):
            if unicodedata.category(char)[0] in {"P", "Z"} or unicodedata.category(char) == "Cf":
                continue
            if offset in positions and (index == 0 or not unfolded[index - 1].isalnum()):
                self.add("PROTECTED_TICKET", label, offset, identity)
            offset += 1

    def add(self, code: str, label: str, offset: int = 0, content_digest: str = "") -> None:
        self.budget.tick()
        if len(self.findings) >= MAX_FINDINGS:
            raise GateError("FINDINGS_LIMIT")
        self.findings.append(Finding(code, label, offset, content_digest))

    def scan(self, label: str, raw: bytes, *, textual: bool = True) -> None:
        self.budget.charge(len(raw))
        try:
            text = raw.decode("utf-8", errors="strict" if textual else "replace")
        except UnicodeError as exc:
            raise GateError("CONTENT_ENCODING_UNSUPPORTED") from exc
        if textual and "\0" in text:
            raise GateError("CONTENT_ENCODING_UNSUPPORTED")
        normalized = _fold(text)
        if len(normalized.encode("utf-8")) > 4 * limits.MAX_SOURCE_BLOB_BYTES:
            raise GateError("NORMALIZED_TEXT_LIMIT")
        identity = hashlib.sha256(raw).hexdigest()
        for code, pattern in self._protected:
            self.budget.tick()
            for match in pattern.finditer(normalized):
                self.add(code, label, match.start(), identity)
        self._scan_tickets(label, text, normalized, identity)
        for code, pattern in _RULES:
            self.budget.tick()
            for match in pattern.finditer(text):
                self.add(code, label, match.start(), identity)
        for pattern in (_IP, _IPV6):
            for match in pattern.finditer(text):
                try:
                    address = ipaddress.ip_address(match.group())
                except ValueError:
                    continue
                if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
                    address = address.ipv4_mapped
                if any(address in network for network in _PRIVATE):
                    self.add("PRIVATE_ADDRESS", label, match.start(), identity)
        for match in _EMAIL.finditer(text):
            if match.group() not in self.policy.allowed_identities:
                self.add("UNAPPROVED_IDENTITY", label, match.start(), identity)
        reported_host_offsets: set[int] = set()
        for pattern, group in ((_HOST, 0), (_URL_HOST, 1), (_DNS_CONTEXT, 1)):
            for match in pattern.finditer(text):
                host = match.group(group).casefold()
                if pattern is _HOST and host.rsplit(".", 1)[-1] not in _DNS_SUFFIXES:
                    continue
                offset = match.start(group)
                if offset not in reported_host_offsets and host not in self.policy.allowed_hosts:
                    self.add("UNAPPROVED_HOST", label, offset, identity)
                    # Matchers share the receipt's occurrence identity. An
                    # allowlisted shorter match never suppresses a later one.
                    reported_host_offsets.add(offset)


def validate_receipt_credentials(raw: bytes, budget: Budget) -> None:
    """Inspect serialized private locators/prose without source-blob accounting.

    Reuse the source credential rules only; generic source signals in a receipt
    are evidence, not additional source occurrences. The newline is part of the
    publication byte budget. Caller retains the scan-wide deadline.
    """
    if len(raw) + 1 > MAX_PRIVATE_JSON_BYTES:
        raise GateError("RECEIPT_LIMIT")
    text = raw.decode("utf-8")
    for code, pattern in _RULES:
        budget.tick()
        if code == "CREDENTIAL_SHAPE" and pattern.search(text):
            raise GateError("RECEIPT_SENSITIVE")
    budget.tick()
