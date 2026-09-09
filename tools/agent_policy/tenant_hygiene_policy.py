"""Closed policy matching; tenant values are supplied only by the caller."""

from __future__ import annotations

import ipaddress
import json
import re
import unicodedata
from dataclasses import dataclass, field
from typing import BinaryIO

SCHEMA = "dpone.tenant-hygiene-policy.v2"
HEADER = "#!" + SCHEMA + "\n"
SEPARATORS = " _-./\\\t\r\n"
_IP = re.compile(r"(?<![\w.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![\w.])")
_NETWORKS = tuple(ipaddress.IPv4Network(value) for value in ((0x0A000000, 8), (0xAC100000, 12), (0xC0A80000, 16)))


def _canonical(value: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFC", value).casefold() if char not in SEPARATORS)


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > 1024:
        raise ValueError
    if any(not isinstance(item, str) or not item or len(item) > 256 for item in value):
        raise ValueError
    return tuple(dict.fromkeys(value))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


class StreamPolicy:
    """Bounded content matching shared by the two policy versions."""

    overlap: int
    full_member: bool

    def matches(self, value: bytes) -> bool:
        raise NotImplementedError

    def scan(self, stream: BinaryIO, size: int, chunk_size: int) -> tuple[bool, bytes]:
        read, tail, prefix, matched = 0, b"", b"", False
        body = bytearray()
        while chunk := stream.read(chunk_size):
            read += len(chunk)
            if read > size:
                raise ValueError
            candidate = tail + chunk
            if self.full_member:
                body.extend(chunk)
            else:
                matched = matched or self.matches(candidate)
            tail = candidate[-self.overlap :] if self.overlap else b""
            prefix = (prefix + chunk)[:512]
        if read != size:
            raise ValueError
        return (self.matches(bytes(body)) if self.full_member else matched), prefix


@dataclass(frozen=True)
class LiteralPolicy(StreamPolicy):
    matcher: re.Pattern[bytes] = field(repr=False)
    overlap: int = field(repr=False)
    full_member: bool = False

    def matches(self, value: bytes) -> bool:
        return self.matcher.search(value) is not None


@dataclass(frozen=True)
class NormalizedPolicy(StreamPolicy):
    matcher: re.Pattern[str] = field(repr=False)
    jira: re.Pattern[str] | None = field(repr=False)
    private_addresses: bool = field(repr=False)
    overlap: int = 0
    full_member: bool = True

    def matches(self, value: bytes) -> bool:
        text = value.decode("utf-8", "surrogateescape")
        if self.matcher.search(_canonical(text)) is not None:
            return True
        if self.jira is not None and self.jira.search(text.casefold()) is not None:
            return True
        if self.private_addresses:
            for match in _IP.finditer(text):
                try:
                    address = ipaddress.IPv4Address(match.group())
                except ipaddress.AddressValueError:
                    continue
                if any(address in network for network in _NETWORKS):
                    return True
        return False


def parse_policy(payload: bytes) -> LiteralPolicy | NormalizedPolicy:
    """Parse bounded input; callers must never render policy exceptions."""
    text = payload.decode("utf-8")
    if not text.startswith("#!dpone.tenant-hygiene-policy."):
        values = tuple(dict.fromkeys(line.encode() for line in text.splitlines() if line))
        if not values:
            raise ValueError
        return LiteralPolicy(re.compile(b"|".join(re.escape(item) for item in values)), max(map(len, values)) - 1)
    if not text.startswith(HEADER):
        raise ValueError
    value = json.loads(text[len(HEADER) :], object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise ValueError
    if set(value) != {"schema", "terms", "jira_prefixes", "detectors"} or value["schema"] != SCHEMA:
        raise ValueError
    terms = tuple(_canonical(term) for term in _strings(value["terms"]))
    if not terms or any(not term or any(not char.isalnum() for char in term) for term in terms):
        raise ValueError
    prefixes = _strings(value["jira_prefixes"])
    if any(not prefix.isascii() or not prefix.isalpha() for prefix in prefixes):
        raise ValueError
    detectors = _strings(value["detectors"])
    if set(detectors) - {"rfc1918"}:
        raise ValueError
    separator = "[" + re.escape(SEPARATORS) + "]"
    jira = None
    if prefixes:
        patterns = [f"{separator}*".join(re.escape(char) for char in prefix.casefold()) for prefix in prefixes]
        jira = re.compile(r"(?<!\w)(?:" + "|".join(patterns) + ")" + separator + r"+[0-9]+(?!\w)")
    return NormalizedPolicy(re.compile("|".join(re.escape(term) for term in terms)), jira, "rfc1918" in detectors)
