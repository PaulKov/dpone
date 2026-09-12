"""Bounded, independent assembler for inspectable historical wire test fixtures.

This is test infrastructure, not a legacy production decoder. Binary values
must expose a nested frame or a documented synthetic/hash construction.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from uuid import UUID

_MAX_BYTES = 262144
_MAX_NODES = 4096
_MAX_DEPTH = 24


def _keys(node, *names):
    if type(node) is not dict or set(node) != {"tag", *names}:
        raise ValueError("fixture node has unknown or missing fields")


def _text(value):
    if type(value) is not str or len(value.encode("utf-8")) > 8192 or unicodedata.normalize("NFC", value) != value:
        raise ValueError("fixture text must be bounded NFC")
    return value


def _pack(value):
    if len(value) > _MAX_BYTES:
        raise ValueError("fixture output exceeds limit")
    return len(value).to_bytes(4, "big") + value


def assemble_fixture(node):
    """Assemble exactly one closed tagged frame without importing domain codecs."""
    count = 0

    def encode(value, depth):
        nonlocal count
        count += 1
        if count > _MAX_NODES or depth > _MAX_DEPTH:
            raise ValueError("fixture complexity exceeds limit")
        if type(value) is not dict or type(value.get("tag")) is not str:
            raise ValueError("fixture requires tagged objects")
        tag = value["tag"]
        if tag in {"tuple", "frame"}:
            _keys(value, *(("domain", "items") if tag == "frame" else ("items",)))
            if type(value["items"]) is not list or len(value["items"]) > _MAX_NODES:
                raise ValueError("fixture items violate list type or count limit")
            prefix = b"q"
            if tag == "frame":
                domain = _text(value["domain"])
                if re.fullmatch(r"dpone-[a-z0-9.-]+(?:\x00[a-z]+)*", domain) is None:
                    raise ValueError("fixture domain is invalid")
                prefix = domain.encode() + b"\0"
            payload = prefix + b"".join(_pack(encode(item, depth + 1)) for item in value["items"])
            if len(payload) > _MAX_BYTES:
                raise ValueError("fixture output exceeds limit")
            if tag == "frame" and depth > 0:
                return b"b" + len(payload).to_bytes(8, "big") + payload
            return payload
        if tag == "null":
            _keys(value)
            return b"n"
        if tag in {"text", "uuid", "int", "bool"}:
            _keys(value, "value")
            raw = value["value"]
            if tag == "text":
                raw = _text(raw).encode()
                return b"s" + len(raw).to_bytes(8, "big") + raw
            if tag == "uuid":
                raw = _text(raw)
                parsed = UUID(raw)
                if str(parsed) != raw:
                    raise ValueError("UUID must use canonical text")
                return b"u" + parsed.bytes
            if tag == "bool" and type(raw) is bool:
                return b"t" if raw else b"f"
            if tag == "int" and type(raw) is int and -(2**63) <= raw < 2**63:
                return b"i" + raw.to_bytes(8, "big", signed=True)
            raise ValueError("fixture scalar type is invalid")
        if tag == "synthetic_digest":
            _keys(value, "role", "byte")
            if type(value["role"]) is not str or value["role"] not in {"D1", "D2"} or type(value["byte"]) is not int:
                raise ValueError("synthetic digest declaration is invalid")
            if value["byte"] != {"D1": 17, "D2": 34}[value["role"]]:
                raise ValueError("synthetic digest byte differs from declaration")
            raw = bytes([value["byte"]]) * 32
        elif tag == "sha256_text":
            _keys(value, "role", "text")
            _text(value["role"])
            raw = hashlib.sha256(_text(value["text"]).encode()).digest()
        elif tag == "sha256_frame":
            _keys(value, "role", "frame")
            _text(value["role"])
            if type(value["frame"]) is not dict or value["frame"].get("tag") != "frame":
                raise ValueError("hash preimage must be a readable frame")
            encoded = encode(value["frame"], depth + 1)
            raw = hashlib.sha256(encoded[9:]).digest()
        else:
            raise ValueError("unknown fixture tag")
        return b"b" + len(raw).to_bytes(8, "big") + raw

    if type(node) is not dict or node.get("tag") != "frame":
        raise ValueError("fixture root must be a frame")
    return encode(node, 0)


def load_fixture(path: Path) -> bytes:
    """Reject duplicate JSON keys, noncanonical representation and excess input."""
    if path.is_symlink() or not path.is_file() or path.stat().st_size > _MAX_BYTES:
        raise ValueError("fixture input must be a bounded regular file")
    with path.open("rb") as stream:
        raw = stream.read(_MAX_BYTES + 1)
    if len(raw) > _MAX_BYTES:
        raise ValueError("fixture input exceeds limit")

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate fixture key")
            value[key] = item
        return value

    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("invalid fixture document") from exc
    expected = (json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    if raw != expected:
        raise ValueError("fixture JSON must use canonical text order and formatting")
    return assemble_fixture(document)
