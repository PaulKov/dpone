"""Shared redacted text rendering primitives for CLI adapters."""

from __future__ import annotations

import sys

from dpone.security_redaction import redact_text


def write_text(text: str) -> None:
    sys.stdout.write(redact_text(text))


def write_line(text: str = "") -> None:
    sys.stdout.write(redact_text(text) + "\n")


__all__ = ["write_line", "write_text"]
