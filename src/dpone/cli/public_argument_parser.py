"""Argparse boundary that never reflects secrets or workstation paths."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, NoReturn

from dpone.security_redaction import redact_absolute_paths, redact_text

_ERROR_FORMAT: ContextVar[str] = ContextVar("dpone_cli_error_format", default="text")
_NEGATIVE_NUMERIC_VALUE = re.compile(
    r"^-(?:(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?|inf(?:inity)?|nan)$",
    re.IGNORECASE,
)


class PublicArgumentParser(argparse.ArgumentParser):
    """Redact parser diagnostics before argparse writes them to stderr."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # argparse only recognizes negative integers and decimals as values.
        # Include scientific and non-finite spellings so the declared argument
        # type can reject them consistently instead of reporting a missing
        # value because the leading ``-`` looked like another option.
        self._negative_number_matcher = _NEGATIVE_NUMERIC_VALUE

    def error(self, message: str) -> NoReturn:
        safe_message = redact_absolute_paths(redact_text(message))
        if _ERROR_FORMAT.get() == "json":
            payload = {
                "passed": False,
                "errors": [
                    {
                        "schema": "dpone.error.v1",
                        "code": "DPONE_CLI_USAGE_INVALID",
                        "stage": "cli_parse",
                        "severity": "error",
                        "message": safe_message,
                        "fixes": [],
                    }
                ],
            }
            sys.stderr.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
            self.exit(2)
        super().error(safe_message)


@contextmanager
def public_error_format(argv: Sequence[str]) -> Iterator[None]:
    """Select structured parser diagnostics from the requested CLI format."""

    selected = (
        "json"
        if any(
            argv[index : index + 2] == ["--format", "json"] or argv[index] == "--format=json"
            for index in range(len(argv))
        )
        else "text"
    )
    token = _ERROR_FORMAT.set(selected)
    try:
        yield
    finally:
        _ERROR_FORMAT.reset(token)


__all__ = ["PublicArgumentParser", "public_error_format"]
