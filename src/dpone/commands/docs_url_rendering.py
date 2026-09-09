"""Convert stable repository docs references into usable CLI links."""

from __future__ import annotations

import re

_ERROR_DOCS = re.compile(r"docs/errors/(?P<code>DPONE_[A-Z0-9_]+)\.md")
_PUBLIC_DOCS_ROOT = "https://paulkov.github.io/dpone"


def public_docs_url(value: str) -> str:
    """Preserve JSON identity while making known error links clickable in text."""

    match = _ERROR_DOCS.fullmatch(value)
    if match is None:
        return value
    return f"{_PUBLIC_DOCS_ROOT}/errors/{match.group('code')}/"


__all__ = ["public_docs_url"]
