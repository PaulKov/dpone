"""Canonical checksum-subject encoding shared by producers and transport readers."""

from collections.abc import Iterable


def encode_tree_integrity_subject(header: str, entries: Iterable[tuple[str, str]]) -> bytes:
    """Encode already validated path/digest pairs in canonical path order.

    The caller owns confinement, bounds and digest validation. This function
    creates no evidence assertion and performs no I/O.
    """
    lines = [header, *(f"{digest}  {path}" for path, digest in sorted(entries))]
    return ("\n".join(lines) + "\n").encode("utf-8")
