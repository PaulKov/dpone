"""Derive the redundant parent checksum subject from its already bound artifacts."""

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.tree_integrity_subject import encode_tree_integrity_subject

COMPOSITION_SUBJECT = "release-subjects.sha256"


def composition_subject_bytes(release: Mapping[str, Any], release_payload: bytes) -> bytes:
    """Reproduce producer bytes without introducing a circular release identity.

    The subject covers the exact descriptor bytes and all parent-bound files. It
    is transported with a derived digest, never treated as a signature or as
    evidence that the source admission algorithm ran.
    """
    entries = [("release-set.json", sha256_bytes(release_payload))]
    entries.extend((row["path"], row["sha256"]) for rows in release["artifacts"].values() for row in rows)
    return encode_tree_integrity_subject(
        "# dpone.dbt-release-subjects.v1", ((path, digest.removeprefix("sha256:")) for path, digest in entries)
    )


def composition_subject_sha256(release: Mapping[str, Any], release_payload: bytes) -> str:
    """Derive the transport pin for the exact redundant checksum subject."""
    return sha256_bytes(composition_subject_bytes(release, release_payload))
