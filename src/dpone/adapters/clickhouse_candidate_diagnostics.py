"""Redacted status export; no SQL, source data, credentials or import capability."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from dpone.contracts.clickhouse_authority import AuthorityError
from dpone.contracts.clickhouse_candidate import CandidateStatus


def status_diagnostics(status: CandidateStatus) -> dict[str, object]:
    """Report original metadata, never imply a fresh observation on readback."""
    return {
        "schema_version": "dpone.clickhouse.protected-publication-status.v1",
        "authority_version": "dpone.clickhouse.authority.v2",
        "binding": asdict(status.binding),
        "revision": status.revision,
        "lifecycle": status.lifecycle,
        "admission_closed": status.admission_closed,
        "source_exhausted": status.source_exhausted,
        "accepted_requests": len(status.accepted),
        "accepted_query_ids": status.accepted,
        "succeeded_requests": len(status.succeeded),
        "succeeded_query_ids": status.succeeded,
        "uncertain_requests": len(status.uncertain),
        "uncertain_query_ids": status.uncertain,
        "expected_rows": status.expected.count,
        "expected_encoded_bytes": status.expected.encoded_bytes,
        "profile_digest": status.profile_digest,
        "sealed": status.seal is not None,
        "publication_state": status.publication_state.value if status.publication_state else None,
        "phase": "publication" if status.publication_state else "prepublication",
        "reason": status.retained_reason,
        "safe_to_retry": False,
        "owner_retained": True,
        "observation_kind": "historical" if status.publication_state else "not_observed",
    }


def write_status(payload: dict[str, object], destination: Path, authority_directory: Path) -> None:
    """Create once atomically outside private storage; do not replace a report."""
    temporary = None
    try:
        if destination.parent.resolve() == authority_directory.resolve():
            raise AuthorityError("Write diagnostics outside the authority directory")
        encoded = json.dumps(payload, sort_keys=True, indent=2, allow_nan=False)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(encoded + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise AuthorityError("Diagnostic output could not be created; existing files preserved") from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
