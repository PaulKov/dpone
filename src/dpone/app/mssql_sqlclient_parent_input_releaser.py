"""Release exact SqlClient input custody from an ordered parent-v4 journal."""

from __future__ import annotations

from dataclasses import asdict

from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_native_v4_snapshot_validation import decode_native_chunk_receipt
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.contracts.mssql_native_parent_journal import NativeParentRetirementReceipt, canonical_digest
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SqlClientInputCustodyReceipt,
    validate_native_chunk_receipt,
)
from dpone.contracts.mssql_tds_api import NativeChunkReceipt

_SCHEMA = "dpone.sqlclient.parent-input-release.v1"
_INVALID = "mssql_native.sqlclient_parent_input_release_invalid"
_FAILED = "mssql_native.sqlclient_parent_input_release_failed"


class SqlClientParentInputReleaser:
    """Delete ordered retained files only from exact retired v4 authority."""

    def __init__(self, journal: NativeChunkJournal, custody: FileSqlClientInputCustody) -> None:
        if type(journal) is not NativeChunkJournal or type(custody) is not FileSqlClientInputCustody:
            raise ValueError(_INVALID)
        self._journal = journal
        self._custody = custody

    def __call__(self, retirement: NativeParentRetirementReceipt) -> str:
        return self.release(retirement)

    def release(self, retirement: NativeParentRetirementReceipt) -> str:
        """Release every exact custody object and bind the ordered result digest."""
        decoded = self._decode(retirement)
        try:
            for parent, custody in decoded:
                self._custody.release(custody, parent, retirement)
        except Exception as error:
            raise RuntimeError(_FAILED) from error
        return canonical_digest(
            {
                "schema": _SCHEMA,
                "retirement_digest": retirement.digest,
                "ordered_custody_ids": [custody.durable_object_id for _, custody in decoded],
            }
        )

    def _decode(
        self, retirement: NativeParentRetirementReceipt
    ) -> tuple[tuple[NativeChunkReceipt, SqlClientInputCustodyReceipt], ...]:
        try:
            if type(retirement) is not NativeParentRetirementReceipt:
                raise ValueError
            retirement.__post_init__()
            data = self._journal.data
            if not isinstance(data, dict) or data.get("version") != 4 or data.get("phase") != "stage_complete":
                raise ValueError
            publication = data.get("publication")
            chunks = data.get("chunks")
            if (
                not isinstance(publication, dict)
                or publication.get("phase") not in {"retired", "checkpoint_required", "succeeded"}
                or publication.get("retirement_receipt") != retirement.to_dict()
                or not isinstance(chunks, dict)
                or set(chunks) != {str(index) for index in range(len(retirement.chunks))}
            ):
                raise ValueError
            result = []
            for ordinal, retired in enumerate(retirement.chunks):
                chunk = chunks[str(ordinal)]
                if not isinstance(chunk, dict) or chunk.get("phase") != "verified":
                    raise ValueError
                parent = decode_native_chunk_receipt(chunk.get("receipt"))
                evidence = validate_native_chunk_receipt(parent)
                if (
                    retired.ordinal != ordinal
                    or retired.attempt_id != parent.attempt_id
                    or retired.verification_receipt_sha256 != canonical_digest(asdict(parent))
                ):
                    raise ValueError
                result.append((parent, evidence.input_custody))
            return tuple(result)
        except Exception as error:
            raise ValueError(_INVALID) from error


__all__ = ("SqlClientParentInputReleaser",)
