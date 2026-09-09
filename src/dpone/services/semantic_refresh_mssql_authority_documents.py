"""Stable application-facing MSSQL authority document codec."""

from __future__ import annotations

from dpone.ports.semantic_refresh_mssql_authority import MssqlCanonicalAuthorityRecord
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    authority_json,
    authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_authority_models import MssqlCanonicalAdmissionBundle


def semantic_refresh_mssql_authority_sha256(bundle: MssqlCanonicalAdmissionBundle) -> str:
    """Return the canonical protected authority digest."""

    return authority_sha256(bundle)


def semantic_refresh_mssql_authority_json(bundle: MssqlCanonicalAdmissionBundle) -> str:
    """Serialize one closed authority document for protected storage."""

    return authority_json(bundle)


def semantic_refresh_mssql_authority_from_record(
    record: MssqlCanonicalAuthorityRecord,
) -> MssqlCanonicalAdmissionBundle:
    """Parse and authenticate one ACTIVE control-table authority record."""

    return authority_from_record(record)
