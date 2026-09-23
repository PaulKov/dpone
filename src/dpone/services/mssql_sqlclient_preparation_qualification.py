"""Pure admission policy for SQLClient preparation qualification evidence."""

from __future__ import annotations

from hashlib import sha256

from dpone.contracts.mssql_sqlclient_preparation_qualification import (
    ERROR,
    AdmittedPreparationBaseline,
    PreparationBaselineAuthority,
    decode_qualification,
)
from dpone.services.mssql_tds_writer_contracts import PROFILE, QUERY_PROFILE, canonical_json_bytes, parse_baseline


def admit_preparation_baseline(
    baseline_payload: bytes,
    qualification_payload: bytes,
    authority: PreparationBaselineAuthority,
) -> AdmittedPreparationBaseline:
    """Admit exact qualified bytes under one independently pinned receipt."""
    if type(authority) is not PreparationBaselineAuthority:
        raise ValueError(ERROR)
    authority.__post_init__()
    baseline = parse_baseline(baseline_payload)
    receipt = decode_qualification(qualification_payload)
    expected = {
        "baseline_sha256": sha256(baseline_payload).hexdigest(),
        "baseline_byte_count": len(baseline_payload),
        "profile": PROFILE,
        "query_profile": QUERY_PROFILE,
        "sql_image_digest": baseline["server_build"]["image_digest"],
        "server_build_sha256": sha256(canonical_json_bytes(baseline["server_build"])).hexdigest(),
        "database_profile_sha256": sha256(canonical_json_bytes(baseline["database_profile"])).hexdigest(),
        "first_raw_sha256": baseline["provenance"]["first_raw_evidence_sha256"],
        "repeat_raw_sha256": baseline["provenance"]["repeat_raw_evidence_sha256"],
        "reviewed_projection_sha256": baseline["provenance"]["reviewed_projection_sha256"],
    }
    if any(receipt[name] != value for name, value in expected.items()):
        raise ValueError(ERROR)
    admitted = AdmittedPreparationBaseline._admit(baseline_payload, qualification_payload, authority)
    admitted.assert_authority(authority)
    return admitted


__all__ = ("admit_preparation_baseline",)
