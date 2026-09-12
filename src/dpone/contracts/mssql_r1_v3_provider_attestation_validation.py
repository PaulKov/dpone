"""Public validation entry points for Provider Attestation Foundation V2."""

from __future__ import annotations

from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import (
    canonical_encoded_size_v1 as canonical_encoded_size_v1,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import (
    preflight_provider_attestation_canonical_v2 as preflight_provider_attestation_canonical_v2,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import (
    validate_provider_attestation_against_authorities_v2 as validate_provider_attestation_against_authorities_v2,
)

__all__ = (
    "canonical_encoded_size_v1",
    "preflight_provider_attestation_canonical_v2",
    "validate_provider_attestation_against_authorities_v2",
)
