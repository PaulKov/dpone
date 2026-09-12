"""Domain-separated digest derivation for schema-2 attestations."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import fields
from typing import Any

_OBSERVED_INVENTORY = b"dpone-r1-schema-observed-inventory-v3-schema-2\0"
_OBSERVED_SECURITY = b"dpone-r1-schema-observed-security-v3-schema-2\0"
_LIVE_IDENTITY = b"dpone-r1-schema-live-identity-v3-schema-2\0"


def derive_attestation_digests(
    attestation: Any,
    canonicalize: Callable[[bytes, tuple[object, ...]], bytes],
) -> dict[str, bytes]:
    """Re-derive every digest from the exact typed attestation fields."""
    schema_inventory = hashlib.sha256(
        canonicalize(
            _OBSERVED_INVENTORY,
            (
                tuple(item.canonical_bytes for item in attestation.ordered_observed_schemas),
                tuple(item.canonical_bytes for item in attestation.ordered_observed_objects),
            ),
        )
    ).digest()
    security_inventory = hashlib.sha256(
        canonicalize(
            _OBSERVED_SECURITY,
            (
                tuple(item.canonical_bytes for item in attestation.ordered_observed_principals),
                tuple(item.canonical_bytes for item in attestation.ordered_observed_role_memberships),
                tuple(item.canonical_bytes for item in attestation.ordered_observed_permissions),
            ),
        )
    ).digest()
    live_identity = hashlib.sha256(
        canonicalize(
            _LIVE_IDENTITY,
            (
                attestation.server_instance_identity_sha256,
                attestation.database_id,
                attestation.database_guid,
                attestation.database_family_guid,
                attestation.recovery_fork_guid,
                schema_inventory,
                security_inventory,
            ),
        )
    ).digest()
    return {
        "expected_schema_contract_digest": hashlib.sha256(attestation.expected_contract_bytes).digest(),
        "principal_authority_set_digest": hashlib.sha256(attestation.principal_authority_set_bytes).digest(),
        "observed_schema_inventory_digest": schema_inventory,
        "observed_security_inventory_digest": security_inventory,
        "live_identity_digest": live_identity,
        "attestation_digest": hashlib.sha256(attestation.identity_bytes).digest(),
    }


def with_derived_attestation_fields(
    cls: type[Any],
    values: dict[str, object],
    canonicalize: Callable[[bytes, tuple[object, ...]], bytes],
) -> Any:
    """Construct through the validating class after computing all derived fields."""
    derived_names = (
        "expected_schema_contract_digest",
        "principal_authority_set_digest",
        "observed_schema_inventory_digest",
        "observed_security_inventory_digest",
        "live_identity_digest",
        "attestation_digest",
    )
    for name in derived_names:
        values[name] = bytes(32)
    provisional = object.__new__(cls)
    for item in fields(cls):
        object.__setattr__(provisional, item.name, values[item.name])
    derived = derive_attestation_digests(provisional, canonicalize)
    values.update({name: value for name, value in derived.items() if name != "attestation_digest"})
    for item in fields(cls):
        object.__setattr__(provisional, item.name, values[item.name])
    values["attestation_digest"] = hashlib.sha256(provisional.identity_bytes).digest()
    return cls(**values)


__all__ = ["derive_attestation_digests", "with_derived_attestation_fields"]
