"""Canonical binding bytes preserve exact registration linkage without authority."""

from dataclasses import replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    decode_catalog_binding,
    encode_catalog_binding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER
from tests.support.dbt_mssql_physical_registration import registration_inputs


def registration():
    value = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    return replace(value, limits=replace(value.limits, max_metadata_bytes=65536))


def binding():
    value = registration()
    return CatalogRegistrationBinding(
        value.registration_id,
        physical_runtime_registration_digest(value),
        value.platform_subject,
        value.trusted_profile,
        "analytics",
        "orders",
        OriginalRef(NATIVE_POLICY_MEMBER, value.platform_subject.platform_policy_sha256),
        "sha256:" + "b" * 64,
        value.model_database.database_name,
        "models",
        value.trusted_profile.reference,
        10,
        1,
        "sha256:" + "c" * 64,
    )


def test_binding_roundtrip_and_registration_link():
    value = binding()
    assert decode_catalog_binding(encode_catalog_binding(value)) == value
    require_catalog_binding_registration(value, registration())


@pytest.mark.parametrize(
    "field,bad",
    [
        ("model_schema_id", True),
        ("model_schema_owner_id", 2),
        ("model_schema", "dbo"),
        ("profile_name", " "),
        ("registration_sha256", "bad"),
    ],
)
def test_invalid_binding_representation_rejects(field, bad):
    with pytest.raises((ValueError, DbtPublishingError)):
        replace(binding(), **{field: bad})


@pytest.mark.parametrize("mode", ["extra", "missing", "noncanonical", "duplicate", "profile", "member"])
def test_canonical_payload_rejects_substitution(mode):
    payload = encode_catalog_binding(binding())
    value = decode_native_delivery_json(payload)
    if mode == "extra":
        value["authenticated"] = True
    elif mode == "missing":
        del value["model_schema"]
    elif mode == "noncanonical":
        payload += b"\n"
    elif mode == "duplicate":
        payload = payload[:-1] + b',"model_schema":"models"}'
    elif mode == "profile":
        value["resource_bounds"] = {"locator": "other", "sha256": "sha256:" + "d" * 64}
    else:
        value["policy_member"] = {"locator": NATIVE_POLICY_MEMBER, "sha256": "sha256:" + "d" * 64}
    if mode not in {"noncanonical", "duplicate"}:
        payload = encode_native_delivery_json(value)
    with pytest.raises((ValueError, DbtPublishingError)):
        decode_catalog_binding(payload)


@pytest.mark.parametrize("field", ["registration_sha256", "model_database_name"])
def test_registration_link_rejects_other_protected_identity(field):
    value = replace(binding(), **{field: "sha256:" + "e" * 64 if field.endswith("sha256") else "other"})
    with pytest.raises((ValueError, DbtPublishingError)):
        require_catalog_binding_registration(value, registration())


def test_binding_metadata_budget_is_enforced():
    registered = registration()
    registered = replace(registered, limits=replace(registered.limits, max_metadata_bytes=1024))
    value = replace(binding(), registration_sha256=physical_runtime_registration_digest(registered))
    with pytest.raises(ValueError, match="metadata budget"):
        require_catalog_binding_registration(value, registered)
