"""Environment wire shape and original authentication do not imply qualification."""

from dataclasses import replace
from itertools import permutations

import pytest

from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.contracts.native_trusted_dbt_environment_codec import (
    decode_trusted_dbt_owned_root,
    decode_trusted_dbt_qualification,
    decode_trusted_dbt_toolchain,
    encode_trusted_dbt_owned_root,
    encode_trusted_dbt_qualification,
    encode_trusted_dbt_toolchain,
)
from dpone.runtime.native_generation_invocation_auth import InvocationOriginalReader
from tests.native_trusted_dbt_fixtures import InvocationFixture

KINDS = (
    "trusted_dbt_command_plan_v1",
    "trusted_dbt_invocation_completion_v1",
    "trusted_dbt_toolchain_v1",
    "trusted_dbt_qualification_v1",
    "trusted_dbt_owned_root_v1",
)
CODECS = (
    ("roots/PROJECT", encode_trusted_dbt_owned_root, decode_trusted_dbt_owned_root),
    ("roots/OUTPUT", encode_trusted_dbt_owned_root, decode_trusted_dbt_owned_root),
    ("invocation/toolchain", encode_trusted_dbt_toolchain, decode_trusted_dbt_toolchain),
    ("invocation/qualification", encode_trusted_dbt_qualification, decode_trusted_dbt_qualification),
)


@pytest.mark.parametrize("locator,encode,decode", CODECS)
def test_environment_codecs_require_exact_fields_and_canonical_bytes(tmp_path, locator, encode, decode):
    fixture = InvocationFixture(tmp_path)
    payload = fixture.store.documents[locator]
    assert encode(decode(payload)) == payload
    body = decode_native_delivery_json(payload)
    for field in body:
        with pytest.raises(ValueError):
            decode(encode_native_delivery_json({key: value for key, value in body.items() if key != field}))
    for changed in (b" " + payload, encode_native_delivery_json({**body, "qualified": True})):
        with pytest.raises(ValueError):
            decode(changed)


@pytest.mark.parametrize(
    "field,value", [("device", True), ("inode", -1), ("role", "TEMP"), ("content_inventory", None)]
)
def test_owned_project_root_rejects_invalid_identity(tmp_path, field, value):
    fixture = InvocationFixture(tmp_path)
    root = decode_trusted_dbt_owned_root(fixture.store.documents["roots/PROJECT"])
    with pytest.raises(NativeSourceCustodyError):
        replace(root, **{field: value})


def test_qualification_requires_unique_evidence_and_preserves_dbt_contract(tmp_path):
    fixture = InvocationFixture(tmp_path)
    qualification = decode_trusted_dbt_qualification(fixture.store.documents["invocation/qualification"])
    for evidence in ((), qualification.evidence * 2):
        with pytest.raises(NativeSourceCustodyError):
            replace(qualification, evidence=evidence)
    toolchain = decode_trusted_dbt_toolchain(fixture.store.documents["invocation/toolchain"])
    from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

    assert toolchain.dbt_contract == DBT_SQLSERVER_1_12_CERTIFIED
    assert toolchain.dbt_contract.sha256 == DBT_SQLSERVER_1_12_CERTIFIED.sha256


@pytest.mark.parametrize("expected,substituted", list(permutations(KINDS, 2)))
def test_no_pair_of_invocation_original_kinds_is_interchangeable(tmp_path, expected, substituted):
    fixture = InvocationFixture(tmp_path)
    reference = fixture.executor.command
    fixture.store.bound[reference.locator] = replace(fixture.store.bound[reference.locator], kind=substituted)
    reader = InvocationOriginalReader(
        originals=fixture.store, bindings=fixture.store, subject=fixture.store.subject, max_bytes=65536
    )
    with pytest.raises(NativeSourceCustodyError):
        reader.read(reference, expected)


@pytest.mark.parametrize("mode", ["oversized", "changed-bytes", "wrong-subject", "wrong-locator", "wrong-digest"])
def test_reader_independently_checks_faulty_provider_outputs(tmp_path, mode):
    fixture = InvocationFixture(tmp_path)
    reference = fixture.executor.command
    if mode == "oversized":
        fixture.store.documents[reference.locator] = b"x" * 65537
    elif mode == "changed-bytes":
        fixture.store.documents[reference.locator] += b" "
    else:
        bound = fixture.store.bound[reference.locator]
        if mode == "wrong-subject":
            from uuid import UUID

            bound = replace(bound, subject=replace(bound.subject, generation_id=UUID(int=9)))
        elif mode == "wrong-locator":
            bound = replace(bound, locator="different/locator")
        else:
            bound = replace(bound, payload_sha256=fixture.executor.profile.sha256)
        fixture.store.bound[reference.locator] = bound
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.validate_before_credentials()
    assert not fixture.delegate.calls
