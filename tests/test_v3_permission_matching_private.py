"""Exact schema2 permission matching; conflicting-rule policy is outside scope."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts import mssql_r1_v3_schema_attestation as subject
from tests import test_postgres_mssql_r1_v3_schema_contract_v2 as fixtures


def test_source_and_fixtures_share_current_checkout():
    root = Path(fixtures.__file__).resolve().parents[1]
    assert Path(subject.__file__).resolve() == root / "src/dpone/contracts/mssql_r1_v3_schema_attestation.py"


def test_identical_complete_expected_permission_duplicate_rejected():
    contract = fixtures._portable_contract()
    rule = contract.ordered_permission_rules[0]
    with pytest.raises(fixtures.MssqlR1V3ContractError, match="permission rules must use strict canonical order"):
        replace(contract, ordered_permission_rules=(rule, rule))


def test_expected_single_grant_rejects_observed_deny():
    *_, permissions = fixtures._attestation_parts()
    assert permissions[0].effect is fixtures.MssqlR1PermissionEffectV3.GRANT
    denied = replace(permissions[0], effect=fixtures.MssqlR1PermissionEffectV3.DENY)
    with pytest.raises(fixtures.MssqlR1V3ContractError, match="observed permissions differ from portable rules"):
        fixtures._attestation(ordered_observed_permissions=(denied,))


def test_missing_permission_observation_rejected():
    with pytest.raises(fixtures.MssqlR1V3ContractError, match="observed permissions differ from portable rules"):
        fixtures._attestation(ordered_observed_permissions=())


def test_extra_permission_observation_rejected():
    *_, permissions = fixtures._attestation_parts()
    extra = replace(permissions[0], permission="CONTROL")
    assert extra.permission != permissions[0].permission
    with pytest.raises(fixtures.MssqlR1V3ContractError, match="observed permissions differ from portable rules"):
        fixtures._attestation(ordered_observed_permissions=fixtures._sorted(permissions[0], extra))


def test_identical_complete_observation_duplicate_rejected():
    *_, permissions = fixtures._attestation_parts()
    with pytest.raises(fixtures.MssqlR1V3ContractError, match="strict canonical order"):
        fixtures._attestation(ordered_observed_permissions=(permissions[0], permissions[0]))


def test_exact_single_permission_roundtrips_and_attests():
    attestation = fixtures._attestation()
    contract = attestation.expected_contract
    assert len(contract.ordered_permission_rules) == 1
    rule = contract.ordered_permission_rules[0]
    assert rule.effect is fixtures.MssqlR1PermissionEffectV3.GRANT
    for value in (rule, contract, attestation):
        decoded = type(value).from_canonical_bytes(value.canonical_bytes)
        assert decoded == value
        assert decoded.canonical_bytes == value.canonical_bytes
    assert len(attestation.ordered_observed_permissions) == 1
    assert attestation.ordered_observed_permissions[0].effect is rule.effect
