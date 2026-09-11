"""Pure nonproduction policy parsing; no signature or live enrollment proof."""

import hashlib
from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.nonproduction_authority import NonproductionAuthorityPolicy, NonproductionLimits, effective_limits
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError, NonproductionScope
from dpone.contracts.strict_json import canonical_json_bytes
from tests.nonproduction_authority_helpers import NOW, digest, identifier, limits, policy, scope


def test_approved_limits_reject_boolean_integer_coercion():
    with pytest.raises(ValueError):
        NonproductionLimits(True, 64, 128, 100_000, 1_073_741_824, 3600)


def test_external_policy_pin_is_required_and_roundtrip_is_exact():
    value = policy()
    assert NonproductionAuthorityPolicy.from_bytes(value.to_bytes(), expected_sha256=value.policy_sha256) == value
    with pytest.raises(NonproductionAuthorityError, match="policy_pin"):
        NonproductionAuthorityPolicy.from_bytes(value.to_bytes(), expected_sha256=digest("caller root"))
    with pytest.raises(TypeError):
        NonproductionAuthorityPolicy.from_bytes(value.to_bytes())


@pytest.mark.parametrize("maximum", list(limits().__dataclass_fields__))
@pytest.mark.parametrize("invalid", [True, 0, -1, "1", 1.0, None])
def test_every_limit_rejects_ambiguous_or_nonpositive_values(maximum, invalid):
    with pytest.raises(NonproductionAuthorityError, match="limits"):
        limits(**{maximum: invalid})


@pytest.mark.parametrize("maximum", list(limits().__dataclass_fields__))
def test_initial_approved_ceilings_cannot_be_increased(maximum):
    value = limits()
    with pytest.raises(NonproductionAuthorityError, match="limits"):
        replace(value, **{maximum: getattr(value, maximum) + 1})


def test_effective_limits_select_each_lowest_bound_and_check_n_plus_one():
    value = effective_limits(limits(max_source_rows=50), limits(max_source_bytes=500), limits(max_attempts=2))
    assert (value.max_source_rows, value.max_source_bytes, value.max_attempts) == (50, 500, 2)
    usage = dict(workloads=64, attempts=2, source_rows=50, source_bytes=500, attempt_seconds=3600)
    value.require_usage(**usage)
    for key in usage:
        with pytest.raises(NonproductionAuthorityError, match="budget_exceeded"):
            value.require_usage(**dict(usage, **{key: usage[key] + 1}))
        with pytest.raises(NonproductionAuthorityError, match="budget_exceeded"):
            value.require_usage(**dict(usage, **{key: True}))


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "signer_root",
        "optional_attestation",
        "version",
        "trust_tier",
        "purpose",
        "bool_epoch",
        "float_limit",
        "empty_participants",
    ],
)
def test_policy_closed_shapes_reject_refingerprinted_malformed_inputs(mutation):
    body = policy().to_dict()
    if mutation == "unknown":
        body["status"] = "PASS"
    elif mutation == "signer_root":
        body["signer"]["trusted_root"] = "caller content"
    elif mutation == "optional_attestation":
        body["attestations"] = "optional"
    elif mutation == "version":
        body["schema"] = "dpone.airflow-deployment-trust-policy.v1"
    elif mutation == "trust_tier":
        body["trust_tier"] = "production"
    elif mutation == "purpose":
        body["purpose"] = "development"
    elif mutation == "bool_epoch":
        body["revocation_epoch"] = True
    elif mutation == "float_limit":
        body["limits"]["max_source_rows"] = 100000.0
    else:
        body["participants"] = []
    with pytest.raises(NonproductionAuthorityError):
        NonproductionAuthorityPolicy.from_bytes(canonical_json_bytes(body), expected_sha256=canonical_fingerprint(body))


@pytest.mark.parametrize("damage", ["space", "duplicate", "duplicate_nested", "nonfinite", "oversized", "bytearray"])
def test_policy_rejects_noncanonical_or_ambiguous_bytes(damage):
    value = policy()
    raw = value.to_bytes()
    if damage == "space":
        raw += b" "
    elif damage == "duplicate":
        raw = raw.replace(b'"revocation_epoch":2', b'"revocation_epoch":2,"revocation_epoch":2')
    elif damage == "duplicate_nested":
        raw = raw.replace(b'"issuer":"synthetic-issuer"', b'"issuer":"synthetic-issuer","issuer":"synthetic-issuer"')
    elif damage == "nonfinite":
        raw = raw.replace(b'"max_source_rows":100000', b'"max_source_rows":NaN')
    elif damage == "oversized":
        raw = b" " * (MAX_DOCUMENT_BYTES + 1)
    else:
        raw = bytearray(raw)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionAuthorityPolicy.from_bytes(raw, expected_sha256=value.policy_sha256)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.invalid/org/repo",
        "https://user:secret@example.invalid/org/repo",
        "https://example.invalid:443/org/repo",
        "https://example.invalid/org/repo?token=x",
        "https://example.invalid/org/../repo",
        "https://example.invalid/org/repo/",
        "HTTPS://example.invalid/org/repo",
        "https://example.invalid./org/repo",
        "https://example..invalid/org/repo",
    ],
)
def test_repository_claim_has_one_credential_free_canonical_spelling(url):
    with pytest.raises(NonproductionAuthorityError):
        scope(source_repository=url)


@pytest.mark.parametrize(
    "damage",
    ["missing_pg", "duplicate_physical", "effect_swap", "alias", "unknown_field", "partial_route", "route_type"],
)
def test_common_scope_covers_complete_physical_and_six_dimension_closure(damage):
    body = scope().to_dict()
    if damage == "missing_pg":
        body["participants"] = [row for row in body["participants"] if row["connector"] != "postgres"]
    elif damage == "duplicate_physical":
        extra = dict(body["participants"][0], write_relations=[digest("other write")])
        body["participants"].insert(1, extra)
    elif damage == "effect_swap":
        row = next(row for row in body["participants"] if row["connector"] == "postgres")
        row["read_relations"] = [digest("business rows")]
        changed = NonproductionScope.from_dict(body)
        with pytest.raises(NonproductionAuthorityError, match="policy_scope"):
            policy().require_scope(changed)
        return
    elif damage == "alias":
        body["participants"][0]["service_id"] = "my_connection_alias"
    elif damage == "unknown_field":
        body["participants"][0]["isolated"] = True
    elif damage == "partial_route":
        del body["routes"][0]["transport"]
    else:
        body["routes"][0]["strategy"] = "incremental_merge"
    with pytest.raises(NonproductionAuthorityError):
        NonproductionScope.from_dict(body)


def test_scope_identity_covers_source_fixture_toolchain_intent_and_full_effects():
    value = scope()
    assert NonproductionScope.from_dict(value.to_dict()) == value
    for field in (
        "fixture_sha256",
        "generator_sha256",
        "compilation_intent_sha256",
        "toolchain_sha256",
        "image_sha256",
        "enrollment_sha256",
    ):
        assert replace(value, **{field: digest("changed")}).scope_sha256 != value.scope_sha256
    assert replace(value, campaign_id=identifier(99)).scope_sha256 != value.scope_sha256


@pytest.mark.parametrize("current", [None, NOW.replace(tzinfo=None)])
def test_current_policy_requires_an_independent_aware_clock(current):
    with pytest.raises(NonproductionAuthorityError, match="clock"):
        policy().require_current(now=current, current_revocation_epoch=2)


@pytest.mark.parametrize("missing_pair", [("mssql", "clickhouse"), ("postgres", "mssql")])
def test_common_scope_requires_both_complete_campaign_transfer_routes(missing_pair):
    value = scope()
    with pytest.raises(NonproductionAuthorityError, match="route_closure"):
        replace(value, routes=tuple(route for route in value.routes if (route.source, route.sink) != missing_pair))


def test_policy_pin_distinguishes_exact_signer_bytes_without_path_normalization():
    first = policy(signer=replace(policy().signer, identity=r"synthetic\key"))
    second = policy(signer=replace(policy().signer, identity="synthetic/key"))
    assert first.to_bytes() != second.to_bytes()
    with pytest.raises(NonproductionAuthorityError, match="policy_pin"):
        NonproductionAuthorityPolicy.from_bytes(second.to_bytes(), expected_sha256=first.policy_sha256)


@pytest.mark.parametrize("signer_identity", ["synthetic/key", r"synthetic\key", "synthetic_ключ"])
def test_policy_and_scope_subjects_are_sha256_of_exact_canonical_utf8_bytes(signer_identity):
    value = policy(signer=replace(policy().signer, identity=signer_identity))
    assert value.policy_sha256 == "sha256:" + hashlib.sha256(value.to_bytes()).hexdigest()
    common_scope = scope(value)
    assert (
        common_scope.scope_sha256
        == "sha256:" + hashlib.sha256(canonical_json_bytes(common_scope.to_dict())).hexdigest()
    )
