"""Synthetic claim values only; these fixtures never certify signing or enrollment."""

from dataclasses import replace
from datetime import UTC, datetime

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.nonproduction_authority import (
    NonproductionAuthorityPolicy,
    NonproductionLimits,
    NonproductionSigner,
)
from dpone.contracts.nonproduction_grants import (
    NonproductionExecutionGrant,
    NonproductionQualificationGrant,
    NonproductionWorkload,
)
from dpone.contracts.nonproduction_scope import (
    PURPOSE,
    TRUST_TIER,
    NonproductionParticipant,
    NonproductionRouteDimensions,
    NonproductionScope,
)

NOW = datetime(2026, 9, 10, 1, 30, tzinfo=UTC)


def digest(label):
    return canonical_fingerprint({"synthetic": label})


def identifier(number):
    return f"10000000-0000-4000-8000-{number:012d}"


def limits(**changes):
    return replace(NonproductionLimits(86400, 64, 128, 100000, 1073741824, 3600), **changes)


def participants():
    return tuple(
        sorted(
            (
                NonproductionParticipant(
                    "postgres", identifier(1), digest("PG database"), (digest("source table"),), ()
                ),
                NonproductionParticipant(
                    "mssql",
                    identifier(2),
                    digest("SQL database"),
                    (digest("native reads"),),
                    tuple(sorted(digest(value) for value in ("native target", "ordinary target", "staging", "state"))),
                ),
                NonproductionParticipant(
                    "clickhouse",
                    identifier(3),
                    digest("CH database"),
                    (),
                    tuple(sorted(digest(value) for value in ("generated target", "CH staging"))),
                ),
            )
        )
    )


def policy(**changes):
    return replace(
        NonproductionAuthorityPolicy(
            signer=NonproductionSigner("cosign_public_key_v1", "synthetic-issuer", "synthetic-key", digest("root")),
            environment_id=identifier(4),
            enrollment_sha256=digest("independent enrollment"),
            participants=participants(),
            source_repository="https://example.invalid/synthetic/repository",
            not_before="2026-09-10T00:00:00Z",
            expires_at="2026-09-11T00:00:00Z",
            revocation_epoch=2,
            revoked_grant_ids=(),
            limits=limits(),
        ),
        **changes,
    )


def scope(value=None, **changes):
    value = value or policy()
    return replace(
        NonproductionScope(
            purpose=PURPOSE,
            trust_tier=TRUST_TIER,
            source_repository=value.source_repository,
            source_commit="a" * 40,
            fixture_sha256=digest("synthetic fixture"),
            generator_sha256=digest("generator"),
            compilation_intent_sha256=digest("full compilation intent"),
            toolchain_sha256=digest("toolchain"),
            image_sha256=digest("image"),
            campaign_id=identifier(5),
            environment_id=value.environment_id,
            policy_sha256=value.policy_sha256,
            enrollment_sha256=value.enrollment_sha256,
            routes=(
                NonproductionRouteDimensions(
                    "mssql", "clickhouse", "full_refresh", "native_bcp_to_clickhouse", "none", "kpo"
                ),
                NonproductionRouteDimensions("postgres", "mssql", "full_refresh", "generic_rows", "none", "kpo"),
            ),
            participants=value.participants,
        ),
        **changes,
    )


def common(value):
    return dict(
        scope=scope(value),
        grant_id=identifier(6),
        not_before="2026-09-10T01:00:00Z",
        expires_at="2026-09-10T02:00:00Z",
        revocation_epoch=value.revocation_epoch,
        limits=limits(),
    )


def qualification(value=None, **changes):
    value = value or policy()
    return replace(
        NonproductionQualificationGrant(
            **common(value),
            phase="qualification",
            qualification_run_id=identifier(7),
            fixture_plan_sha256=digest("fixture plan"),
            qualification_plan_sha256=digest("qualification plan"),
        ),
        **changes,
    )


def execution(value=None, **changes):
    value = value or policy()
    return replace(
        NonproductionExecutionGrant(
            **dict(common(value), grant_id=identifier(8)),
            phase="execution",
            qualified_set_sha256=digest("qualifications"),
            native_release_id=digest("native release"),
            parent_release_id=digest("parent release"),
            deployment_id=digest("sealed deployment"),
            activation_id=identifier(9),
            workloads=(
                NonproductionWorkload("a_native", "native", digest("native pack"), limits()),
                NonproductionWorkload("b_generated", "native", digest("generated pack"), limits()),
                NonproductionWorkload("c_ordinary", "standalone", digest("ordinary pack"), limits()),
            ),
        ),
        **changes,
    )


def verification_inputs(grant, value):
    """Inputs for structural comparisons; no test claim of authentic verification."""
    return dict(
        policy=value,
        expected_policy_sha256=value.policy_sha256,
        expected_scope=grant.scope,
        signature_subject=grant.signature_subject(value),
        now=NOW,
        current_revocation_epoch=value.revocation_epoch,
    )
