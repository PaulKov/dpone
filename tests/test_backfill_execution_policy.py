from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.backfill.execution_policy import (
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    BackfillExecutionPolicy,
    execution_policy_from_load_config,
    issue_backfill_runtime_authority,
    normalize_backfill_execution_policy,
    validate_backfill_advisor_options,
)
from dpone.backfill.shadow_append_authority import (
    SHADOW_APPEND_AUTHORITY_OPTION,
    issue_shadow_append_authority,
    require_shadow_append_authority,
)
from dpone.contracts.portable_relation_scope import parse_portable_relation_scope, portable_scope_sha256


def _options() -> dict[str, object]:
    return {
        "inner_mode": "incremental_merge",
        "parallel_workers": 2,
        "chunk": {
            "column": "id",
            "from": 1,
            "to": 3,
            "step": 1,
            "kind": "integer",
        },
        "max_chunks": 3,
        "state": {
            "backend": "audit_schema",
            "schema": "DWH_Tech",
            "require_distributed_lock": True,
        },
        "state_dir": ".dpone/backfill",
        "retry_policy": "non_committed",
        "backfill_id": "orders-2026",
        "predicate_dialect": "postgres",
        "lease_ttl_minutes": 5,
    }


def _runtime_proof(policy: BackfillExecutionPolicy) -> tuple[dict[str, object], dict[str, object]]:
    portable_scope = {
        "version": 1,
        "kind": "range",
        "column": "id",
        "lower": {"inclusive": True, "value": {"type": "integer", "value": 1}},
        "upper": {"inclusive": True, "value": {"type": "integer", "value": 1}},
    }
    scope_sha256 = portable_scope_sha256(parse_portable_relation_scope(portable_scope)).hex()
    context = {
        "schema": "dpone.backfill.chunk-context.v1",
        "run_key": "orders-2026",
        "plan_hash": "plan-sha256",
        "index": 1,
        "idempotency_key": "chunk-1",
        "start": "1",
        "end": "1",
        "portable_scope": portable_scope,
        "portable_scope_sha256": scope_sha256,
        "execution_policy_sha256": policy.digest,
    }
    scope = {
        "kind": "backfill_disjoint_range_v1",
        "run_key": "orders-2026",
        "plan_hash": "plan-sha256",
        "chunk_index": 1,
        "chunk_idempotency_key": "chunk-1",
        "start": "1",
        "end": "1",
        "portable_scope": portable_scope,
        "portable_scope_sha256": scope_sha256,
        "execution_policy_sha256": policy.digest,
        "proven_disjoint": True,
    }
    return context, scope


def test_execution_policy_normalizes_all_public_execution_fields() -> None:
    policy = normalize_backfill_execution_policy(_options())

    assert isinstance(policy, BackfillExecutionPolicy)
    assert policy.inner_mode == "incremental_merge"
    assert policy.parallel_workers == 2
    assert policy.chunk is not None and policy.chunk.max_chunks == 3
    assert policy.state.backend == "audit_schema"
    assert policy.state.require_distributed_lock is True
    assert policy.publication.mode == "direct"
    assert policy.publication.retain_backup is True
    assert policy.publication.artifact_scope == "stable"
    assert policy.retry_policy == "non_committed"
    assert policy.lease_ttl_minutes == 5
    assert len(policy.digest) == 64


def test_execution_policy_normalizes_shadow_publication_and_binds_it_to_identity() -> None:
    raw = _options()
    raw["inner_mode"] = "incremental_append"
    raw["publication"] = {
        "mode": "shadow_swap",
        "retain_backup": True,
        "artifact_scope": "campaign",
    }

    policy = normalize_backfill_execution_policy(raw)

    assert policy.publication.mode == "shadow_swap"
    assert policy.publication.retain_backup is True
    assert policy.publication.artifact_scope == "campaign"
    assert policy.to_jsonable()["publication"] == {
        "mode": "shadow_swap",
        "retain_backup": True,
        "artifact_scope": "campaign",
    }
    assert policy.digest != normalize_backfill_execution_policy(_options()).digest


def test_legacy_stable_publication_preserves_pre_feature_identity() -> None:
    implicit = _options()
    implicit["inner_mode"] = "incremental_append"
    implicit["publication"] = {"mode": "shadow_swap", "retain_backup": True}
    explicit = deepcopy(implicit)
    explicit["publication"]["artifact_scope"] = "stable"

    implicit_policy = normalize_backfill_execution_policy(implicit)
    explicit_policy = normalize_backfill_execution_policy(explicit)

    assert implicit_policy.publication.to_jsonable() == {
        "mode": "shadow_swap",
        "retain_backup": True,
    }
    assert explicit_policy.digest == implicit_policy.digest


@pytest.mark.parametrize(
    ("publication", "message"),
    [
        ([], "backfill.publication must be an object"),
        ({"mode": "rename"}, "backfill.publication.mode"),
        ({"mode": "shadow_swap", "retain_backup": "true"}, "retain_backup"),
        ({"mode": "direct", "retain_backup": True}, "only valid for mode=shadow_swap"),
        ({"mode": "shadow_swap", "artifact_scope": "run"}, "artifact_scope"),
        ({"mode": "direct", "artifact_scope": "campaign"}, "only valid for mode=shadow_swap"),
        ({"mode": "shadow_swap", "cleanup": True}, "unsupported options"),
    ],
)
def test_execution_policy_rejects_invalid_publication_authoring(
    publication: object,
    message: str,
) -> None:
    raw = _options()
    raw["publication"] = publication

    with pytest.raises(ValueError, match=message):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"parallel_workers": 0}), "parallel_workers"),
        (lambda value: value.update({"max_chunks": 0}), "max_chunks"),
        (lambda value: value.update({"lease_ttl_minutes": 0}), "lease_ttl_minutes"),
        (lambda value: value.update({"retry_policy": "failed-only"}), "retry_policy"),
        (lambda value: value.update({"predicate_dialect": "oracle"}), "predicate_dialect"),
        (lambda value: value.update({"unexpected": True}), "unsupported options"),
        (lambda value: value["chunk"].update({"unexpected": True}), "backfill.chunk"),
        (lambda value: value.update({"state": {"backend": "redis"}}), "backfill.state.backend"),
        (
            lambda value: value.update({"state": {"backend": "audit_schema", "require_distributed_lock": "true"}}),
            "require_distributed_lock",
        ),
    ],
)
def test_execution_policy_rejects_invalid_authoring_before_runtime(
    mutate: object,
    message: str,
) -> None:
    raw = _options()
    mutate(raw)  # type: ignore[operator]

    with pytest.raises(ValueError, match=message):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("invalid", [[], ""])
def test_execution_policy_rejects_falsey_non_object_backfill(invalid: object) -> None:
    with pytest.raises(ValueError, match="backfill must be an object"):
        normalize_backfill_execution_policy(invalid)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="backfill must be an object"):
        execution_policy_from_load_config(SimpleNamespace(options={"backfill": invalid}))


@pytest.mark.parametrize("invalid", [[], ""])
def test_execution_policy_rejects_falsey_non_object_load_options(invalid: object) -> None:
    with pytest.raises(ValueError, match="load options must be an object"):
        execution_policy_from_load_config(SimpleNamespace(options=invalid))


@pytest.mark.parametrize("field", ["parallel_workers", "max_chunks", "lease_ttl_minutes"])
@pytest.mark.parametrize("invalid", ["1", 1.0, True])
def test_execution_policy_requires_exact_integer_authority(field: str, invalid: object) -> None:
    raw = _options()
    raw[field] = invalid

    with pytest.raises(ValueError, match=field):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("invalid", [False, 0, ""])
def test_execution_policy_rejects_falsey_retry_policy(invalid: object) -> None:
    raw = _options()
    raw["retry_policy"] = invalid

    with pytest.raises(ValueError, match="backfill.retry_policy"):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("invalid", [False, 0, ""])
def test_execution_policy_rejects_falsey_inner_mode(invalid: object) -> None:
    raw = _options()
    raw["inner_mode"] = invalid

    with pytest.raises(ValueError, match="backfill.inner_mode"):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("field", ["kind", "column"])
@pytest.mark.parametrize("invalid", [False, 0, ""])
def test_execution_policy_requires_exact_chunk_text_fields(field: str, invalid: object) -> None:
    raw = _options()
    raw["chunk"][field] = invalid  # type: ignore[index]

    with pytest.raises(ValueError, match=rf"backfill\.chunk\.{field}"):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("field", ["from", "to"])
@pytest.mark.parametrize("invalid", [False, 1.5, ""])
def test_execution_policy_requires_string_or_integer_chunk_bounds(field: str, invalid: object) -> None:
    raw = _options()
    raw["chunk"][field] = invalid  # type: ignore[index]

    with pytest.raises(ValueError, match=rf"backfill\.chunk(?:\.{field}| requires keys)"):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("invalid", [False, 1.5, ""])
def test_execution_policy_requires_string_or_integer_chunk_step(invalid: object) -> None:
    raw = _options()
    raw["chunk"]["step"] = invalid  # type: ignore[index]

    with pytest.raises(ValueError, match=r"backfill\.chunk(?:\.step| requires keys)"):
        normalize_backfill_execution_policy(raw)


def test_execution_policy_preserves_integer_zero_boundary_and_integer_step() -> None:
    raw = _options()
    raw["chunk"] = {"column": "id", "from": 0, "to": 2, "step": 1, "kind": "integer"}

    policy = normalize_backfill_execution_policy(raw)

    assert policy.chunk is not None
    assert (policy.chunk.start, policy.chunk.end, policy.chunk.step) == ("0", "2", "1")


@pytest.mark.parametrize("field", ["state", "state_dir", "backfill_id", "predicate_dialect"])
def test_execution_policy_rejects_explicit_null_optional_authoring(field: str) -> None:
    raw = _options()
    raw[field] = None

    with pytest.raises(ValueError, match=rf"backfill\.{field}"):
        normalize_backfill_execution_policy(raw)


@pytest.mark.parametrize("invalid", [None, 1, True, []])
def test_execution_policy_requires_string_state_schema(invalid: object) -> None:
    raw = _options()
    raw["state"] = {
        "backend": "audit_schema",
        "schema": invalid,
        "require_distributed_lock": True,
    }

    with pytest.raises(ValueError, match="backfill.state.schema"):
        normalize_backfill_execution_policy(raw)


def test_advisor_is_strict_non_execution_metadata() -> None:
    first = _options()
    second = _options()
    first["advisor"] = {"optimize_for": "speed"}
    second["advisor"] = {"optimize_for": "source_safety"}

    assert normalize_backfill_execution_policy(first).digest == normalize_backfill_execution_policy(second).digest
    assert validate_backfill_advisor_options(first) == {"optimize_for": "speed"}

    with pytest.raises(ValueError, match="backfill.advisor"):
        normalize_backfill_execution_policy({**_options(), "advisor": {"optimise_for": "speed"}})


def test_predicate_dialect_is_validated_compatibility_metadata_not_execution_authority() -> None:
    first = _options()
    second = _options()
    first["predicate_dialect"] = "postgres"
    second["predicate_dialect"] = "mssql"

    assert normalize_backfill_execution_policy(first).digest == normalize_backfill_execution_policy(second).digest


def test_execution_policy_digest_binds_state_retry_lease_and_chunk() -> None:
    baseline = normalize_backfill_execution_policy(_options()).digest
    mutations = []
    state = deepcopy(_options())
    state["state"]["schema"] = "BackfillState"  # type: ignore[index]
    mutations.append(state)
    retry = deepcopy(_options())
    retry["retry_policy"] = "failed_only"
    mutations.append(retry)
    lease = deepcopy(_options())
    lease["lease_ttl_minutes"] = 6
    mutations.append(lease)
    chunk = deepcopy(_options())
    chunk["chunk"]["to"] = 4  # type: ignore[index]
    mutations.append(chunk)

    assert all(normalize_backfill_execution_policy(value).digest != baseline for value in mutations)


def test_free_form_chunk_context_is_rejected_but_runtime_issued_scope_is_accepted() -> None:
    authored = _options()
    policy = normalize_backfill_execution_policy(authored)
    context, scope = _runtime_proof(policy)
    runtime_options = {**authored, "chunk_context": context}

    with pytest.raises(ValueError, match="runtime-issued"):
        normalize_backfill_execution_policy(runtime_options, operation_scope=scope)

    authority = issue_backfill_runtime_authority(policy=policy, chunk_context=context, operation_scope=scope)
    assert (
        normalize_backfill_execution_policy(
            runtime_options,
            operation_scope=scope,
            runtime_authority=authority,
        )
        == policy
    )
    assert BACKFILL_RUNTIME_AUTHORITY_OPTION.startswith("__dpone_")


@pytest.mark.parametrize("invalid", ["1", 1.0, True])
def test_runtime_chunk_proof_requires_exact_integer_index(invalid: object) -> None:
    policy = normalize_backfill_execution_policy(_options())
    context, scope = _runtime_proof(policy)
    scope["chunk_index"] = invalid

    with pytest.raises(ValueError, match="runtime chunk index must be a positive integer"):
        issue_backfill_runtime_authority(policy=policy, chunk_context=context, operation_scope=scope)


def test_shadow_append_authority_cannot_be_authored_or_retargeted() -> None:
    raw = _options()
    raw["inner_mode"] = "incremental_append"
    raw["publication"] = {"mode": "shadow_swap"}
    config = SimpleNamespace(
        options={"backfill": raw},
        target_database="DWH",
        target_schema="dbo",
        target_table="orders",
    )
    authority = issue_shadow_append_authority(
        config,
        run_key="campaign-a",
        live_table="orders",
        shadow_table="orders__dpone_initial_shadow",
    )
    config.options[SHADOW_APPEND_AUTHORITY_OPTION] = authority
    config.target_table = "orders__dpone_initial_shadow"

    assert require_shadow_append_authority(config) is authority

    config.target_table = "other_shadow"
    with pytest.raises(ValueError, match="does not match"):
        require_shadow_append_authority(config)

    config.target_table = "orders__dpone_initial_shadow"
    config.target_database = "other_database"
    with pytest.raises(ValueError, match="coordinates do not match"):
        require_shadow_append_authority(config)

    config.options[SHADOW_APPEND_AUTHORITY_OPTION] = {
        "run_key": "campaign-a",
        "shadow_table": "other_shadow",
    }
    with pytest.raises(ValueError, match="runtime-issued"):
        require_shadow_append_authority(config)
