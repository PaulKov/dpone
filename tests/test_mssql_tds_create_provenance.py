"""Real supervisor algorithm over synthetic Harness ports; not live authority."""

from copy import deepcopy
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.app.mssql_tds_create_provenance import TdsCoordinatorCreateProvenance, validate_create_provenance
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind as Kind
from dpone.contracts.mssql_tds_coordinator_evidence import decode_local_exit
from tests.test_mssql_tds_coordinator_supervisor import Harness


def components():
    h = Harness()
    outcome = h.run()
    p = TdsCoordinatorCreateProvenance(
        request=h.request,
        admission=h.admission,
        startup=h.startup,
        authority=h.authority,
        grant=h.grant,
        local_proof=decode_local_exit(h.saved[Kind.LOCAL_EXIT].payload),
        snapshot=h.current,
    )
    return h, deepcopy(outcome), deepcopy(p)


def validate(h, o, p, **changes):
    kwargs = dict(
        response=o.response,
        local_exit=o.local_exit,
        receipts=o.receipts,
        request=h.request,
        identity=h.identity,
        ownership=h.owner,
    )
    kwargs.update(changes)
    validate_create_provenance(p, **kwargs)


def test_actual_synthetic_supervisor_components_match_all_six_producers():
    h, o, p = components()
    validate(h, o, p)
    assert {r.kind for r in o.receipts} == set(Kind)
    assert h.events[-2:] == ["evidence.close", "writer.close"]


@pytest.mark.parametrize("kind", list(Kind))
@pytest.mark.parametrize(
    "field,bad",
    [("byte_count", 1), ("payload_sha256", "0" * 64), ("operation_sha256", "1" * 64), ("relative_name", "forged.json")],
)
def test_each_receipt_exact_fields(kind, field, bad):
    h, o, p = components()
    receipt = next(r for r in o.receipts if r.kind is kind)
    object.__setattr__(receipt, field, bad)
    with pytest.raises(ValueError, match=r"^mssql_native.tds_create_provenance_invalid$"):
        validate(h, o, p)


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "list", "float", "kind_alias"])
def test_receipt_cardinality_types_and_equal_value_aliases(mutation):
    h, o, p = components()
    receipts = o.receipts
    if mutation == "duplicate":
        receipts = (receipts[0],) * 6
    elif mutation == "missing":
        receipts = receipts[:-1]
    elif mutation == "list":
        receipts = list(receipts)
    elif mutation == "float":
        object.__setattr__(receipts[0], "byte_count", float(receipts[0].byte_count))
    else:
        object.__setattr__(receipts[0], "kind", receipts[0].kind.value)
    with pytest.raises(ValueError):
        validate(h, o, p, receipts=receipts)


@pytest.mark.parametrize(
    "field,bad", [("owner", "different"), ("supervisor_id", "55555555-5555-4555-8555-555555555555"), ("fence", 3)]
)
def test_full_original_ownership_not_fence_only(field, bad):
    h, o, p = components()
    with pytest.raises(ValueError):
        validate(h, o, p, ownership=replace(h.owner, **{field: bad}))


@pytest.mark.parametrize("field", ["slot_index", "operation_id", "command_sha256", "implementation_sha256"])
def test_original_operation_identity(field):
    h, o, p = components()
    values = dict(
        slot_index=1,
        operation_id=UUID("55555555-5555-4555-8555-555555555555"),
        command_sha256="0" * 64,
        implementation_sha256="0" * 64,
    )
    with pytest.raises(ValueError):
        validate(h, o, p, identity=replace(h.identity, **{field: values[field]}))


def test_original_request_and_nonzero_exit():
    h, o, p = components()
    with pytest.raises(ValueError):
        validate(h, o, p, request=replace(h.request, object_nonce=UUID("55555555-5555-4555-8555-555555555555")))
    for changes in ({"exit_code": 1}, {"reaped": False}, {"identity": replace(o.local_exit.identity, pid=999)}):
        with pytest.raises(ValueError):
            validate(h, o, p, local_exit=replace(o.local_exit, **changes))


@pytest.mark.parametrize(
    "path,field,bad",
    [
        (("request", "parent"), "ordinal", 0.0),
        (("request",), "object_nonce", "55555555-5555-4555-8555-555555555555"),
        (("startup", "process"), "pid", True),
        (("authority", "session"), "session_id", True),
        (("authority", "database"), "database_guid", "55555555-5555-4555-8555-555555555555"),
        (("authority", "execution_owner"), "fence", True),
        (("grant",), "grant_id", "55555555-5555-4555-8555-555555555555"),
        (("grant", "ownership"), "fence", True),
        (("local_proof", "exit"), "reaped", 1),
        (("snapshot",), "revision", 1.0),
        (("snapshot", "state"), "sequence", 6.0),
        (("snapshot", "state", "identity"), "command", "create"),
        (("snapshot", "state", "identity", "parent"), "attempt", False),
    ],
)
def test_forged_nested_values_before_normalization(path, field, bad):
    h, o, p = components()
    nested = p
    for name in path:
        nested = getattr(nested, name)
    object.__setattr__(nested, field, bad)
    with pytest.raises(ValueError, match=r"^mssql_native.tds_create_provenance_invalid$"):
        validate(h, o, p)


@pytest.mark.parametrize("field", ["authentication_sha256", "authority_sha256", "local", "result", "error", "remote"])
def test_final_snapshot_exact_successful_state(field):
    h, o, p = components()
    state = p.snapshot.state
    bad = None if field in ("local", "result") else "0" * 64
    object.__setattr__(state, field, bad)
    with pytest.raises(ValueError):
        validate(h, o, p)


def test_startup_grant_authority_and_local_proof_binding():
    for change in ("startup_nonce", "startup_source", "proof_hash", "admission_space"):
        h, o, p = components()
        with pytest.raises(ValueError):
            if change == "startup_nonce":
                p = replace(p, startup=replace(p.startup, launch_nonce=b"z" * 32))
            elif change == "startup_source":
                p = replace(p, startup=replace(p.startup, implementation_sha256="0" * 64))
            elif change == "proof_hash":
                p = replace(p, local_proof=replace(p.local_proof, authority_sha256="0" * 64))
            else:
                p = replace(p, admission=p.admission + b" ")
            validate(h, o, p)


def test_authority_hash_domains_not_interchangeable():
    h, o, p = components()
    with pytest.raises(ValueError):
        validate(
            h,
            o,
            replace(p, local_proof=replace(p.local_proof, authority_sha256=p.authority.session.authority_sha256.hex())),
        )


def test_lock_string_alias_and_secret_canary_redacted():
    class TextAlias(str):
        pass

    h, o, p = components()
    object.__setattr__(p.authority.lock, "resource", TextAlias(p.authority.lock.resource))
    with pytest.raises(ValueError, match=r"^mssql_native.tds_create_provenance_invalid$"):
        validate(h, o, p)
    h, o, p = components()
    object.__setattr__(p, "admission", b'{"password":"private-canary"}')
    with pytest.raises(ValueError, match=r"^mssql_native.tds_create_provenance_invalid$"):
        validate(h, o, p)


def test_response_nested_alias_rejected_before_evidence_hash(monkeypatch):
    from dpone.app import mssql_tds_coordinator_request as codec

    h, outcome, provenance = components()
    object.__setattr__(outcome.response.evidence.columns[0], "ordinal", 1.0)
    calls = []

    def forbidden(value):
        calls.append(value)
        raise AssertionError("nested value reached hashing")

    monkeypatch.setattr(codec, "create_evidence_digest", forbidden)
    with pytest.raises(ValueError, match="tds_create_provenance_invalid"):
        validate(h, outcome, provenance)
    assert not calls


def test_snapshot_nested_alias_rejected_before_state_hash(monkeypatch):
    from dpone.contracts import mssql_tds_coordinator as coordinator

    h, outcome, provenance = components()
    object.__setattr__(provenance.snapshot.state.identity, "slot_index", 0.0)
    calls = []

    def forbidden(value):
        calls.append(value)
        raise AssertionError("nested value reached hashing")

    monkeypatch.setattr(coordinator, "coordinator_identity_digest", forbidden)
    with pytest.raises(ValueError, match="tds_create_provenance_invalid"):
        validate(h, outcome, provenance)
    assert not calls
