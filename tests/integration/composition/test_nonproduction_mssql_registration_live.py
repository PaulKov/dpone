"""Nine real registration storage cases; inert originals do not prove signatures."""

from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from tests.integration.composition import nonproduction_mssql_registration_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.integration.composition.nonproduction_mssql_trust_live_support import (
    acquire_shared_transaction,
    require_sql_rejection,
)
from tests.nonproduction_authority_helpers import NOW, digest, limits, qualification

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_registration_schema import (
    REGISTRATION_COLUMNS,
    nonproduction_registration_trigger_sql,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.nonproduction_registration import NonproductionRegistrationOriginals
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
registration_case = support.registration_case

CASE_NAMES = (
    (
        "external_registration_ddl_and_catalog",
        "complete_binary_originals_survive_independent_readback",
        "append_only_and_invalid_direct_dml",
        "execution_replay_rebind_and_documentary_candidates",
        "qualification_consumes_once_without_run_rebind",
        "complete_campaign_membership_obeys_current_ceilings",
        "paged_history_audits_later_originals",
        "historical_reads_survive_current_trust_changes",
        "actual_ledger_identity_revision_and_clock_preconditions",
    ),
    (
        "concurrent_qualification_has_one_durable_consumption",
        "concurrent_execution_preserves_membership_ceiling",
        "partial_write_failure_rolls_back_complete_registration",
        "lost_commit_ack_reconciles_exact_durable_originals",
        "failed_commit_and_unavailable_readback_return_no_ack",
        "membership_corruption_and_overflow_fail_closed",
        "installed_catalog_drift_blocks_registration",
        "restricted_login_cannot_bypass_registration_storage",
        "session_options_reject_new_writes_without_mutation",
    ),
)


def expanded(grant, count):
    """Bounded complete storage parent: two original native and one ordinary."""
    extras = tuple(
        replace(grant.workloads[1], workload_id=f"b_extra_{index:02}", pack_sha256=digest(str(index)))
        for index in range(count - 3)
    )
    return replace(grant, workloads=tuple(sorted((*grant.workloads, *extras), key=lambda row: row.workload_id)))


def different_parent(grant):
    """Three valid current members whose union with the prior parent is four."""
    return replace(grant, workloads=(replace(grant.workloads[0], workload_id="aa_native"), *grant.workloads[1:]))


def raw_insert(case, row):
    return case.sql(
        f"INSERT INTO {case.table('grants')} ({', '.join(REGISTRATION_COLUMNS)}) "
        f"VALUES ({', '.join('?' for _ in row)});",
        *row,
    )


def originals_for(grant, policy, **changes):
    """Complete expected originals, shared by fixture-input and real-byte checks."""
    values = support.inputs(grant, policy) | changes
    return NonproductionRegistrationOriginals(
        values["grant_bytes"], values["signature_bundle"], values["signature_subject_bytes"], values["request_bytes"]
    )


def observe_denial(case, index, label, operation, codes):
    """Record each attempted operation before enforcing its unchanged denial codes."""
    payload = {"operation_index": index, "operation": label}
    try:
        operation()
        payload["unexpected_success"] = True
    except SqlFailure as error:
        if error.code is None:
            payload["unclassified_sql_error"] = True
        else:
            payload["sql_error"] = error.code
    finally:
        case.record("denial_" + str(index), payload)
    assert payload.get("sql_error") in codes, "unexpected_sql_rejection"
    return payload["sql_error"]


def storage_denials(case):
    """Fixed restricted-principal operations; no caller SQL or chosen objects."""
    return (
        ("update", f"UPDATE {case.table('grants')} SET schema_version=2;"),
        ("delete", f"DELETE FROM {case.table('memberships')};"),
        ("alter", f"ALTER TABLE {case.table('grants')} ADD forbidden int;"),
        ("truncate", f"TRUNCATE TABLE {case.table('memberships')};"),
        ("disable_trigger", f"DISABLE TRIGGER {case.trigger('grants')} ON {case.table('grants')};"),
        ("impersonate", "EXECUTE AS LOGIN=N'sa';"),
    )


def observe_storage_denials(case, connection):
    """Prove the fixed target is impersonatable before observing permission denial.

    SQL Server 2022 error15406 also covers missing/non-impersonatable principals.
    An independent administrator executes the exact target first; only the
    restricted impersonation operation may then accept that observed code.
    """
    rows = case.sql("EXECUTE AS LOGIN=N'sa'; SELECT CASE WHEN SUSER_SNAME()=N'sa' THEN 1 ELSE 0 END; REVERT;")
    allowed = rows == ((1,),) and type(rows[0][0]) is int
    case.record("impersonation_target", {"allowed": int(allowed)})
    assert allowed, "impersonation_target"
    denied = []
    for index, (label, statement) in enumerate(storage_denials(case), 1):
        codes = {15406} if label == "impersonate" else {229, 1088, 15151, 15247}
        denied.append(
            observe_denial(case, index, label, lambda statement=statement: execute(connection, statement), codes)
        )
    return denied


def require_history_cycle(case, original_policy, grant):
    """Refuse epoch rollback, then observe exact B-C-B policy bytes at that epoch."""
    newer, revision = case.policy, case.expected
    require_sql_rejection(lambda: case.append_policy(original_policy), {51000})
    assert case.trust.provider().read_revision() == revision and case.policy == newer
    case.append_policy(replace(newer, revoked_grant_ids=tuple(sorted((*newer.revoked_grant_ids, str(uuid4()))))))
    case.append_policy(newer)
    assert case.expected.snapshot == revision.snapshot and case.expected.revision == revision.revision + 2
    case.reject_read(grant, "trust_revision_changed", expected=revision)


def test_external_registration_ddl_and_catalog(registration_case):
    case = registration_case
    assert case.snapshot() == ((), ())
    case.require_schema()
    catalog = case.sql(
        "SELECT i.is_unique,i.has_filter,i.filter_definition FROM sys.indexes i "
        "WHERE i.object_id=OBJECT_ID(?) AND i.name='uq_np_qualification_run';",
        case.table("grants"),
    )
    assert catalog == ((True, True, "([phase]='qualification')"),), "filtered_predicate_catalog"
    modules = []
    for kind in ("grants", "memberships"):
        expected = nonproduction_registration_trigger_sql(case.schema, kind).encode("utf-16le")
        actual = case.sql(
            "SELECT HASHBYTES('SHA2_256',definition),DATALENGTH(definition) "
            "FROM sys.sql_modules WHERE object_id=OBJECT_ID(?);",
            case.trigger(kind),
        )
        assert actual == ((sha256(expected).digest(), len(expected)),), "installed_trigger_original"
        modules.append(actual[0][0])
    case.record("catalog", {"rows": 2, "digest": "sha256:" + sha256(b"".join(modules)).hexdigest()})


def test_complete_binary_originals_survive_independent_readback(registration_case):
    case = registration_case
    grant = case.grant()
    bundle = bytes(range(256)) * (8 * 1024 * 1024 // 256)
    first = case.register(grant, signature_bundle=bundle)
    assert first.originals == originals_for(grant, case.policy, signature_bundle=bundle)
    reopened = case.read(grant)
    assert reopened == first and reopened.originals.bundle_bytes == bundle
    row = case.original_row(grant.consumption_subject_sha256)
    originals = first.originals
    assert (row[7], row[9], row[11], row[13]) == (
        originals.grant_bytes,
        bundle,
        originals.signature_subject_bytes,
        originals.request_bytes,
    )
    for column, raw in zip(
        ("grant_document", "signature_bundle", "signature_subject_document", "request_document"),
        (row[7], row[9], row[11], row[13]),
        strict=True,
    ):
        assert case.sql(
            f"SELECT DATALENGTH({column}),HASHBYTES('SHA2_256',{column}) FROM {case.table('grants')} "
            "WHERE consumption_subject_sha256=?;",
            grant.consumption_subject_sha256,
        ) == ((len(raw), sha256(raw).digest()),)
    assert len(case.snapshot()[1]) == 3
    case.record("bytes", {"bytes": len(bundle), "digest": originals.bundle_sha256, "rows": 1, "members": 3})


def test_append_only_and_invalid_direct_dml(registration_case):
    case = registration_case
    grant = case.grant()
    case.register(grant)
    before, row = case.snapshot(), case.original_row(grant.consumption_subject_sha256)
    rejected = []
    for statement in (
        f"UPDATE {case.table('grants')} SET grant_sha256=grant_sha256;",
        f"DELETE FROM {case.table('grants')};",
        f"DELETE FROM {case.table('memberships')};",
    ):
        rejected.append(require_sql_rejection(lambda statement=statement: case.sql(statement), {51000}))
        assert case.snapshot() == before
    for index, value in ((8, "sha256:" + "0" * 64), (9, b""), (9, b"x" * (8 * 1024 * 1024 + 1)), (13, None)):
        changed = list(row)
        changed[index] = value
        rejected.append(require_sql_rejection(lambda: raw_insert(case, changed), {51000}))
        assert case.snapshot() == before
    require_sql_rejection(lambda: raw_insert(case, row), {2601, 2627})
    assert case.read(grant) is not None
    case.record("state", {"rows": 1, "members": 3, "denied": len(rejected) + 1})


def test_execution_replay_rebind_and_documentary_candidates(registration_case):
    case = registration_case
    grant = case.grant()
    first = case.register(grant)
    before = case.snapshot()
    assert case.register(grant) == first and case.snapshot() == before
    case.reject(grant, "registration_rebind", signature_bundle=b"different public bundle")
    changed = replace(grant, deployment_id=digest("different deployment"))
    case.reject(changed, "registration_rebind")
    other = case.grant(activation_id=grant.activation_id)
    second = case.register(other)
    assert case.read(grant) == first and case.read(other) == second
    assert (len(case.snapshot()[0]), len(case.snapshot()[1])) == (2, 3)
    assert case.sql(f"SELECT COUNT(*) FROM [{case.schema}].[composition_activations];") == ((0,),)
    case.record("state", {"rows": 2, "members": 3, "denied": 2})


def test_qualification_consumes_once_without_run_rebind(registration_case):
    case = registration_case
    grant = qualification(case.policy)
    original = case.register(grant)
    case.reject(grant, "qualification_consumed")
    case.reject(replace(grant, grant_id=str(uuid4())), "registration_subject_reuse")
    assert case.read(grant) == original and case.snapshot()[1] == ()
    row = case.original_row(grant.consumption_subject_sha256)
    assert row[13:15] == (None, None)
    case.record("state", {"rows": 1, "members": 0, "denied": 2})


def test_complete_campaign_membership_obeys_current_ceilings(registration_case):
    case = registration_case
    case.append_policy(replace(case.policy, limits=limits(max_workloads=3)))
    first = case.grant()
    case.register(first)
    more = different_parent(case.grant())
    before = case.snapshot()
    case.reject(more, "membership_budget")
    assert case.snapshot() == before
    case.append_policy(replace(case.policy, limits=limits(max_workloads=4)))
    more = expanded(case.grant(), 4)
    candidate = different_parent(case.grant())
    case.reject(replace(candidate, limits=limits(max_workloads=3)), "membership_budget")
    lower = replace(
        candidate, workloads=(replace(candidate.workloads[0], limits=limits(max_workloads=3)), *candidate.workloads[1:])
    )
    case.reject(lower, "membership_budget")
    case.register(more)
    assert len(case.snapshot()[1]) == 4 and set(before[1]).issubset(case.snapshot()[1])
    case.append_policy(replace(case.policy, limits=limits(max_workloads=3)))
    case.reject(case.grant(), "membership_budget")
    case.append_policy(replace(case.policy, limits=limits(max_workloads=64)))
    largest = expanded(case.grant(), 64)
    case.register(largest)
    assert case.read(largest) is not None and len(case.snapshot()[1]) == 64
    case.record("state", {"rows": 3, "members": 64, "denied": 4, "revision": case.expected.revision})


def test_paged_history_audits_later_originals(registration_case):
    case = registration_case
    grants = sorted((case.grant() for _ in range(5)), key=lambda grant: grant.consumption_subject_sha256)
    for index, grant in enumerate(grants):
        if index == 2:
            case.trust.append(2)
            case.expected = case.trust.provider().read_revision()
        case.register(grant)
    assert all(case.read(grant) is not None for grant in grants)
    key = grants[-1].consumption_subject_sha256
    original = case.original_row(key)[9]
    statement = f"UPDATE {case.table('grants')} SET signature_bundle=? WHERE consumption_subject_sha256=?;"
    with case.fault(
        lambda: case.rewrite("grants", statement, b"damaged later original", key),
        lambda: case.rewrite("grants", statement, original, key),
    ):
        case.reject_read(grants[0], "registration_identity")
    assert case.read(grants[0]) is not None
    case.record("history", {"rows": 5, "members": 3, "revision": 2, "denied": 1})


def test_historical_reads_survive_current_trust_changes(registration_case):
    case = registration_case
    first_policy = case.policy
    grant, qualified = case.grant(), qualification(case.policy)
    revoked_unused = str(uuid4())
    originals = (case.register(grant), case.register(qualified))
    case.append_policy(
        replace(
            first_policy,
            revocation_epoch=first_policy.revocation_epoch + 1,
            revoked_grant_ids=tuple(sorted((grant.grant_id, qualified.grant_id, revoked_unused))),
        )
    )
    case.reject(replace(case.grant(), grant_id=revoked_unused), "revoked")
    case.clock = lambda: NOW + timedelta(hours=2)
    assert (case.read(grant), case.read(qualified)) == originals
    case.reject(case.grant(), "expired_or_not_yet_valid")
    require_history_cycle(case, first_policy, grant)
    assert (case.read(grant), case.read(qualified)) == originals
    case.record("history", {"rows": 2, "members": 3, "revision": 4, "denied": 4})


def test_actual_ledger_identity_revision_and_clock_preconditions(registration_case):
    case = registration_case
    grant = case.grant()
    with closing(case.connect()) as connection, closing(connection.cursor()) as cursor:
        ledger = CompositionMssqlLedger(cursor, case.schema)
        for shared in (False, True):
            try:
                if shared:
                    result = acquire_shared_transaction(connection)
                    assert len(result) == 1 and result[0][0] in {0, 1}
                with pytest.raises(NonproductionAuthorityError, match="trust_ledger_lock"):
                    case.store().read_in(ledger, grant.consumption_subject_sha256, expected_revision=case.expected)
            finally:
                execute(connection, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;")
        states = execute(connection, "SELECT @@TRANCOUNT,XACT_STATE();")
        assert states == ((0, 0),)
        try:
            ledger.begin(case.service_id)
            foreign = case.store(provider=case.trust.provider(environment_id=str(uuid4())))
            with pytest.raises(NonproductionAuthorityError):
                foreign.read_in(ledger, grant.consumption_subject_sha256, expected_revision=case.expected)
        finally:
            execute(connection, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;")
    actual_service = case.service_id
    try:
        case.service_id = str(uuid4())
        with pytest.raises(CompositionAdmissionError, match="control_authority"):
            case.register(grant)
    finally:
        case.service_id = actual_service
    actual_revision = case.expected
    try:
        case.expected = replace(actual_revision, revision=actual_revision.revision + 1)
        case.reject(grant, "trust_revision_changed")
    finally:
        case.expected = actual_revision
    times = iter((NOW, NOW - timedelta(seconds=1)))
    case.clock = lambda: next(times)
    case.reject(grant, "registration_clock")
    assert case.snapshot() == ((), ())
    case.record(
        "transaction",
        {"rows": 0, "members": 0, "denied": 6, "transaction_count": states[0][0], "xact_state": states[0][1]},
    )
