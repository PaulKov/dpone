"""SQL ledger doubles are neither live SQL nor authenticated grant evidence."""

from dataclasses import replace
from datetime import timedelta

import pytest

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_registration import MssqlNonproductionRegistrationStore
from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider, NonproductionTrustRevision
from dpone.contracts.nonproduction_registration import signature_subject_bytes
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.nonproduction_authority_helpers import NOW, execution, identifier, limits, qualification
from tests.nonproduction_signature_helpers import BUNDLE, github_policy, trust
from tests.test_nonproduction_activation import request
from tests.test_nonproduction_mssql_registration_schema import SCHEMA, catalog
from tests.test_nonproduction_mssql_schema import catalog as trust_catalog

SERVICE = identifier(90)


class LedgerDouble:
    """Records DBAPI statements; SQL trigger/transaction behavior is not simulated."""

    def __init__(self, policy=None):
        self.snapshots = {1: trust(policy or github_policy())}
        self.revision = 1
        self.grants = []
        self.members = []
        self.commands = []
        self.result = []
        self.fail = None
        self.lock = (1, 1, "Exclusive")
        self.service = SERVICE
        self.history_revision = None
        self.options = 4472

    @property
    def expected(self):
        return NonproductionTrustRevision(
            SERVICE, github_policy().environment_id, self.revision, self.snapshots[self.revision]
        )

    def execute(self, sql, *parameters):
        self.commands.append((sql, parameters))
        if self.fail and self.fail(sql):
            raise RuntimeError("secret database error")
        if "APPLOCK_MODE" in sql:
            self.result = [self.lock]
        elif sql == "SELECT @@OPTIONS;":
            self.result = [(self.options,)]
        elif "composition_authority]" in sql:
            self.result = [(1, 1, self.service)]
        elif "sys." in sql:
            target = parameters[0]
            kind = "grants" if target.endswith("_grants]") else "memberships"
            is_trust = target.endswith("_trust]")
            sections = [
                "temporal_type",
                "sys.columns c WHERE",
                "sys.indexes i",
                "sys.objects WHERE",
                "sys.foreign_keys",
                "sys.triggers t",
                "sys.trigger_events",
                "sys.security_predicates",
            ]
            index = next(i for i, marker in enumerate(sections) if marker in sql)
            self.result = trust_catalog()[index - (1 if index > 4 else 0)] if is_trust else catalog(kind)[index]
        elif "FROM [dpone_control].[composition_nonproduction_trust]" in sql:
            snapshot = self.snapshots[self.revision if "TOP (1)" in sql else parameters[1]]
            values = (
                1,
                snapshot.policy_bytes,
                snapshot.policy_sha256,
                snapshot.verifier_policy_bytes,
                snapshot.verifier_policy_sha256,
                snapshot.current_revocation_epoch,
            )
            if "TOP (1)" in sql:
                self.result = [(github_policy().environment_id, self.revision, *values)]
            else:
                self.result = [(github_policy().environment_id, self.history_revision or parameters[1], *values)]
        elif sql.startswith("INSERT"):
            (self.grants if "nonproduction_grants]" in sql else self.members).append(tuple(parameters))
            self.result = []
        elif "nonproduction_grants]" in sql:
            if sql.startswith("SELECT TOP (1) consumption_subject_sha256"):
                candidates = sorted(
                    row[0]
                    for row in self.grants
                    if (row[3], row[4], row[2]) == parameters[:3] and (len(parameters) == 3 or row[0] > parameters[3])
                )
                self.result = [(candidates[0],)] if candidates else []
            elif "WHERE consumption_subject_sha256 = ?" in sql:
                self.result = [row for row in self.grants if row[0] == parameters[0]]
            elif "WHERE phase = ?" in sql:
                self.result = [row for row in self.grants if (row[2], row[5]) == parameters]
            else:
                self.result = [row for row in self.grants if (row[3], row[4], row[2]) == parameters]
        elif "nonproduction_memberships]" in sql:
            self.result = [row for row in self.members if row[:3] == parameters]
        else:
            pytest.fail("unexpected SQL statement: " + sql)
        return self

    def fetchall(self):
        return list(self.result)

    def fetchone(self):
        assert len(self.result) <= 1
        return self.result[0] if self.result else None

    def commit(self):
        pytest.fail("registration cannot commit")

    def rollback(self):
        pytest.fail("registration cannot rollback")

    def close(self):
        pytest.fail("registration does not own the cursor")


def setup(policy=None, clock=lambda: NOW):
    database = LedgerDouble(policy)
    provider = MssqlNonproductionTrustProvider(
        lambda: pytest.fail("no connection"),
        expected_service_id=SERVICE,
        expected_environment_id=github_policy().environment_id,
    )
    return (
        database,
        CompositionMssqlLedger(database, SCHEMA),
        MssqlNonproductionRegistrationStore(provider, clock=clock),
    )


def inputs(grant, policy):
    values = dict(
        grant_bytes=grant.to_bytes(),
        signature_bundle=BUNDLE,
        signature_subject_bytes=signature_subject_bytes(grant.signature_subject(policy)),
    )
    if grant.phase == "execution":
        values["request_bytes"] = request(grant).to_bytes()
    return values


def register(store, ledger, database, grant=None, policy=None):
    policy = policy or github_policy()
    grant = grant or execution(policy)
    return store.register_execution_in(ledger, **inputs(grant, policy), expected_revision=database.expected)


def test_execution_inserts_complete_membership_once_and_rereads_every_original():
    database, ledger, store = setup()
    first = register(store, ledger, database)
    assert len(database.grants) == 1 and len(database.members) == 3
    count = sum(sql.startswith("INSERT") for sql, _ in database.commands)
    assert register(store, ledger, database) == first
    assert sum(sql.startswith("INSERT") for sql, _ in database.commands) == count
    assert (
        store.read_in(
            ledger, execution(github_policy()).consumption_subject_sha256, expected_revision=database.expected
        )
        == first
    )


def test_qualification_consumption_is_once_and_separate_read_is_documentary():
    database, ledger, store = setup()
    grant = qualification(github_policy())
    values = inputs(grant, github_policy())
    first = store.consume_qualification_in(ledger, **values, expected_revision=database.expected)
    assert not database.members
    with pytest.raises(NonproductionAuthorityError, match="qualification_consumed"):
        store.consume_qualification_in(ledger, **values, expected_revision=database.expected)
    assert store.read_in(ledger, grant.consumption_subject_sha256, expected_revision=database.expected) == first
    changed = replace(grant, grant_id=identifier(89))
    with pytest.raises(NonproductionAuthorityError, match="registration_subject_reuse"):
        store.consume_qualification_in(ledger, **inputs(changed, github_policy()), expected_revision=database.expected)


@pytest.mark.parametrize("field", ["signature_bundle", "signature_subject_bytes", "grant_bytes", "request_bytes"])
def test_rebinding_any_original_rejects_without_appending(field):
    database, ledger, store = setup()
    register(store, ledger, database)
    grant = execution(github_policy())
    values = inputs(grant, github_policy())
    if field == "grant_bytes":
        grant = replace(grant, qualified_set_sha256="sha256:" + "b" * 64)
        values = inputs(grant, github_policy())
    elif field == "request_bytes":
        values[field] = replace(request(grant), source_subject_sha256="sha256:" + "b" * 64).to_bytes()
    else:
        values[field] += b" changed"
    with pytest.raises(NonproductionAuthorityError):
        store.register_execution_in(ledger, **values, expected_revision=database.expected)
    assert len(database.grants) == 1 and len(database.members) == 3


def test_other_grants_and_activations_retain_campaign_union_and_current_lower_ceiling():
    policy = github_policy(limits=limits(max_workloads=3))
    database, ledger, store = setup(policy)
    register(store, ledger, database, policy=policy)
    grant = execution(policy, grant_id=identifier(87), activation_id=identifier(86))
    grant = replace(grant, workloads=(replace(grant.workloads[0], workload_id="aa_native"), *grant.workloads[1:]))
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        register(store, ledger, database, grant, policy)
    assert len(database.members) == 3 and len(database.grants) == 1
    higher = github_policy(limits=limits(max_workloads=4))
    database.snapshots[2], database.revision = trust(higher), 2
    grant = execution(higher, grant_id=grant.grant_id, activation_id=grant.activation_id, workloads=grant.workloads)
    register(store, ledger, database, grant, higher)
    assert len(database.members) == 4 and len(database.grants) == 2


@pytest.mark.parametrize("index", range(17))
def test_every_stored_registration_field_is_checked(index):
    database, ledger, store = setup()
    first = register(store, ledger, database)
    changed = list(database.grants[0])
    changed[index] = b"changed" if isinstance(changed[index], bytes) else "changed"
    if index == 0:
        changed[index] = execution(github_policy()).consumption_subject_sha256
        changed[1] = identifier(22)
    database.grants[0] = tuple(changed)
    with pytest.raises(NonproductionAuthorityError):
        store.read_in(ledger, first.originals.grant.consumption_subject_sha256, expected_revision=database.expected)


@pytest.mark.parametrize("change", ["missing", "extra", "owner", "document", "digest", "pool"])
def test_complete_membership_is_reopened_and_corruption_rejects(change):
    database, ledger, store = setup()
    record = register(store, ledger, database)
    if change == "missing":
        database.members.pop()
    elif change == "extra":
        database.members.append(database.members[0])
    else:
        row = list(database.members[0])
        index = {"owner": 5, "document": 4, "digest": 3, "pool": 1}[change]
        row[index] = b"changed" if index == 4 else "sha256:" + "a" * 64 if index in {3, 5} else identifier(78)
        database.members[0] = tuple(row)
    with pytest.raises(NonproductionAuthorityError, match="registration_membership"):
        store.read_in(ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected)


@pytest.mark.parametrize("change", ["service", "lock", "revision", "clock", "expired", "revoked", "session"])
def test_current_protected_trust_time_and_ownership_preconditions(change):
    clock = (
        (lambda: None)
        if change == "clock"
        else (lambda: NOW + timedelta(hours=2))
        if change == "expired"
        else lambda: NOW
    )
    database, ledger, store = setup(clock=clock)
    expected = database.expected
    if change == "service":
        database.service = identifier(91)
    elif change == "lock":
        database.lock = (1, -1, "Shared")
    elif change == "revision":
        database.snapshots[2], database.revision = trust(), 2
    elif change == "session":
        database.options = 0
    elif change == "revoked":
        value = github_policy(revoked_grant_ids=(execution(github_policy()).grant_id,))
        database.snapshots[1] = trust(value)
        expected = database.expected
    with pytest.raises(NonproductionAuthorityError):
        store.register_execution_in(
            ledger, **inputs(execution(github_policy()), github_policy()), expected_revision=expected
        )
    assert not database.grants and not database.members


def test_expired_historical_execution_and_consumed_qualification_remain_readable():
    instants = [NOW]
    database, ledger, store = setup(clock=lambda: instants[0])
    record = register(store, ledger, database)
    instants[0] = NOW + timedelta(days=2)
    assert register(store, ledger, database) == record
    assert (
        store.read_in(ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected)
        == record
    )


@pytest.mark.parametrize("statement", ["SELECT", "INSERT", "MEMBERSHIP"])
def test_sql_failure_is_sanitized_and_never_replayed_or_committed(statement):
    database, ledger, store = setup()
    database.fail = lambda sql: (
        sql.startswith("INSERT") and "nonproduction_memberships]" in sql
        if statement == "MEMBERSHIP"
        else sql.startswith(statement)
    )
    with pytest.raises(NonproductionAuthorityError) as error:
        register(store, ledger, database)
    assert "secret" not in str(error.value) and error.value.__suppress_context__
    assert sum(database.fail(sql) for sql, _ in database.commands) == 1
    if statement == "MEMBERSHIP":
        assert len(database.grants) == 1 and not database.members  # Caller must roll back uncommitted state.


def test_nullable_request_projection_distinguishes_oversize_from_absent():
    database, ledger, store = setup()
    register(store, ledger, database)
    reads = [sql for sql, _ in database.commands if sql.startswith("SELECT") and "request_document" in sql]
    assert reads
    assert all("CASE WHEN request_document IS NULL THEN NULL" in sql and "ELSE 0x END" in sql for sql in reads)


def test_original_maximum_bundle_is_persisted_without_base64_or_truncation():
    database, ledger, store = setup()
    values = inputs(execution(github_policy()), github_policy())
    values["signature_bundle"] = b"original" * (1024 * 1024)
    record = store.register_execution_in(ledger, **values, expected_revision=database.expected)
    assert database.grants[0][9] == record.originals.bundle_bytes == values["signature_bundle"]


def test_current_policy_revocation_keeps_old_original_history_and_blocks_new_use():
    database, ledger, store = setup()
    first = register(store, ledger, database)
    changed = github_policy(revoked_grant_ids=(first.originals.grant.grant_id,))
    database.snapshots[2], database.revision = trust(changed), 2
    assert (
        store.read_in(ledger, first.originals.grant.consumption_subject_sha256, expected_revision=database.expected)
        == first
    )
    values = inputs(execution(changed), changed)
    with pytest.raises(NonproductionAuthorityError, match="registration_rebind"):
        store.register_execution_in(ledger, **values, expected_revision=database.expected)


def test_smaller_current_membership_ceiling_blocks_new_registration_without_reset():
    database, ledger, store = setup()
    register(store, ledger, database)
    changed = github_policy(limits=limits(max_workloads=3))
    database.snapshots[2], database.revision = trust(changed), 2
    grant = execution(changed, grant_id=identifier(83), activation_id=identifier(84))
    grant = replace(grant, workloads=(replace(grant.workloads[0], workload_id="aa_native"), *grant.workloads[1:]))
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        register(store, ledger, database, grant, changed)
    assert len(database.members) == 3 and len(database.grants) == 1


def test_every_workloads_lower_ceiling_applies_to_complete_retained_union():
    database, ledger, store = setup()
    register(store, ledger, database)
    grant = execution(github_policy(), grant_id=identifier(83), activation_id=identifier(84))
    grant = replace(
        grant,
        workloads=(
            replace(grant.workloads[0], workload_id="aa_native", limits=limits(max_workloads=3)),
            *grant.workloads[1:],
        ),
    )
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        register(store, ledger, database, grant)


@pytest.mark.parametrize("clock", [lambda: NOW.replace(tzinfo=None), lambda: "now", lambda: None])
def test_missing_or_nonindependent_clock_never_appends(clock):
    database, ledger, store = setup(clock=clock)
    with pytest.raises(NonproductionAuthorityError, match="registration_clock"):
        register(store, ledger, database)
    assert not database.grants


@pytest.mark.parametrize("end", [NOW - timedelta(seconds=1), NOW + timedelta(hours=1)])
def test_clock_rollback_or_expiry_during_preparation_prevents_append(end):
    clocks = iter((NOW, end))
    database, ledger, store = setup(clock=lambda: next(clocks))
    with pytest.raises(NonproductionAuthorityError):
        register(store, ledger, database)
    assert not database.grants


def test_unknown_missing_readback_retains_uncommitted_originals_without_returning_receipt():
    database, ledger, store = setup()
    database.fail = lambda sql: bool(database.grants) and sql.startswith("SELECT")
    with pytest.raises(NonproductionAuthorityError, match="registration_unavailable"):
        register(store, ledger, database)
    assert len(database.grants) == 1 and len(database.members) == 3


def test_all_pool_grants_are_audited_not_only_requested_registration():
    database, ledger, store = setup()
    first = register(store, ledger, database)
    second = execution(github_policy(), grant_id=identifier(83), activation_id=identifier(84))
    register(store, ledger, database, second)
    row = list(database.grants[1])
    row[9] = b"changed stored bundle"
    database.grants[1] = tuple(row)
    with pytest.raises(NonproductionAuthorityError, match="registration_identity"):
        store.read_in(ledger, first.originals.grant.consumption_subject_sha256, expected_revision=database.expected)


def test_wrong_historical_revision_metadata_cannot_hide_equal_policy_bytes():
    database, ledger, store = setup()
    record = register(store, ledger, database)
    database.snapshots[2], database.revision = trust(), 2
    database.history_revision = 99
    with pytest.raises(NonproductionAuthorityError, match="registration_history"):
        store.read_in(ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected)


def test_distinct_execution_registrations_can_name_one_activation_without_selecting_it():
    database, ledger, store = setup()
    first = register(store, ledger, database)
    second_grant = execution(github_policy(), grant_id=identifier(93))
    second = register(store, ledger, database, second_grant)
    assert first.originals.request_sha256 != second.originals.request_sha256
    assert len(database.grants) == 2 and len(database.members) == 3
    for record in (first, second):
        assert (
            store.read_in(
                ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected
            )
            == record
        )


def test_history_is_keyset_paged_with_nested_old_trust_reads_and_bounded_original_fetches():
    database, ledger, store = setup()
    first = register(store, ledger, database)
    database.snapshots[2], database.revision = trust(), 2
    register(store, ledger, database, execution(github_policy(), grant_id=identifier(92), activation_id=identifier(93)))
    database.commands.clear()
    assert (
        store.read_in(ledger, first.originals.grant.consumption_subject_sha256, expected_revision=database.expected)
        == first
    )
    pages = [(sql, parameters) for sql, parameters in database.commands if sql.startswith("SELECT TOP (1) consumption")]
    assert len(pages) == 3
    assert [None if len(parameters) == 3 else parameters[-1] for _, parameters in pages] == [
        None,
        *sorted(row[0] for row in database.grants),
    ]
    for sql, _ in database.commands:
        if sql.startswith("SELECT") and "FROM [dpone_control].[composition_nonproduction_grants]" in sql:
            assert "TOP (1)" in sql or "TOP (2)" in sql


def test_sixty_fifth_membership_is_an_explicit_overflow_not_truncated_success():
    database, ledger, store = setup()
    record = register(store, ledger, database)
    database.members = database.members[:1] * 65
    with pytest.raises(NonproductionAuthorityError, match="registration_membership_overflow"):
        store.read_in(ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected)


@pytest.mark.parametrize("key", ["", " ", "\t"])
def test_first_history_page_does_not_skip_malformed_keys(key):
    database, ledger, store = setup()
    first = register(store, ledger, database)
    register(store, ledger, database, execution(github_policy(), grant_id=identifier(94)))
    row = list(database.grants[1])
    row[0] = key
    database.grants[1] = tuple(row)
    with pytest.raises(NonproductionAuthorityError):
        store.read_in(ledger, first.originals.grant.consumption_subject_sha256, expected_revision=database.expected)
