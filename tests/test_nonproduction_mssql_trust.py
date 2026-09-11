"""Fresh SQL read/CAS boundary doubles, not live trust or authentication proof."""

import json
from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider, NonproductionTrustRevision
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.nonproduction_authentication import NonproductionGrantAuthenticator
from tests.composition_mssql_catalog_helpers import catalog_observation, install_offline_catalog_references
from tests.nonproduction_authority_helpers import NOW, identifier, qualification
from tests.nonproduction_signature_helpers import SignatureDouble, github_policy, qualification_inputs, sha256, trust
from tests.test_nonproduction_mssql_schema import SCHEMA, catalog

SERVICE = identifier(20)
ENVIRONMENT = github_policy().environment_id


def record(snapshot=None, revision=1):
    value = snapshot or trust()
    return (
        ENVIRONMENT,
        revision,
        1,
        value.policy_bytes,
        value.policy_sha256,
        value.verifier_policy_bytes,
        value.verifier_policy_sha256,
        value.current_revocation_epoch,
    )


def reads(values=None):
    return [*catalog(), [record() if values is None else values]]


@pytest.fixture(autouse=True)
def offline_catalog(monkeypatch):
    install_offline_catalog_references(monkeypatch)


class Connection:
    def __init__(self, values=None):
        self.answers = reads(values)
        self.lock = (1, 1, "Exclusive")
        self.transaction_id = 7
        self.authority = [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE)]
        self.lock_result = 0
        self.rows = None
        self.commands = []
        self.events = []
        self.autocommit = True
        self.fail = None

    def cursor(self):
        self.events.append("cursor")
        return self

    def execute(self, sql, *parameters):
        self.commands.append((sql, parameters))
        assert not sql.startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "EXEC(N'"))
        if self.fail == "execute":
            raise RuntimeError("secret SQL connection")
        self.rows = catalog_observation(sql, parameters)
        if "APPLOCK_MODE" in sql:
            self.rows = [(*self.lock, self.transaction_id)] if "CURRENT_TRANSACTION_ID" in sql else [self.lock]
        elif "composition_authority] WITH" in sql:
            self.rows = self.authority
        elif "sp_getapplock" in sql:
            self.rows = [(self.lock_result,)]
        return self

    def fetchone(self):
        values = self.fetchall()
        assert len(values) <= 1
        return values[0] if values else None

    def fetchall(self):
        values = self.answers.pop(0) if self.rows is None else self.rows
        self.rows = None
        return values

    def commit(self):
        self.events.append("commit")
        if self.fail == "commit":
            raise RuntimeError("secret commit ACK")

    def rollback(self):
        self.events.append("rollback")
        if self.fail == "rollback":
            raise RuntimeError("secret rollback")

    def close(self):
        self.events.append("close")
        if self.fail == "close":
            raise RuntimeError("secret cleanup")


def provider(factory):
    return MssqlNonproductionTrustProvider(factory, expected_service_id=SERVICE, expected_environment_id=ENVIRONMENT)


def test_each_read_uses_fresh_protected_transaction_and_preserves_snapshot_api():
    connections = []

    def connect():
        value = Connection()
        connections.append(value)
        return value

    value = provider(connect)
    assert value.read() == trust()
    assert value.read_revision() == NonproductionTrustRevision(SERVICE, ENVIRONMENT, 1, trust())
    assert len(connections) == 2
    for connection in connections:
        assert not connection.answers and connection.autocommit is False
        assert connection.events == ["cursor", "commit", "close", "close"]
        sql, params = next(item for item in connection.commands if "ORDER BY revision DESC" in item[0])
        assert "TOP (1)" in sql and "ORDER BY revision DESC" in sql and "HOLDLOCK" in sql
        assert params == (ENVIRONMENT,)


def test_same_ledger_comparison_never_connects_or_commits():
    def forbidden():
        pytest.fail("same-ledger read cannot open a second connection")

    connection = Connection()
    connection.answers = reads()
    observed = provider(forbidden).read_revision_in(CompositionMssqlLedger(connection, SCHEMA))
    connection.answers = reads()
    assert provider(forbidden).require_revision_in(CompositionMssqlLedger(connection, SCHEMA), observed) == observed
    assert connection.events == []


@pytest.mark.parametrize("changed", ["revision", "policy", "verifier", "epoch", "service", "environment"])
def test_private_revision_comparison_rejects_any_changed_authority(changed):
    value = provider(lambda: Connection())
    expected = value.read_revision()
    changes = {
        "revision": {"revision": 3},
        "policy": {
            "snapshot": trust(replace(github_policy(), source_repository="https://example.invalid/test/changed"))
        },
        "verifier": {"snapshot": trust(verifier_policy_bytes=b"{}", verifier_policy_sha256=sha256(b"{}"))},
        "epoch": {"snapshot": trust(github_policy(revocation_epoch=3))},
        "service": {"service_id": identifier(21)},
        "environment": {"environment_id": identifier(22)},
    }
    connection = Connection()
    connection.answers = reads()
    with pytest.raises(NonproductionAuthorityError):
        value.require_revision_in(CompositionMssqlLedger(connection, SCHEMA), replace(expected, **changes[changed]))


@pytest.mark.parametrize(
    "position,value",
    [
        (0, identifier(90)),
        (1, 0),
        (1, True),
        (1, "1"),
        (1, 2**63),
        (2, 2),
        (3, b" "),
        (3, bytearray(b"x")),
        (3, b"x" * 1048577),
        (4, "sha256:" + "0" * 64),
        (5, b"changed"),
        (6, "sha256:" + "0" * 64),
        (7, True),
        (7, -1),
        (7, 3),
    ],
)
def test_wrong_original_bytes_identity_epoch_or_revision_reject(position, value):
    values = list(record())
    values[position] = value
    with pytest.raises(NonproductionAuthorityError):
        provider(lambda: Connection(tuple(values))).read()


@pytest.mark.parametrize("values", [(), (None,), tuple(range(9))])
def test_wrong_row_shape_rejects(values):
    with pytest.raises(NonproductionAuthorityError):
        provider(lambda: Connection(values)).read()


@pytest.mark.parametrize(
    "section,value",
    [
        ("lock", ()),
        ("lock", (0, 0, "NoLock")),
        ("lock", (1, -1, "Exclusive")),
        ("lock", (1, 1, "Shared")),
        ("authority", [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, identifier(90))]),
        ("enrollment", []),
    ],
)
def test_same_ledger_requires_existing_lock_service_and_enrollment(section, value):
    connection = Connection()
    connection.answers = reads()
    if section == "enrollment":
        connection.answers[-1] = value
    else:
        setattr(connection, section, value)
    with pytest.raises(NonproductionAuthorityError) as caught:
        provider(lambda: connection).read_revision_in(CompositionMssqlLedger(connection, SCHEMA))
    if section == "lock":
        assert caught.value.reason == "trust_ledger_lock"
        assert len(connection.commands) == 1
    assert not connection.events


@pytest.mark.parametrize("failure", ["execute", "commit"])
def test_unavailable_or_uncertain_read_does_not_return_or_replay(failure):
    connection = Connection()
    connection.fail = failure
    with pytest.raises(NonproductionAuthorityError) as caught:
        provider(lambda: connection).read()
    assert "secret" not in str(caught.value) and caught.value.__suppress_context__
    assert "rollback" in connection.events and connection.events.count("close") == 2


def test_cleanup_failure_preserves_read_result_without_claiming_connection_closure():
    connection = Connection()
    connection.fail = "close"
    assert provider(lambda: connection).read() == trust()
    assert connection.events.count("close") == 2


def test_connection_creation_failure_is_sanitized():
    def fail():
        raise RuntimeError("secret credentials")

    with pytest.raises(NonproductionAuthorityError) as caught:
        provider(fail).read()
    assert "secret" not in str(caught.value) and caught.value.__suppress_context__


def test_authenticator_keeps_original_tuple_api_with_fresh_sql_provider():
    policy = github_policy()
    grant = qualification(policy)
    subject = grant.signature_subject(policy)
    connections = []

    def connect():
        connection = Connection()
        connections.append(connection)
        return connection

    authenticator = NonproductionGrantAuthenticator(
        trust_provider=provider(connect), clock=lambda: NOW, verifier=SignatureDouble(subject)
    )
    assert authenticator.authenticate_qualification(**qualification_inputs(grant)) == (grant, subject)
    assert len(connections) == 2 and all(not item.answers for item in connections)


def test_aba_original_bytes_cannot_reuse_an_earlier_revision_at_admission():
    original = trust()
    changed = trust(replace(github_policy(), revoked_grant_ids=(identifier(70),)))
    observations = iter(
        [Connection(record(original, 1)), Connection(record(changed, 2)), Connection(record(original, 3))]
    )
    reader = provider(lambda: next(observations))
    expected = reader.read_revision()
    assert reader.read() == changed
    assert reader.read() == expected.snapshot
    transaction = Connection()
    transaction.answers = reads(record(original, 3))
    with pytest.raises(NonproductionAuthorityError, match="trust_revision_changed"):
        reader.require_revision_in(CompositionMssqlLedger(transaction, SCHEMA), expected)
    assert transaction.events == []


@pytest.mark.parametrize("change", ["noncanonical", "unknown", "environment", "epoch"])
def test_recomputed_policy_digest_does_not_hide_invalid_document_or_binding(change):
    body = json.loads(trust().policy_bytes)
    if change == "unknown":
        body["caller_pass"] = True
    elif change == "environment":
        body["environment_id"] = identifier(71)
    elif change == "epoch":
        body["revocation_epoch"] += 1
    raw = canonical_json_bytes(body) + (b"\n" if change == "noncanonical" else b"")
    values = record(trust(policy_bytes=raw, policy_sha256=sha256(raw)))
    with pytest.raises(NonproductionAuthorityError):
        provider(lambda: Connection(values)).read()


@pytest.mark.parametrize("field", ["expected_service_id", "expected_environment_id"])
@pytest.mark.parametrize("value", [None, "alias", "00000000-0000-0000-0000-000000000000", "{" + SERVICE + "}"])
def test_external_identity_pins_reject_before_connecting(field, value):
    pins = {"expected_service_id": SERVICE, "expected_environment_id": ENVIRONMENT}
    pins[field] = value
    with pytest.raises(NonproductionAuthorityError):
        MssqlNonproductionTrustProvider(lambda: pytest.fail("no connection"), **pins)


def test_rollback_failure_preserves_the_sanitized_primary_failure():
    connection = Connection()
    connection.lock_result = -1
    connection.fail = "rollback"
    with pytest.raises(NonproductionAuthorityError) as caught:
        provider(lambda: connection).read()
    assert "secret" not in str(caught.value) and caught.value.__suppress_context__
    assert connection.events == ["cursor", "rollback", "close", "close"]
