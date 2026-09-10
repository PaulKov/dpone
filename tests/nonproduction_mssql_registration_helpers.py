"""Stateful offline registration SQL observations; never live trust evidence."""

import pytest

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_registration import MssqlNonproductionRegistrationStore
from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider, NonproductionTrustRevision
from tests.nonproduction_authority_helpers import NOW, identifier
from tests.nonproduction_signature_helpers import github_policy, trust
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
        self.transaction_id = 7
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
            self.result = [(*self.lock, self.transaction_id)] if "CURRENT_TRANSACTION_ID" in sql else [self.lock]
        elif sql == "SELECT @@OPTIONS;":
            self.result = [(self.options,)]
        elif "composition_authority]" in sql:
            self.result = [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, self.service)]
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
