from contextlib import contextmanager
from uuid import UUID

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_sqlclient_checkpoint import SqlClientCheckpointCas
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.adapters.mssql_sqlclient_native_retirement_window_store import (
    WindowStoreSqlClientNativeRetirementState,
)
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_sqlclient_native_parent_composition import (
    SqlClientNativeParentDeployment,
    SqlClientParentJournalFacade,
    compose_sqlclient_native_parent_capabilities,
)
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody


class _Reader:
    def read(self, _name, _size, _digest):
        return b""


class _CustodyJournal:
    def observe_or_advance(self, _request_sha256, advance):
        return advance()


@contextmanager
def _management_session():
    yield object()


def _retirement(lifecycle, directory, limits):
    server = SqlClientServerAuthority("server", "machine", "instance", "physical")
    database = SqlClientDatabaseAuthority(7, "database", str(UUID(int=1)), "aa")
    admission = SqlClientObserverAdmission(
        server,
        database,
        SqlClientLoginAuthority(5, "login", "aa", "login", "aa", 1, True),
        SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE"),
    )
    return SqlClientNativeRetirementDeployment(
        open_management_session=_management_session,
        admission=admission,
        expected_server=server,
        expected_database=database,
        lifecycle_observer=lifecycle,
        directory_observer=directory,
        directory_limits=limits,
    )


def _journal(tmp_path):
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    return NativeChunkJournal(store, lease, plan, parent_schema_version=4)


def test_parent_composer_binds_one_exact_journal_and_concrete_retirement_state(tmp_path):
    journal = _journal(tmp_path)
    lifecycle = TdsAttemptJournal(journal.store, backend="mssql_sqlclient")
    directory = TdsCoordinatorDirectoryJournal(journal.store, parent_observer=lifecycle)
    limits = TdsDirectoryLimits(2, 1, 2, 1)
    capabilities = compose_sqlclient_native_parent_capabilities(
        SqlClientNativeParentDeployment(
            journal=journal,
            lifecycle_observer=lifecycle,
            directory_observer=directory,
            rollback_no_commit=lambda: {"proof": "confirmed"},
            evidence_reader=_Reader(),
            retirement=_retirement(lifecycle, directory, limits),
            file_custody=FileSqlClientInputCustody(tmp_path / "custody"),
            input_custody=_CustodyJournal(),
            checkpoint=SqlClientCheckpointCas(cas=lambda *_: None, observe=lambda *_: None),
            directory_limits=limits,
            attempt_retirement_custody=SqlClientAttemptRetirementCustody(lambda *args: None),
            recover_verified_retirement=lambda request, deadline: None,
        )
    )
    assert type(capabilities.journal) is SqlClientParentJournalFacade
    assert type(capabilities.retirement_state) is WindowStoreSqlClientNativeRetirementState
    assert capabilities.fence() == journal.lease.fence
    assert capabilities.journal.data == journal.data


def test_parent_composer_rejects_non_v4_journal(tmp_path):
    journal = _journal(tmp_path)
    journal.parent_schema_version = None
    try:
        SqlClientParentJournalFacade(journal)
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_parent_composition_invalid"
    else:
        raise AssertionError("non-v4 journal accepted")
