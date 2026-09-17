"""Bounded catalog reads reject partial facts and uncertain settlement."""

import pytest

from dpone.adapters.dbt_mssql_physical_catalog import MssqlPhysicalCatalogReader, PhysicalCatalogReadError
from tests.support.dbt_mssql_physical_catalog import INVOCATION, STAMP, CatalogConnection, catalog_case


def make_reader(*, failure=None, clock=lambda: 0.0):
    registration, plan, source, rows = catalog_case()
    connection = CatalogConnection(source, rows, failure=failure)
    reader = MssqlPhysicalCatalogReader(
        connection_factory=lambda: connection, registration=registration, operation_timeout_seconds=30, clock=clock
    )
    return reader, connection, plan


def read(reader, plan):
    return reader.read(
        plan=plan,
        executor_invocation_id=INVOCATION,
        object_id=42,
        expected_object_name="orders",
        expected_object_create_time=STAMP,
    )


def test_complete_read_is_detached_and_commits_once_after_two_headers():
    reader, connection, plan = make_reader()
    result = read(reader, plan)
    assert result.results["COUNT"][0].row_count_exact == 3
    assert connection.headers == 2
    assert [event[0] for event in connection.events].count("commit") == 1
    assert not any(event[0] == "rollback" for event in connection.events)
    assert connection.events[-3:] == [("commit",), ("close",), ("close",)]
    with pytest.raises(TypeError):
        result.results["COUNT"] = ()


@pytest.mark.parametrize(
    "failure",
    [
        "HEADER",
        "TABLE",
        "COLUMN",
        "INDEX",
        "INDEX_COLUMN",
        "PARTITION",
        "DEPENDENCY",
        "FORBIDDEN_PROPERTY",
        "COUNT",
        "fetch",
        "commit",
        "extra",
        "drift",
    ],
)
def test_faults_never_return_an_accepted_observation(failure):
    reader, connection, plan = make_reader(failure=failure)
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert ("rollback",) in connection.events
    assert connection.events[-2:] == [("close",), ("close",)]
    if failure != "commit":
        assert ("commit",) not in connection.events


def test_cancel_rolls_back_and_preserves_cancellation():
    reader, connection, plan = make_reader(failure="cancel")
    with pytest.raises(KeyboardInterrupt):
        read(reader, plan)
    assert ("rollback",) in connection.events


def test_advertised_oversize_rejects_without_draining_cursor():
    reader, connection, plan = make_reader()
    row = list(connection.rows["COLUMN"][0])
    row[4] = 11
    connection.rows["COLUMN"] = [tuple(row)] * 1000
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert connection.fetched < 12
    assert ("commit",) not in connection.events


def test_database_identity_mismatch_never_commits():
    reader, connection, plan = make_reader()
    row = list(connection.rows["HEADER"][0])
    row[5] = 100
    connection.rows["HEADER"] = [tuple(row)]
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert ("commit",) not in connection.events


def test_structure_mismatch_blocks_settlement():
    reader, connection, plan = make_reader()
    row = list(connection.rows["COLUMN"][0])
    row[6] = "ID"
    connection.rows["COLUMN"] = [tuple(row)]
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert ("commit",) not in connection.events


def test_expired_connect_budget_closes_without_source_execution():
    ticks = iter([0.0, 31.0])
    reader, connection, plan = make_reader(clock=lambda: next(ticks))
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert not any(event[0] == "execute" for event in connection.events)
    assert ("rollback",) in connection.events


@pytest.mark.parametrize("timeout", [True, 0, -1, 1.5, 2**31])
def test_invalid_deadline_is_rejected_before_connect(timeout):
    registration, _, _, _ = catalog_case()
    with pytest.raises(ValueError):
        MssqlPhysicalCatalogReader(
            connection_factory=lambda: pytest.fail("must not connect"),
            registration=registration,
            operation_timeout_seconds=timeout,
            clock=lambda: 0.0,
        )


def test_wrong_source_blocks_all_catalog_reads():
    reader, connection, plan = make_reader()
    source = list(connection.source)
    source[2] = b"sha256:" + b"0" * 64
    connection.source = tuple(source)
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert connection.headers == 0


def test_commit_after_deadline_cannot_return_success():
    now = [0.0]
    reader, connection, plan = make_reader(clock=lambda: now[0])
    original_commit = connection.commit

    def late_commit():
        original_commit()
        now[0] = 31.0

    connection.commit = late_commit
    with pytest.raises(PhysicalCatalogReadError):
        read(reader, plan)
    assert ("commit",) in connection.events
    assert ("rollback",) in connection.events


@pytest.mark.parametrize("limit,size,accepted", [(1, 1, True), (2, 2, True), (2, 3, False)])
def test_incremental_fetch_at_budget_and_one_over(limit, size, accepted):
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget, fetch_catalog_result

    _, _, source, rows = catalog_case()
    connection = CatalogConnection(source, rows)
    # The overflow vector lies about its advertised count: actual fetch bound
    # must hold independently of the envelope claim.
    connection.pending = iter(
        (1, "FORBIDDEN_PROPERTY", 42, i + 1, min(limit, size), "CHECK_CONSTRAINT", 42, None) for i in range(size)
    )
    args = dict(
        kind="FORBIDDEN_PROPERTY",
        object_id=42,
        row_limit=limit,
        definition_limit=1,
        budget=CatalogReadBudget(30, 16384, lambda: 0.0),
        header_count_limit=limit,
    )
    if accepted:
        assert len(fetch_catalog_result(connection, **args)) == size
    else:
        with pytest.raises(ValueError, match="row budget"):
            fetch_catalog_result(connection, **args)
    assert connection.fetched <= limit + 1


def test_text_byte_budget_is_cumulative_before_retention():
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget

    row = (1, "FORBIDDEN_PROPERTY", 42, 1, 1, "X", None, None)
    single_size = 8 * len(row) + 2 * (len("FORBIDDEN_PROPERTY") + 1)
    budget = CatalogReadBudget(30, single_size, lambda: 0.0)
    budget.charge(row, 1)
    assert budget.remaining_bytes == 0
    with pytest.raises(ValueError):
        budget.charge(row, 1)


def test_definition_utf16_budget_counts_supplementary_characters():
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget

    _, _, _, rows = catalog_case()
    row = list(rows["INDEX"][0])
    row[15] = "😀"
    CatalogReadBudget(30, 16384, lambda: 0.0).charge(tuple(row), 4)
    with pytest.raises(ValueError):
        CatalogReadBudget(30, 16384, lambda: 0.0).charge(tuple(row), 3)


def test_each_statement_gets_new_cursor_with_remaining_driver_timeout():
    now = [0.0]
    reader, connection, plan = make_reader(clock=lambda: now[0])
    created = []

    class Cursor:
        def __init__(self):
            self.timeout = connection.timeout
            self.closed = False
            self.executed = False

        def execute(self, sql, *parameters):
            assert not self.closed and not self.executed
            self.executed = True
            assert self.timeout == connection.timeout > 0
            connection.execute(sql, *parameters)
            now[0] += 1.0
            return self

        def fetchone(self):
            assert not self.closed
            return connection.fetchone()

        def nextset(self):
            return connection.nextset()

        def close(self):
            self.closed = True

    def new_cursor():
        assert not created or created[-1].closed
        cursor = Cursor()
        created.append(cursor)
        return cursor

    connection.cursor = new_cursor
    read(reader, plan)
    assert len(created) == 12  # Transaction setup, source, nine kinds, closing HEADER.
    assert [cursor.timeout for cursor in created] == list(range(30, 18, -1))
    assert all(cursor.closed for cursor in created)
    assert ("commit",) in connection.events


def test_exact_count_runs_once_before_first_header_in_same_transaction():
    reader, connection, plan = make_reader()
    read(reader, plan)
    kinds = [event[2][-1] for event in connection.events if event[0] == "execute" and "physical_catalog_v1" in event[1]]
    assert kinds == [
        "COUNT",
        "HEADER",
        "TABLE",
        "COLUMN",
        "INDEX",
        "INDEX_COLUMN",
        "PARTITION",
        "DEPENDENCY",
        "FORBIDDEN_PROPERTY",
        "HEADER",
    ]


@pytest.mark.parametrize("version", [0, 3, True, "v2"])
def test_unknown_catalog_version_rejects_before_connect(version):
    registration, _, _, _ = catalog_case()
    with pytest.raises(ValueError, match="catalog version"):
        MssqlPhysicalCatalogReader(
            connection_factory=lambda: pytest.fail("must not connect"),
            registration=registration,
            operation_timeout_seconds=30,
            clock=lambda: 0.0,
            catalog_version=version,
        )


def test_v2_read_explicitly_selects_binding_checked_entry():
    from dataclasses import replace

    registration, plan, source, rows = catalog_case()
    plan = replace(plan, spec=replace(plan.spec, resource_bounds=registration.trusted_profile.reference))
    connection = CatalogConnection(source, rows)
    reader = MssqlPhysicalCatalogReader(
        connection_factory=lambda: connection,
        registration=registration,
        operation_timeout_seconds=30,
        clock=lambda: 0.0,
        catalog_version=2,
    )
    read(reader, plan)
    sql = [event[1] for event in connection.events if event[0] == "execute"]
    assert any("physical_catalog_v2" in statement for statement in sql)
    assert not any("physical_catalog_v1" in statement for statement in sql)


def test_v2_bounds_reference_mismatch_rejects_before_connect():
    registration, plan, _, _ = catalog_case()
    reader = MssqlPhysicalCatalogReader(
        connection_factory=lambda: pytest.fail("must not connect"),
        registration=registration,
        operation_timeout_seconds=30,
        clock=lambda: 0.0,
        catalog_version=2,
    )
    with pytest.raises(ValueError, match="resource bounds"):
        read(reader, plan)
