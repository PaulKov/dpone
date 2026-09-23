"""Real SQL authority producer, scripted catalog and actual SQLite locator actors."""

from contextlib import contextmanager
from dataclasses import replace
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_grant_inventory_composition import (
    SqlClientGrantInventoryCollector,
    SqlClientGrantInventoryUnknown,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import admit_sqlclient_state_domain
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantInventoryLimits, encode_grant_inventory
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from tests.test_mssql_sqlclient_grant_catalog import Cursor, sql_owner
from tests.test_mssql_sqlclient_stage_locator import REQUEST
from tests.test_mssql_sqlclient_stage_locator_journal import setup as setup


@pytest.fixture
def context(setup):
    store, lease, _, locator, journal = setup
    journal.create(locator, REQUEST, lease)
    pool = TdsActorPool(capacity=1)
    events = []
    store.exit_hook = lambda: None
    store.exit_failure_expected = False

    @contextmanager
    def factory():
        events.append("actor.enter")
        try:
            yield store
        finally:
            store.exit_hook()
            events.append("actor.exit")

    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 3)
    cursor = Cursor()
    sql = sql_owner(cursor)
    kwargs = dict(
        management_admission=cursor.management,
        writer_admission=cursor.writer,
        writer_principal=SqlClientDatabasePrincipal(5, "writer_user", "aa"),
        admitted_factory=admitted,
        pool=pool,
        limits=SqlClientGrantInventoryLimits(),
        deadline=monotonic() + 5,
    )
    yield sql, kwargs, store, events
    try:
        if store.exit_failure_expected:
            from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown

            with pytest.raises(TdsJournalActorUnknown):
                pool.close(deadline=monotonic() + 3)
        else:
            pool.close(deadline=monotonic() + 3)
    finally:
        sql.close()


def test_complete_observation_preserves_rows_and_resolves_nonempty_stage(context):
    from dpone.adapters.mssql_tds_coordinator_sql import _ACQUIRE

    sql, kwargs, _, events = context
    sql.cursor.permissions.extend(
        [
            (0, 0, 0, 0, 1, "CO", "CONNECT", "G"),
            (1, -2, 1, 5, 1, "SL", "SELECT", "D"),
            (42, 4, 0, 5, 1, "XX", "FUTURE", "R"),
            (1, sql.cursor.stage.object_id, 1, 5, 1, "UP", "UPDATE", "W"),
        ]
    )
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)
    before = len(events)
    result = collector.collect()
    assert len(result.permissions) == 5 and len(result.members) == 1
    assert result.members[0].stage == sql.cursor.stage
    assert events[before:] == ["actor.enter", "actor.exit"]
    assert sum(q == _ACQUIRE for q, _ in sql.cursor.calls) == 1
    assert not any("READCOMMITTEDLOCK" in q or "SELECT 1 FROM [" in q for q, _ in sql.cursor.calls)
    assert kwargs["pool"].live_count == 0
    assert encode_grant_inventory(result)
    assert not any(hasattr(result, key) for key in ("safe", "prepared", "verified", "allowed_to_launch"))
    count = len(sql.cursor.calls)
    with pytest.raises(SqlClientGrantInventoryUnknown):
        collector.collect()
    assert len(sql.cursor.calls) == count


@pytest.mark.parametrize(
    "fault",
    [
        "visibility",
        "version",
        "public_missing",
        "public_wrong",
        "writer_wrong",
        "missing_object",
        "duplicate_properties",
        "null_owner",
        "null_nonce",
        "schema",
        "table",
        "nonce",
        "feature",
        "member_cap",
        "byte_cap",
        "closing_permissions",
        "closing_principal",
        "closing_object",
        "closing_session",
    ],
)
def test_invalid_or_changed_inventory_returns_no_result_and_never_retries(context, fault):
    from dpone.adapters import mssql_sqlclient_grant_catalog as catalog
    from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL

    sql, kwargs, _, _ = context
    cursor = sql.cursor

    def mutate(statement, rows):
        rows = [list(r) for r in rows]
        closing = cursor.profile_reads >= 2
        if statement == OWN_INCARNATION_SQL:
            if fault == "visibility":
                rows[0][50] = 0
            if fault == "version":
                rows[0][11] = 15
            if fault == "closing_session" and closing:
                rows[0][20] = __import__("uuid").UUID(int=88)
        if statement == catalog.PRINCIPALS_SQL:
            if fault == "public_missing":
                return rows[1:]
            if fault == "public_wrong":
                rows[0][3] = "SQL_USER"
            if fault == "writer_wrong" or (fault == "closing_principal" and closing):
                rows[1][2] = b"\xff"
        if statement == catalog.BATCH_MEMBERS_SQL:
            if fault == "view":
                rows[0][4] = "VIEW"
            if fault == "missing_object":
                return []
            if fault == "schema":
                rows[0][2] = "other-schema"
            if fault == "member_cap":
                for row in rows:
                    row[4] = "USER_TABLE"
        if statement == catalog.BATCH_OBJECTS_SQL:
            if fault == "duplicate_properties":
                return rows * 2
            if fault == "null_owner":
                rows[0][3] = None
            if fault == "null_nonce":
                rows[0][4] = None
            if fault == "table":
                rows[0][1] = "other-table"
            if fault == "nonce" or (fault == "closing_object" and closing):
                rows[0][4] = str(__import__("uuid").UUID(int=89))
        if statement == catalog.BATCH_FEATURES_SQL and fault == "feature":
            rows[0][12] = 1
        if statement == catalog.PERMISSIONS_SQL and fault == "closing_permissions" and closing:
            rows[0][-1] = "D"
        return rows

    cursor.mutate = mutate
    if fault == "member_cap":
        kwargs["limits"] = replace(kwargs["limits"], members=1)
        cursor.permissions.append((1, cursor.stage.object_id + 1, 0, 5, 1, "SL", "SELECT", "G"))
    if fault == "byte_cap":
        kwargs["limits"] = replace(kwargs["limits"], observation_bytes=1)
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)
    with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
        collector.collect()
    assert caught.value.sql is sql and caught.value.pool is kwargs["pool"]
    count = len(cursor.calls)
    with pytest.raises(SqlClientGrantInventoryUnknown):
        collector.collect()
    assert len(cursor.calls) == count


@pytest.mark.parametrize("missing", ["locator", "parent", "directory", "coordinator"])
def test_missing_original_journal_or_locator_retains_actual_failed_actor(context, monkeypatch, missing):
    from dpone.contracts.mssql_tds_coordinator import coordinator_key
    from dpone.contracts.mssql_tds_directory import directory_key
    from tests.test_mssql_sqlclient_stage_locator import OPERATION, PARENT

    sql, kwargs, store, _ = context
    load = store.load
    selected = {
        "locator": "sqlclient-stage-locator/",
        "parent": "mssql-tds-attempt-v1/",
        "directory": directory_key(PARENT),
        "coordinator": coordinator_key(OPERATION),
    }[missing]
    monkeypatch.setattr(
        store,
        "load",
        lambda key: (
            None if (key.startswith(selected) if missing in ("locator", "parent") else key == selected) else load(key)
        ),
    )
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)
    with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
        collector.collect()
    assert caught.value.gateway is not None
    caught.value.close_locator(deadline=kwargs["deadline"])
    assert kwargs["pool"].live_count == 0
    assert sql.cursor.profile_reads == 1


def test_lock_loss_around_actual_locator_stops_closing_reobservation(context, monkeypatch):
    from dpone.app import mssql_sqlclient_grant_inventory_composition as composition

    sql, kwargs, _, _ = context
    original = composition.read_sqlclient_stage_locator

    def read(**parameters):
        result = original(**parameters)
        sql.cursor.lock = "NoLock"
        return result

    monkeypatch.setattr(composition, "read_sqlclient_stage_locator", read)
    with pytest.raises(SqlClientGrantInventoryUnknown):
        SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    assert sql.cursor.profile_reads == 1 and kwargs["pool"].live_count == 0


@pytest.mark.parametrize("point", ["read", "exit"])
def test_actual_late_actor_retained_until_thread_exit_without_new_deadline(context, monkeypatch, point):
    from threading import Event

    from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown

    sql, kwargs, store, _ = context
    release, entered = Event(), Event()
    original = store.load

    def wait():
        entered.set()
        release.wait(3)

    def load(key):
        result = original(key)
        if key.startswith("sqlclient-stage-locator/"):
            wait()
        return result

    if point == "read":
        monkeypatch.setattr(store, "load", load)
    else:
        store.exit_hook = wait
    kwargs["deadline"] = monotonic() + 0.15
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)
    try:
        with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
            collector.collect()
        retained = caught.value
        assert entered.is_set() and retained.gateway is not None
        assert retained.sql is sql and retained.pool is kwargs["pool"]
        assert kwargs["pool"].live_count == 1 and sql.cursor.closed is False
        assert sql.cursor.profile_reads == 1
        with pytest.raises(TdsJournalActorUnknown):
            retained.close_locator(deadline=monotonic() + 10)
        assert retained.deadline == kwargs["deadline"]
    finally:
        release.set()
        kwargs["pool"].close(deadline=monotonic() + 2)
    assert kwargs["pool"].live_count == 0


def test_failed_actor_context_exit_retains_real_gateway(context):
    from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown

    sql, kwargs, store, _ = context

    def fail():
        raise OSError("synthetic context teardown failure")

    store.exit_hook = fail
    store.exit_failure_expected = True
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)
    with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
        collector.collect()
    assert caught.value.gateway is not None
    with pytest.raises(TdsJournalActorUnknown):
        caught.value.close_locator(deadline=kwargs["deadline"])
    # Pool truth counts the actual finished thread; uncertainty is not a live thread.
    assert kwargs["pool"].live_count == 0


@pytest.mark.parametrize("query", ["own", "writer"])
def test_raw_uuid_alias_rejected_before_existing_parser_can_normalize(context, query):
    from uuid import UUID

    from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
    from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL

    sql, kwargs, _, _ = context

    def mutate(statement, rows):
        selected = OWN_INCARNATION_SQL if query == "own" else ADMISSION_SQL
        if statement == selected:
            rows = [list(r) for r in rows]
            malformed = UUID(int=1)
            object.__setattr__(malformed, "int", True)
            rows[0][44 if query == "own" else 6] = malformed
        return rows

    sql.cursor.mutate = mutate
    with pytest.raises(SqlClientGrantInventoryUnknown):
        SqlClientGrantInventoryCollector(sql, **kwargs).collect()


def test_duplicate_raw_permissions_remain_lossless(context):
    sql, kwargs, _, _ = context
    sql.cursor.permissions *= 2
    result = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    assert len(result.permissions) == 2 and result.permissions[0] == result.permissions[1]
    assert len(result.members) == 1


def test_unrelated_object_permission_is_lossless_without_stage_member(context):
    from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientPermissionRow

    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    unrelated = SqlClientPermissionRow(1, record.members[0].stage.object_id + 100, 0, 0, 1, "SL", "SELECT", "G")
    projected = replace(
        record,
        permissions=record.permissions + (unrelated,),
        excluded_object_ids=(unrelated.major_id,),
    )
    assert encode_grant_inventory(projected)


def test_every_object_permission_candidate_is_classified_exactly_once(context):
    from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientPermissionRow

    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    stage_id = record.members[0].stage.object_id
    unrelated_id = stage_id + 100
    unrelated = SqlClientPermissionRow(1, unrelated_id, 0, 0, 1, "SL", "SELECT", "G")
    permissions = record.permissions + (unrelated,)

    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=permissions)
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=permissions, excluded_object_ids=(stage_id, unrelated_id))
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=permissions, excluded_object_ids=(unrelated_id, unrelated_id))
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=permissions, excluded_object_ids=(unrelated_id, stage_id + 200))
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=permissions, excluded_object_ids=(True,))


def test_stage_permission_cannot_be_omitted_from_members(context):
    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, members=())


def test_stage_member_still_requires_a_matching_object_permission(context):
    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_invalid"):
        replace(record, permissions=())


def test_strict_component_byte_boundary(context):
    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    cap = len(encode_grant_inventory(record))
    while True:
        candidate = replace(record, limits=replace(record.limits, observation_bytes=cap))
        payload = encode_grant_inventory(candidate)
        if len(payload) == cap:
            break
        cap = len(payload)
    assert len(encode_grant_inventory(candidate)) == cap
    with pytest.raises(ValueError):
        encode_grant_inventory(replace(candidate, limits=replace(candidate.limits, observation_bytes=cap - 1)))


def test_maximum_narrow_members_fit_and_wide_overflow_is_rejected(context):
    from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantMember, SqlClientPermissionRow
    from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType

    sql, kwargs, _, _ = context
    base = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    original = base.members[0]
    members = []
    for index in range(1024):
        name = f"stage_{index}"
        operation = original.locator.locator.create_operation
        locator = replace(
            original.locator,
            locator=replace(
                original.locator.locator,
                create_operation=replace(operation, parent=replace(operation.parent, table=name)),
            ),
        )
        members.append(
            SqlClientGrantMember(
                replace(original.stage, object_id=index + 100, table_name=name, columns=original.stage.columns[:1]),
                locator,
            )
        )
    permissions = tuple(SqlClientPermissionRow(1, m.stage.object_id, 0, 5, 1, "SL  ", "SELECT", "G") for m in members)
    maximal = replace(base, members=tuple(members), permissions=permissions)
    assert len(encode_grant_inventory(maximal)) <= 8388608
    with pytest.raises(ValueError):
        replace(maximal, members=tuple(members) + (members[0],))
    columns = tuple(
        TdsCreateObservedColumn(i, "c" * 120 + str(i), TdsCreateType.BIGINT, False, 8, 19, 0, None)
        for i in range(1, 101)
    )
    wide = replace(maximal, members=tuple(replace(m, stage=replace(m.stage, columns=columns)) for m in members))
    with pytest.raises(ValueError):
        encode_grant_inventory(wide)


@pytest.mark.parametrize("field", ["command", "management", "writer", "principal", "limits", "raw_factory"])
def test_preflight_rejects_unapproved_context_before_sql(context, field):
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand

    sql, kwargs, _, _ = context
    before = len(sql.cursor.calls)
    if field == "command":
        sql.identity = replace(sql.identity, command=TdsCoordinatorCommand.CREATE)
    elif field == "management":
        kwargs["management_admission"] = kwargs["writer_admission"]
    elif field == "writer":
        kwargs["writer_admission"] = kwargs["management_admission"]
    elif field == "principal":
        kwargs["writer_principal"] = SqlClientDatabasePrincipal(1, "dbo", "bb")
    elif field == "limits":
        object.__setattr__(kwargs["limits"], "members", True)
    else:
        kwargs["admitted_factory"] = kwargs["admitted_factory"]._factory
    with pytest.raises(ValueError):
        SqlClientGrantInventoryCollector(sql, **kwargs)
    assert len(sql.cursor.calls) == before


def test_caught_reentry_permanently_poisoned_outer_collector(context):
    from dpone.adapters.mssql_sqlclient_grant_catalog import PERMISSIONS_SQL

    sql, kwargs, _, _ = context
    collector = SqlClientGrantInventoryCollector(sql, **kwargs)

    def mutate(statement, rows):
        if statement == PERMISSIONS_SQL:
            with pytest.raises(SqlClientGrantInventoryUnknown):
                collector.collect()
        return rows

    sql.cursor.mutate = mutate
    with pytest.raises(SqlClientGrantInventoryUnknown):
        collector.collect()
    assert sql.cursor.profile_reads == 1


def test_4096_unrelated_candidates_are_exhaustive_with_four_catalog_batches_per_snapshot(context):
    from dpone.adapters import mssql_sqlclient_grant_catalog as catalog

    sql, kwargs, _, _ = context
    # This is a cardinality/query-budget test.  Keep its authority deadline
    # independent of host contention while the production path remains bounded.
    kwargs["deadline"] = monotonic() + 30
    stage_id = sql.cursor.stage.object_id
    object_ids = tuple(value for value in range(1, 4098) if value != stage_id)[:4096]
    sql.cursor.permissions = [(1, value, 0, 5, 1, "SL", "SELECT", "G") for value in object_ids]
    result = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    assert result.members == ()
    assert result.excluded_object_ids == object_ids
    assert sum(statement == catalog.BATCH_MEMBERS_SQL for statement, _ in sql.cursor.calls) == 8
    assert not any(
        statement in (catalog.BATCH_OBJECTS_SQL, catalog.BATCH_FEATURES_SQL, catalog.BATCH_COLUMNS_SQL)
        for statement, _ in sql.cursor.calls
    )


@pytest.mark.parametrize("binding", ["database", "session", "member_server"])
def test_inventory_contract_rejects_contradictory_context_bindings(context, binding):
    from uuid import UUID

    sql, kwargs, _, _ = context
    record = SqlClientGrantInventoryCollector(sql, **kwargs).collect()
    with pytest.raises(ValueError):
        if binding == "database":
            replace(
                record, authority=replace(record.authority, database=replace(record.authority.database, name="other"))
            )
        elif binding == "session":
            own = replace(record.management_before, connection_id=UUID(int=998))
            replace(record, management_before=own, management_after=own)
        else:
            member = record.members[0]
            locator = member.locator.locator
            changed = replace(
                member,
                locator=replace(
                    member.locator, locator=replace(locator, server=replace(locator.server, server_name="other"))
                ),
            )
            replace(record, members=(changed,))
