"""Hermetic native-session barriers; these tests do not certify a live server."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource
from tests.test_mssql_native_policy import config

SETTINGS = {
    "limit": 0,
    "offset": 0,
    "extremes": 0,
    "sort_overflow_mode": "throw",
    "final": 0,
    "use_query_cache": 0,
    "apply_mutations_on_fly": 0,
    "apply_patch_parts": 1,
    "apply_deleted_mask": 1,
    "max_parallel_replicas": 1,
    "skip_unavailable_shards": 0,
    "read_overflow_mode": "throw",
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}


class Socket:
    def getpeername(self):
        return ("192.0.2.1", 9440)

    def getpeercert(self, binary_form=False):
        return b"synthetic certificate"


class RawConnector:
    driver = "native"

    def __init__(self):
        self.selects = []
        self.closed = False
        self.rows = [(1, b"p1", 0), (1, b"p1", 1), (None, b"p1", 2)]
        self.table = dict(
            engine="ReplacingMergeTree",
            engine_full="ReplacingMergeTree()",
            uuid="11111111-1111-4111-8111-111111111111",
            database_engine="Atomic",
            partition_key="",
            sorting_key="value",
            primary_key="value",
        )
        self.columns = [dict(name="value", type="Nullable(UInt64)", default_kind="")]
        self.settings = {
            **SETTINGS,
            "max_execution_time": 60,
            "additional_table_filters": "[]",
            "additional_result_filter": "",
        }
        self.server_timezone = self.session_timezone = "UTC"
        self.roles = ["private_role"]
        self.policies = []
        self.parts = [
            dict(
                name="p1",
                partition_id="all",
                rows=3,
                has_lightweight_delete=0,
                data_version=0,
                level=0,
                hash_of_all_files="1" * 32,
                hash_of_uncompressed_files="2" * 32,
                uncompressed_hash_of_compressed_files="3" * 32,
            )
        ]
        self.mutations = []
        self.replica = [dict(replica_name="private_replica", zookeeper_path="/private/coord")]
        self.on_acquire = lambda: None
        self.on_eof = lambda: None
        self.connection = SimpleNamespace(
            connection=SimpleNamespace(
                secure_socket=True,
                verify_cert=True,
                check_hostname=True,
                hosts=[("private_host", 9440)],
                connect_timeout=10,
                send_receive_timeout=60,
                socket=Socket(),
                connected=True,
                disable_reconnect=False,
                settings_is_important=False,
            ),
            connections=[],
            execute=self.execute,
            execute_iter=self.execute_iter,
            disconnect=self.disconnect,
        )

    def execute(self, query, params=None, **kwargs):
        if "system.tables" in query:
            records = [self.table]
        elif "system.columns" in query:
            records = self.columns
        elif "system.settings" in query:
            records = [dict(name=k, value=str(v)) for k, v in self.settings.items()]
        elif "system.row_policies" in query:
            records = self.policies
        elif "system.parts" in query:
            records = self.parts
        elif "system.mutations" in query:
            records = self.mutations
        elif "system.replicas" in query:
            records = self.replica
        elif "currentUser()" in query:
            records = [
                dict(
                    principal="private_user",
                    roles=self.roles,
                    revision="54480",
                    server="private_host",
                    server_timezone=self.server_timezone,
                    session_timezone=self.session_timezone,
                )
            ]
        else:
            raise AssertionError(query)
        names = list(records[0]) if records else []
        return [tuple(row[name] for name in names) for row in records], [(name, "String") for name in names]

    def execute_iter(self, query, params, **kwargs):
        self.on_acquire()
        self.selects.append((query, params, kwargs))
        snapshot = list(self.rows)
        yield [("value", "Nullable(UInt64)"), ("_part", "String"), ("_part_offset", "UInt64")]
        yield from snapshot
        self.on_eof()

    def disconnect(self):
        self.closed = True


def raw_config(scope="single_server"):
    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    value.options["native_transfer"].setdefault("execution", {})["verification_backend"] = "target_local"
    value.options["native_transfer"]["source_snapshot"] = dict(mode="exact_raw_rows", replica_scope=scope)
    return value


def source(connector):
    return ClickHouseNativeSource(connector, schema_guard_factory=lambda cfg: nullcontext())


def extract(connector, value=None):
    from dpone.contracts.clickhouse_raw_snapshot import raw_source_query_binding

    value = value or raw_config()
    instance = source(connector)
    profile = instance.snapshot_profile(value)
    binding = raw_source_query_binding("a" * 64, profile)
    return instance.extract(value, expected_profile=profile, source_query_binding=binding)


def test_raw_snapshot_preserves_duplicates_nulls_and_strips_provenance():
    connector = RawConnector()
    result = extract(connector)
    assert list(result.artifact.iter_native_rows()) == [{"value": 1}, {"value": 1}, {"value": None}]
    assert len(connector.selects) == 1
    query, _, options = connector.selects[0]
    assert " FINAL" not in query and "OFFSET " not in query
    assert all(options["settings"][key] == value for key, value in SETTINGS.items())
    eof = result.artifact.raw_snapshot_eof
    assert eof.rows == 3 and eof.endpoint_authority_agreement
    assert eof.before_physical_profile_sha256 == eof.after_physical_profile_sha256
    document = str(result.artifact.raw_snapshot_profile.document())
    assert not any(value in document for value in ["private_user", "private_role", "private_host", "192.0.2.1"])


@pytest.mark.parametrize("engine", ["MergeTree", "SharedReplacingMergeTree", "ReplacingMergeTreeSuffix", "Distributed"])
def test_exact_engine_admission(engine):
    connector = RawConnector()
    connector.table.update(engine=engine, engine_full=engine + "()")
    with pytest.raises(ValueError):
        source(connector).snapshot_profile(raw_config())
    assert not connector.selects


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "_part"),
        ("name", "_part_offset"),
        ("default_kind", "DEFAULT"),
        ("type", "DateTime64(9)"),
        ("type", "Array(UInt64)"),
    ],
)
def test_schema_admission(field, value):
    connector = RawConnector()
    connector.columns[0][field] = value
    with pytest.raises(ValueError):
        source(connector).snapshot_profile(raw_config())


@pytest.mark.parametrize("setting", list(SETTINGS))
def test_effective_setting_override_fails_before_rows(setting):
    connector = RawConnector()
    connector.settings[setting] = "wrong"
    with pytest.raises(ValueError, match="read_profile"):
        source(connector).snapshot_profile(raw_config())


@pytest.mark.parametrize("field", ["secure_socket", "verify_cert", "check_hostname"])
def test_tls_is_required(field):
    connector = RawConnector()
    setattr(connector.connection.connection, field, False)
    with pytest.raises(ValueError, match="read_profile"):
        source(connector).snapshot_profile(raw_config())


def test_profile_drift_before_acquisition_is_rejected():
    connector = RawConnector()
    instance = source(connector)
    profile = instance.snapshot_profile(raw_config())
    connector.roles.append("another_role")
    with pytest.raises(ValueError, match="profile"):
        instance.extract(
            raw_config(), expected_profile=profile, source_query_binding="clickhouse.raw-query.v1:" + "a" * 64
        )
    assert not connector.selects


@pytest.mark.parametrize("change", ["insert", "merge", "patch", "mask"])
def test_acquisition_barrier_uses_query_visible_rows_and_records_physical_drift(change):
    connector = RawConnector()

    def commit():
        connector.parts[0]["hash_of_all_files"] = "4" * 32
        connector.rows = [(7, b"p1", 0)] if change != "mask" else []

    connector.on_acquire = commit
    result = extract(connector)
    assert list(result.artifact.iter_native_rows()) == ([{"value": 7}] if change != "mask" else [])
    assert not result.artifact.raw_snapshot_eof.endpoint_authority_agreement


def test_profile_drift_at_eof_never_seals_success():
    connector = RawConnector()
    connector.on_eof = lambda: connector.roles.append("another_role")
    result = extract(connector)
    with pytest.raises(ValueError, match="profile"):
        list(result.artifact.iter_native_rows())
    assert getattr(result.artifact, "raw_snapshot_eof", None) is None


def test_empty_snapshot_still_has_verified_eof():
    connector = RawConnector()
    connector.rows = []
    result = extract(connector)
    assert list(result.artifact.iter_native_rows()) == []
    assert result.artifact.raw_snapshot_eof.rows == 0


@pytest.mark.parametrize(
    "engine,scope,valid",
    [
        ("ReplacingMergeTree", "single_server", True),
        ("ReplacingMergeTree", "connected_replica", False),
        ("ReplicatedReplacingMergeTree", "single_server", False),
        ("ReplicatedReplacingMergeTree", "connected_replica", True),
    ],
)
def test_replica_scope_is_exact(engine, scope, valid):
    connector = RawConnector()
    signature = engine + ("('/private/coord', 'private_replica', version)" if engine.startswith("Replicated") else "()")
    connector.table.update(engine=engine, engine_full=signature)
    if valid:
        profile = source(connector).snapshot_profile(raw_config(scope))
        assert profile.replica_scope == scope
        assert "/private/coord" not in str(profile.document())
        assert "private_replica" not in str(profile.document())
    else:
        with pytest.raises(ValueError, match="replica_scope"):
            source(connector).snapshot_profile(raw_config(scope))


@pytest.mark.parametrize(
    "signature",
    [
        "ReplacingMergeTreeOther()",
        "ReplacingMergeTree() junk",
        "ReplacingMergeTree(x + 1)",
        "ReplacingMergeTree(a,b,c)",
    ],
)
def test_engine_signature_parser_rejects_unsupported_grammar(signature):
    connector = RawConnector()
    connector.table["engine_full"] = signature
    with pytest.raises(ValueError):
        source(connector).snapshot_profile(raw_config())


def test_unseen_part_at_acquisition_fails_closed():
    connector = RawConnector()
    connector.on_acquire = lambda: setattr(connector, "rows", [(1, b"newpart", 0)])
    result = extract(connector)
    with pytest.raises(ValueError, match="provenance_incomplete"):
        list(result.artifact.iter_native_rows())


def test_pending_classic_mutation_fails_before_rows():
    connector = RawConnector()
    connector.mutations = [{"mutation_id": "private_mutation"}]
    with pytest.raises(ValueError, match="read_profile"):
        source(connector).snapshot_profile(raw_config())
    assert not connector.selects


def test_unknown_physical_checksum_is_not_success():
    connector = RawConnector()
    connector.parts[0]["hash_of_all_files"] = ""
    with pytest.raises(ValueError, match="provenance_incomplete"):
        extract(connector)
    assert not connector.selects


def test_native_socket_replacement_fails_closed():
    connector = RawConnector()
    result = extract(connector)
    connector.connection.connection.socket = Socket()
    with pytest.raises(ValueError, match="replica_scope"):
        list(result.artifact.iter_native_rows())


def test_raw_cancellation_closes_session_without_eof():
    connector = RawConnector()
    result = extract(connector)
    rows = result.artifact.iter_native_rows()
    next(rows)
    rows.close()
    assert connector.closed
    assert getattr(result.artifact, "raw_snapshot_eof", None) is None


def test_policy_predicates_are_bound_but_never_exposed():
    connector = RawConnector()
    connector.policies = [
        dict(
            id="11111111-1111-4111-8111-111111111112",
            select_filter="value = 123456789",
            is_restrictive=0,
            apply_to_all=0,
            apply_to_list=["private_role"],
            apply_to_except=[],
        )
    ]
    first = source(connector).snapshot_profile(raw_config())
    assert first.policy_filtered
    assert "123456789" not in str(first.document())
    connector.policies[0]["select_filter"] = "value = 987654321"
    second = source(connector).snapshot_profile(raw_config())
    assert first.row_policy_sha256 != second.row_policy_sha256


def test_legacy_profile_performs_no_io_or_adds_artifact_fields():
    from tests.test_clickhouse_native_source import Connector

    connector = Connector()
    instance = source(connector)
    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    assert instance.snapshot_profile(value) is None
    result = instance.extract(value)
    list(result.artifact.iter_native_rows())
    assert connector.selects[0][0] == "SELECT `value` FROM `synthetic`.`events`"
    assert not hasattr(result.artifact, "raw_snapshot_profile")
    assert not hasattr(result.artifact, "source_query_binding")


def test_cancel_before_iteration_can_cleanup_without_query():
    connector = RawConnector()
    result = extract(connector)
    result.artifact.cleanup()
    assert connector.closed and not connector.selects


@pytest.mark.parametrize(
    "signature",
    [
        "ReplacingMergeTree ORDER BY value SETTINGS index_granularity = 8192",
        "ReplacingMergeTree(value) ORDER BY value",
        "ReplacingMergeTree(`value`, `deleted`) ORDER BY value",
    ],
)
def test_engine_full_catalog_clauses_are_bound(signature):
    connector = RawConnector()
    connector.table["engine_full"] = signature
    first = source(connector).snapshot_profile(raw_config())
    connector.table["engine_full"] = signature + ", max_parts_in_total = 999"
    second = source(connector).snapshot_profile(raw_config())
    assert first.engine_signature != second.engine_signature


@pytest.mark.parametrize("field", ["has_lightweight_delete", "partition_id", "data_version", "level"])
def test_incomplete_physical_metadata_is_rejected(field):
    connector = RawConnector()
    connector.parts[0].pop(field, None)
    with pytest.raises(ValueError, match="provenance_incomplete"):
        extract(connector)


def test_mutation_at_eof_excludes_repeatability_without_changing_snapshot():
    connector = RawConnector()
    connector.on_eof = lambda: connector.mutations.append({"mutation_id": "new-mutation"})
    result = extract(connector)
    assert len(list(result.artifact.iter_native_rows())) == 3
    assert not result.artifact.raw_snapshot_eof.endpoint_authority_agreement


def test_stream_failure_preserves_sanitized_primary_when_disconnect_fails():
    connector = RawConnector()

    def bad_stream(*args, **kwargs):
        raise RuntimeError("private_host private_user")

    def bad_disconnect():
        raise RuntimeError("private_secret")

    connector.connection.execute_iter = bad_stream
    connector.connection.disconnect = bad_disconnect
    result = extract(connector)
    with pytest.raises(ValueError, match="provenance_incomplete") as error:
        list(result.artifact.iter_native_rows())
    assert "private" not in str(error.value)


def test_disconnect_failure_prevents_eof_success():
    connector = RawConnector()
    result = extract(connector)

    def bad_disconnect():
        raise RuntimeError("private_secret")

    connector.connection.disconnect = bad_disconnect
    with pytest.raises(ValueError, match="provenance_incomplete"):
        list(result.artifact.iter_native_rows())
    assert getattr(result.artifact, "raw_snapshot_eof", None) is None


def test_primary_admission_error_survives_cleanup_failure():
    connector = RawConnector()
    connector.table["engine"] = "Distributed"

    def bad_disconnect():
        raise RuntimeError("private_host")

    connector.connection.disconnect = bad_disconnect
    with pytest.raises(ValueError, match="replacing_raw"):
        source(connector).snapshot_profile(raw_config())


def test_catalog_error_does_not_expose_coordinates():
    connector = RawConnector()

    def bad_execute(*args, **kwargs):
        raise RuntimeError("private_host private_user private_predicate")

    connector.connection.execute = bad_execute
    with pytest.raises(ValueError, match="read_profile") as error:
        source(connector).snapshot_profile(raw_config())
    assert "private" not in str(error.value)


@pytest.mark.parametrize(
    "expression,partitions",
    [
        ("toYYYYMM(observed_at)", ("202608", "202609")),
        ("toYYYYMMDD(observed_at)", ("20260831", "20260901")),
        ("toDate(`observed_at`)", ("20260831", "20260901")),
    ],
)
def test_window_partition_identity_is_bounded_and_exact(expression, partitions):
    from dpone.runtime.sources.clickhouse_raw_snapshot import RawSnapshot

    connector = RawConnector()
    connector.columns = [dict(name="observed_at", type="DateTime64(6)", default_kind="")]
    connector.table["partition_key"] = expression
    value = raw_config()
    value.load_strategy = "partition_replace"
    value.options.update(
        mssql_native_window={"column": "observed_at", "anchor": "data_interval_end", "lookback": "P2D"},
        interval={"interval_end": "2026-09-02T00:00:00Z"},
    )
    snapshot = RawSnapshot(connector, value)
    profile = snapshot.acquire_profile(value)
    assert snapshot.partition_ids == partitions
    assert profile.window == ("observed_at", "2026-08-31T00:00:00+00:00", "2026-09-02T00:00:00+00:00")
    assert "`__dpone_native_source`.`observed_at`" in snapshot.query
    assert snapshot.query_params["end"] == "2026-09-02 00:00:00.000000"


def test_window_rejects_uninspectable_partition_expression():
    connector = RawConnector()
    connector.table["partition_key"] = "cityHash64(value)"
    value = raw_config()
    value.load_strategy = "partition_replace"
    value.options.update(
        mssql_native_window={"column": "observed_at", "anchor": "data_interval_end", "lookback": "P1D"},
        interval={"interval_end": "2026-09-02T00:00:00Z"},
    )
    with pytest.raises(ValueError, match="provenance_incomplete"):
        source(connector).snapshot_profile(value)


def test_profile_mismatch_preserves_error_when_disconnect_fails():
    connector = RawConnector()
    instance = source(connector)
    profile = instance.snapshot_profile(raw_config())
    connector.roles.append("drift")
    connector.connection.disconnect = lambda: (_ for _ in ()).throw(RuntimeError("private_host"))
    with pytest.raises(ValueError, match="profile_changed"):
        instance.extract(
            raw_config(), expected_profile=profile, source_query_binding="clickhouse.raw-query.v1:" + "a" * 64
        )


def test_physical_cap_is_not_silent_truncation():
    connector = RawConnector()
    connector.parts = [{**connector.parts[0], "name": f"p{index}"} for index in range(8193)]
    with pytest.raises(ValueError, match="read_profile"):
        extract(connector)
    assert not connector.selects


def test_post_acquisition_change_does_not_reopen_or_mutate_stream():
    connector = RawConnector()
    result = extract(connector)
    rows = result.artifact.iter_native_rows()
    first = next(rows)
    connector.rows[:] = [(9, b"p1", 0)]
    connector.parts[0]["hash_of_all_files"] = "5" * 32
    assert [first, *rows] == [{"value": 1}, {"value": 1}, {"value": None}]
    assert len(connector.selects) == 1
    assert not result.artifact.raw_snapshot_eof.endpoint_authority_agreement


def test_non_native_driver_is_rejected_without_constructing_client():
    class HttpConnector:
        driver = "http"

        @property
        def connection(self):
            raise AssertionError("HTTP client must not be constructed")

    with pytest.raises(ValueError, match="native_clickhouse_driver_required"):
        source(HttpConnector()).snapshot_profile(raw_config())


class SettingAwareConnector(RawConnector):
    """Model native client merging connection defaults with query overrides."""

    def __init__(self):
        super().__init__()
        self.defaults = {"limit": 100, "offset": 7, "extremes": 1, "sort_overflow_mode": "break"}
        self.constrained = {}
        self.sort_budget = None
        self.effective_queries = []

    def execute(self, query, params=None, **kwargs):
        effective = {**self.settings, **self.defaults, **kwargs.get("settings", {}), **self.constrained}
        self.effective_queries.append((query, effective))
        if "system.settings" in query:
            requested = params["names"]
            values = [(name, str(effective[name])) for name in requested if name in effective]
            return values, [("name", "String"), ("value", "String")]
        rows, columns = super().execute(query, params, **kwargs)
        if "ORDER BY" in query and self.sort_budget is not None and len(rows) > self.sort_budget:
            if effective["sort_overflow_mode"] == "throw":
                raise RuntimeError("synthetic sort budget exceeded")
            rows = rows[: self.sort_budget]
        return rows, columns

    def execute_iter(self, query, params, **kwargs):
        hook, self.on_acquire = self.on_acquire, lambda: None
        hook()
        effective = {**self.settings, **self.defaults, **kwargs.get("settings", {}), **self.constrained}
        rows = list(self.rows)
        if effective.get("additional_table_filters", "[]") != "[]":
            rows = [row for row in rows if row[0] is not None and row[0] > 9]
        offset, limit = int(effective.get("offset", 0)), int(effective.get("limit", 0))
        self.rows = rows[offset : offset + limit] if limit else rows[offset:]
        try:
            yield from super().execute_iter(query, params, **kwargs)
            if effective.get("extremes"):
                yield rows[0]
        finally:
            self.rows = rows


def test_result_shaping_defaults_cannot_seal_partial_or_extra_rows():
    connector = SettingAwareConnector()
    connector.parts[0]["rows"] = 200
    connector.rows = [(index, b"p1", index) for index in range(200)]
    result = extract(connector)
    rows = list(result.artifact.iter_native_rows())
    assert len(rows) == result.artifact.raw_snapshot_eof.rows == 200
    assert rows == [{"value": index} for index in range(200)]
    assert dict(result.artifact.raw_snapshot_profile.read_settings)["limit"] == 0


def test_constrained_result_limit_fails_before_data():
    connector = SettingAwareConnector()
    connector.constrained = {"limit": 100}
    with pytest.raises(ValueError, match="read_profile"):
        extract(connector)
    assert not connector.selects


def test_catalog_sort_budget_throws_instead_of_partial_provenance():
    connector = SettingAwareConnector()
    connector.parts = [{**connector.parts[0], "name": f"p{i}"} for i in range(3)]
    connector.sort_budget = 1
    with pytest.raises(ValueError, match="read_profile"):
        extract(connector)
    assert not connector.selects


@pytest.mark.parametrize(
    "name,value",
    [
        ("additional_table_filters", "[('synthetic.events', 'value > 9')]"),
        ("additional_result_filter", "value > 9"),
        ("filter", "value > 9"),
    ],
)
def test_extra_read_filter_is_rejected_without_overriding_security(name, value):
    connector = SettingAwareConnector()
    connector.defaults[name] = value
    with pytest.raises(ValueError, match="read_profile"):
        extract(connector)
    assert connector.defaults[name] == value
    assert all(effective[name] == value for _, effective in connector.effective_queries)
    assert not connector.selects


def test_table_filter_drift_before_select_is_rejected():
    connector = SettingAwareConnector()
    result = extract(connector)
    connector.defaults["additional_table_filters"] = "[('synthetic.events', 'value > 9')]"
    with pytest.raises(ValueError, match="read_profile"):
        list(result.artifact.iter_native_rows())
    assert not connector.selects


def window_config():
    value = raw_config()
    value.load_strategy = "partition_replace"
    value.options.update(
        mssql_native_window={"column": "observed_at", "anchor": "data_interval_end", "lookback": "P1D"},
        interval={"interval_end": "2026-10-01T00:00:00Z"},
    )
    return value


@pytest.mark.parametrize(
    "dtype,server,session,accepted",
    [
        ("DateTime", "UTC", "UTC", True),
        ("DateTime64(6)", "Europe/Moscow", "UTC", False),
        ("DateTime", "UTC", "Europe/Moscow", False),
        ("DateTime('UTC')", "Europe/Moscow", "Europe/Moscow", True),
        ("Nullable(DateTime64(6, 'UTC'))", "Europe/Moscow", "UTC", True),
    ],
)
def test_window_timezone_admission_before_row_io(dtype, server, session, accepted):
    connector = RawConnector()
    connector.server_timezone, connector.session_timezone = server, session
    connector.columns = [dict(name="observed_at", type=dtype, default_kind="")]
    connector.table["partition_key"] = "toYYYYMMDD(observed_at)"
    if accepted:
        profile = source(connector).snapshot_profile(window_config())
        assert profile.window[1].startswith("2026-09-30")
    else:
        with pytest.raises(ValueError, match="read_profile"):
            source(connector).snapshot_profile(window_config())
    assert not connector.selects


def test_implicit_partition_timezone_drift_is_rejected():
    connector = RawConnector()
    connector.columns = [dict(name="observed_at", type="DateTime64(6)", default_kind="")]
    connector.table["partition_key"] = "toYYYYMM(observed_at)"
    instance = source(connector)
    profile = instance.snapshot_profile(window_config())
    connector.server_timezone = "Europe/Moscow"
    with pytest.raises(ValueError, match="read_profile"):
        instance.extract(
            window_config(), expected_profile=profile, source_query_binding="clickhouse.raw-query.v1:" + "a" * 64
        )
    assert not connector.selects


def test_table_filter_committed_at_acquisition_never_seals_filtered_eof():
    connector = SettingAwareConnector()
    connector.parts[0]["rows"] = 20
    connector.rows = [(index, b"p1", index) for index in range(20)]
    connector.on_acquire = lambda: connector.defaults.update(
        additional_table_filters="[('synthetic.events', 'value > 9')]"
    )
    result = extract(connector)
    observed = []
    with pytest.raises(ValueError, match="read_profile"):
        for row in result.artifact.iter_native_rows():
            observed.append(row)
    assert observed == [{"value": index} for index in range(10, 20)]
    assert getattr(result.artifact, "raw_snapshot_eof", None) is None
    assert connector.defaults["additional_table_filters"] != "[]"


@pytest.mark.parametrize("name", ["additional_table_filters", "additional_result_filter"])
def test_required_filter_inspection_is_fail_closed(name):
    connector = SettingAwareConnector()
    del connector.settings[name]
    with pytest.raises(ValueError, match="read_profile"):
        extract(connector)
    assert not connector.selects


def test_partition_identity_binds_implicit_versus_explicit_timezone_authority():
    connector = RawConnector()
    connector.columns = [dict(name="observed_at", type="DateTime64(6)", default_kind="")]
    connector.table["partition_key"] = "toYYYYMM(observed_at)"
    implicit = source(connector).snapshot_profile(window_config())
    connector.columns[0]["type"] = "DateTime64(6, 'UTC')"
    explicit = source(connector).snapshot_profile(window_config())
    assert implicit.partition_key_sha256 != explicit.partition_key_sha256
    connector.server_timezone = connector.session_timezone = "Europe/Moscow"
    assert source(connector).snapshot_profile(window_config()).partition_key_sha256 == explicit.partition_key_sha256


def test_canonical_etc_utc_server_name_preserves_utc_partition_semantics():
    connector = RawConnector()
    connector.columns = [dict(name="observed_at", type="DateTime64(6)", default_kind="")]
    connector.table["partition_key"] = "toYYYYMM(observed_at)"
    utc = source(connector).snapshot_profile(window_config())
    connector.server_timezone = connector.session_timezone = "Etc/UTC"
    canonical = source(connector).snapshot_profile(window_config())
    assert canonical.partition_key_sha256 == utc.partition_key_sha256


@pytest.mark.parametrize("empty", ["{}", "[]"])
def test_empty_table_filter_map_server_representations_are_admitted(empty):
    connector = SettingAwareConnector()
    connector.settings["additional_table_filters"] = empty
    assert source(connector).snapshot_profile(raw_config()) is not None
    assert not connector.selects


@pytest.mark.parametrize("nonempty", ["{'synthetic.events':'value > 9'}", "[('synthetic.events', 'value > 9')]"])
def test_nonempty_table_filter_map_server_representations_are_rejected(nonempty):
    connector = SettingAwareConnector()
    connector.settings["additional_table_filters"] = nonempty
    with pytest.raises(ValueError, match="read_profile"):
        source(connector).snapshot_profile(raw_config())
    assert not connector.selects
