"""Local disposable inventory must never bless a changed recovery binding."""

from dataclasses import replace

import pytest
from tools.native_delivery_local.inventory import Inventory, InventoryStore


def inventory():
    return Inventory(
        invocation_id="a" * 32,
        profile="narrow",
        rows=4,
        seed=7,
        strategy="full_refresh",
        target_database="dda_target",
        state_database="dda_state",
        source_database="dda_source",
        schema="dda_" + "a" * 32,
        table="business",
        configuration={"max_rows": 2},
        authority={},
        objects={},
    )


def test_inventory_create_attach_exact_and_refuse_overwrite(tmp_path):
    store = InventoryStore(tmp_path)
    value = inventory()
    store.create(value)
    assert store.load(value.invocation_id) == value
    with pytest.raises(FileExistsError):
        store.create(replace(value, rows=8))
    assert store.load(value.invocation_id).rows == 4


@pytest.mark.parametrize("identity", ["../escape", "a" * 31, "A" * 32, "a" * 32 + "/x"])
def test_inventory_rejects_paths(tmp_path, identity):
    with pytest.raises(ValueError, match="invocation"):
        InventoryStore(tmp_path).load(identity)


def test_inventory_tamper_is_not_a_new_authority(tmp_path):
    store = InventoryStore(tmp_path)
    value = inventory()
    store.create(value)
    path = tmp_path / value.invocation_id / "inventory.json"
    path.write_text(path.read_text().replace('"rows":4', '"rows":8'))
    with pytest.raises(ValueError, match="digest"):
        store.load(value.invocation_id)


def test_inventory_rejects_credentials(tmp_path):
    with pytest.raises(ValueError, match="secret"):
        InventoryStore(tmp_path).create(replace(inventory(), configuration={"password": "do-not-write"}))
    assert not tuple(tmp_path.rglob("inventory.json"))


def test_inventory_symlink_cannot_escape_root(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    root.mkdir()
    (root / ("a" * 32)).symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        InventoryStore(root).load("a" * 32)


def test_business_ddl_preserves_wire_nullability():
    from tools.native_delivery_live_support.profiles import Dataset
    from tools.native_delivery_local.layout import business_definitions

    definitions = business_definitions(Dataset("null", 4))
    assert definitions == ["[id] bigint NOT NULL", "[event_at] datetime2(6) NOT NULL", "[value_0] nvarchar(200) NULL"]


def test_stage_scan_counter_delegates_actual_reads():
    from tools.native_delivery_local.faults import Faults, TargetDelegate

    class Connector:
        def get_records_iterator(self, query):
            return iter([(query,)])

    faults = Faults()
    target = TargetDelegate(Connector(), faults)
    assert list(target.get_records_iterator("SELECT [id] FROM [dda].[dpone_native_raw_a]"))
    assert faults.stage_reads == 1
    assert list(target.get_records_iterator("SELECT [id] FROM [dda].[business]"))
    assert faults.stage_reads == 1


def test_describe_is_immutable_and_never_opens_connections():
    from tools.native_delivery_live_support.execution import environment_record
    from tools.native_delivery_local.factory import LocalRouteFactory

    factory = object.__new__(LocalRouteFactory)
    factory._descriptor = {
        "versions": {
            "python": "3.12.14",
            "dpone": "0.79.0",
            "clickhouse": "24.8",
            "mssql": "16.0",
            "bcp": "18.6",
            "distribution.pyodbc": "5.3.0",
        },
        "target_layout_sha256": "a" * 64,
        "resource_profile": {"cpu_count": 2, "memory_bytes": 1073741824},
    }
    assert environment_record(factory)["resource_profile"]["cpu_count"] == 2
    returned = factory.describe()
    returned["versions"]["bcp"] = "changed"
    assert factory.describe()["versions"]["bcp"] == "18.6"


def test_descriptor_requires_explicit_benchmark_preparation():
    from tools.native_delivery_local.factory import LocalRouteFactory

    factory = object.__new__(LocalRouteFactory)
    factory._descriptor = None
    with pytest.raises(RuntimeError, match="benchmark_not_prepared"):
        factory.describe()


def test_canonical_owner_report_preserves_internal_identity(tmp_path):
    from tools.native_delivery_live_support.artifacts import ArtifactStore
    from tools.native_delivery_live_support.maintenance import record_owner
    from tools.native_delivery_local.session import Session

    session = object.__new__(Session)
    session.ownership_id = "a" * 32
    assert session.invocation_id == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert session.ownership_id == "a" * 32
    store = ArtifactStore(tmp_path / "report.json")
    store.preflight()
    record_owner(store, session, "smoke")


def test_runner_descriptor_observes_cgroup_caps(tmp_path):
    from tools.native_delivery_local.descriptor import runner_resources

    (tmp_path / "cpu.max").write_text("200000 100000")
    (tmp_path / "memory.max").write_text("1073741824")
    observed = runner_resources(tmp_path)
    assert observed["cpu_count"] <= 2
    assert observed["memory_bytes"] == 1073741824
