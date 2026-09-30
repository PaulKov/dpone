"""Historical-default proof on fresh, isolated, task-owned Docker servers.

These tests exercise a certification-only producer, not the future production
observer. They cannot activate the range route or certify protected publication.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(os.getenv("DPONE_EPOCH_PROBE_LIVE") != "1", reason="owned Docker epoch probe is opt-in"),
]


@pytest.fixture
def epoch(tmp_path: Path, request: pytest.FixtureRequest):
    from tests.integration.clickhouse_table_compatibility_probe import OwnedDockerEpochSource

    with OwnedDockerEpochSource.bootstrap(tmp_path, persistent=getattr(request, "param", False)) as source:
        yield source


def test_omitted_defaults_require_precreation_epoch(epoch):
    """Current globals and real table existence must not fabricate CREATE coverage."""
    epoch.server.query("CREATE TABLE default.uncovered (id UInt64) ENGINE=MergeTree ORDER BY id")
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("uncovered")


def test_owned_epoch_existing_table_inherits_omitted_setting_across_process(epoch):
    """A fresh interpreter resolves omitted values only from original coverage."""
    epoch.create("covered", "index_granularity=4096")
    process = subprocess.run(
        [sys.executable, "-m", "tests.integration.clickhouse_table_compatibility_probe", str(epoch.root), "covered"],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    evidence = json.loads(process.stdout)
    assert evidence["explicit_settings"]["index_granularity"] == "4096"
    assert evidence["inherited_settings"]["index_granularity_bytes"] == "10485760"
    assert evidence["table_uuid"]
    assert evidence["coverage_sequence"] > evidence["activation_sequence"]
    assert evidence["server_version"] == "24.8.14.39"


def test_same_globals_after_restart_invalidates_epoch(epoch):
    """A server restart cannot reuse old provenance even if defaults match."""
    epoch.create("covered")
    original = epoch.server.globals()
    epoch.server.restart()
    assert epoch.server.globals() == original
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("covered")


def test_reload_invalidates_epoch(epoch):
    epoch.create("covered")
    epoch.invalidate("configuration_reload")
    epoch.server.query("SYSTEM RELOAD CONFIG")
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("covered")


def test_missing_create_ack_is_not_reconstructed_from_catalog(epoch):
    epoch.create("uncertain", acknowledge=False)
    assert epoch.server.query("EXISTS TABLE default.uncertain")[0]["result"] == 1
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("uncertain")


def test_journal_copy_cannot_replace_original(epoch):
    import shutil

    epoch.create("covered")
    replacement = epoch.root / "replacement.db"
    shutil.copyfile(epoch.journal, replacement)
    os.replace(replacement, epoch.journal)
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("covered")


@pytest.mark.parametrize("fault", ["truncate", "delete_tail", "corrupt_payload"])
def test_damaged_epoch_journal_fails_closed(epoch, fault):
    import sqlite3

    epoch.create("covered")
    if fault == "truncate":
        with epoch.journal.open("r+b") as output:
            output.truncate(128)
    else:
        with sqlite3.connect(epoch.journal) as db:
            if fault == "delete_tail":
                db.execute("DELETE FROM events WHERE sequence=(SELECT max(sequence) FROM events)")
            else:
                db.execute("UPDATE events SET payload='{}' WHERE sequence=2")
    with pytest.raises(ValueError, match="settings_provenance_unverified"):
        epoch.observe("covered")


@pytest.mark.parametrize(
    ("settings", "part_type"),
    [
        ("index_granularity=4096, min_rows_for_wide_part=0, min_bytes_for_wide_part=0", "Wide"),
        ("index_granularity=8192, min_rows_for_wide_part=10000, min_bytes_for_wide_part=1000000", "Compact"),
        (
            "index_granularity=4096, index_granularity_bytes=0, enable_mixed_granularity_parts=0, "
            "min_rows_for_wide_part=0, min_bytes_for_wide_part=0",
            "Wide",
        ),
    ],
)
def test_pinned_settings_and_parts_support_equal_settings_replace(epoch, settings, part_type):
    """Server characterization only: preserve UUID and the complete tuple() snapshot."""
    for name in ("target", "candidate"):
        epoch.create(name, settings)
    epoch.server.query("INSERT INTO default.target VALUES (99)")
    epoch.server.query("INSERT INTO default.candidate VALUES (1), (3), (3)")
    before = epoch.observe("target")
    parts = epoch.server.query(
        "SELECT table, partition_id, part_type, rows FROM system.parts "
        "WHERE database='default' AND active ORDER BY table, name"
    )
    assert {part["part_type"] for part in parts} == {part_type}
    assert {part["partition_id"] for part in parts} == {"all"}
    epoch.server.query("ALTER TABLE default.target REPLACE PARTITION tuple() FROM default.candidate")
    # The pinned server quotes UInt64 in JSONEachRow; compare exact wire values.
    assert epoch.server.query("SELECT id FROM default.target ORDER BY id") == [{"id": "1"}, {"id": "3"}, {"id": "3"}]
    assert epoch.observe("target")["table_uuid"] == before["table_uuid"]
    assert epoch.server.query("SELECT id FROM default.candidate ORDER BY id") == [{"id": "1"}, {"id": "3"}, {"id": "3"}]
    (epoch.root / "parts.json").write_text(json.dumps(parts, indent=2), encoding="utf-8")


@pytest.mark.parametrize(
    "settings", ["index_granularity=0", "index_granularity_bytes=1023, min_index_granularity_bytes=1024"]
)
def test_pinned_server_rejects_invalid_joint_granularity_bounds(epoch, settings):
    with pytest.raises(RuntimeError, match="Owned Docker probe command failed"):
        epoch.create("invalid", settings)
    assert epoch.server.query("EXISTS TABLE default.invalid")[0]["result"] == 0


def test_pinned_globals_remain_cached_across_config_reload(epoch):
    """Characterize whether current globals can resolve an older loaded table.

    This intentionally changes only this fixture's config and invalidates its
    epoch first. It is not permission for dpone to reconfigure a user's server.
    """
    before = epoch.server.globals()["min_rows_for_wide_part"]
    assert before == "0"
    epoch.server.query("CREATE TABLE default.manual (id UInt64) ENGINE=MergeTree ORDER BY id")
    epoch.invalidate("controlled_configuration_experiment")
    config = epoch.server.config
    config.chmod(0o644)
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "<clickhouse>",
            "<clickhouse><merge_tree><min_rows_for_wide_part>1234</min_rows_for_wide_part></merge_tree>",
            1,
        ),
        encoding="utf-8",
    )
    config.chmod(0o444)
    epoch.server.query("SYSTEM RELOAD CONFIG")
    assert epoch.server.globals()["min_rows_for_wide_part"] == before
    # The same changed file is effective on a new server incarnation.
    epoch.server.restart()
    assert epoch.server.globals()["min_rows_for_wide_part"] == "1234"


@pytest.mark.parametrize("epoch", [True], indirect=True)
def test_pinned_restart_and_alter_resolve_preexisting_table_settings(epoch):
    """No CREATE receipt: persisted data, current defaults and overrides suffice.

    Physical parts independently witness the setting used by a loaded table;
    reading system.merge_tree_settings alone would not prove that relationship.
    """
    epoch.server.query(
        "CREATE TABLE default.manual (id UInt64) ENGINE=MergeTree PARTITION BY id ORDER BY id "
        "SETTINGS min_bytes_for_wide_part=0"
    )
    epoch.server.query("SYSTEM STOP MERGES default.manual")
    original = epoch.table("manual")
    epoch.server.query("INSERT INTO default.manual VALUES (1)")
    assert epoch.server.query(
        "SELECT part_type FROM system.parts WHERE database='default' AND table='manual' AND active"
    ) == [{"part_type": "Wide"}]
    epoch.invalidate("controlled_configuration_experiment")
    config = epoch.server.config
    config.chmod(0o644)
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "<clickhouse>",
            "<clickhouse><merge_tree><min_rows_for_wide_part>1234</min_rows_for_wide_part></merge_tree>",
            1,
        ),
        encoding="utf-8",
    )
    config.chmod(0o444)
    epoch.server.query("SYSTEM RELOAD CONFIG")
    epoch.server.query("INSERT INTO default.manual VALUES (2)")
    assert epoch.server.globals()["min_rows_for_wide_part"] == "0"
    assert epoch.server.query(
        "SELECT DISTINCT part_type FROM system.parts WHERE database='default' AND table='manual' AND active"
    ) == [{"part_type": "Wide"}]
    epoch.server.restart()
    assert epoch.server.query("EXISTS TABLE default.manual")[0]["result"] == 1
    assert epoch.table("manual") == original
    assert epoch.server.query("SELECT id FROM default.manual ORDER BY id") == [{"id": "1"}, {"id": "2"}]
    assert epoch.server.globals()["min_rows_for_wide_part"] == "1234"
    epoch.server.query("SYSTEM STOP MERGES default.manual")
    epoch.server.query("INSERT INTO default.manual VALUES (3)")
    epoch.server.query("ALTER TABLE default.manual MODIFY SETTING min_rows_for_wide_part=0")
    epoch.server.query("INSERT INTO default.manual VALUES (4)")
    assert "min_rows_for_wide_part = 0" in epoch.table("manual")["ddl"]
    epoch.server.query("ALTER TABLE default.manual RESET SETTING min_rows_for_wide_part")
    assert "min_rows_for_wide_part" not in epoch.table("manual")["ddl"]
    epoch.server.query("INSERT INTO default.manual VALUES (5)")
    parts = epoch.server.query("SELECT id, _part FROM default.manual ORDER BY id")
    layouts = {
        row["name"]: row["part_type"]
        for row in epoch.server.query(
            "SELECT name, part_type FROM system.parts WHERE database='default' AND table='manual' AND active"
        )
    }
    # Each witness has its own partition and cannot merge with another row
    # during the restart-to-STOP-MERGES window. Keep all layout assertions.
    assert [(row["id"], layouts[row["_part"]]) for row in parts] == [
        ("1", "Wide"),
        ("2", "Wide"),
        ("3", "Compact"),
        ("4", "Wide"),
        ("5", "Compact"),
    ]
    assert epoch.table("manual")["uuid"] == original["uuid"]
    (epoch.root / "current-settings-characterization.json").write_text(
        json.dumps(
            {
                "scope": "raw pinned-server characterization, not production resolver certification",
                "creation_receipt_present": False,
                "before": original,
                "after": epoch.table("manual"),
                "current_globals": epoch.server.globals(),
                "rows_and_parts": parts,
                "part_layouts": layouts,
                "server_identity": epoch.server.identity(),
                "server_version": epoch.server.query("SELECT version() AS version")[0]["version"],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_bootstrap_journal_failure_still_stops_owned_server(tmp_path, monkeypatch):
    from tests.integration.clickhouse_table_compatibility_probe import OwnedDockerEpochSource
    from tests.integration.clickhouse_table_compatibility_support import docker

    original_append = OwnedDockerEpochSource.append
    servers = []

    def fail_after_start(source, kind, facts):
        if kind in {"activated", "invalidated"}:
            servers.append(source.server)
            raise OSError("injected epoch append failure")
        return original_append(source, kind, facts)

    monkeypatch.setattr(OwnedDockerEpochSource, "append", fail_after_start)
    try:
        with pytest.raises(OSError, match="injected epoch append failure"):
            OwnedDockerEpochSource.bootstrap(tmp_path)
        state = json.loads(docker("inspect", servers[0].name))[0]["State"]
        assert state["Running"] is False
    finally:
        # RED must not leak this exact fixture even when bootstrap cleanup fails.
        if servers:
            servers[0].stop()


def test_epoch_transaction_releases_database_connection(tmp_path):
    import sqlite3

    from tests.integration.clickhouse_table_compatibility_probe import OwnedDockerEpochSource
    from tests.integration.clickhouse_table_compatibility_support import OwnedClickHouse

    source = OwnedDockerEpochSource(tmp_path, OwnedClickHouse("unused", tmp_path / "unused.xml"))
    with source.connection() as connection:
        connection.execute("CREATE TABLE resource_lifetime (value INTEGER)")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
