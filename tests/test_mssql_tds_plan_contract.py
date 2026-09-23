"""Durable transport identity preserves historical bytes and rejects reinterpretation."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.cli.main import main
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan


def setup_journal(tmp_path, *, mode=None, transport=None):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(
        "run", "target", "query", "window", "schema", "wire", source_read_mode=mode, transport=transport
    )
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    return store, lease, plan, journal


@pytest.mark.parametrize("mode,version", [(None, 1), ("raw_single_query", 2)])
def test_legacy_journal_literal_bytes_are_unchanged(tmp_path, mode, version):
    store, lease, plan, journal = setup_journal(tmp_path, mode=mode)
    raw_field = '"source_read_mode":"raw_single_query",' if version == 2 else ""
    expected = (
        '{"chunks":{},"complete":null,"completion_metadata":{},"identity":'
        '{"run_id":"run","schema_fingerprint":"schema","source_query_id":"query",'
        + raw_field
        + '"target_id":"target","window_fingerprint":"window","wire_fingerprint":"wire"},'
        '"limits":null,"observations":[],"phase":"staging","publication":null,'
        '"rollback_history":[],"version":' + str(version) + "}"
    )
    record = store.load(journal.key)
    assert record.payload == expected
    NativeChunkJournal(store, lease, plan)
    assert store.load(journal.key) == record


@pytest.mark.parametrize("mode", [None, "raw_single_query"])
@pytest.mark.parametrize("backend,address_space", [("mssql_python", 64 << 20), ("mssql_sqlclient", 8 << 30)])
def test_explicit_transport_uses_v3_and_roundtrips_without_writes(tmp_path, mode, backend, address_space):
    policy = NativeBulkTransportPolicy(backend, "rows", address_space)
    store, lease, plan, journal = setup_journal(tmp_path, mode=mode, transport=policy)
    original = store.load(journal.key)
    assert json.loads(original.payload)["version"] == 3
    assert NativeChunkJournal(store, lease, plan).data == journal.data
    assert store.load(journal.key) == original
    for changed in (
        replace(plan, transport=None),
        replace(plan, transport=replace(policy, input="arrow")),
        replace(plan, transport=replace(policy, batch_rows=1)),
    ):
        with pytest.raises(WindowContractError, match="journal_identity_changed"):
            NativeChunkJournal(store, lease, changed)
    assert store.load(journal.key) == original


@pytest.mark.parametrize("mode", [None, "raw_single_query"])
def test_legacy_cannot_resume_as_tds(tmp_path, mode):
    store, lease, plan, journal = setup_journal(tmp_path, mode=mode)
    with pytest.raises(WindowContractError, match="journal_identity_changed"):
        NativeChunkJournal(
            store, lease, replace(plan, transport=NativeBulkTransportPolicy("mssql_python", "rows", 64 << 20))
        )


@pytest.mark.parametrize("mutation", ["missing_transport", "unresolved_policy", "extra_identity", "raw_mode"])
def test_malformed_v3_rejected_without_repair(tmp_path, mutation):
    store, lease, plan, journal = setup_journal(
        tmp_path, transport=NativeBulkTransportPolicy("mssql_python", "rows", 64 << 20)
    )
    data = journal.data
    data["version"] = 3
    if mutation == "missing_transport":
        del data["identity"]["transport"]
    elif mutation == "unresolved_policy":
        del data["identity"]["transport"]["batch_rows"]
    elif mutation == "extra_identity":
        data["identity"]["extra"] = True
    else:
        data["identity"]["source_read_mode"] = "final"
    store.save(journal.key, journal.revision, json.dumps(data), lease)
    record = store.load(journal.key)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan)
    assert store.load(journal.key) == record


def manifest(tmp_path, value):
    sample = Path(__file__).resolve().parents[1] / "examples/native/clickhouse-to-mssql-native.yaml"
    raw = yaml.safe_load(sample.read_text())
    raw["defaults"]["source"]["options"]["native_transfer"]["execution"]["native_chunks"]["transport"] = value
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize("mode", ["rows", "arrow"])
@pytest.mark.parametrize("output_format", ["text", "json", "md"])
def test_real_cli_plan_exposes_transport_requirements_without_io(tmp_path, monkeypatch, capsys, mode, output_format):
    path = manifest(tmp_path, NativeBulkTransportPolicy("mssql_python", mode, 64 << 20).to_dict())
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 0
    output = capsys.readouterr()
    for expected in (
        "mssql_python",
        mode,
        "linux",
        "address_space_not_rss",
        "separate_restricted_writer_and_coordinator",
        "bounded_mssql_native_tds",
        "unverified",
    ):
        assert expected in output.out
    assert not output.err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"backend": "mssql_python"},
        {"backend": "bcp", "input": "rows", "max_worker_address_space_bytes": 64 << 20},
    ],
)
def test_real_cli_invalid_transport_returns_configuration_error(tmp_path, monkeypatch, capsys, caplog, value):
    path = manifest(tmp_path, value)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", "json"])
    assert error.value.code == 2
    assert "transport" in caplog.text
    assert "Traceback" not in caplog.text
    assert capsys.readouterr().out == ""
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("recovered", [False, True])
def test_transport_mismatch_blocks_source_and_resume(tmp_path, recovered):
    from tests.test_mssql_native_policy import config
    from tests.test_mssql_native_runtime import runtime

    value, events, _ = runtime(tmp_path, recovered=recovered)
    cfg = config()
    cfg.options["native_transfer"]["execution"]["native_chunks"]["transport"] = NativeBulkTransportPolicy(
        "mssql_python", "rows", 64 << 20
    ).to_dict()
    with pytest.raises(WindowContractError, match="transport_policy_mismatch"):
        value.run(cfg, owner="invocation")
    assert "source" not in events
    assert "resume" not in events
