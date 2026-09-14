"""CLI/compiled-identity contracts with frozen pre-policy legacy authority.

Risk matrix: valid absent/raw modes and all output formats; malformed authored
policies fail before row I/O; compile/reload is deterministic; legacy journal
reopen preserves exact bytes. Local SQLite is real; planning remains offline,
and these tests do not certify ClickHouse or SQL Server integration.
"""

import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

import dpone
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.etl.mssql_transaction_route_identity import invocation_route_fingerprint
from dpone.services.manifest import resolve_single_process

MANIFEST = {
    "name": "synthetic_raw_identity",
    "source": {
        "type": "clickhouse",
        "connection_id": "synthetic_source",
        "table": {"schema": "synthetic", "name": "events"},
        "options": {
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "chunking": {"mode": "bounded_stream", "parallelism": 2, "checkpointing": "resumable"},
                    "native_chunks": {
                        "max_total_encoded_bytes": 100000,
                        "stage_allocated_bytes_stop_threshold": 100000,
                    },
                },
            }
        },
    },
    "sink": {
        "type": "mssql",
        "connection_id": "synthetic_target",
        "table": {"schema": "dbo", "name": "events"},
        "strategy": {"mode": "full_refresh"},
    },
}
# Captured from pre-policy 7e6091386983cf3cdef08a97b43a315fc5e54631, not recalculated by
# production serialization inside the assertion.
LEGACY_JOURNAL = (
    '{"chunks":{},"complete":null,"completion_metadata":{},"identity":{"run_id":"run",'
    '"schema_fingerprint":"schema","source_query_id":"query","target_id":"target",'
    '"window_fingerprint":"window","wire_fingerprint":"wire"},"limits":null,"observations":[],'
    '"phase":"staging","publication":null,"rollback_history":[],"version":1}'
)
LEGACY_FINGERPRINT = "cab83b34ff578b52e5c54e1464a1f6ae76ee5dd5cb64a6d871c566d246a1592a"


def write_manifest(tmp_path, policy="absent"):
    raw = deepcopy(MANIFEST)
    if policy != "absent":
        raw["source"]["options"]["native_transfer"]["source_read"] = policy
    path = tmp_path / "synthetic.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


def compiled_config(path):
    manifest = ManifestLoaderRouter().load(path, metadata_only=True)
    return resolve_single_process(manifest, selector=None).config.load_config


def fingerprint(config):
    # Version-one identity includes these compilation paths. Bind only the two
    # host-dependent locations to fixed synthetic values before comparing the
    # baseline; preserve all compiled policy/strategy/source options unchanged.
    config = deepcopy(config)
    config.options["manifest_dir"] = "/synthetic/manifests"
    config.options["repo_root"] = "/synthetic/repository"
    identity = SourcePhysicalIdentity(
        dialect="clickhouse",
        cluster_identifier="synthetic-cluster",
        database="synthetic",
        effective_principal="reader",
        session_principal="reader",
    )
    return invocation_route_fingerprint(config, target_identity=b"t" * 32, source_identity=identity).hex()


def test_legacy_v1_journal_payload_is_frozen_and_reopen_is_read_only(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    original = store.load(journal.key)
    assert original.payload == LEGACY_JOURNAL
    assert NativeChunkJournal(store, lease, plan).data == json.loads(LEGACY_JOURNAL)
    assert store.load(journal.key) == original


def test_real_manifest_compilation_preserves_policy_and_legacy_route_fingerprint(tmp_path):
    legacy_path = write_manifest(tmp_path)
    legacy = compiled_config(legacy_path)
    assert "source_read" not in legacy.options["native_transfer"]
    assert fingerprint(legacy) == LEGACY_FINGERPRINT
    assert fingerprint(compiled_config(legacy_path)) == LEGACY_FINGERPRINT
    raw_path = write_manifest(tmp_path, {"mode": "raw_single_query"})
    raw = compiled_config(raw_path)
    assert raw.options["native_transfer"]["source_read"] == {"mode": "raw_single_query"}
    assert fingerprint(raw) != LEGACY_FINGERPRINT
    assert fingerprint(compiled_config(raw_path)) == fingerprint(raw)


def invoke_cli(path, output_format):
    """Run the entrypoint from the exact package selected by the parent test.

    The subprocess changes cwd. Resolve inherited PYTHONPATH entries against the
    parent cwd first, and prefer the imported package's location over an older
    editable install. This also respects installed-wheel test selection.
    """
    environment = os.environ.copy()
    inherited = environment.get("PYTHONPATH")
    search_paths = [str(Path(dpone.__file__).resolve().parent.parent)]
    if inherited is not None:
        search_paths.extend(str(Path(entry).resolve()) for entry in inherited.split(os.pathsep))
    environment["PYTHONPATH"] = os.pathsep.join(search_paths)
    return subprocess.run(
        [sys.executable, "-c", "from dpone.cli.main import main; main()", "plan", str(path), "--format", output_format],
        cwd=path.parent,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize("output_format", ["json", "text", "md"])
@pytest.mark.parametrize("raw", [False, True])
def test_real_cli_valid_policy_plans_without_claiming_execution(tmp_path, output_format, raw):
    path = write_manifest(tmp_path, {"mode": "raw_single_query"} if raw else "absent")
    result = invoke_cli(path, output_format)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert set(item.name for item in tmp_path.iterdir()) == {"synthetic.yaml"}
    if output_format == "json":
        payload = json.loads(result.stdout)
        native = payload["mssql_native"]
        assert native["status"] == "composition_required"
        assert native["live_preflight"] == "not_run"
        assert native["source_query_count"] == 1
        assert payload["native_transfer_route_decision"]["certification_status"] == "unverified"
        if raw:
            assert native["source_read_mode"] == "raw_single_query"
        else:
            assert "source_read_mode" not in native
    else:
        assert "composition_required" in result.stdout
        if raw:
            assert "raw_single_query" in result.stdout
        else:
            assert "raw_single_query" not in result.stdout


@pytest.mark.parametrize("output_format", ["json", "text", "md"])
@pytest.mark.parametrize("policy", [None, {}, {"mode": "other"}, {"mode": "raw_single_query", "extra": True}])
def test_real_cli_rejects_malformed_policy_with_no_success_plan(tmp_path, output_format, policy):
    path = write_manifest(tmp_path, policy)
    result = invoke_cli(path, output_format)
    assert result.returncode == 2
    assert "mssql_native.source_read_invalid" in result.stderr
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert set(item.name for item in tmp_path.iterdir()) == {"synthetic.yaml"}


@pytest.mark.parametrize("installed_mode", ["0", "1"])
def test_cli_subprocess_keeps_imported_package_with_relative_pythonpath(tmp_path, monkeypatch, installed_mode):
    """Changing subprocess cwd must not switch back to a stale installed dpone."""
    monkeypatch.setenv("PYTHONPATH", "src")
    monkeypatch.setenv("DPONE_TEST_USE_INSTALLED_PACKAGE", installed_mode)
    path = write_manifest(tmp_path, {"mode": "raw_single_query"})
    result = invoke_cli(path, "json")
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert json.loads(result.stdout)["mssql_native"]["source_read_mode"] == "raw_single_query"
