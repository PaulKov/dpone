"""Source-free, read-only inspection of durable MSSQL native v2 journals."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest

from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader
from dpone.app import mssql_native_recovery_application
from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication, _digest
from dpone.cli.parser import build_parser
from dpone.commands import mssql_native_recovery_cmd
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_custody import NativeTargetCustodyRecord
from dpone.contracts.mssql_native_recovery import classify_mssql_native_recovery, mssql_native_recovery_v2_schema
from dpone.contracts.mssql_native_verification_identity import build_bcp_target_local_verification_identity

SCHEMA_PATH = Path("src/dpone/schema/dpone.mssql-native-recovery.v2.schema.json")


def _identity():
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    return build_bcp_target_local_verification_identity(plan, timeout_seconds=30)


def _projection(identity, *, phase="staging", publication=None):
    return {
        "version": 2,
        "identity": identity.document(),
        "phase": phase,
        "chunks": {},
        "events": {},
        "nonces": {},
        "complete": None,
        "observations": [],
        "publication": publication,
        "completion_metadata": {},
        "rollback_history": [],
        "limits": None,
    }


def _database(path: Path, identity, projection) -> None:
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE window_records (key TEXT PRIMARY KEY, revision INTEGER, payload TEXT)")
        database.execute(
            "INSERT INTO window_records VALUES (?,?,?)",
            (
                f"mssql-native-chunks-v2/{identity.invocation_key}",
                1,
                json.dumps(projection, sort_keys=True, separators=(",", ":")),
            ),
        )


def test_checked_in_recovery_schema_matches_producer() -> None:
    schema = mssql_native_recovery_v2_schema()
    assert json.loads(SCHEMA_PATH.read_text(encoding="utf-8")) == schema


def test_inspect_returns_opaque_closed_projection(tmp_path: Path) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))

    result = MssqlNativeRecoveryJournalReader(path).inspect(identity.invocation_key)

    assert result["invocation_id"] == identity.invocation_key
    assert result["state"] == "EMPTY_STAGING"
    assert result["permitted_actions"] == ["retire"]
    assert len(result["artifact_refs"]) == 1
    assert result["attempts"] == []
    assert not list(jsonschema.Draft7Validator(mssql_native_recovery_v2_schema()).iter_errors(result))
    serialized = json.dumps(result)
    assert "target" not in serialized and "query" not in serialized


def test_inspect_all_counts_held_recovery_records(tmp_path: Path) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))

    result = MssqlNativeRecoveryJournalReader(path).inspect_all()

    assert result == {
        "schema_version": 1,
        "kind": "dpone.mssql-native-recovery-index.v1",
        "retained_custody_count": 0,
        "items": [MssqlNativeRecoveryJournalReader(path).inspect(identity.invocation_key)],
    }


def test_inspect_authenticates_pre_eof_recovery_plan(tmp_path: Path) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))
    plan = {
        "schema_version": 2,
        "kind": "dpone.mssql-native-recovery-plan.v2",
        "invocation_id": identity.invocation_key,
        "schema": [["row_key", "bigint"]],
        "window": {
            "column": "observed_at",
            "start": "2026-09-21T00:00:00+00:00",
            "end": "2026-09-28T00:00:00+00:00",
        },
        "recovery_authority_v1": {"sealed": True},
    }
    with sqlite3.connect(path) as database:
        database.execute(
            "INSERT INTO window_records VALUES (?,?,?)",
            (
                f"mssql-native/recovery-plan-v1/{identity.invocation_key}",
                1,
                json.dumps(plan, sort_keys=True, separators=(",", ":")),
            ),
        )

    reader = MssqlNativeRecoveryJournalReader(path)
    snapshot = reader.load(identity.invocation_key)
    inspected = reader.inspect(identity.invocation_key)

    assert snapshot.recovery_plan == plan
    assert snapshot.recovery_plan_sha256 in inspected["artifact_refs"]
    assert len(inspected["artifact_refs"]) == 2


def test_recovery_restores_exact_sealed_rolling_window_without_operator_input() -> None:
    config = LoadConfig(
        "source",
        "target",
        "raw",
        "events",
        "dbo",
        "events",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        options={
            "mssql_native_window": {
                "column": "observed_at",
                "anchor": "data_interval_end",
                "lookback": "P7D",
                "chunk_interval": "P1D",
                "timezone": "UTC",
            }
        },
    )
    sealed = {
        "column": "observed_at",
        "start": "2026-09-21T00:00:00+00:00",
        "end": "2026-09-28T00:00:00+00:00",
    }
    expected = _digest((sealed["column"], sealed["start"], sealed["end"]))

    restored = MssqlNativeRecoveryApplication._restore_window(
        config,
        {"schema_version": 2, "window": sealed},
        expected,
    )

    assert "interval" not in config.options
    assert restored.options["interval"] == {
        "interval_start": sealed["start"],
        "interval_end": sealed["end"],
    }


def test_recovery_rejects_sealed_window_that_does_not_match_identity() -> None:
    config = LoadConfig(
        "source",
        "target",
        "raw",
        "events",
        "dbo",
        "events",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        options={
            "mssql_native_window": {
                "column": "observed_at",
                "anchor": "data_interval_end",
                "lookback": "P7D",
                "timezone": "UTC",
            }
        },
    )
    sealed = {
        "column": "observed_at",
        "start": "2026-09-21T00:00:00+00:00",
        "end": "2026-09-28T00:00:00+00:00",
    }

    with pytest.raises(ValueError, match="recovery_window_changed"):
        MssqlNativeRecoveryApplication._restore_window(
            config,
            {"schema_version": 2, "window": sealed},
            "f" * 64,
        )


def test_recovery_validates_target_identity_before_storage_preflight(tmp_path: Path, monkeypatch) -> None:
    identity = build_bcp_target_local_verification_identity(
        NativeChunkPlan("run", "target", "query", _digest(None), "schema", "wire"),
        timeout_seconds=30,
    )
    snapshot = SimpleNamespace(
        identity=identity,
        projection={"completion_metadata": {}},
        recovery_plan=None,
    )

    class Reader:
        path = tmp_path / "window-state.sqlite"

        def inspect(self, _invocation_id):
            return {"permitted_actions": ["retire"]}

        def load(self, _invocation_id):
            return snapshot

    config = LoadConfig(
        "source",
        "target",
        "raw",
        "events",
        "dbo",
        "events",
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable"},
                    "native_chunks": {
                        "max_total_encoded_bytes": 1024,
                        "stage_allocated_bytes_stop_threshold": 1024,
                    },
                },
            },
        },
    )
    process = SimpleNamespace(
        load_config=config,
        raw_config={"runtime": {"storage": {"work_dir": str(tmp_path / "must-not-exist")}}},
        sink_obj=SimpleNamespace(connector=object(), state_storage=object()),
        ensure_runtime_bindings=lambda: None,
    )
    monkeypatch.setattr(mssql_native_recovery_application, "MssqlNativeRecoveryJournalReader", lambda _: Reader())
    monkeypatch.setattr(
        mssql_native_recovery_application,
        "live_target_coordinates",
        lambda _: ("db", "dbo", "events"),
    )
    monkeypatch.setattr(
        mssql_native_recovery_application,
        "resolve_atomic_mssql_target",
        lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"changed",
            database_name="db",
            schema_name="dbo",
            table_name="events",
        ),
    )

    class ForbiddenPreflight:
        def check(self, _policy):
            pytest.fail("storage preflight must follow sealed target validation")

    monkeypatch.setattr(mssql_native_recovery_application, "StoragePreflightService", ForbiddenPreflight)

    with pytest.raises(ValueError, match="recovery_target_changed"):
        MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=tmp_path,
            invocation_id=identity.invocation_key,
            action="retire",
            owner="test",
            confirmed=True,
        )
    assert not (tmp_path / "must-not-exist").exists()


def test_inspect_all_counts_actual_held_custody_and_does_not_let_event_limit_hide_root(tmp_path: Path) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))
    target_sha256 = hashlib.sha256(identity.plan.target_id.encode()).hexdigest()
    custody = NativeTargetCustodyRecord(
        1,
        "dpone.mssql-native-target-custody",
        target_sha256,
        1,
        "held",
        identity.invocation_key,
        1,
        None,
    )
    with sqlite3.connect(path) as database:
        database.execute(
            "INSERT INTO window_records VALUES (?,?,?)",
            (f"mssql-native-target-custody-v1/{target_sha256}", 1, custody.payload()),
        )
        database.executemany(
            "INSERT INTO window_records VALUES (?,?,?)",
            [
                (f"mssql-native-chunks-v2/{identity.invocation_key}/{ordinal:020d}/event", 1, "{}")
                for ordinal in range(1001)
            ],
        )

    result = MssqlNativeRecoveryJournalReader(path).inspect_all()

    assert result["retained_custody_count"] == 1
    assert [item["invocation_id"] for item in result["items"]] == [identity.invocation_key]


def test_inspect_rejects_identity_key_mismatch(tmp_path: Path) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE window_records SET key=?",
            (f"mssql-native-chunks-v2/{'f' * 64}",),
        )

    with pytest.raises(ValueError, match="identity_mismatch"):
        MssqlNativeRecoveryJournalReader(path).inspect("f" * 64)


def test_inspect_is_read_only_and_requires_exact_database(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(ValueError, match="journal_unavailable"):
        MssqlNativeRecoveryJournalReader(missing)
    assert not missing.exists()


def test_terminal_projection_distinguishes_retirement_and_released_custody() -> None:
    identity = _identity()
    retired = _projection(identity)
    retired["events"] = {"attempt": [{"event": "RETIRED"}]}
    assert classify_mssql_native_recovery(retired)[0] == "RETIRED"
    succeeded = _projection(identity, publication={"phase": "succeeded"})
    assert classify_mssql_native_recovery(succeeded, custody_state="clear")[0] == "CUSTODY_RELEASED"


@pytest.mark.parametrize(
    ("projection", "state", "action"),
    [
        ({"events": {"attempt": [{"event": "RETIRED"}]}}, "RETIRED", "retire"),
        ({"publication": {"phase": "succeeded"}}, "SUCCEEDED", "resume"),
    ],
)
def test_terminal_projection_keeps_cleanup_action_while_custody_is_held(
    projection: dict[str, object], state: str, action: str
) -> None:
    identity = _identity()
    value = _projection(identity)
    value.update(projection)

    classified = classify_mssql_native_recovery(value, custody_state="held")

    assert classified == (state, classified[1], ["inspect", action])
    assert classify_mssql_native_recovery(value, custody_state="clear")[2] == ["inspect"]


def test_public_cli_inspects_without_application_context(tmp_path: Path, capsys) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))
    args = build_parser().parse_args(
        [
            "ops",
            "mssql-native-recovery",
            "inspect",
            identity.invocation_key,
            "--journal-root",
            str(path),
            "--format",
            "json",
        ]
    )

    assert args._command.run(args, None) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "EMPTY_STAGING"


def test_inspect_all_markdown_lists_actionable_invocations(tmp_path: Path, capsys) -> None:
    identity = _identity()
    path = tmp_path / "window-state.sqlite"
    _database(path, identity, _projection(identity))
    args = build_parser().parse_args(
        ["ops", "mssql-native-recovery", "inspect-all", "--journal-root", str(path), "--format", "md"]
    )

    assert args._command.run(args, None) == 0
    output = capsys.readouterr().out
    assert identity.invocation_key in output
    assert "EMPTY_STAGING" in output
    assert "retire" in output


def test_mutation_rejects_missing_confirmation_before_loading_manifest(monkeypatch, capsys) -> None:
    def forbidden(_value):
        pytest.fail("manifest loading must follow explicit mutation confirmation")

    monkeypatch.setattr(mssql_native_recovery_cmd.ETLProcessConfig, "from_yaml", forbidden)
    args = build_parser().parse_args(
        [
            "ops",
            "mssql-native-recovery",
            "retire",
            "a" * 64,
            "--manifest",
            "missing.yml",
            "--journal-root",
            "missing.sqlite",
        ]
    )

    assert args._command.run(args, None) == 1
    assert capsys.readouterr().err.strip() == "mssql_native.recovery_confirmation_required"


@pytest.mark.parametrize("action", ["reconcile", "resume", "retire"])
def test_public_cli_registers_confirmed_manifest_bound_mutations(
    action: str, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    calls = []
    monkeypatch.setattr(
        mssql_native_recovery_cmd.ETLProcessConfig,
        "from_yaml",
        lambda value: {"manifest": value},
    )

    class Application:
        def execute(self, process, **values):
            calls.append((process, values))
            return {
                "kind": "dpone.mssql-native-recovery.v2",
                "invocation_id": "a" * 64,
                "state": "RETIRED",
                "diagnostic_code": "mssql_native.stages_retired",
                "permitted_actions": ["inspect"],
            }

    monkeypatch.setattr(mssql_native_recovery_cmd, "MssqlNativeRecoveryApplication", Application)
    args = build_parser().parse_args(
        [
            "ops",
            "mssql-native-recovery",
            action,
            "a" * 64,
            "--manifest",
            "route.yml#daily",
            "--journal-root",
            "state.sqlite",
            "--yes",
        ]
    )

    assert args._command.run(args, None) == 0
    assert calls[0][0] == {"manifest": "route.yml#daily"}
    assert calls[0][1]["action"] == action
    assert calls[0][1]["confirmed"] is True
    assert json.loads(capsys.readouterr().out)["state"] == "RETIRED"


@pytest.mark.parametrize("version", [1, 2, 3])
def test_raw_feature_does_not_introduce_recovery_plan_v3(tmp_path, version):
    identity = _identity()
    path = tmp_path / "state.sqlite"
    _database(path, identity, _projection(identity))
    plan = {
        "schema_version": version,
        "kind": f"dpone.mssql-native-recovery-plan.v{version}",
        "invocation_id": identity.invocation_key,
        "schema": [["id", "bigint"]],
        "recovery_authority_v1": {"sealed": True},
    }
    if version != 1:
        plan["window"] = None
    with sqlite3.connect(path) as database:
        database.execute(
            "INSERT INTO window_records VALUES (?,?,?)",
            (f"mssql-native/recovery-plan-v1/{identity.invocation_key}", 1, json.dumps(plan)),
        )
    reader = MssqlNativeRecoveryJournalReader(path)
    if version == 3:
        with pytest.raises(ValueError, match="recovery_plan_invalid"):
            reader.load(identity.invocation_key)
    else:
        assert reader.load(identity.invocation_key).recovery_plan == plan
