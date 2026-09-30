from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def _module():
    path = Path("tools/mssql_sqlclient_transport_campaign.py")
    spec = importlib.util.spec_from_file_location("mssql_sqlclient_transport_campaign", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _evidence(module, cell, commit: str, tree: str, image: str) -> dict[str, object]:
    success = cell.scenario == "success"
    return {
        "schema_version": module._EVIDENCE_VERSION,
        "status": "PASS",
        "scenario": cell.scenario,
        "source_commit_sha": commit,
        "source_tree_oid": tree,
        "runner_image_sha256": image,
        "fixture_id": cell.fixture_id,
        "row_count": cell.row_count,
        "receipt_count": 1 if success else 0,
        "import_parallelism": cell.import_parallelism,
        "layout_version": cell.layout_version,
        "elapsed_seconds": 1.0,
        "stage_seconds": 0.8 if success else None,
        "verification_seconds": 0.2 if success else None,
        "delivery_phases": (
            {
                name: {
                    "effective_parallelism": 1.0,
                    "failed_operations": 0,
                    "operations": 1,
                    "peak_workers": 1,
                    "wall_seconds": 0.5,
                    "worker_active_seconds": 0.5,
                }
                for name in ("encode", "import_verify")
            }
            if success
            else {}
        ),
        "writer_phases": ({"launch_seconds": 0.1, "write_seconds": 0.5, "dispose_seconds": 0.1} if success else {}),
        "business_rows_read_back": 0,
        "bcp_process_count": 0,
        "synthetic_only": True,
        "package_version": "0.88.0",
        "artifact_sha256": "a" * 64,
        "writer_identity_sha256": "b" * 64,
        "runtime_identity_sha256": "c" * 64,
        "protocol": f"dpone.mssql-sqlclient.ipc.v{cell.layout_version}",
        "recovery_classification": "not_required" if success else "partial_retired",
        "recovery_observation": (
            None
            if success
            else {
                "initial_terminal": "UNKNOWN",
                "initial_writer_outcome": "lost_ack",
                "bulk_copy_active": True,
                "transaction_active": True,
                "session_applock_held": True,
                "exact_stage_lock_held": True,
                "competing_writer_active": True,
                "competing_writer_ignored": True,
                "barrier_settled": True,
                "repeated_digest_stable": True,
                "final_terminal": "RETIRED",
                "stage_absent": True,
                "custody_clear": True,
                "source_reopened": False,
                "writer_relaunched": False,
            }
        ),
    }


def _complete_dir(tmp_path: Path, module, commit: str, tree: str = "e" * 40, image: str = "f" * 64) -> Path:
    root = tmp_path / "evidence"
    root.mkdir(parents=True)
    for index, cell in enumerate(module.REQUIRED_CELLS):
        (root / f"cell-{index}.json").write_text(
            json.dumps(_evidence(module, cell, commit, tree, image)), encoding="utf-8"
        )
    return root


def _runner(tmp_path: Path, commit: str, tree: str = "e" * 40, image: str = "f" * 64) -> Path:
    path = tmp_path / "runner.json"
    evidence = tmp_path / "evidence"
    artifacts = {item.name: hashlib.sha256(item.read_bytes()).hexdigest() for item in sorted(evidence.glob("*.json"))}
    path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.mssql-sqlclient.certification-runner.v4",
                "status": "PASS",
                "source_commit_sha": commit,
                "source_tree_oid": tree,
                "runner_image_sha256": image,
                "runner_platform": "linux/amd64",
                "docker_server_architecture": "amd64",
                "source_mode": "exact_git_archive",
                "worktree_dirty": False,
                "execution_count": 7,
                "executions": [
                    {
                        "scenario": scenario,
                        "fixture_id": fixture,
                        "row_count": rows,
                        "layout_version": layout,
                        "import_parallelism": parallelism,
                        "max_rows": (
                            5_000
                            if scenario == "force_kill_recovery"
                            else 8_192
                            if fixture == "wide100-sqlclient-v1"
                            else 65_536
                        ),
                        "max_bytes": 48 << 20,
                        "max_pending": 1,
                        "max_staging_tables": 128,
                        "encoding_parallelism": 2,
                        "retained_work_capacity": max(2, parallelism) + 1,
                        "container_memory_limit_bytes": 2 << 30,
                        "container_memory_swap_limit_bytes": 2 << 30,
                        "container_oom_kill_disabled": False,
                        "container_sampled_cache_adjusted_memory_bytes": 512 << 20,
                        "container_image_sha256": image,
                    }
                    for scenario, fixture, rows, layout, parallelism in (
                        ("success", "narrow-sqlclient-v1", 10_000, 1, 1),
                        ("success", "wide100-sqlclient-v1", 10_000, 1, 1),
                        ("success", "narrow-sqlclient-v1", 10_000, 2, 2),
                        ("success", "wide100-sqlclient-v1", 10_000, 2, 2),
                        ("success", "narrow-sqlclient-v1", 1_000_000, 2, 2),
                        ("success", "wide100-sqlclient-v1", 1_000_000, 2, 2),
                        ("force_kill_recovery", "wide100-sqlclient-v1", 10_000, 2, 2),
                    )
                ],
                "evidence_artifacts": artifacts,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_campaign_closes_exact_matrix_without_overwrite(tmp_path: Path) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)

    output = module.close_campaign(
        root,
        source_commit_sha=commit,
        runner_receipt=_runner(tmp_path, commit),
        package_version="0.88.0",
        output=tmp_path / "campaign.json",
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.mssql-sqlclient.transport-campaign.v3"
    assert payload["status"] == "PASS"
    assert payload["cell_count"] == len(module.REQUIRED_CELLS)
    assert len({item["artifact"] for item in payload["cells"]}) == len(module.REQUIRED_CELLS)


def test_campaign_rejects_runner_with_untrusted_resource_evidence(tmp_path: Path) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    runner = _runner(tmp_path, commit)
    payload = json.loads(runner.read_text(encoding="utf-8"))
    payload["executions"][0]["container_sampled_cache_adjusted_memory_bytes"] = 0
    runner.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="invalid_runner_receipt"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=runner,
            package_version="0.88.0",
            output=tmp_path / "campaign.json",
        )


def test_campaign_rejects_non_native_docker_server_architecture(tmp_path: Path) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    runner = _runner(tmp_path, commit)
    payload = json.loads(runner.read_text(encoding="utf-8"))
    payload["docker_server_architecture"] = "aarch64"
    runner.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="invalid_runner_receipt"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=runner,
            package_version="0.88.0",
            output=tmp_path / "campaign.json",
        )


def test_campaign_rejects_missing_or_wrong_commit_cells(tmp_path: Path) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    next(root.glob("*.json")).unlink()
    with pytest.raises(ValueError, match="cell_closure_mismatch"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=_runner(tmp_path, commit),
            package_version="0.88.0",
            output=tmp_path / "missing.json",
        )

    root = _complete_dir(tmp_path / "second", module, commit)
    path = next(root.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source_commit_sha"] = "e" * 40
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="untrusted_evidence"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=_runner(tmp_path / "second", commit),
            package_version="0.88.0",
            output=tmp_path / "wrong.json",
        )


def test_campaign_scans_actual_artifact_bytes_for_secrets(tmp_path: Path) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    path = next(root.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["diagnostic"] = "private-coordinate"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="private_value"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=_runner(tmp_path, commit),
            package_version="0.88.0",
            output=tmp_path / "campaign.json",
            secret_needles=("private-coordinate",),
        )


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("bcp_process_count", 1, "privacy_or_readback"),
        ("receipt_count", 0, "invalid_success"),
        ("protocol", "dpone.mssql-sqlclient.ipc.v1", "invalid_protocol"),
        ("runner_image_sha256", "0" * 64, "untrusted_evidence"),
    ],
)
def test_campaign_rejects_false_pass_facts(tmp_path: Path, field: str, value: object, error: str) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    path = next(
        path
        for path in root.glob("*.json")
        if json.loads(path.read_text())["layout_version"] == 2 and json.loads(path.read_text())["scenario"] == "success"
    )
    payload = json.loads(path.read_text())
    payload[field] = value
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=error):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=_runner(tmp_path, commit),
            package_version="0.88.0",
            output=tmp_path / "campaign.json",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("row_count", 10_000.0),
        ("import_parallelism", 1.9),
        ("layout_version", True),
        ("receipt_count", True),
    ],
)
def test_campaign_rejects_non_integer_counters(tmp_path: Path, field: str, value: object) -> None:
    module = _module()
    commit = "d" * 40
    root = _complete_dir(tmp_path, module, commit)
    path = next(root.glob("*.json"))
    payload = json.loads(path.read_text())
    payload[field] = value
    path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="invalid_integer"):
        module.close_campaign(
            root,
            source_commit_sha=commit,
            runner_receipt=_runner(tmp_path, commit),
            package_version="0.88.0",
            output=tmp_path / "campaign.json",
        )


def test_atomic_create_never_replaces_a_concurrent_winner(tmp_path: Path) -> None:
    module = _module()
    output = tmp_path / "campaign.json"
    payloads = (b'{"writer":1}\n', b'{"writer":2}\n')

    def create(payload: bytes) -> str:
        try:
            module._atomic_create(output, payload)
        except FileExistsError:
            return "lost"
        return "won"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(create, payloads))

    assert sorted(outcomes) == ["lost", "won"]
    assert output.read_bytes() in payloads
