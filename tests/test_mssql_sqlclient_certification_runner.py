from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _module():
    path = Path("tools/mssql_sqlclient_certification_runner.py")
    spec = importlib.util.spec_from_file_location("mssql_sqlclient_certification_runner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _image_receipt(tmp_path: Path, *, digest: str = "c" * 64) -> Path:
    path = tmp_path / "image.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.mssql-sqlclient.certification-image.v2",
                "status": "PASS",
                "source_commit_sha": "a" * 40,
                "source_tree_oid": "b" * 40,
                "runner_image_sha256": digest,
                "runner_platform": "linux/amd64",
                "source_mode": "exact_git_archive",
                "worktree_dirty": False,
            }
        )
    )
    return path


def test_runner_binds_every_execution_and_artifact_to_inspected_image(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    digest = "c" * 64
    evidence = tmp_path / "evidence"
    created = []
    monkeypatch.setattr(
        module,
        "_inspect_image",
        lambda *_args: {
            "Id": f"sha256:{digest}",
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {
                "Labels": {
                    "org.opencontainers.image.revision": "a" * 40,
                    "dev.dpone.certification.source-tree": "b" * 40,
                }
            },
        },
    )

    def create(**values):
        cell = values["cell"]
        created.append((values["immutable_image"], cell))
        name = (
            f"{cell.scenario}-{cell.fixture_id}-{cell.row_count}-layout{cell.layout_version}"
            f"-parallel{cell.import_parallelism}.json"
        )
        (values["evidence_dir"] / name).write_text("{}")
        return f"{len(created):012x}"

    monkeypatch.setattr(module, "_create_container", create)
    monkeypatch.setattr(
        module,
        "_applied_resource_policy",
        lambda *_args: {
            "container_memory_limit_bytes": 2 << 30,
            "container_memory_swap_limit_bytes": 2 << 30,
            "container_oom_kill_disabled": False,
        },
    )
    monkeypatch.setattr(module, "_capture", lambda _command: f"sha256:{digest}\n")
    monkeypatch.setattr(module, "_run_attached", lambda *_args, **_kwargs: 456_789)
    monkeypatch.setattr(module, "_container_was_oom_killed", lambda *_args: False)
    monkeypatch.setattr(module, "_run", lambda *_args, **_kwargs: None)
    output = tmp_path / "runner.json"

    result = module.run_campaign(
        docker="docker",
        image="candidate",
        image_receipt=_image_receipt(tmp_path, digest=digest),
        network="certification",
        evidence_dir=evidence,
        output=output,
        pass_env=(),
    )

    assert len(created) == 7
    assert result["schema_version"] == "dpone.mssql-sqlclient.certification-runner.v3"
    assert {image for image, _cell in created} == {f"sha256:{digest}"}
    assert result["execution_count"] == 7
    assert {item["container_sampled_cache_adjusted_memory_bytes"] for item in result["executions"]} == {456_789}
    wide_million = next(
        item
        for item in result["executions"]
        if item["fixture_id"] == "wide100-sqlclient-v1" and item["row_count"] == 1_000_000
    )
    assert wide_million["max_rows"] == 8_192
    assert wide_million["max_bytes"] == 48 << 20
    assert wide_million["max_pending"] == 1
    assert wide_million["retained_work_capacity"] == 3
    assert wide_million["container_memory_limit_bytes"] == 2 << 30
    assert wide_million["container_memory_swap_limit_bytes"] == 2 << 30
    assert wide_million["container_oom_kill_disabled"] is False
    assert len(result["evidence_artifacts"]) == 7
    assert json.loads(output.read_text()) == result


def test_runner_rejects_tag_resolving_to_another_image(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(
        module,
        "_inspect_image",
        lambda *_args: {"Id": "sha256:" + "d" * 64, "Os": "linux", "Architecture": "amd64"},
    )

    with pytest.raises(ValueError, match="image_digest_mismatch"):
        module.run_campaign(
            docker="docker",
            image="candidate",
            image_receipt=_image_receipt(tmp_path),
            network="certification",
            evidence_dir=tmp_path / "evidence",
            output=tmp_path / "runner.json",
            pass_env=(),
        )


@pytest.mark.parametrize("cell_index", [0, 6])
def test_container_executes_only_its_bound_fixture(tmp_path: Path, monkeypatch, cell_index: int) -> None:
    module = _module()
    commands = []
    monkeypatch.setattr(module, "_capture", lambda command: commands.append(command) or "a" * 12)
    cell = module.EXECUTION_CELLS[cell_index]

    module._create_container(
        docker="docker",
        immutable_image="sha256:" + "c" * 64,
        receipt={
            "source_commit_sha": "a" * 40,
            "source_tree_oid": "b" * 40,
            "runner_image_sha256": "c" * 64,
        },
        network="certification",
        evidence_dir=tmp_path,
        pass_env=(),
        cell=cell,
    )

    assert len(commands) == 1
    command = commands[0]
    assert (
        "tests/integration/mssql/test_mssql_sqlclient_route_live.py::"
        f"test_sqlclient_transport_certification_matrix[{cell.fixture_id}]"
    ) in command
    assert ("DPONE_SQLCLIENT_FORCE_KILL=1" in command) is (cell.scenario == "force_kill_recovery")
    assert f"DPONE_SQLCLIENT_CERT_MAX_ROWS={cell.max_rows}" in command
    assert f"DPONE_SQLCLIENT_CERT_MAX_BYTES={cell.max_bytes}" in command
    assert f"DPONE_SQLCLIENT_CERT_MAX_PENDING={cell.max_pending}" in command
    assert "--memory" in command and str(cell.container_memory_limit_bytes) in command
    assert "--memory-swap" in command


def test_wide_million_policy_fits_the_closed_staging_and_memory_envelopes() -> None:
    module = _module()
    cell = next(
        item
        for item in module.EXECUTION_CELLS
        if item.fixture_id == "wide100-sqlclient-v1" and item.row_count == 1_000_000
    )

    raw_stages = (cell.row_count + cell.max_rows - 1) // cell.max_rows

    assert raw_stages == 123
    assert raw_stages + 1 <= cell.max_staging_tables
    assert cell.retained_work_capacity == 3
    assert (cell.retained_work_capacity + 1) * cell.max_bytes == 192 << 20


def test_runner_rejects_an_oom_killed_cell(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(module, "_capture", lambda _command: "a" * 12)
    monkeypatch.setattr(module, "_run_attached", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(module, "_container_was_oom_killed", lambda *_args: True)

    with pytest.raises(RuntimeError, match="cell_oom_killed"):
        module._execute_container(
            docker="docker",
            container="a" * 12,
            cell=module.EXECUTION_CELLS[0],
            image_digest="c" * 64,
            applied_resources={
                "container_memory_limit_bytes": 2 << 30,
                "container_memory_swap_limit_bytes": 2 << 30,
                "container_oom_kill_disabled": False,
            },
        )


@pytest.mark.parametrize(
    "host_config",
    [
        {"Memory": 0, "MemorySwap": 2 << 30, "OomKillDisable": False},
        {"Memory": 2 << 30, "MemorySwap": 0, "OomKillDisable": False},
        {"Memory": 2 << 30, "MemorySwap": 2 << 30, "OomKillDisable": True},
    ],
)
def test_runner_rejects_unenforced_container_resource_policy(monkeypatch, host_config) -> None:
    module = _module()
    monkeypatch.setattr(module, "_inspect_container", lambda *_args: {"HostConfig": host_config})

    with pytest.raises(ValueError, match="resource_policy_mismatch"):
        module._applied_resource_policy("docker", "a" * 12, module.EXECUTION_CELLS[0])


def test_runner_records_only_inspected_container_resource_policy(monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(
        module,
        "_inspect_container",
        lambda *_args: {"HostConfig": {"Memory": 2 << 30, "MemorySwap": 2 << 30, "OomKillDisable": False}},
    )

    assert module._applied_resource_policy("docker", "a" * 12, module.EXECUTION_CELLS[0]) == {
        "container_memory_limit_bytes": 2 << 30,
        "container_memory_swap_limit_bytes": 2 << 30,
        "container_oom_kill_disabled": False,
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [("512B", 512), ("1.5KiB", 1_536), ("48MiB", 48 << 20), ("2GiB", 2 << 30), ("unknown", 0)],
)
def test_memory_parser_uses_docker_binary_units(value: str, expected: int) -> None:
    module = _module()

    assert module._memory_bytes(value) == expected
