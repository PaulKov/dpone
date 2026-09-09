from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path

import pytest

from tests.test_airflow_desired_state_preparation_cli import (
    _DIGESTS,
    _patch_authority,
    _promotion,
    _run,
)


def test_prepare_conflict_preserves_create_once_winner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation = tmp_path / "preparation.json"
    status = tmp_path / "prepare-status.json"
    _promotion(promotion)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    args = [
        "airflow",
        "desired-state",
        "prepare",
        "--promotion-evidence",
        str(promotion),
        "--expected-revision",
        "absent",
        "--output",
        str(preparation),
        "--status-output",
        str(status),
    ]
    assert _run(args) == 0
    winner = preparation.read_bytes()
    payload = json.loads(promotion.read_text(encoding="utf-8"))
    payload["deployment_id"] = _DIGESTS[2]
    promotion.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    assert _run(args) == 2
    assert preparation.read_bytes() == winner
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("output", "promotion"),
        ("output", "authority"),
        ("promotion", "authority"),
        ("status", "output"),
        ("status", "promotion"),
        ("status", "authority"),
    ],
)
def test_prepare_rejects_complete_path_collision_matrix_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    first: str,
    second: str,
) -> None:
    paths = {name: tmp_path / f"{name}.json" for name in ("promotion", "authority", "output", "status")}
    paths[second] = paths[first]
    originals = _write_sentinels(paths)
    _configure_collision_test(monkeypatch, paths)

    code = _run(
        [
            "airflow",
            "desired-state",
            "prepare",
            "--promotion-evidence",
            str(paths["promotion"]),
            "--expected-revision",
            "absent",
            "--output",
            str(paths["output"]),
            "--status-output",
            str(paths["status"]),
        ]
    )

    assert code == 2
    _assert_collision_outputs(
        originals=originals,
        status_path=paths["status"],
        status_is_aliased="status" in {first, second},
    )


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("output", "promotion"),
        ("output", "preparation"),
        ("output", "authority"),
        ("promotion", "preparation"),
        ("promotion", "authority"),
        ("preparation", "authority"),
        ("status", "output"),
        ("status", "promotion"),
        ("status", "preparation"),
        ("status", "authority"),
    ],
)
def test_publish_rejects_complete_path_collision_matrix_before_remote_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    first: str,
    second: str,
) -> None:
    paths = {name: tmp_path / f"{name}.json" for name in ("promotion", "preparation", "authority", "output", "status")}
    paths[second] = paths[first]
    originals = _write_sentinels(paths)
    _configure_collision_test(monkeypatch, paths)
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed for path collision"),
    )

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(paths["promotion"]),
            "--preparation",
            str(paths["preparation"]),
            "--output",
            str(paths["output"]),
            "--status-output",
            str(paths["status"]),
        ]
    )

    assert code == 2
    _assert_collision_outputs(
        originals=originals,
        status_path=paths["status"],
        status_is_aliased="status" in {first, second},
    )


@pytest.mark.parametrize("alias_kind", ["symlink", "hardlink"])
def test_prepare_rejects_filesystem_alias_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    alias_kind: str,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "output.json"
    status = tmp_path / "status.json"
    _promotion(promotion)
    original = promotion.read_bytes()
    if alias_kind == "symlink":
        output.symlink_to(promotion)
    else:
        os.link(promotion, output)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")

    code = _run(
        [
            "airflow",
            "desired-state",
            "prepare",
            "--promotion-evidence",
            str(promotion),
            "--expected-revision",
            "absent",
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert promotion.read_bytes() == original
    assert output.read_bytes() == original
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


def test_prepare_rejects_existing_output_directory_and_emits_safe_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "output"
    status = tmp_path / "status.json"
    _promotion(promotion)
    output.mkdir()
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")

    code = _run(
        [
            "airflow",
            "desired-state",
            "prepare",
            "--promotion-evidence",
            str(promotion),
            "--expected-revision",
            "absent",
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


@pytest.mark.parametrize("alias_kind", ["symlink", "hardlink"])
def test_publish_rejects_filesystem_alias_before_remote_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    alias_kind: str,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation = tmp_path / "preparation.json"
    output = tmp_path / "output.json"
    status = tmp_path / "status.json"
    _promotion(promotion)
    preparation.write_text("not-read-before-path-validation", encoding="utf-8")
    original = promotion.read_bytes()
    if alias_kind == "symlink":
        output.symlink_to(promotion)
    else:
        os.link(promotion, output)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed for path alias"),
    )

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--preparation",
            str(preparation),
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert promotion.read_bytes() == original
    assert output.read_bytes() == original
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


@pytest.mark.parametrize("invalid_output", [".", "/"])
def test_publish_rejects_non_file_output_and_emits_safe_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    invalid_output: str,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation = tmp_path / "preparation.json"
    status = tmp_path / "status.json"
    _promotion(promotion)
    preparation.write_text("not-read-before-output-validation", encoding="utf-8")
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed for invalid output"),
    )

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--preparation",
            str(preparation),
            "--output",
            invalid_output,
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


def test_publish_rejects_existing_output_directory_and_emits_safe_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation = tmp_path / "preparation.json"
    output = tmp_path / "output"
    status = tmp_path / "status.json"
    _promotion(promotion)
    preparation.write_text("not-read-before-output-validation", encoding="utf-8")
    output.mkdir()
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed for invalid output"),
    )

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--preparation",
            str(preparation),
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")


def _configure_collision_test(
    monkeypatch: pytest.MonkeyPatch,
    paths: dict[str, Path],
) -> None:
    monkeypatch.setenv(
        "DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE",
        str(paths["authority"]),
    )
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")


def _write_sentinels(paths: dict[str, Path]) -> dict[Path, bytes]:
    for path in set(paths.values()):
        path.write_bytes(f"sentinel:{path.name}".encode())
    return _read_all(set(paths.values()))


def _read_all(paths: Iterable[Path]) -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in paths}


def _assert_collision_outputs(
    *,
    originals: dict[Path, bytes],
    status_path: Path,
    status_is_aliased: bool,
) -> None:
    preserved = dict(originals)
    if not status_is_aliased:
        preserved.pop(status_path)
        assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status_path.read_text(encoding="utf-8")
    assert _read_all(preserved) == preserved
