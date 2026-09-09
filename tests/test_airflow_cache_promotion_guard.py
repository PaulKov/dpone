from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pytest


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def test_file_digest_precondition_accepts_unchanged_guard(tmp_path: Path) -> None:
    from dpone.readiness.airflow_promotion_guard import file_digest_precondition

    guard = tmp_path / "activation-guard.json"
    payload = b'{"activation_sequence":42}\n'
    guard.write_bytes(payload)

    precondition = file_digest_precondition(
        guard,
        expected_sha256=_digest(payload),
    )

    precondition.enforce()


def test_file_digest_precondition_blocks_changed_guard(tmp_path: Path) -> None:
    from dpone.readiness.airflow_promotion_guard import file_digest_precondition
    from dpone.runtime.deployment_cache import DeploymentCacheError

    guard = tmp_path / "activation-guard.json"
    original = b'{"activation_sequence":42}\n'
    guard.write_bytes(original)
    precondition = file_digest_precondition(
        guard,
        expected_sha256=_digest(original),
    )
    guard.write_bytes(b'{"activation_sequence":43}\n')

    with pytest.raises(DeploymentCacheError) as exc:
        precondition.enforce()

    assert exc.value.code == "DPONE_PROMOTION_PRECONDITION_CHANGED"


def test_file_digest_precondition_rejects_noncanonical_digest(tmp_path: Path) -> None:
    from dpone.readiness.airflow_promotion_guard import (
        PromotionPreconditionError,
        file_digest_precondition,
    )

    with pytest.raises(PromotionPreconditionError) as exc:
        file_digest_precondition(
            tmp_path / "activation-guard.json",
            expected_sha256="not-a-digest",
        )

    assert exc.value.code == "DPONE_PROMOTION_PRECONDITION_INVALID"


def test_cache_sync_parser_exposes_paired_precommit_guard_options() -> None:
    from dpone.commands.airflow_cache_sync_cmd import register_cache_sync_parser

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    register_cache_sync_parser(subparsers)

    args = parser.parse_args(
        [
            "cache-sync",
            "--cache-root",
            "/cache",
            "--deployment-dir",
            "/cache/deployments/dev/sha256-" + "1" * 64,
            "--environment",
            "dev",
            "--promoted-by",
            "ci://controller",
            "--allowed-promoter",
            "ci://controller",
            "--expect-current-absent",
            "--precommit-guard-path",
            "/etc/dpone/desired/activation-guard.json",
            "--expected-precommit-guard-sha256",
            "sha256:" + "2" * 64,
        ]
    )

    assert args.precommit_guard_path.endswith("activation-guard.json")
    assert args.expected_precommit_guard_sha256 == "sha256:" + "2" * 64
