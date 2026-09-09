"""Generate exact dbt runtime fixtures against an approved MSSQL profile."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle

DBT_CORE_VERSION = "1.12.3"
DBT_ADAPTER_VERSION = "1.11.1"
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROJECT = ROOT / "tests" / "fixtures" / "dbt-runtime-correctness-v1"


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    project = Path(args.project_dir).resolve()
    profiles = Path(args.profiles_dir).resolve()
    output = Path(args.output_dir).resolve()
    try:
        _require_toolchain()
        with TemporaryDirectory(prefix="dpone-dbt-runtime-fixture-") as raw:
            temporary = Path(raw)
            command = _command(
                project=project,
                profiles=profiles,
                target_name=args.target,
                target_path=temporary / "target",
                log_path=temporary / "logs",
            )
            completed = subprocess.run(  # noqa: S603 - fixed tool and flags
                command,
                cwd=project,
                check=False,
                shell=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=args.timeout_seconds,
                env=_environment(),
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"dbt build failed with exit code {completed.returncode}; "
                    "inspect the approved local profile and database"
                )
            manifest = _artifact(temporary / "target" / "manifest.json")
            run_results = _artifact(temporary / "target" / "run_results.json")
            _validate_fixture(manifest, run_results)
            _publish(
                output,
                manifest=manifest,
                run_results=run_results,
                target_name=args.target,
                project=project,
                environment_id=_safe_label(args.environment_id, "environment id"),
                database_version=_safe_label(args.database_version, "database version"),
            )
    except (
        FileNotFoundError,
        importlib.metadata.PackageNotFoundError,
        StrictJsonError,
        subprocess.TimeoutExpired,
        RuntimeError,
        ValueError,
    ) as exc:
        print(f"fixture generation failed: {exc}", file=sys.stderr)
        return 1
    print(f"fixture generated: {output}")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the exact certified dbt/sqlserver toolchain and persist secret-free manifest/run-results fixtures."
        )
    )
    parser.add_argument("--profiles-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--project-dir", default=str(DEFAULT_PROJECT))
    parser.add_argument("--target", default="runtime_fixture")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--database-version", required=True)
    return parser


def _require_toolchain() -> None:
    observed = {
        "dbt-core": importlib.metadata.version("dbt-core"),
        "dbt-sqlserver": importlib.metadata.version("dbt-sqlserver"),
    }
    expected = {
        "dbt-core": DBT_CORE_VERSION,
        "dbt-sqlserver": DBT_ADAPTER_VERSION,
    }
    if observed != expected:
        raise RuntimeError(f"toolchain mismatch: expected {expected}, observed {observed}")


def _command(
    *,
    project: Path,
    profiles: Path,
    target_name: str,
    target_path: Path,
    log_path: Path,
) -> tuple[str, ...]:
    return (
        "dbt",
        "--no-use-colors",
        "--no-send-anonymous-usage-stats",
        "build",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
        "--profile",
        "dpone_runtime_fixture",
        "--target",
        target_name,
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--select",
        "+fqn:dpone_runtime_fixture.orders",
    )


def _artifact(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"dbt artifact is missing: {path.name}")
    payload = path.read_bytes()
    if not payload or len(payload) > MAX_ARTIFACT_BYTES:
        raise RuntimeError(f"dbt artifact is empty or oversized: {path.name}")
    strict_json_object(payload)
    return payload


def _validate_fixture(manifest_bytes: bytes, run_results_bytes: bytes) -> None:
    manifest = strict_json_object(manifest_bytes)
    run_results = strict_json_object(run_results_bytes)
    if OfficialDbtRunResultsValidator().validate(run_results, version=6):
        raise RuntimeError("run_results.json does not satisfy the official v6 schema")
    nodes = manifest.get("nodes")
    unit_tests = manifest.get("unit_tests")
    if not isinstance(nodes, dict) or not isinstance(unit_tests, dict):
        raise RuntimeError("manifest does not contain model and unit-test inventories")
    ephemeral = nodes.get("model.dpone_runtime_fixture.ephemeral_orders")
    config = ephemeral.get("config") if isinstance(ephemeral, dict) else None
    if not isinstance(config, dict) or config.get("materialized") != "ephemeral":
        raise RuntimeError("fixture ephemeral model was not compiled")
    observed = {item.get("unique_id") for item in run_results.get("results", []) if isinstance(item, dict)}
    if "model.dpone_runtime_fixture.ephemeral_orders" in observed:
        raise RuntimeError("ephemeral model unexpectedly emitted a run-result")
    required_prefixes = (
        "model.dpone_runtime_fixture.orders",
        "test.dpone_runtime_fixture.",
        "unit_test.dpone_runtime_fixture.",
    )
    if any(not any(str(item).startswith(prefix) for item in observed) for prefix in required_prefixes):
        raise RuntimeError("run-results does not contain model, data-test and unit-test outcomes")


def _publish(
    output: Path,
    *,
    manifest: bytes,
    run_results: bytes,
    target_name: str,
    project: Path,
    environment_id: str,
    database_version: str,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    project_bundle = build_dbt_project_bundle(project)
    provenance = {
        "schema": "dpone.dbt-runtime-fixture-provenance.v1",
        "dbt_core_version": DBT_CORE_VERSION,
        "dbt_adapter": "sqlserver",
        "dbt_adapter_version": DBT_ADAPTER_VERSION,
        "target_name": target_name,
        "environment": "approved_local_mssql",
        "environment_id": environment_id,
        "database_version": database_version,
        "generated_at": datetime.now(UTC).isoformat(),
        "generator_command": (
            "uv run python tools/dbt_self_service/generate_runtime_fixture.py "
            "--profiles-dir <profiles> --output-dir <output> "
            "--environment-id <approved-environment> --database-version <version>"
        ),
        "source_commit": _source_commit(),
        "source_tree_sha256": project_bundle.bundle.archive_sha256,
        "manifest_sha256": _sha256(manifest),
        "run_results_sha256": _sha256(run_results),
    }
    files = {
        "manifest.v12.json": manifest,
        "run-results.v6.json": run_results,
        "provenance.json": (
            json.dumps(
                provenance,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode(),
    }
    for name, payload in files.items():
        temporary = output / f".{name}.tmp"
        temporary.write_bytes(payload)
        os.replace(temporary, output / name)


def _environment() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.startswith("DPONE_DBT_FIXTURE_") or key in {"HOME", "LANG", "LC_ALL", "PATH"}
    }


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _source_commit() -> str:
    completed = subprocess.run(
        ("git", "-C", str(ROOT), "rev-parse", "HEAD"),
        check=False,
        shell=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=10,
    )
    if completed.returncode != 0:
        raise RuntimeError("source commit is unavailable")
    value = completed.stdout.decode("ascii").strip()
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeError("source commit is unavailable")
    return value


def _safe_label(value: str, label: str) -> str:
    normalized = str(value or "").strip()
    if (
        not normalized
        or len(normalized) > 128
        or any(not (character.isalnum() or character in "._-") for character in normalized)
    ):
        raise ValueError(f"{label} must contain only letters, digits, dot, underscore, or dash")
    return normalized


if __name__ == "__main__":
    raise SystemExit(main())
