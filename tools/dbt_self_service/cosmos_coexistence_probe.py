"""Build real independent Cosmos and dpone DAGs in one Airflow DagBag."""

from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.models import DagBag
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64


def main() -> int:
    repository_root = Path(__file__).resolve().parents[2]
    manifest = repository_root / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json"
    with TemporaryDirectory(prefix="dpone-cosmos-coexistence-") as temporary:
        root = Path(temporary)
        index_path = _write_dpone_cache(root)
        profiles_path = _write_profiles(root)
        project_path = _write_dbt_project(root)
        _write_cosmos_dag(
            root,
            manifest=manifest,
            profiles=profiles_path,
            project_path=project_path,
        )
        _write_dpone_dag(root, index_path=index_path)

        bag = _build_dag_bag(root)
        if bag.import_errors:
            raise RuntimeError(f"coexistence DagBag import failed: {sorted(bag.import_errors.items())}")
        expected = {"cosmos_coexistence_probe", "dpone_coexistence_probe"}
        if not expected <= set(bag.dag_ids):
            raise RuntimeError(
                "coexistence DagBag is missing DAGs: "
                f"{sorted(expected - set(bag.dag_ids))}; loaded={sorted(bag.dag_ids)}"
            )
        cosmos_dag = bag.dags.get("cosmos_coexistence_probe")
        dpone_dag = bag.dags.get("dpone_coexistence_probe")
        if cosmos_dag is None or dpone_dag is None:
            raise RuntimeError("coexistence DAG materialization failed")
        if not cosmos_dag.task_ids or not dpone_dag.task_ids:
            raise RuntimeError("coexistence DAGs must contain their real provider tasks")
    return 0


def dpone_only_main() -> int:
    """Parse one exact-cache dpone DAG through a lexical filesystem alias."""

    with TemporaryDirectory(prefix="dpone-dagbag-probe-") as temporary:
        root = Path(temporary)
        index_path = _write_dpone_cache(root)
        alias_root = root.parent / f"{root.name}-lexical-alias"
        alias_root.symlink_to(root, target_is_directory=True)
        try:
            alias_index_path = alias_root / index_path.relative_to(root)
            _write_dpone_dag(root, index_path=alias_index_path)
            bag = _build_dag_bag(root)
            if bag.import_errors:
                raise RuntimeError(f"dpone DagBag import failed: {sorted(bag.import_errors.items())}")
            dag = bag.dags.get("dpone_coexistence_probe")
            if dag is None or not dag.task_ids:
                raise RuntimeError("dpone DagBag probe did not materialize its expected DAG and task")
        finally:
            alias_root.unlink(missing_ok=True)
    return 0


def _build_dag_bag(root: Path) -> DagBag:
    """Construct DagBag across Airflow 2.x/3.x without safe_mode skips.

    Airflow safe_mode only loads files that literally contain ``DAG`` and
    ``airflow``; a Cosmos ``DbtDag`` module can otherwise be silently skipped.
    ``include_examples`` was removed in Airflow 3.3+.
    """

    kwargs: dict[str, object] = {
        "dag_folder": root.as_posix(),
        "safe_mode": False,
    }
    parameters = inspect.signature(DagBag.__init__).parameters
    if "include_examples" in parameters:
        kwargs["include_examples"] = False
    return DagBag(**kwargs)


def _write_dpone_cache(root: Path) -> Path:
    cache = root / ".dpone-cache"
    with cache_write_lease(cache):
        pass
    release_name = RELEASE_ID.replace(":", "-", 1)
    dag_dir = cache / "releases" / release_name / "dags"
    pack_dir = cache / "releases" / release_name / "packs"
    deployment_dir = cache / "deployments" / "test" / DEPLOYMENT_ID.replace(":", "-", 1)
    dag_dir.mkdir(parents=True)
    pack_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)

    pack_path = pack_dir / "dpone_probe.airflow-pack.json"
    _write_json(
        pack_path,
        {
            "kind": "gitops.airflow_pack",
            "kpo_kwargs": {
                "task_id": "dpone_probe__runtime",
                "name": "dpone-probe",
                "namespace": "airflow",
            },
            "runtime_command": "echo verified-at-parse-only",
            "steps": [],
        },
    )
    spec = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "dpone Cosmos coexistence CI",
        "dag_id": "dpone_coexistence_probe",
        "schedule": None,
        "start_date": "2026-01-01",
        "nodes": [
            {
                "node_id": "dpone_probe",
                "workload_id": "dpone_probe",
                "pack_ref": "cached://workloads/dpone_probe",
            }
        ],
        "edges": [],
        "topological_order": ["dpone_probe"],
    }
    spec["spec_fingerprint"] = compute_dag_spec_fingerprint(spec)
    spec_path = dag_dir / "dpone_coexistence_probe.dag-spec.json"
    _write_json(spec_path, spec)
    index_path = deployment_dir / "airflow-index.json"
    _write_json(
        index_path,
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "environment": "test",
            "release_id": RELEASE_ID,
            "deployment_id": DEPLOYMENT_ID,
            "dag_specs": [
                {
                    "id": "dpone_coexistence_probe",
                    "artifact_ref": (f"cache://releases/{release_name}/dags/dpone_coexistence_probe.dag-spec.json"),
                    "sha256": _sha256(spec_path),
                    "bytes": spec_path.stat().st_size,
                }
            ],
            "workload_packs": [
                {
                    "id": "dpone_probe",
                    "artifact_ref": (f"cache://releases/{release_name}/packs/dpone_probe.airflow-pack.json"),
                    "sha256": _sha256(pack_path),
                    "bytes": pack_path.stat().st_size,
                }
            ],
            "runtime_artifact_delivery": {"mode": "local_preview"},
        },
    )
    return index_path


def _write_profiles(root: Path) -> Path:
    path = root / "profiles.yml"
    path.write_text(
        "inline_demo:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: sqlserver\n"
        "      driver: ODBC Driver 18 for SQL Server\n"
        "      server: localhost\n"
        "      port: 1433\n"
        "      database: analytics\n"
        "      schema: dbo\n"
        "      user: parse_only\n"
        "      password: \"{{ env_var('DPONE_COSMOS_PROBE_PASSWORD') }}\"\n",
        encoding="utf-8",
    )
    return path


def _write_dbt_project(root: Path) -> Path:
    """Minimal project tree required by Cosmos ExecutionConfig path validation."""

    project = root / "dbt_project"
    models = project / "models"
    models.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: inline_demo\nversion: 1.0.0\nconfig-version: 2\nprofile: inline_demo\nmodel-paths: [models]\n",
        encoding="utf-8",
    )
    (models / "example.sql").write_text("select 1 as id\n", encoding="utf-8")
    return project


def _write_cosmos_dag(
    root: Path,
    *,
    manifest: Path,
    profiles: Path,
    project_path: Path,
) -> None:
    # Keep literal ``airflow`` / ``DAG`` tokens so DagBag safe_mode also accepts the file.
    (root / "cosmos_probe.py").write_text(
        "from datetime import datetime, timezone\n"
        "from airflow.models import DAG  # noqa: F401 - DagBag safe_mode token\n"
        "from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig\n"
        "from cosmos.constants import ExecutionMode, LoadMode\n"
        "dag = DbtDag(\n"
        "    dag_id='cosmos_coexistence_probe',\n"
        "    schedule=None,\n"
        "    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),\n"
        "    project_config=ProjectConfig(\n"
        f"        manifest_path={manifest.as_posix()!r},\n"
        "        project_name='inline_demo',\n"
        "    ),\n"
        f"    profile_config=ProfileConfig(profile_name='inline_demo', target_name='dev', profiles_yml_filepath={profiles.as_posix()!r}),\n"
        # Cosmos 1.15: project path belongs on ExecutionConfig, not ProjectConfig.
        f"    execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL, dbt_project_path={project_path.as_posix()!r}),\n"
        "    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),\n"
        ")\n",
        encoding="utf-8",
    )


def _write_dpone_dag(root: Path, *, index_path: Path) -> None:
    (root / "dpone_probe.py").write_text(
        "from airflow.providers.dpone import load_dpone_dags\n"
        f"report = load_dpone_dags(globals(), index_path={index_path.as_posix()!r}, "
        "invalid_dag_policy='fail_all', duplicate_policy='fail_all')\n"
        "assert not report.fatal and not report.errors\n",
        encoding="utf-8",
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
