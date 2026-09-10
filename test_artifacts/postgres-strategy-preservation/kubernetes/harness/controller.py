"""Run public dpone producers and actual Airflow DAG.test in the approved Pod.

The host supplies pinned images, namespace, local MinIO credentials and a
warehouse Airflow connection. This program never imports test doubles or
changes sealed deployment artifacts. Results describe observations only;
cluster admission, container files and Pod events are collected by the host.
Set DPONE_LIVE_CASES to a comma-separated subset for focused live iteration.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

COMMIT = os.environ["DPONE_SOURCE_COMMIT"]
ROOT = Path("/work")
RESULTS = ROOT / "results"
CASES = ("refresh-first", "refresh-changed", "refresh-replay", "refresh-rejected", "refresh-retry")


def require(condition: object, label: str) -> None:
    """Raise a safe fixed-label assertion without dumping secret-bearing objects."""
    if not condition:
        raise AssertionError(label)


def redact(text: str) -> str:
    """Remove exact injected values before retaining CLI output or exception text."""
    secrets = [os.environ.get(name, "") for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")]
    uri = os.environ.get("AIRFLOW_CONN_WAREHOUSE", "")
    secrets.extend((uri, unquote(urlsplit(uri).password or "")))
    for secret in sorted(set(secrets), key=len, reverse=True):
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def save(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact(json.dumps(payload, sort_keys=True, indent=2, default=str)) + "\n")


def emit(result: dict) -> None:
    save(RESULTS / (result["case_id"] + ".json"), result)
    print("DPONE_LIVE_CASE " + redact(json.dumps(result, sort_keys=True, default=str)), flush=True)


def configure() -> dict[str, str]:
    """Set Airflow configuration and env-only connections before any Airflow import."""
    names = (
        "DPONE_LIVE_IMAGE_REF",
        "DPONE_LIVE_XCOM_IMAGE_REF",
        "DPONE_LIVE_REGISTRY_SHA",
        "DPONE_LIVE_NAMESPACE",
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AIRFLOW_CONN_WAREHOUSE",
    )
    require(all(os.environ.get(name) for name in names), "required_controller_environment_missing")
    config = {name: os.environ[name] for name in names}
    for name in ("DPONE_LIVE_IMAGE_REF", "DPONE_LIVE_XCOM_IMAGE_REF"):
        require(re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", config[name]), "image_must_have_exact_digest")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", config["DPONE_LIVE_REGISTRY_SHA"]), "registry_digest_invalid")
    for path in (ROOT / "airflow", ROOT / "dags", RESULTS):
        path.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        {
            "AIRFLOW_HOME": "/work/airflow",
            "USER": "dpone",
            "LOGNAME": "dpone",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////work/airflow/airflow.db",
            "AIRFLOW__CORE__DAGS_FOLDER": "/work/dags",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__LOGGING__BASE_LOG_FOLDER": "/work/airflow/logs",
            "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE": "False",
            "AIRFLOW__CORE__HIDE_SENSITIVE_VAR_CONN_FIELDS": "True",
            "DPONE_LAUNCH_PIN_STORE_BACKEND": "kubernetes_configmap",
            "DPONE_LAUNCH_PIN_STORE_NAMESPACE": config["DPONE_LIVE_NAMESPACE"],
            "AIRFLOW_CONN_KUBERNETES_DEFAULT": json.dumps({"conn_type": "kubernetes", "extra": {"in_cluster": True}}),
            "AIRFLOW_CONN_S3_DPONE_ARTIFACTS_READER": json.dumps(
                {
                    "conn_type": "aws",
                    "login": config["AWS_ACCESS_KEY_ID"],
                    "password": config["AWS_SECRET_ACCESS_KEY"],
                    "extra": {"endpoint_url": config["AWS_ENDPOINT_URL"], "region_name": "us-east-1"},
                }
            ),
            "AWS_DEFAULT_REGION": "us-east-1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return config


def initialize_airflow() -> None:
    from airflow.models.pool import Pool
    from airflow.utils import db

    db.initdb()
    Pool.create_or_update_pool("dpone_dev", 8, "Approved local live test", False)


def cli(project: Path, step: str, arguments: list[str]) -> dict:
    """Invoke the installed public command, preserving redacted original streams."""
    completed = subprocess.run(
        ["dpone", *arguments, "--format", "json"], cwd=project, capture_output=True, text=True, check=False, timeout=300
    )
    evidence = {
        "arguments": arguments,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    save(RESULTS / project.name / (step + ".json"), evidence)
    print(f"DPONE_LIVE_STEP case={project.name} step={step} exit={completed.returncode}", flush=True)
    require(completed.returncode == 0, "cli_failed_" + step)
    payload = json.loads(completed.stdout)
    require(isinstance(payload, dict), "cli_result_must_be_json_object_" + step)
    return payload


def write_yaml(path: Path, value: dict) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, sort_keys=False))


def author(project: Path, case_id: str, config: dict[str, str]) -> str:
    """Author credentials references, namespace binding, manifest and DAG declaration."""
    import yaml

    table = case_id.replace("-", "_")
    binding_path = project / "environments/dev/binding-set.yaml"
    binding = yaml.safe_load(binding_path.read_text())
    binding["bindings"] = {"warehouse": {"connection_ref": "warehouse"}}
    binding["runtime"].update(
        kubernetes_namespace=config["DPONE_LIVE_NAMESPACE"],
        service_account="dpone-live",
        pool="dpone_dev",
        resource_profile="small",
    )
    write_yaml(binding_path, binding)
    mount = "/run/secrets/dpone/airflow-connections"
    credentials = {
        "resolver": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "payload_format": "airflow_connection_uri",
        "mount_path": mount + "/warehouse",
        "fields": {"uri": "uri"},
    }
    write_yaml(
        project / "platform/connection-registries/dev.yaml",
        {
            "schema": "dpone.connection-registry.v1",
            "environment": "dev",
            "connections": {"warehouse": {"type": "postgres", "credentials": credentials}},
        },
    )
    write_yaml(
        project / "environments/dev/credential-runtime.yaml",
        {
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "runtime": {"mode": "local_development"},
        },
    )
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": credentials["secret_name"],
        "mount_path": mount,
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "connections": [
            {
                "connection_id": "warehouse",
                "connection_ref": "warehouse",
                "registry_connection_ref": "warehouse",
                "secret_key": "AIRFLOW_CONN_WAREHOUSE",
                "mount_path": credentials["mount_path"],
                "fields": {"uri": "uri"},
            }
        ],
    }
    manifest = {
        "name": "orders",
        "execution": {"visibility": "task"},
        "source": {
            "type": "postgres",
            "connection_ref": "warehouse",
            "table": {"schema": "preserve_src", "name": table},
            "options": {
                "export_format": "csv",
                "batch_size": 100,
            },
        },
        "sink": {
            "type": "postgres",
            "connection_ref": "warehouse",
            "table": {"schema": "preserve_dst", "name": table},
            "strategy": {"mode": "full_refresh"},
            "options": {"technical_columns": "forbidden"},
        },
    }
    dag_id = "DAG__live__" + table
    write_yaml(project / "pipeline.yaml", manifest)
    write_yaml(
        project / "catalog.yaml",
        {
            "domain": "live",
            "workloads": {"orders": {"manifest": "pipeline.yaml"}},
            "dags": {
                dag_id: {
                    "description": "Approved local PostgreSQL strategy evidence",
                    "schedule": None,
                    "start_date": "2026-09-01",
                    "timezone": "UTC",
                    "catchup": False,
                    "max_active_runs": 1,
                    "tags": ["dpone", "local-live"],
                    "default_args": {"retries": 0, "retry_delay_minutes": 1},
                    "operator_overrides": {"in_cluster": True},
                    "workloads": ["orders"],
                    "wiring": {"mode": "explicit", "dependencies": {}},
                }
            },
        },
    )
    write_yaml(
        project / "workloads.yaml",
        {
            "gitops": {
                "includes": [{"path": "catalog.yaml"}],
                "defaults": {
                    "image": config["DPONE_LIVE_IMAGE_REF"],
                    "airflow": {"connection_projection": projection},
                },
            }
        },
    )
    return dag_id


def arguments(command: str, **options: object) -> list[str]:
    """Build shell-free public CLI arguments from readable named options."""
    result = command.split()
    for key, value in options.items():
        if value is not None and value is not False:
            result.append("--" + key.replace("_", "-"))
            if value is not True:
                result.append(str(value))
    return result


def produce(project: Path, case_id: str, config: dict[str, str]) -> tuple[str, dict]:
    project.mkdir(parents=True, exist_ok=True)
    cli(project, "01-init", arguments("init project", airflow=True))
    dag_id = author(project, case_id, config)
    cli(
        project,
        "02-reconcile",
        arguments("gitops airflow reconcile", workload_set="workloads.yaml", all_workloads=True, env="dev"),
    )
    released = cli(
        project,
        "03-release-materialize",
        arguments(
            "gitops airflow release-materialize",
            pack_root=".dpone/gitops/airflow",
            cache_root=".dpone-cache",
            xcom_sidecar_image=config["DPONE_LIVE_XCOM_IMAGE_REF"],
        ),
    )
    shared = dict(release_id=released["release_id"], environment="dev", artifact_registry_ref="dpone-local-artifacts")
    build = cli(
        project,
        "04-build",
        arguments(
            "airflow build",
            **shared,
            trust_tier="non_production",
            runtime_image_ref=config["DPONE_LIVE_IMAGE_REF"],
            runtime_image_digest=config["DPONE_LIVE_IMAGE_REF"].split("@", 1)[1],
            registry_config_map_name="dpone-artifact-registry",
            registry_config_map_key="registry.json",
            registry_config_sha256=config["DPONE_LIVE_REGISTRY_SHA"],
            airflow_bundle_ref="git:" + COMMIT,
        ),
    )
    cli(
        project,
        "05-publish",
        arguments(
            "airflow publish",
            **shared,
            cache_root=".dpone-cache",
            deployment_id=build["deployment"]["deployment_id"],
            registry_uri="s3://dpone-artifacts/pg-strategy-preservation",
            identity_mode="workload_identity",
            publication_mode="exact",
        ),
    )
    cli(
        project,
        "06-cache-sync",
        arguments(
            "airflow cache-sync",
            cache_root=".dpone-cache",
            deployment_dir=build["deployment_dir"],
            environment="dev",
            promoted_by="ci://local-minikube",
            allowed_promoter="ci://local-minikube",
            expect_current_absent=True,
            confirm_promote=True,
        ),
    )
    index = json.loads((project / ".dpone-cache/current/airflow-index.json").read_text())
    save(RESULTS / project.name / "deployment-index.json", index)
    require(index["schema"] == "dpone.airflow-deployment-index.v2", "strict_v2_index_required")
    require(index["runtime_artifact_delivery"]["mode"] == "init_fetch", "init_fetch_delivery_required")
    require(
        index["release_id"] == released["release_id"]
        and index["deployment_id"] == build["deployment"]["deployment_id"],
        "identity_mismatch",
    )
    for descriptor in index["workload_packs"]:
        raw = (project / ".dpone-cache" / descriptor["artifact_ref"].removeprefix("cache://")).read_bytes()
        require(descriptor["sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest(), "released_pack_digest_mismatch")
        (RESULTS / project.name / "released-pack.json").write_bytes(raw)
    return dag_id, index


def load_dag(project: Path, dag_id: str):
    from airflow.dag_processing.dagbag import DagBag

    index_path = str(project / ".dpone-cache/current/airflow-index.json")
    loader = ROOT / "dags" / (project.name.replace("-", "_") + ".py")
    loader.write_text(
        "from airflow.providers.dpone import load_dpone_dags\n"
        f"report = load_dpone_dags(globals(), index_path={index_path!r}, "
        "invalid_dag_policy='fail_all', duplicate_policy='fail_all')\n"
        "if report.fatal or report.errors or report.skipped or not report.loaded:\n"
        "    raise RuntimeError('live_loader_did_not_load_exact_DAG')\n"
    )
    bag = DagBag(dag_folder=str(loader), safe_mode=False)
    save(RESULTS / project.name / "07-dag-load.json", {"dag_ids": sorted(bag.dags), "import_errors": bag.import_errors})
    require(not bag.import_errors and dag_id in bag.dags, "actual_dag_load_failed")
    dag = bag.dags[dag_id]
    require(Path(dag.fileloc).resolve() == loader.resolve(), "dag_file_must_be_discoverable")
    return dag


def task_kinds(dag) -> dict[str, str]:
    kinds = {}
    for task in dag.tasks:
        values = getattr(task, "env_vars", {})
        env = values if isinstance(values, dict) else {item.name: item.value for item in values}
        if "DPONE_INIT_FETCH_PLAN_B64" in env:
            plan = json.loads(base64.b64decode(env["DPONE_INIT_FETCH_PLAN_B64"], validate=True))
            kinds[task.task_id] = plan["execution"]["kind"]
    require(set(kinds.values()) == {"runtime"}, "real_runtime_task_required")
    return kinds


def database_snapshot(label: str) -> dict:
    import psycopg

    with psycopg.connect(os.environ["AIRFLOW_CONN_WAREHOUSE"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id,label FROM preserve_dst.orders ORDER BY id")
            rows = cursor.fetchall()
            metadata = {}
            queries = {
                "oid": "SELECT 'preserve_dst.orders'::regclass::oid",
                "constraints": "SELECT conname,contype,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='preserve_dst.orders'::regclass ORDER BY conname",
                "columns": "SELECT attname,attnotnull,format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid='preserve_dst.orders'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",
                "indexes": "SELECT indexrelid,pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid='preserve_dst.orders'::regclass ORDER BY indexrelid",
                "view": "SELECT id,label FROM preserve_dst.orders_view ORDER BY id",
            }
            for name, query in queries.items():
                cursor.execute(query)
                metadata[name] = cursor.fetchall()
    snapshot = {"rows": rows, "metadata": metadata}
    save(RESULTS / label / "catalog.json", snapshot)
    return snapshot


def prepare_fixture() -> None:
    import psycopg

    with psycopg.connect(os.environ["AIRFLOW_CONN_WAREHOUSE"]) as connection:
        connection.execute("CREATE SCHEMA preserve_src; CREATE SCHEMA preserve_dst; CREATE SCHEMA staging")
        connection.execute("CREATE TABLE preserve_src.orders(id integer PRIMARY KEY, label text NOT NULL)")
        connection.execute(
            "CREATE TABLE preserve_dst.orders(id integer PRIMARY KEY, label text NOT NULL DEFAULT 'default' CHECK(label <> 'invalid'))"
        )
        connection.execute("CREATE INDEX orders_label ON preserve_dst.orders(label)")
        connection.execute("CREATE VIEW preserve_dst.orders_view AS SELECT id,label FROM preserve_dst.orders")
        connection.execute("INSERT INTO preserve_dst.orders VALUES (90,'previous')")


def replace_source(rows: list) -> None:
    import psycopg

    with psycopg.connect(os.environ["AIRFLOW_CONN_WAREHOUSE"]) as connection:
        connection.execute("DELETE FROM preserve_src.orders")
        with connection.cursor() as cursor:
            cursor.executemany("INSERT INTO preserve_src.orders VALUES (%s,%s)", rows)


def execute_case(case_id: str, dag, index: dict, expected: list, baseline: dict) -> dict:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom import XComModel
    from airflow.utils.session import create_session
    from sqlalchemy import select

    kinds = task_kinds(dag)
    run = dag.test(logical_date=datetime.now(UTC), use_executor=False)
    with create_session() as session:
        stored = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.run_id == run.run_id))
        states = {
            ti.task_id: str(ti.state)
            for ti in session.scalars(
                select(TaskInstance).where(TaskInstance.dag_id == dag.dag_id, TaskInstance.run_id == run.run_id)
            )
        }
        state = str(stored.state)
        xcoms = [
            {"task_id": x.task_id, "key": x.key, "value": x.value}
            for x in session.scalars(
                select(XComModel).where(XComModel.dag_id == dag.dag_id, XComModel.run_id == run.run_id)
            )
        ]
    save(RESULTS / case_id / "xcoms.json", xcoms)
    snapshot = database_snapshot(case_id)
    observed = {
        "case_id": case_id,
        "status": "FAIL",
        "source_commit": COMMIT,
        "run_id": run.run_id,
        "dag_id": dag.dag_id,
        "dag_state": state,
        "task_states": states,
        "task_kinds": kinds,
        "release_id": index["release_id"],
        "deployment_id": index["deployment_id"],
    }
    save(RESULTS / case_id / "observed.json", observed)
    rejected = case_id == "refresh-rejected"
    require(state == ("failed" if rejected else "success"), "dag_outcome_mismatch")
    require(
        any(s == "failed" for s in states.values()) if rejected else all(s == "success" for s in states.values()),
        "task_outcome_mismatch",
    )
    require(snapshot["rows"] == expected and snapshot["metadata"]["view"] == expected, "rows_or_view_mismatch")
    for name in ("oid", "constraints", "columns", "indexes"):
        require(snapshot["metadata"][name] == baseline["metadata"][name], "catalog_changed_" + name)
    summaries = [x["value"] for x in xcoms if x["key"] == "return_value" and x["task_id"] in kinds]
    require(len(summaries) == 1, "persisted_runtime_xcom_missing")
    require(summaries[0]["status"] == ("failed" if rejected else "passed"), "runtime_xcom_outcome_mismatch")
    if rejected:
        require("CheckViolation" in json.dumps(summaries[0]), "expected_check_constraint_failure_missing")
    observed.update(
        status="PASS", rows=snapshot["rows"], scope="actual strict DAG and independent database observations"
    )
    return observed


def main() -> int:
    import boto3
    from botocore.exceptions import ClientError

    config = configure()
    storage = boto3.client("s3")
    try:
        storage.head_bucket(Bucket="dpone-artifacts")
    except ClientError as error:
        require(error.response["ResponseMetadata"]["HTTPStatusCode"] == 404, "unexpected_bucket_error")
        storage.create_bucket(Bucket="dpone-artifacts")
    initialize_airflow()
    prepare_fixture()
    baseline = database_snapshot("before")
    project = ROOT / "cases/orders"
    dag_id, index = produce(project, "orders", config)
    inputs = [[(1, "alpha"), (2, "beta")], [(3, "changed")], [(3, "changed")], [(4, "invalid")], [(5, "retry")]]
    previous = baseline["rows"]
    failed = False
    for case_id, rows in zip(CASES, inputs, strict=True):
        replace_source(rows)
        try:
            expected = previous if case_id == "refresh-rejected" else rows
            emit(execute_case(case_id, load_dag(project, dag_id), index, expected, baseline))
            previous = expected
        except Exception as error:
            failed = True
            emit(
                {
                    "case_id": case_id,
                    "status": "FAIL",
                    "error_type": type(error).__name__,
                    "message": redact(str(error)),
                }
            )
    return int(failed)


if __name__ == "__main__":
    try:
        code = main()
    except Exception as error:
        emit(
            {
                "case_id": "controller",
                "status": "FAIL",
                "error_type": type(error).__name__,
                "message": redact(str(error)),
            }
        )
        code = 97
    print(f"DPONE_LIVE_CONTROLLER_DONE exit={code}", flush=True)
    time.sleep(120)
    sys.exit(code)
