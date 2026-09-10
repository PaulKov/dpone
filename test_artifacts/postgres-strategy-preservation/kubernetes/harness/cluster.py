"""Local-only minikube fixture and safe evidence collector for PostgreSQL strategy preservation.

All Kubernetes commands use the dedicated kubeconfig. Credentials are generated
in memory and stored only in the approved local cluster's Secret. Evidence is
redacted before writing; no kubeconfig or Secret objects are exported.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import secrets
import subprocess
import sys
import tarfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "dpone-pg-preserve-ec30"
PROFILE = "dpone-airflow-live-b0912cc"
KUBE = [
    "/Applications/Docker.app/Contents/Resources/bin/kubectl",
    "--kubeconfig",
    str(Path.home() / ".kube" / (PROFILE + ".config")),
    "--context",
    PROFILE,
]
DOCKER = "/Applications/Docker.app/Contents/Resources/bin/docker"
SECRET_VALUES: list[str] = []


def safe(text: str) -> str:
    for value in SECRET_VALUES:
        text = text.replace(value, "[REDACTED]")
    return text


def save(name: str, value: object) -> None:
    target = ROOT / name
    target.parent.mkdir(parents=True, exist_ok=True)
    content = value if isinstance(value, str) else json.dumps(value, indent=2)
    target.write_text(safe(content))


def save_original(name: str, raw: bytes) -> None:
    """Preserve exact service bytes; reject credential-bearing originals."""
    assert all(value.encode() not in raw for value in SECRET_VALUES), "credential_in_service_artifact"
    target = ROOT / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)


def kube(*args: str, payload: object = None, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [*KUBE, *args],
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        capture_output=True,
        timeout=120,
    )
    if check and result.returncode:
        raise RuntimeError(safe(result.stderr))
    return result


def apply(document: dict) -> None:
    kube("apply", "-f", "-", payload=document)


def document(kind: str, name: str, **fields: object) -> dict:
    return {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": NAMESPACE}, **fields}


def credentials() -> tuple[str, str]:
    existing = kube("get", "secret", "dpone-live-s3", "-n", NAMESPACE, "-o", "json", check=False)
    if existing.returncode == 0:
        data = json.loads(existing.stdout)["data"]
        login, password = (base64.b64decode(data[key]).decode() for key in ("login", "password"))
    else:
        login, password = "live" + secrets.token_hex(8), secrets.token_hex(24)
    SECRET_VALUES.extend([login, password])
    warehouse = kube("get", "secret", "dpone-live-postgres", "-n", NAMESPACE, "-o", "json", check=False)
    if warehouse.returncode == 0:
        SECRET_VALUES.extend(
            base64.b64decode(value).decode() for value in json.loads(warehouse.stdout)["data"].values()
        )
    return login, password


def service_pod(name: str, image: str, port: int, env: list[dict], args: list[str]) -> None:
    apply(document("Service", name, spec={"selector": {"app": name}, "ports": [{"port": port}]}))
    pod = document(
        "Pod",
        name,
        spec={
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": name,
                    "image": image,
                    "args": args,
                    "env": env,
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "1", "memory": "512Mi"},
                    },
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                }
            ],
            "volumes": [{"name": "data", "emptyDir": {}}],
        },
    )
    pod["metadata"]["labels"] = {"app": name}
    if name == "postgres":
        pod["spec"]["containers"][0]["readinessProbe"] = {
            "exec": {"command": ["pg_isready", "-U", "postgres"]},
            "periodSeconds": 2,
        }
    else:
        pod["spec"]["containers"][0]["readinessProbe"] = {
            "httpGet": {"path": "/minio/health/ready", "port": port},
            "periodSeconds": 2,
        }
    apply(pod)


def bootstrap() -> None:
    context = json.loads(kube("get", "nodes", "-o", "json").stdout)
    assert len(context["items"]) == 1 and context["items"][0]["metadata"]["name"] == PROFILE
    save("cluster/nodes.json", context)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": NAMESPACE, "labels": {"dpone-live-campaign": "pg-preservation"}},
        }
    )
    for name in ("dpone-live", "dpone-controller"):
        apply(document("ServiceAccount", name))
    rules = [
        {
            "apiGroups": [""],
            "resources": ["pods", "pods/log", "pods/exec", "secrets", "configmaps", "events"],
            "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"],
        }
    ]
    role = document("Role", "dpone-local-controller", rules=rules)
    role["apiVersion"] = "rbac.authorization.k8s.io/v1"
    apply(role)
    binding = document(
        "RoleBinding",
        "dpone-local-controller",
        roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role["metadata"]["name"]},
        subjects=[
            {"kind": "ServiceAccount", "name": n, "namespace": NAMESPACE} for n in ("dpone-controller", "dpone-live")
        ],
    )
    binding["apiVersion"] = "rbac.authorization.k8s.io/v1"
    apply(binding)
    login, password = credentials()
    apply(document("Secret", "dpone-live-s3", stringData={"login": login, "password": password}))
    warehouse = kube("get", "secret", "dpone-live-postgres", "-n", NAMESPACE, "-o", "json", check=False)
    database_password = (
        base64.b64decode(json.loads(warehouse.stdout)["data"]["password"]).decode()
        if warehouse.returncode == 0
        else secrets.token_hex(24)
    )
    database_uri = "postgres://postgres:" + database_password + "@postgres:5432/postgres"
    SECRET_VALUES.extend([database_password, database_uri])
    apply(document("Secret", "dpone-live-postgres", stringData={"password": database_password, "uri": database_uri}))
    apply(document("Secret", "dpone-airflow-connection-bridge", stringData={"AIRFLOW_CONN_WAREHOUSE": database_uri}))
    registry = {
        "schema": "dpone.artifact-registry-runtime-config.v1",
        "registries": {
            "dpone-local-artifacts": {
                "registry_uri": "s3://dpone-artifacts/pg-strategy-preservation",
                "access": {"mode": "workload_identity"},
            }
        },
    }
    registry_text = json.dumps(registry, sort_keys=True)
    apply(document("ConfigMap", "dpone-artifact-registry", data={"registry.json": registry_text}))
    save("cluster/registry-config.json", registry_text)
    images = json.loads((ROOT / "registry-images.json").read_text())
    service_pod(
        "postgres",
        images["postgres"]["node_ref"],
        5432,
        [
            {"name": "POSTGRES_HOST_AUTH_METHOD", "value": "scram-sha-256"},
            {"name": "PGDATA", "value": "/data/pgdata"},
            {
                "name": "POSTGRES_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": "dpone-live-postgres", "key": "password"}},
            },
        ],
        [],
    )
    service_pod(
        "minio",
        images["minio"]["node_ref"],
        9000,
        [
            {"name": name, "valueFrom": {"secretKeyRef": {"name": "dpone-live-s3", "key": key}}}
            for name, key in (("MINIO_ROOT_USER", "login"), ("MINIO_ROOT_PASSWORD", "password"))
        ],
        ["server", "/data", "--console-address", ":9001"],
    )
    print("Local fixture manifests applied", flush=True)


def snapshot_pods() -> None:
    pods = json.loads(kube("get", "pods", "-n", NAMESPACE, "-o", "json").stdout)
    for pod in pods["items"]:
        # Admitted specs are evidence, but secret-valued environment fields are not.
        for container in [*pod["spec"].get("initContainers", []), *pod["spec"].get("containers", [])]:
            for entry in container.get("env", []):
                if any(
                    token in entry["name"] for token in ("PASSWORD", "ACCESS_KEY", "SECRET", "TOKEN", "AIRFLOW_CONN")
                ):
                    if "value" in entry:
                        entry["value"] = "[REDACTED]"
        uid = pod["metadata"]["uid"]
        save(f"pods/{uid}.json", pod)
        if pod["metadata"]["name"] == "dpone-controller" and pod["status"]["phase"] == "Running":
            archive = subprocess.run(
                [
                    *KUBE,
                    "exec",
                    "dpone-controller",
                    "-n",
                    NAMESPACE,
                    "--",
                    "tar",
                    "-C",
                    "/work",
                    "-cf",
                    "-",
                    "results",
                    "airflow/logs",
                ],
                capture_output=True,
                timeout=15,
            )
            if archive.returncode == 0:
                with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as files:
                    for member in files.getmembers():
                        if (
                            member.isfile()
                            and member.name.startswith("results/")
                            and ".." not in Path(member.name).parts
                        ):
                            save_original(f"controller/{uid}/{member.name}", files.extractfile(member).read())
                        elif (
                            member.isfile()
                            and member.name.startswith("airflow/logs/")
                            and ".." not in Path(member.name).parts
                        ):
                            save(
                                f"controller/{uid}/{member.name}",
                                files.extractfile(member).read().decode("utf-8", errors="replace"),
                            )
        for container in [*pod["spec"].get("initContainers", []), *pod["spec"]["containers"]]:
            result = kube("logs", pod["metadata"]["name"], "-n", NAMESPACE, "-c", container["name"], check=False)
            if result.returncode == 0:
                save(f"pod-logs/{uid}/{container['name']}.log", result.stdout)


def launch_controller() -> None:
    credentials()
    images = json.loads((ROOT / "registry-images.json").read_text())
    apply(
        document(
            "ConfigMap", "dpone-live-harness", data={"controller.py": (ROOT / "harness/controller.py").read_text()}
        )
    )
    registry_text = json.loads(
        kube("get", "configmap", "dpone-artifact-registry", "-n", NAMESPACE, "-o", "json").stdout
    )["data"]["registry.json"]
    env = {
        "DPONE_SOURCE_COMMIT": images["runtime"]["source_commit"],
        "DPONE_LIVE_IMAGE_REF": images["runtime"]["node_ref"],
        "DPONE_LIVE_XCOM_IMAGE_REF": images["xcom"]["node_ref"],
        "DPONE_LIVE_NAMESPACE": NAMESPACE,
        "DPONE_LIVE_REGISTRY_SHA": "sha256:" + hashlib.sha256(registry_text.encode()).hexdigest(),
        "AWS_ENDPOINT_URL": "http://minio.dpone-pg-preserve-ec30.svc.cluster.local:9000",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AIRFLOW_CONN_KUBERNETES_DEFAULT": json.dumps({"conn_type": "kubernetes", "extra": {"in_cluster": True}}),
        "DPONE_LAUNCH_PIN_STORE_BACKEND": "kubernetes_configmap",
        "DPONE_LAUNCH_PIN_STORE_NAMESPACE": NAMESPACE,
    }
    entries = [{"name": k, "value": v} for k, v in env.items()]
    entries.append(
        {"name": "AIRFLOW_CONN_WAREHOUSE", "valueFrom": {"secretKeyRef": {"name": "dpone-live-postgres", "key": "uri"}}}
    )
    entries.extend(
        {"name": name, "valueFrom": {"secretKeyRef": {"name": "dpone-live-s3", "key": key}}}
        for name, key in (("AWS_ACCESS_KEY_ID", "login"), ("AWS_SECRET_ACCESS_KEY", "password"))
    )
    apply(
        document(
            "Pod",
            "dpone-controller",
            spec={
                "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 5,
                "serviceAccountName": "dpone-controller",
                "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
                "containers": [
                    {
                        "name": "controller",
                        "image": images["runtime"]["node_ref"],
                        "command": ["python", "-u", "/harness/controller.py"],
                        "env": entries,
                        "resources": {
                            "requests": {"cpu": "250m", "memory": "512Mi"},
                            "limits": {"cpu": "2", "memory": "2Gi"},
                        },
                        "volumeMounts": [
                            {"name": "work", "mountPath": "/work"},
                            {"name": "harness", "mountPath": "/harness", "readOnly": True},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "work", "emptyDir": {}},
                    {"name": "harness", "configMap": {"name": "dpone-live-harness"}},
                ],
            },
        )
    )
    pod = json.loads(kube("get", "pod", "dpone-controller", "-n", NAMESPACE, "-o", "json").stdout)
    source = (ROOT / "harness/controller.py").read_bytes()
    kube(
        "annotate",
        "pod",
        "dpone-controller",
        "-n",
        NAMESPACE,
        "dpone-live-harness-sha256=" + hashlib.sha256(source).hexdigest(),
    )
    save_original(f"controller/{pod['metadata']['uid']}/harness/controller.py", source)


def collect() -> None:
    credentials()
    deadline = time.monotonic() + 7200
    while time.monotonic() < deadline:
        try:
            snapshot_pods()
        except Exception as exc:
            save("collector-last-error.txt", str(exc))
        time.sleep(0.4)


if __name__ == "__main__":
    {"bootstrap": bootstrap, "controller": launch_controller, "collect": collect}[sys.argv[1]]()
