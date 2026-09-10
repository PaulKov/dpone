"""Run the isolated synthetic PostgreSQL campaign without exporting credentials."""

import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

DOCKER = "/Applications/Docker.app/Contents/Resources/bin/docker"
ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = Path(__file__).resolve().parent
NAME = "dpone-pg-preservation-ec30"


def docker(*args):
    return subprocess.check_output([DOCKER, *args], text=True)


try:
    info = json.loads(docker("inspect", NAME))[0]
except subprocess.CalledProcessError:
    env = dict(os.environ, POSTGRES_PASSWORD=secrets.token_urlsafe(32))
    subprocess.run(
        [
            DOCKER,
            "run",
            "-d",
            "--name",
            NAME,
            "--label",
            "dpone.campaign=postgres-strategy-preservation",
            "-p",
            "127.0.0.1::5432",
            "-e",
            "POSTGRES_PASSWORD",
            "-e",
            "POSTGRES_USER=dpone",
            "-e",
            "POSTGRES_DB=dpone_it",
            "postgres:16-alpine",
        ],
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    info = json.loads(docker("inspect", NAME))[0]
settings = dict(item.split("=", 1) for item in info["Config"]["Env"])
port = info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
for _ in range(30):
    ready = subprocess.run([DOCKER, "exec", NAME, "pg_isready", "-U", "dpone"], capture_output=True)
    if ready.returncode == 0:
        break
    time.sleep(1)
else:
    raise RuntimeError("isolated PostgreSQL failed readiness")
run_dir = ARTIFACTS / (sys.argv[1] if len(sys.argv) > 1 else "live-direct-initial")
run_dir.mkdir(exist_ok=True)


def tree_fingerprint():
    digest = hashlib.sha256()
    for directory in ("src", "tests"):
        for path in sorted((ROOT / directory).rglob("*.py")):
            digest.update(str(path.relative_to(ROOT)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


started_digest = tree_fingerprint()
identity = {
    "container_id": info["Id"],
    "image_id": info["Image"],
    "image_ref": info["Config"]["Image"],
    "source_and_test_sha256": started_digest,
    "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
}
(run_dir / "environment.json").write_text(json.dumps(identity, indent=2) + "\n")
env = dict(
    os.environ,
    DPONE_RUN_INTEGRATION="1",
    DPONE_RUN_INTEGRATION_LIVE="1",
    DPONE_IT_PG_HOST="127.0.0.1",
    DPONE_IT_PG_PORT=port,
    DPONE_IT_PG_DATABASE="dpone_it",
    DPONE_IT_PG_USER="dpone",
    DPONE_IT_PG_PASSWORD=settings["POSTGRES_PASSWORD"],
    DPONE_PG_EVIDENCE_DIR=str(run_dir),
    DPONE_PG_SOURCE_COMMIT=identity["source_head"],
    DPONE_PG_SOURCE_SHA256=started_digest,
)
result = subprocess.run(
    [
        str(ROOT / ".venv/bin/python"),
        "-m",
        "pytest",
        "tests/integration/postgres/test_postgres_strategy_preservation_live.py",
        "-q",
        "-rA",
        *sys.argv[2:],
    ],
    cwd=ROOT,
    env=env,
    capture_output=True,
    text=True,
)
output = result.stdout + result.stderr
output = output.replace(settings["POSTGRES_PASSWORD"], "<redacted>")
(run_dir / "pytest.log").write_text(output)
end_digest = tree_fingerprint()
(run_dir / "run-receipt.json").write_text(
    json.dumps(
        {
            "command": result.args,
            "exit_code": result.returncode,
            "start_source_and_test_sha256": started_digest,
            "end_source_and_test_sha256": end_digest,
            "unchanged_source_and_tests": started_digest == end_digest,
        },
        indent=2,
    )
    + "\n"
)
if started_digest != end_digest:
    raise RuntimeError("Source/test bytes changed during run; this evidence is not frozen-source proof")
print(f"live exit={result.returncode}; evidence={run_dir}")
sys.exit(result.returncode)
