"""Build a pinned local image and verify every Python file against frozen source."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import subprocess
import tarfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CAMPAIGN = Path(__file__).resolve().parent / "kubernetes"
IMAGE = CAMPAIGN / "image"
DOCKER = "/Applications/Docker.app/Contents/Resources/bin/docker"
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
assert not subprocess.check_output(["git", "diff", "HEAD", "--", "src", "packages", "pyproject.toml"], cwd=ROOT)
base_id = subprocess.check_output(
    [DOCKER, "image", "inspect", "dpone-airflow-live:b0912cc", "--format", "{{.Id}}"], text=True
).strip()
assert base_id == "sha256:316e4b7ea683f65fedd655a55b39fa566a56161f7b6018aee24a69d1aacc23de"
(IMAGE / "wheels").mkdir(parents=True, exist_ok=True)
roots = [ROOT / "src", *(p / "src" for p in (ROOT / "packages").iterdir() if (p / "src").is_dir())]
archive_bytes = subprocess.check_output(["git", "archive", "--format=tar", commit, "src", "packages"], cwd=ROOT)
with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as source_archive:
    source_files = {
        m.name: source_archive.extractfile(m).read() for m in source_archive if m.isfile() and m.name.endswith(".py")
    }
claims = {}
for wheel in sorted((ROOT / "dist").glob("*.whl")):
    members = {}
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if not name.endswith(".py"):
                continue
            candidates = [path / name for path in roots if (path / name).is_file()]
            assert len(candidates) == 1, name
            original = source_files[str(candidates[0].relative_to(ROOT))]
            assert archive.read(name) == original, name
            members[name] = hashlib.sha256(original).hexdigest()
    shutil.copy2(wheel, IMAGE / "wheels" / wheel.name)
    claims[wheel.name] = {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(), "python_files": members}
assert len(claims) == 4
provenance = {"source_commit": commit, "base_image_id": base_id, "wheels": claims}
(IMAGE / "source-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
verify = """import hashlib,json,sysconfig
from pathlib import Path
claim=json.loads(Path('/opt/dpone-live/source-provenance.json').read_text())
root=Path(sysconfig.get_paths()['purelib'])
for wheel in claim['wheels'].values():
    for name, digest in wheel['python_files'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest() == digest, name
print('Verified exact-source installed Python files:', sum(len(w['python_files']) for w in claim['wheels'].values()))
"""
(IMAGE / "verify_installed.py").write_text(verify)
(IMAGE / "Dockerfile").write_text(f'''FROM dpone-airflow-live:b0912cc
USER root
COPY wheels /tmp/dpone-live-wheels
RUN python -m pip install --no-cache-dir --force-reinstall --no-deps /tmp/dpone-live-wheels/*.whl && python -m pip check && rm -rf /tmp/dpone-live-wheels
COPY source-provenance.json verify_installed.py /opt/dpone-live/
RUN python /opt/dpone-live/verify_installed.py
LABEL org.opencontainers.image.revision="{commit}"
USER 65532:65532
ENTRYPOINT ["dpone"]
CMD ["--help"]
''')
tag = "127.0.0.1:5005/postgres-strategy-runtime:" + commit[:12]
with (CAMPAIGN / "image-build.log").open("w") as log:
    subprocess.run(
        [DOCKER, "build", "--pull=false", "-t", tag, str(IMAGE)],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        check=True,
    )
with (CAMPAIGN / "image-push.log").open("w") as log:
    subprocess.run([DOCKER, "push", tag], stdout=log, stderr=subprocess.STDOUT, check=True)
request = urllib.request.Request(
    "http://127.0.0.1:5005/v2/postgres-strategy-runtime/manifests/" + commit[:12],
    headers={"Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json"},
)
with urllib.request.urlopen(request) as response:
    raw = response.read()
    digest = response.headers["Docker-Content-Digest"]
(IMAGE / "runtime-manifest.json").write_bytes(raw)
manifest = json.loads(raw)
allowed = [digest] + [
    m["digest"] for m in manifest.get("manifests", []) if m.get("platform", {}).get("architecture") == "arm64"
]
images = {
    "runtime": {
        "node_ref": "dpone-airflow-registry:5000/postgres-strategy-runtime@" + digest,
        "source_commit": commit,
        "allowed_image_digests": allowed,
    },
    "postgres": {
        "node_ref": "dpone-airflow-registry:5000/postgres@sha256:738d1359df5aa0b6d50a9071e989c49fdd39152a2a805c6ff131bf5e2243e0b3"
    },
    "minio": {
        "node_ref": "dpone-airflow-registry:5000/minio@sha256:9966a92a734f9411e32f4f41d7d9d826fcdc0f68c4e20b70295bd4e7c11f8a2f"
    },
    "xcom": {
        "node_ref": "dpone-airflow-registry:5000/xcom@sha256:2c9d26f410d032d5b1525aa8a873e238b05b90c4ae8618743d4311f0cc827e37"
    },
}
(CAMPAIGN / "registry-images.json").write_text(json.dumps(images, indent=2) + "\n")
print(images["runtime"])
