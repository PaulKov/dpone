"""Export original service bytes from the dedicated local node capture.

Only Pod UIDs already observed in this evidence root are accepted. Secret
volumes are never read. Metadata is obtained on the node before export; known
fixture credentials cause rejection rather than alteration of original bytes.
Run again after all containers finish to capture their final versions.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import tarfile

from cluster import DOCKER, PROFILE, ROOT, credentials, save, save_original

CAPTURE = "/tmp/dpone-pg-preserve-ec30"
NAMES = {"runtime-evidence.json", "return.json", "runtime-stderr.log", "runtime-startup-error.json"}


def export() -> None:
    credentials()
    uids = {p.stem for p in (ROOT / "pods").glob("*.json")}
    uids.update(p.name for p in (ROOT / "pod-events").iterdir() if p.is_dir())
    assert all(re.fullmatch(r"[0-9a-f-]{36}", uid) for uid in uids)
    command = (
        'cd "$1"; shift; for uid do for name in runtime-evidence.json return.json '
        'runtime-stderr.log runtime-startup-error.json; do file="$uid/$name"; '
        '[ ! -f "$file" ] || stat -c "%n %u %g %a %s" "$file"; done; done'
    )
    observed = subprocess.run(
        [DOCKER, "exec", PROFILE, "sh", "-c", command, "capture-stat", CAPTURE, *sorted(uids)],
        capture_output=True,
        check=True,
    )
    metadata: dict[str, dict] = {}
    paths = []
    for line in observed.stdout.decode().splitlines():
        path, owner, group, mode, size = line.split()
        uid, name = path.split("/")
        assert uid in uids and name in NAMES
        metadata.setdefault(uid, {})[name] = {"uid": int(owner), "gid": int(group), "mode": mode, "bytes": int(size)}
        paths.append(path)
    if not paths:
        raise RuntimeError("No captured service files")
    archive = subprocess.run(
        [DOCKER, "exec", PROFILE, "tar", "-C", CAPTURE, "-cf", "-", *paths], capture_output=True, check=True
    )
    fingerprints = {}
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as bundle:
        for member in bundle:
            assert member.isfile() and member.name in paths
            stream = bundle.extractfile(member)
            assert stream is not None
            raw = stream.read()
            uid, name = member.name.split("/")
            assert len(raw) == metadata[uid][name]["bytes"], "Capture changed during export; retry after completion"
            save_original("service-files/" + member.name, raw)
            fingerprints[member.name] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    for uid, files in metadata.items():
        save("service-files/" + uid + "/metadata.json", files)
    save("service-files/export-receipt.json", {"status": "PASS", "files": fingerprints})
    print(json.dumps({"exported_pods": len(metadata), "exported_files": len(fingerprints)}))


if __name__ == "__main__":
    export()
