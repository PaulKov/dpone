"""Retain API watch transitions, including Pods deleted immediately by KPO."""

import json
import signal
import subprocess
import sys
from datetime import datetime, timezone

from cluster import DOCKER, KUBE, NAMESPACE, PROFILE, credentials, save

UTC = timezone.utc  # noqa: UP017 - host Python is the macOS system Python 3.9.

credentials()
process = subprocess.Popen(
    [*KUBE, "get", "pods", "-n", NAMESPACE, "--watch", "--output-watch-events", "-o", "json"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
seen = set()
decoder = json.JSONDecoder()
pending = ""
coverage = {"status": "RUNNING", "started_at": datetime.now(UTC).isoformat(), "events": 0}
save("pod-watch-coverage.json", coverage)


def stop_watch(_signal, _frame):
    coverage.update(status="COMPLETE", ended_at=datetime.now(UTC).isoformat())
    save("pod-watch-coverage.json", coverage)
    process.terminate()
    sys.exit(0)


signal.signal(signal.SIGTERM, stop_watch)
signal.signal(signal.SIGINT, stop_watch)
while True:
    character = process.stdout.read(1)
    if not character:
        coverage.update(status="FAILED", ended_at=datetime.now(UTC).isoformat())
        save("pod-watch-coverage.json", coverage)
        raise RuntimeError("Pod watch ended before the collector was stopped")
    pending += character
    if character != "}":
        continue
    try:
        event, offset = decoder.raw_decode(pending.lstrip())
    except json.JSONDecodeError:
        continue
    pending = pending.lstrip()[offset:]
    pod = event["object"]
    if pod.get("kind") != "Pod":
        continue
    for container in [*pod["spec"].get("initContainers", []), *pod["spec"].get("containers", [])]:
        for entry in container.get("env", []):
            if any(token in entry["name"] for token in ("PASSWORD", "ACCESS_KEY", "SECRET", "TOKEN", "AIRFLOW_CONN")):
                if "value" in entry:
                    entry["value"] = "[REDACTED]"
    metadata = pod["metadata"]
    uid = metadata["uid"]
    if uid not in seen:
        import re

        assert re.fullmatch(r"[0-9a-f-]{36}", uid)
        subprocess.run(
            [DOCKER, "exec", PROFILE, "mkdir", "-p", "/tmp/dpone-pg-preserve-ec30/allowed/" + uid], check=True
        )
        seen.add(uid)
    save(f"pod-events/{metadata['uid']}/{metadata['resourceVersion']}.json", event)
    coverage["events"] += 1
    coverage["last_event_at"] = datetime.now(UTC).isoformat()
    save("pod-watch-coverage.json", coverage)
