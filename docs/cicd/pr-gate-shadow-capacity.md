# PR Gate shadow capacity

The capacity probe is a diagnostic prerequisite for the future PR Gate shadow
reconciler. It does not authorize a merge, change any required check, or create
a daily reconciliation root. Its only result is an immutable `UNVERIFIED`
receipt.

## What the workflow does

`PR Gate shadow capacity` can run only through `workflow_dispatch` on the
default branch. It checks out that exact trusted revision with no persisted
credential, uses read-only `contents`, `actions`, and `pull-requests`
permissions, and never checks out a pull-request head. The workflow authenticates
its own provider run/attempt before it reads history.

The probe then performs two independent observations of the same fixed,
closed 14-day UTC-second interval. Each observation includes producer and
auditor runs, every observed attempt, complete Jobs and artifact inventories,
authenticated audit-receipt archives, and an exact producer lookup for each
receipt. Git requests, API body bytes, ZIP archive bytes, redirects, and wall
time share hard parent limits. A missing, moved, non-terminal, changed, or
ambiguous provider record blocks the diagnostic rather than being interpreted
as an empty result.

The observation implementation is itself source-bound: its static manifest,
the diagnostic CLI and workflow, Git commit/tree/blob objects, SHA-256 values,
regular-file modes, and the local default-branch checkout must all agree before
history is scanned. The manifest's own immutable blob digest is included in the
published bundle digest, so a source change requires a fresh capacity receipt.

### Refreshing the source manifest after a release bump

The manifest also includes `pyproject.toml` and `uv.lock`. After an intentional
version or dependency change, regenerate their hashes from the reviewed working
tree before committing. From the repository root, this producer validates every
existing entry and Git mode, then updates only the content hashes. It does not
add paths, change modes, relax verification, or produce a capacity receipt.

```bash
uv run --frozen python - <<'PY'
import hashlib
import subprocess
from pathlib import Path

import yaml

from dpone.services.ci.shadow_observation_bundle import entries_from_manifest

root = Path.cwd().resolve()
manifest = root / ".agents/policy/ci-shadow-reconciliation-observation-bundle-v1.yml"
original = manifest.read_text(encoding="utf-8")
entries = entries_from_manifest(yaml.safe_load(original))
updated = original
for entry in entries:
    source = root / entry.path
    if source.resolve() != source or not source.is_file():
        raise ValueError(f"Not a regular, confined source file: {entry.path}")
    tracked = subprocess.check_output(
        ["git", "ls-files", "--stage", "-z", "--", entry.path], text=True
    ).split("\0")
    if len(tracked) != 2 or tracked[-1] != "":
        raise ValueError(f"Expected one tracked entry: {entry.path}")
    metadata, tracked_path = tracked[0].split("\t", 1)
    mode, _, stage = metadata.split()
    local_mode = "100755" if source.stat().st_mode & 0o111 else "100644"
    if tracked_path != entry.path or mode != entry.mode or local_mode != mode or stage != "0":
        raise ValueError(f"Git path, mode or merge stage mismatch: {entry.path}")
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    block = (
        f"  - path: {entry.path}\n"
        f'    mode: "{entry.mode}"\n'
        f'    blob_sha256: "{entry.blob_sha256}"'
    )
    if updated.count(block) != 1:
        raise ValueError(f"Unexpected manifest formatting: {entry.path}")
    updated = updated.replace(block, block.replace(entry.blob_sha256, digest), 1)
refreshed = entries_from_manifest(yaml.safe_load(updated))
if [(e.path, e.mode) for e in refreshed] != [(e.path, e.mode) for e in entries]:
    raise ValueError("Source inventory changed")
if updated != original:
    manifest.write_text(updated, encoding="utf-8")
print(f"Validated {len(refreshed)} source entries; review the manifest diff.")
PY
uv run --frozen pytest tests/test_ci_shadow_pr4c_observation_bundle_verifier.py -q
```

Commit the reviewed source changes and manifest together. The `.agents/` change
requires a fresh agent-governance artifact and PR receipt through normal CI.
Historical capacity receipts remain unchanged and do not certify the new bundle;
run a new authenticated diagnostic only when fresh capacity evidence is needed.

## Reading the receipt

The artifact filename is
`pr-gate-shadow-reconciliation-capacity-<run-id>-<attempt>.json`. It is
create-only and retained for 90 days. The upload is intentionally archived as
a ZIP so the provider artifact size and digest authenticate the actual transport
bytes; the sole capacity JSON member is separately bounded and hashed before
strict JSON parsing. Auditor receipts observed by the probe follow the same
ZIP-then-payload rule. Do not compare provider digest metadata directly with
extracted JSON bytes.

`RECONCILIATION_CAPACITY_CALIBRATION_ONLY` means the two snapshots matched and
usage was no more than half the parent limits. It is still `UNVERIFIED`: it only
permits the next implementation phase after fresh evidence and the separate
public-fork lifecycle canary. `RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED`
means the evidence was incomplete, differed, crossed a hard limit, or exceeded
the half-limit safety threshold.

The workflow is expected to end red after a receipt is uploaded, because a
diagnostic receipt is deliberately never a pass. Download the exact artifact
for its run and attempt; never edit or overwrite an earlier receipt.

For copyable dispatch, inspection, evidence-boundary, and recovery steps, use
the [capacity runbook](pr-gate-shadow-capacity-runbook.md). The request policy
is 3,000 hard requests and 1,500 qualifying requests; byte and wall-time
thresholds remain unchanged.

## Recovery

| Symptom | Meaning | Safe action |
| --- | --- | --- |
| No artifact | Source authentication, checkout, bundle binding, or persistence failed before evidence could be created. | Fix the trusted default-branch implementation and dispatch a new run. |
| `...IMPLEMENTATION_BLOCKED` | Observation was incomplete, unequal, stale, or over budget. | Inspect counters and source identity; do not raise a limit without an approved parent amendment. |
| Artifact/archive mismatch | Metadata does not bind the fetched ZIP or its sole payload. | Treat the receipt as unusable and run a fresh authenticated attempt. |
| Old receipt | Capacity evidence expires for approval after 24 hours. | Dispatch a fresh default-branch probe. |

Historical receipts use
[`capacity.v1`](../../test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-capacity.schema.json).
The amended workflow emits
[`capacity.v2`](../../test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-capacity-v2.schema.json);
keep V1 for historical validation and use only V2 for the amended prerequisite.
