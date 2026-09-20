# Airflow runtime-authority operations and recovery

Use this runbook for protected development runtime-authority input failures.
Never paste immutable payload bytes or Base64 into tickets, logs, or evidence.

| Symptom | Detection boundary | Recovery |
|---|---|---|
| Both source modes or a partial option pair | CLI input validation; exit 2, stderr, no deployment write | Select one complete mode. |
| Missing, unreadable, symlink, non-regular, empty, oversized, or digest-mismatched file | CLI bounded read; exit 2, redacted stderr, no deployment write | Restore a regular 1..4,096-byte non-secret file, recompute SHA-256, rerun. |
| Malformed/noncanonical Base64, byte-count, or digest mismatch | Provider v5 parse before DAG installation | Rebuild from the original file with matching packages; do not edit generated artifacts. |
| Plan exceeds 16 KiB | Provider plan construction or runtime decode | Reduce authority or other selected plan content; the limit is not configurable. |
| v5 index on an old provider | Unsupported closed schema before DAG installation | Upgrade core, pack, provider, and runtime image as one exact set, or rebuild using v4 Secret mode. |
| Provider-owned volume, mount, or environment collision | Pod composition before operator installation | Remove the caller override; the path and volume are not customizable. |
| Init materialization failure | Init container before authority, registry, ready, or connection access | Inspect only the stable error and Pod resource status; retry creates a fresh volume. |
| Base file mismatch | Base startup before its authority call | Treat as integrity failure; replace the Pod rather than repairing the file in place. |
| Secret object/key unavailable | Kubernetes pod startup in v4 mode | Restore the namespace-local Secret/key; dpone does not create it. |

Retries reconstruct the same projection from the immutable index and share no
state across Pods. Cancellation or Pod deletion removes the memory-backed
volume. No checkpoint, ready manifest, XCom result, or success evidence is
published for an authority-input failure.

To roll back immutable mode, rebuild the protected deployment with the existing
Secret flags or pin the preceding exact core/pack/provider/runtime-image set.
Do not relabel v5 artifacts as v4 or hand-edit payload/digest fields.
