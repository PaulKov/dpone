# DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED

The separate optional live-sample command planned a temporary copy, but no
platform-configured certified-copy executor is available in this environment.
dpone stays fail-closed: it does not invent credentials, call Vault, read a
source, write a target, or run an untrusted data path.

This is the expected blocker on a laptop that completed the offline five-command
journey and then attempted the platform-gated live action. The offline preview
and hermetic test have already succeeded. Exit code `3` means that the optional
sample is a blocked handoff, not a successful sample execution.

## Fix

1. Confirm the command used `--sample ... --target temporary` (no additional
   beginner flag).
2. Hand the printed plan / JSON to platform CI so it can attach a signed
   deployment overlay and certified-copy executor.
3. Re-run the same `dpone run ... --sample ... --target temporary` command after
   the overlay is present; do not add ad-hoc connection flags.

See [First Airflow DAG](../getting-started/first-airflow-dag.md) and
[Airflow self-service](../airflow-self-service.md).
