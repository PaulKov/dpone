# DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE

A deployment-scoped live overlay exists, but it is only partially materialized.
dpone refuses credential and database I/O until the overlay is complete and
trusted.

## Fix

Re-sync the complete overlay directory atomically from platform CI. Do not
hand-edit individual files inside the overlay. Then re-run the same
`dpone run ... --sample ... --target temporary` command.

See [Airflow route attestation](../airflow-route-attestation.md).
