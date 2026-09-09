# DPONE_CREDENTIAL_BACKEND_UNAVAILABLE

The runtime could not read the configured credential backend at workload start.
The error intentionally omits backend exception text, Vault paths, tokens, and
secret values.

## Fix

Restore the backend, workload identity, network path, role, or policy through
platform tooling, then retry the failed workload. A failed resolution is not
cached. See the [credential resolver recovery runbook](../airflow-credential-resolver-lifecycle.md#recovery-runbook).
