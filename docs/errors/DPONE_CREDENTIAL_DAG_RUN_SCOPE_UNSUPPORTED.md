# DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED

The registry requests `resolution_scope: dag_run_start`, but dpone currently
creates one credential scope per workload. Executing it as workload-scoped would
silently violate the requested contract, so readiness and runtime fail closed.

## Fix

Use `resolution_scope: workload_start`. Keep the workload blocked if it truly
requires one credential snapshot shared by an entire DAG run.
