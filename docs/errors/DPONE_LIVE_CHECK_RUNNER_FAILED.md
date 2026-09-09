# DPONE_LIVE_CHECK_RUNNER_FAILED

The configured bounded live preflight runner raised an exception before it
could return probe results.

dpone converts the exception into a structured `dpone.error.v1` payload and
redacts common inline secret assignments such as `password=...` and `token=...`.

## Fix

Inspect the platform runner logs and verify:

- the runner can access its runtime dependencies;
- Vault/Kubernetes/database clients are configured only in the runtime plane;
- any live credentials are resolved in memory and never passed through CLI
  arguments, Airflow Variables, XCom, or logs;
- the runner returns probe results instead of raising for expected dependency
  failures.
