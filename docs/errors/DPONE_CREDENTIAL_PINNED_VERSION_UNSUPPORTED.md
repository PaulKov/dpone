# DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED

The registry requests `version_policy: pinned`, but the current runtime client
cannot select and prove a historical Vault data version. Reading latest instead
would break reproducibility, so dpone fails before backend I/O.

## Fix

Use `version_policy: latest` with `resolution_scope: workload_start`, or keep the
workload blocked until a historical-version resolver contract is available.
