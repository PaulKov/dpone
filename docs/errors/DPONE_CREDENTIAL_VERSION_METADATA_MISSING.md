# DPONE_CREDENTIAL_VERSION_METADATA_MISSING

An explicit Vault KV v2 resolution returned data without a positive secret
version. dpone cannot produce trustworthy rotation evidence from that response.

## Fix

Use a supported public Vault client adapter that returns data and version
metadata atomically. Do not hash secret values or call private client internals
to manufacture a version. See the [current client limitation](../airflow-credential-resolver-lifecycle.md#current-vault-client-limitation).
