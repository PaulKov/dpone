# ADR 0012: Vault runtime authentication

## Status

Accepted.

## Context

dpone already supports Vault via `vault-kv-client`; production Airflow/KPO execution should not resolve secrets in the scheduler.

## Decision

Vault resolution happens in the dpone runtime pod. The production auth path is Kubernetes ServiceAccount -> Vault Kubernetes Auth -> Vault role -> Vault KV -> in-memory resolved connection.

Binding-sets and registries contain logical Vault mount/path references, not Vault tokens, Kubernetes JWTs, AppRole `secret_id`, passwords, or full Vault HTTP paths.

The implemented rotation boundary is `version_policy: latest` plus
`resolution_scope: workload_start`. The runtime resolves each logical reference
once per workload and records the actual positive Vault KV v2 version. Planned
`pinned` and `dag_run_start` values remain schema-readable for migration but fail
closed before backend I/O until their distinct contracts exist.

## Consequences

- Airflow parse never imports or calls Vault.
- Credential rotation is handled at workload start.
- Evidence records safe credential metadata only.
- A failed resolution is not cached; a retry after backend recovery resolves
  again. An already resolved workload keeps its in-memory version.
- The primary Vault path is not production-certified until the public
  `vault-kv-client` adapter exposes KV data and version metadata atomically and
  approved live evidence validates Kubernetes Auth, rotation, and recovery.

Operational details and resolver status are in
[Airflow credential resolver lifecycle](../airflow-credential-resolver-lifecycle.md).
