# ADR 0010: Binding-set and credential resolvers

## Status

Accepted.

## Context

dpone deployments must support Airflow Connections, HashiCorp Vault KV, Kubernetes Secrets, environment variables, and future secret managers without embedding secrets in manifests, packs, or DAG specs.

## Decision

`dpone.binding-set.v1` maps logical pipeline connection IDs to `connection_ref` aliases. A platform-owned `dpone.connection-registry.v1` maps aliases to backend-neutral credential resolver definitions.

`connection_ref` aliases are backend-neutral logical names. They are validated
as short identifiers, not file paths, Vault paths, Airflow URIs, Kubernetes
object references, or secret payloads. The same alias contract applies to
pipeline authoring refs, binding-set keys and values, and connection-registry
keys.

Resolvers are a tagged union. `vault_kv` is the production golden path,
`kubernetes_secret_volume` is production-supported, `airflow_connection` is a
compatibility bridge, and `env_var` is development/legacy only. Runtime
evidence stores resolver-safe metadata only: Kubernetes Secret resolvers expose
a `credential_ref_fingerprint` for the non-secret resolver reference instead of
serializing Secret names, namespaces, mount paths, field filenames, or secret
values.

## Consequences

- Users do not need Vault paths in pipeline files.
- Vault can be reorganized without changing pipeline authoring sources.
- Resolver implementation is isolated behind runtime credential ports.
