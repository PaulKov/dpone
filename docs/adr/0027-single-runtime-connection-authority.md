# ADR 0027: Runtime connections have one logical authority

## Status

Accepted.

## Context

ADR 0010 establishes backend-neutral binding-sets and connection registries.
ADR 0012 establishes runtime-only Vault resolution. Legacy GCS, state, proxy,
and API code still infers tenant-specific buckets, projects, Vault paths,
connection IDs, and API identifiers. Some helpers create credential managers or
vendor clients outside the runtime composition root, and API connector
construction relies on inconsistent `from_vault` signatures.

Those behaviors create multiple authorities, make environment-neutral releases
non-portable, disclose backend topology, and can discover missing configuration
after data I/O has started.

## Decision

Every runtime connection capability has exactly one authority:

1. workload authoring declares a logical `connection_ref`;
2. the binding-set maps it to a platform registry alias;
3. the registry owns non-secret connection metadata and credential resolver
   references;
4. a workload-scoped resolver produces one immutable successful snapshot per
   alias;
5. typed capability projections are injected by the runtime composition root;
6. connectors do not discover Vault, infer topology, or construct hidden global
   clients.

Canonical and legacy configuration for the same capability is rejected. Bucket,
project, endpoint identifiers, proxy topology, and Vault paths are never
inferred. Vault locators remain restricted registry data and do not appear in
normal DAGs, logs, errors, or evidence.

API providers implement one explicit factory protocol over a resolved
connection and provider options. The registry does not inspect signatures,
catch provider `TypeError`, or forward arbitrary `from_vault` arguments.

Implicit runtime defaults are removed immediately. Existing public imports
remain for one minor release as explicit-only compatibility adapters controlled
by platform policy. No compatibility mode restores an embedded default.

Required connection resolution and typed validation complete before source or
target I/O. GCS replacement uses deterministic attempt-owned objects and never
deletes the active generation before the replacement is validated and accepted.

A bounded, non-disclosing hygiene gate scans a frozen commit and exact built
wheels/sdists for protected tenant identifiers and defaults.

## Consequences

- Pipeline manifests and release artifacts remain environment-neutral.
- A registry change affects deployment identity, not release identity.
- Missing or conflicting authority fails before data side effects.
- Credential rotation remains scoped to workload start.
- Connector construction becomes explicit and testable through dependency
  injection.
- Legacy callers must supply explicit values and receive deprecation warnings.
- Some deployments require a migration plan and new registry entries before
  upgrade.
- Source and package publication gain an additional bounded fail-closed gate.
- Live Vault/GCS production readiness still requires current approved evidence.

## Related material

- [Feature design: runtime connection authority and tenant hygiene](../feature-design-runtime-connection-authority-and-hygiene-v0732.md)
- [ADR 0010: Binding-set and credential resolvers](0010-binding-set-and-credential-resolvers.md)
- [ADR 0012: Vault runtime authentication](0012-vault-runtime-authentication.md)
- [Airflow self-service global review](../../test_artifacts/airflow-self-service-global-review-v0731/review-report.md)
