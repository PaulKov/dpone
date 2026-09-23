# ADR 0071: Deployment-owned native credential projection

- Status: Accepted
- Date: 2026-09-23
- Implementation: pending exact path allocation and offline acceptance

## Context

Native release packs deliberately contain an empty connection projection. They
cannot own environment-specific Kubernetes credential topology. Rewriting an
Airflow operator-bridge registry into file resolvers currently discards source
connection IDs and Secret keys, so sealed runtime snapshots alone cannot build
the necessary Pod mounts. Guessing a key from a logical alias is incorrect.

The protected desired-state authority v2 already owns the workspace-control
connection ref. Repeating that selector in domain or binding-set configuration
would introduce contradictory authorities. Deployment/index/init-fetch v1-v5
are closed contracts, not extensible transport envelopes.

## Decision

Compile one secret-free `dpone.runtime-credential-projection.v1` from verified
workload requirements, environment bindings, source-registry bridge metadata and
the existing protected authority. Preserve exact logical ref -> registry ref ->
source connection ID -> Secret key mapping before registry rewriting. Distinct
aliases may share one key while retaining distinct resolver paths. Select only
the bounded workload-plus-control closure; unrelated registry entries do not
grant credential access.

The control ref is derived from the protected authority, never separately
authored. Bind its existing publication-authority fingerprint. Build composition
injects the typed authority; preparation/publication and activation compare it;
provider/runtime compare sealed projection and pointer-derived control ref.

Allocate closed deployment-set v6, Airflow deployment-index v6 and runtime
init-fetch-plan v6. Each carries the same exact projection descriptor. Reuse the
existing three-field descriptor shape. Its content-addressed URI is independent
of deployment ID, preventing a hash cycle. Preserve all earlier wire contracts
and portable release bytes. V6 supports production and non-production and does
not inherit v5's development-only requirement. Existing development authority
sources retain their existing conditional restrictions.

The provider verifies local projection bytes before creating operators and
materializes exact read-only Secret items only on the base container. No target
or control credentials are mounted into artifact-fetch init or XCom containers.
Init-fetch transports only the non-secret projection artifact. Base independently
checks its exact descriptor, membership, pointer ref and resolver paths before
connector access. Ready v1/v2 remain unchanged only because exact verified plan
and deployment subjects transitively bind this artifact; test that chain.

Public artifacts contain source references and fingerprints, never credentials,
URI values or Secret resource data. Platform provisioning still owns Secret
values, namespace/RBAC and watcher mounts. No source plugin, arbitrary Pod mount
API, runtime fallback or user monkeypatch is introduced.

## Consequences

Source-ID/key changes change deployment identity; credential value rotation does
not. Alias fan-out is explicit and reproducible. Domain authors do not duplicate
platform control connections. Missing or contradictory closure fails before
Pod creation or connector activity. Old readers reject v6 rather than accepting
an under-provisioned Pod.

Readers, provider and runtime image must be upgraded before v6 emission. Platform
watcher credential provisioning and protected-authority consistency are mandatory
rollout prerequisites. SQL lifecycle and workspace handover remain separate
contracts. This ADR approves architecture, not live certification or publication.

## Related contracts

- [Approved feature specification](../feature-specs/deployment-credential-projection.md)
- [ADR 0024: strict Airflow init-fetch wire](0024-airflow-executable-init-fetch-wire-boundary.md)
- [ADR 0027: single runtime connection authority](0027-single-runtime-connection-authority.md)
- [ADR 0070: immutable runtime authority](0070-immutable-runtime-authority-payload.md)
