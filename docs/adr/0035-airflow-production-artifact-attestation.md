# ADR 0035: Airflow production artifacts use one signed deployment overlay

## Status

Accepted.

Rollout status: **FAIL-CLOSED PREVIEW**. Contract and hermetic integration
coverage exist; signed dev restart and staged production cutover/rollback
evidence remain `UNVERIFIED`. Acceptance of this ADR is not certification of a
specific environment.

## Context

Exact Airflow publication and desired-state activation already bind immutable
content, registry authority, source identity, CAS ordering, and local atomic
activation. They do not prove that an approved build authority produced the
bytes. The cache materializer and KPO init-fetch intentionally expose verifier
ports and fail closed for production when no concrete verifier is installed.

Embedding a signature in content-addressed deployment identity would create a
circular digest. Fetching signatures during DAG parse would make scheduler
availability depend on object storage. Separate cache and runtime trust
formats would allow policy drift between the control and execution planes.

## Decision

One deterministic, deployment-scoped attestation overlay is signed externally
and verified by both cache materialization and runtime init-fetch.

The first production-target backend is Cosign blob signing with a protected CI
private key and a digest-pinned public key mounted read-only to consumers.
dpone builds, verifies, publishes, and consumes canonical evidence but never
receives the signing key through its API or runtime. Activation remains blocked
until the exact environment completes the live rollout gates.

The overlay is stored outside deployment identity:

```text
attestations/deployments/{environment}/sha256-{deployment}/
  artifact-attestation.json
  artifact-attestation.sigstore.json
  _SUCCESS
```

Signed claims bind exact release, deployment, index, image, environment,
registry authority, publication evidence, and protected source identity.
`_SUCCESS` is written last. Missing, partial, invalid, replayed, revoked, or
unverifiable evidence fails production closed before cache installation or
runtime extraction.

“Partial” above means an incomplete registry package: statement or bundle
without a valid completion marker. It does not mean the KPO must manufacture
observations unavailable in its isolated pod. The cache consumer observes the
full deployment subject. KPO init-fetch observes release/deployment roots and
records `airflow_index_sha256` under `unobserved_claims`; that bounded partial
observation is explicit, valid evidence rather than a successful full-subject
claim.

Attestations do not use short expiry. Immutable rollback remains available
while the signing public key is trusted and the attestation ID is not revoked.
Key rotation retains old public keys for the rollback window.

Airflow parsing reads only the already verified local cache. It never invokes
Cosign and never performs network I/O.

## Consequences

- The same producer identity and subject policy protects scheduler and runtime
  artifacts.
- Existing deployment IDs remain stable and can receive an attestation overlay
  after publication.
- The private key is a CI-only concern. Without Vault/KMS it may live in a
  protected hidden GitLab file variable; this is an explicit interim custody
  boundary, not a runtime secret.
- Consumers require a pinned Cosign binary, reviewed policy, and public key.
- Public-key rotation and emergency revocation require coordinated policy
  rollout before desired-state promotion.
- Non-production checksum verification remains backward-compatible.
- Public Sigstore keyless signing is deferred until the self-managed GitLab
  issuer and trusted-root path are independently certified.

## References

- [Feature specification](../feature-design-airflow-production-artifact-attestation-v07327.md)
- [Operations runbook](../airflow-artifact-attestation-operations.md)
- [ADR 0009: artifact delivery](0009-artifact-delivery-and-cache-materializer.md)
- [ADR 0024: executable init-fetch](0024-airflow-executable-init-fetch-wire-boundary.md)
- [ADR 0033: S3 desired state](0033-airflow-s3-desired-state-pull.md)
- [Sigstore blob verification](https://docs.sigstore.dev/cosign/verifying/verify/)
- [SLSA artifact verification](https://slsa.dev/spec/v1.2/verifying-artifacts)
