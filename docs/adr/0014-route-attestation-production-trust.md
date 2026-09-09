# ADR 0014: Route attestation as production safe-sample trust

## Status

Accepted.

## Context

The production safe-sample policy requires external proof that a route was
certified. The initial Phase 1B implementation left the composition boundary
open for a future verifier and accepted a set of `verified_route_ids` from an
injected command context. A route ID alone is not a production trust decision:
it is not cryptographically bound to the route evidence, release, deployment,
environment, runtime image, or authorization profile, and it can be replayed in
another context.

Route certification and runtime authorization are different authorities.
Certification proves that a source/sink/strategy route passed a defined test
profile. Runtime authorization grants one certified route to one immutable
deployment context for a bounded period. Combining them would either make route
evidence environment-specific or weaken build-once/promote-by-digest.

## Decision

Production safe-sample execution requires a content-addressed
`dpone.route-attestation.v1` blob signed outside dpone and verified with a
Sigstore bundle before credential resolution, artifact fetch, or database I/O.
The signed claims bind all of the following:

- route ID and the complete route tuple;
- raw SHA-256 of the immutable route-certification bundle;
- certification profile and level;
- release ID, deployment ID, environment, and runtime image digest;
- authorization profile;
- issue, not-before, and expiry timestamps.

The first supported verifier is a dedicated `cosign verify-blob` adapter. Its
policy uses an exact certificate identity, exact OIDC issuer, a locally pinned
trusted root, a bounded command timeout, a supported cosign version, validity
limits, and local revocation lists. Regular-expression signer identities,
digest-only signatures, HMAC verification inside the runtime, and implicit
network trust-root refresh are not production paths.

The verification result has three explicit states:

- `verified`: signature, identity, evidence chain, subject bindings, validity,
  policy, and revocation checks all passed;
- `invalid`: supplied evidence is malformed, forged, mismatched, expired, or
  revoked;
- `unverified`: required verification infrastructure is unavailable or cannot
  establish trust.

Both `invalid` and `unverified` fail production closed. The runtime composition
root consumes only a `verified` decision and never accepts a raw route ID from
CLI context. The decision is recorded in runtime evidence, but it is not part
of release or deployment identity and cannot be serialized back into authoring
sources.

Signing remains an external CI responsibility. dpone builds a deterministic
unsigned authorization blob and verifies its detached Sigstore bundle; it
never receives a signing key or OIDC token.

## Consequences

- A copied route ID or edited execution plan cannot authorize production I/O.
- Route certification remains environment-neutral and reusable across
  deployments; the short-lived authorization overlay is environment-specific.
- Production operators must materialize a trusted root and a supported cosign
  executable in the runtime image or sidecar boundary.
- Secret, artifact, and database I/O remain downstream of the trust decision.
- Development rehearsal remains fail-closed and network-free unless the
  platform explicitly materializes the deployment-scoped authorization
  overlay described below; the five-command beginner journey does not gain
  another step.
- Live certification remains `UNVERIFIED` until an approved environment runs
  the exact signed-attestation path.
- A future verifier backend requires a separate compatibility decision; this
  ADR does not create a generic trust-plugin framework.

## 2026-07-15 addendum: beginner facade dispatch

The five-command beginner facade may automatically dispatch the existing
safe-sample execution plan to the existing live runtime assembly. Automatic
dispatch is permitted only when platform automation has materialized all four
authorization inputs under the already-pinned deployment and pipeline:

```text
.dpone-cache/route-authorizations/
  sha256-<deployment digest>/<pipeline_id>/
    route-attestation.json
    route-attestation.sigstore.json
    route-certification-bundle.json
    route-attestation-policy.json
```

This directory is an authorization overlay, not a new trust authority and not
part of release/deployment identity. Its path is derived from the verified
deployment context; a user manifest or beginner CLI option cannot select it.
The existing signature, subject, certification, fingerprint, policy, validity,
and revocation checks remain mandatory before credential resolver construction
or database I/O.

The dispatch policy is intentionally asymmetric:

- absent overlay: preserve the network-free local handoff;
- complete and verified overlay: execute through the existing live assembly
  and runtime runner;
- partially materialized, unsafe, invalid, expired, revoked, or mismatched
  overlay: fail closed and do not fall back to a less-trusted live path.

Runtime evidence records `execution_mode=live_copy` or
`execution_mode=local_handoff`. Signing remains external. No private key, OIDC
token, secret value, or mutable `current` resolution is introduced into the
beginner command.

## References

- [Sigstore blob verification](https://docs.sigstore.dev/cosign/verifying/verify/)
- [Sigstore blob signing](https://docs.sigstore.dev/cosign/signing/signing_with_blobs/)
- [SLSA artifact verification](https://slsa.dev/spec/v1.2/verifying-artifacts)
- [ADR 0007: Release-set and deployment-set](0007-release-set-and-deployment-set.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0013: Reproducible rerun and retention](0013-reproducible-rerun-and-retention.md)
