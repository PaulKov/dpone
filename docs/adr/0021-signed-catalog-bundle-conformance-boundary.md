# ADR 0021: Signed catalog bundles and closed extension conformance

## Status

Accepted.

## Context

ADR 0016 gives declarative recipes exact local byte pins, but local byte
identity does not establish publisher identity. Environment connection
registries are also fingerprinted into deployments without embedding secret
values, yet their promotion trust is external. Phase 4 requires signed recipe
and registry bundles plus a conformance suite without introducing scheduler
code execution, mutable catalog resolution, or false production certification.

The existing route-attestation path already has a bounded cosign invocation,
but its port and stable errors are route-specific. Duplicating that subprocess
policy would violate DRY; directly importing route policy into catalog services
would violate responsibility and terminology boundaries.

## Decision

dpone signs and verifies one deterministic catalog manifest, not individual
YAML files or an archive.

- `dpone.catalog-bundle.v1` is a content-addressed directory containing
  `catalog-bundle.json`, a confined plain-file `payload/`, and `_SUCCESS`.
- v1 supports only `recipe_catalog` and `connection_registry`.
- CI signs the exact manifest bytes outside dpone. Verification requires exact
  signer identity, OIDC issuer, trusted-root digest, verifier version range,
  every payload digest/size, and semantic validation.
- Catalog verification occurs only in CI/materializer/build plane. Airflow DAG
  parsing, task execution, Vault resolution, and connector runtime do not fetch
  or verify catalogs.
- A generic `BlobSignatureVerifier` port and cosign adapter own fixed
  cryptographic process mechanics. Route and catalog application services map
  generic results to their own stable domain error codes.
- Existing route imports and behavior remain compatible through a delegating
  wrapper.
- Extension conformance has four closed profiles: Airflow provider, recipe
  catalog, credential resolver, and route. Each profile has a fixed evidence
  checklist. Requests cannot load Python entry points or define new checks.
- `PASS` means all required profile evidence is exact and passed. Required
  missing/skipped/unavailable evidence is `UNVERIFIED`; explicit contradiction
  is `FAIL`. Resolver and route profiles require approved live evidence for
  `PASS`.
- Conformance is release evidence, not runtime authorization and not a
  substitute for route certification.

## Consequences

- Platform teams can prove who published exact recipe/registry bytes while the
  beginner pipeline journey remains unchanged.
- One generic infrastructure mechanism serves two real trust use cases without
  creating a generic plugin framework.
- Plain directories make transfer less compact than archives but avoid
  decompression and archive traversal risks.
- Offline verification is possible when the Sigstore bundle and pinned trusted
  root are materialized locally.
- Connection registry bodies remain restricted even though they contain no
  secret values.
- Live conformance remains `UNVERIFIED` without an approved environment; the
  suite cannot manufacture production readiness.

## Alternatives rejected

- Sign each file: excessive ceremony and fragmented signer identity.
- Sign tar/zip: adds traversal and decompression-bomb risks.
- Verify inside Airflow parsing: violates parse-side-effect and latency budgets.
- Reuse route-specific service as catalog policy: wrong dependency and errors.
- Arbitrary conformance plugins: unbounded execution and ambiguous PASS.
- Local HMAC for production identity: shared-secret evidence is not publisher
  provenance.

## References

- [Feature design](../feature-design-signed-catalog-extension-conformance-v1.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0014: Route attestation production trust](0014-route-attestation-production-trust.md)
- [ADR 0016: Content-pinned declarative recipes](0016-content-pinned-declarative-recipes.md)
- [Sigstore blob signing](https://docs.sigstore.dev/cosign/signing/signing_with_blobs/)
- [Sigstore verification](https://docs.sigstore.dev/cosign/verifying/verify/)
