# ADR 0036: Runtime image certification v2 requires Cosign

## Status

Accepted.

## Context

`dpone.runtime-image-certification.v1` is durable release evidence with exactly
nine digest-bound checks. Production Airflow deployment attestation adds a
Cosign executable to the runtime trust boundary. Adding a tenth required check
to `v1` would make historical valid evidence fail its original schema and
would silently redefine a public contract.

Historical evidence must remain readable for audit and completed-journal
recovery. It must not authorize a new alias mutation after the stronger
runtime image requirement is introduced.

## Decision

Freeze `dpone.runtime-image-certification.v1` with its original nine checks.
New releases emit `dpone.runtime-image-certification.v2` with the same
identity, provenance, SBOM, and context fields plus one required `cosign`
check.

Validation accepts both versions:

- `v1` is historical evidence;
- `v2` is the only schema that authorizes a new GHCR alias mutation;
- an already complete, exact certification-SHA-bound journal may be replayed
  without registry I/O;
- an incomplete `v1` journal cannot resume and requires recertification as
  `v2`;
- no synthetic `v1` to `v2` conversion exists.

The `cosign` receipt is created only after `cosign version --json` succeeds
inside the pushed digest reference. Missing, skipped, failed, duplicate, or
digest-mismatched checks prevent certification.

The existing `idempotency_key` remains the candidate identity. The versioned
evidence identity is the pair of schema discriminator and idempotency key, and
all recovery continues to bind the complete certification SHA-256.

## Consequences

- Historical evidence and its JSON Schema remain stable.
- New image promotion cannot bypass the Cosign requirement with a valid old
  receipt.
- Operators can inspect old evidence and recover an already committed output,
  but must produce `v2` before any new alias write.
- The producer, promotion validator, workflow, and schema files change
  together.
- Runtime image `v2` does not weaken digest pinning or existing provenance and
  SBOM verification.

## References

- [ADR 0026: digest-first runtime image promotion](0026-digest-first-runtime-image-promotion.md)
- [Runtime Docker image](../runtime-image.md)
- [Production Airflow artifact trust](../airflow-artifact-trust.md)
- [Feature specification](../feature-design-airflow-production-artifact-attestation-v07327.md)
