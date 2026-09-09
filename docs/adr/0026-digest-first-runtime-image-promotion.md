# ADR 0026: Digest-first runtime image certification and fail-closed GHCR promotion

## Status

Accepted.

## Context

The runtime-image workflow currently pushes version, SHA, and `latest` aliases
before digest-pinned smoke, SBOM generation, and attestation verification are
complete. A failed post-push check can leave an uncertified public alias.
Manifest lookup also collapses absence, authorization failure, throttling, and
registry outage into one result.

GHCR and the OCI Distribution API do not provide a portable atomic
create-if-absent operation, permanent immutable-tag policy, or multi-tag
transaction. Workflow ordering can reduce risk but cannot honestly claim those
registry guarantees.

## Decision

Use two explicit commit boundaries:

1. **Certification:** build and push only a digest-addressed candidate, test
   `IMAGE@DIGEST`, generate and verify provenance and SBOM attestations, then
   write `dpone.runtime-image-certification.v1` with PASS last.
2. **Promotion:** a separately permissioned, globally serialized job consumes
   that exact certification and reconciles version, SHA, and `latest` aliases.

Registry lookup has three results:

- authenticated HTTP 200 with a canonical digest is `PRESENT`;
- authenticated HTTP 404 is `ABSENT`;
- every other response or transport/parsing failure is `ERROR`.

Fixed aliases use create-or-compare semantics. The expected digest is a no-op;
another digest blocks. `latest` advances only to a greater certified stable
SemVer, remains on the same digest, or preserves a newer version. It never
regresses.

Every alias write is followed by an authenticated digest postcondition.
Promotion evidence records each non-atomic transition. A partial alias state is
workflow failure and can be repaired idempotently with the same certification.
No automated workflow overwrites a conflicting fixed alias or deletes a failed
candidate.

Build and promotion permissions remain separate. Public deployment contracts
continue to require image digests.

Starting with dpone `0.73.28`, the producer writes
`dpone.runtime-image-certification.v2`, which adds a required Cosign binary
check for deployment-scoped Airflow attestation. Promotion continues to accept
historical `v1` evidence with its original nine checks; its schema and meaning
are frozen. New releases use `v2` and ten checks. This is an additive evidence
version, not an in-place expansion of `v1`.

## Consequences

- Smoke or attestation failure cannot publish release aliases.
- Registry/authentication failures fail closed rather than masquerading as
  absence.
- Same-digest reruns repair interrupted promotion safely.
- Different-digest fixed aliases become incident blockers.
- `latest` is monotonic by certified SemVer, not completion time.
- Version and SHA aliases are workflow-governed create-or-compare references,
  not registry-enforced immutable objects.
- Promotion is not atomic across aliases; evidence and recovery expose this
  limitation.
- External writers can still mutate GHCR tags. Stronger immutability requires
  external registry policy and drift monitoring.
- Replacing the wall-clock image label removes guaranteed rerun drift, but full
  reproducible builds remain out of scope until all build inputs are pinned.

## Related

- `docs/feature-design-runtime-image-digest-first-promotion-v0732.md`
- `docs/runtime-image.md`
- `docs/release.md`
- `docs/adr/0036-runtime-image-certification-v2.md`
- `.github/workflows/runtime-image.yml`
- `tools/agent_policy/runtime_image_promotion.py`
