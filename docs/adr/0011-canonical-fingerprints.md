# ADR 0011: Canonical fingerprints

## Status

Accepted.

## Context

Release and deployment identities must be deterministic across independent builds while excluding volatile metadata.

## Decision

Compute fingerprints from canonical JSON, preferably RFC 8785 JSON Canonicalization Scheme semantics. Exclude fields such as IDs, timestamps, source commit, build ID, labels, attestations, and signatures from the corresponding identity payloads.

`source_commit` is provenance, not release identity.

Serialized release, deployment, binding, registry, and runtime identities use
one canonical lowercase `sha256:<64 hex>` representation. Uppercase hexadecimal
is rejected before filesystem or control-state mutation so case-insensitive and
case-sensitive filesystems cannot resolve the same identity differently.
Artifact checksum metadata may be compared case-insensitively because it is not
used as a cache path or CAS identity.

Workload-pack semantic identity is derived from canonical JSON by the pure
`dpone-airflow-pack` identity contract so the lightweight provider and the root
build/runtime package share one implementation. The identity payload binds the
complete pack by default, preserves array order and excludes only the
self-referential `pack_fingerprint` plus explicitly named advisory report
metadata. Strict consumers recompute the identity; comparing two copied claims
is not verification. The identity algorithm is declared by
`pack_identity.schema: dpone.airflow-pack-identity.v1`.

An optional top-level `promotion` release variant is also identity-bearing.
It distinguishes releases that have the same artifact inventory but different
delivery semantics, such as compact packs promoted onto the verified
`RuntimeConnectionContext` path. The v1 root remains open for backward
compatibility: older readers ignore an unknown top-level variant, while current
readers validate the explicitly signalled dpone compact namespace before
projection and activation. Unrelated legacy `promotion` extension shapes remain
valid under the open v1 contract.

## Consequences

- Two builds of identical artifacts and the same release variant produce the
  same `release_id`.
- Changing an identity-bearing release variant changes `release_id`.
- One byte changed in a pack changes `release_id`.
- Environment changes affect `deployment_id`, not `release_id`.
- Any execution-bearing semantic pack mutation changes `pack_fingerprint`.
- Legacy packs without the identity schema remain compatibility-only and
  cannot be promoted into strict v2 execution.
