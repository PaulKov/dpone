# DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH

**Audience:** CI maintainers and platform engineers.

The candidate's current raw-byte digest or semantic fingerprint differs from
the identity approved by protected CI policy.

Do not retry promotion with identities recomputed from the changed pathname.
Discard the candidate, run `dpone workload index` again, regenerate
`dpone workload impact --current`, and obtain fresh approval. Candidate
replacement after approval is intentionally fail-closed.

[Domain-first discovery and CI](../domain-first-discovery-ci.md) ·
[Domain-first error overview](index.md)
