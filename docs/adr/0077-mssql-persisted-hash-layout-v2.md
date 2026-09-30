# ADR 0077: MSSQL persisted-hash stages use a versioned physical identity

- Status: Accepted
- Date: 2026-09-28
- Scope: optional ClickHouse to MSSQL native layout v2
- Related: [ADR 0072](0072-mssql-native-writer-proof-capabilities.md),
  [ADR 0076](0076-mssql-sqlclient-companion-boundary.md), and the
  [approved feature specification](../feature-specs/mssql-sqlclient-target-local-verification-v2.md)

## Context

Target-local verification originally recomputed the canonical multiset digest
from every business column after each bulk write. That preserves proof authority,
but repeated wide scans can dominate the transport. Storing a canonical row hash
with each raw-stage row makes later aggregation proportional to a fixed-width
proof column.

The extra column changes physical schema, writer mappings, stage identity,
recovery compatibility, and evidence. Treating it as an internal optimization
would allow an invocation to resume against different bytes or mistake a stale
hash for proof after a row mutation.

## Decision

Persisted hashes are an explicit `layout_version: 2` selection available only
with `import_backend: mssql_sqlclient` and `verification_backend: target_local`.
Omission remains layout v1. BCP and existing journals retain their previous
physical layout and identity.

Layout v2 adds a non-null fixed-width SHA-256 column to every owned raw stage.
The companion computes it from the same versioned canonical business-row bytes
used by the verifier. The writable-column order, canonicalization algorithm,
companion artifact, protocol capability, wire layout, and layout version are
bound into the verification identity and durable recovery authority. Recovery
may resume only when every bound value matches exactly; it never upgrades or
downgrades an existing invocation.

The hash is a verification accelerator, not publication authority. Target-local
code still verifies row count and aggregate digest under the existing
application-lock barrier. A rowversion mutation watermark proves that the raw
stage did not change between verified observations. Missing rowversion support,
unsupported types, hash/layout drift, or a changed watermark fails closed before
preparation or publication. A matching probabilistic multiset digest is reported
as such and is not described as mathematically exact equality.

Protocol v1 remains the package-discovery baseline. Protocol v2 is selected only
for layout-v2 requests and carries the additional persisted-hash fields through
the same closed, length-bounded anonymous-pipe exchange. Unknown fields,
versions, layouts, and protocols are rejected; there is no fallback after source
I/O begins.

Successful publication preserves the existing order: target-local verification,
prepared verification, durable publication receipt, evidence, checkpoint,
exact-stage cleanup, and custody release. Lost acknowledgement retains custody
and the exact owned stage. Source-free recovery uses the sealed v2 authority and
never recomputes scope from object names or stage contents.

Capability metadata reports package availability and that certification evidence
is required. Route certification remains bound to an exact source commit,
runner, environment, fixture, and retained receipt; the package never exposes a
timeless `certified=true` claim.

## Consequences

- Operators opt in to a faster repeated-verification layout without changing
  the default BCP or SqlClient v1 behavior.
- Layout selection becomes durable public identity. Changing it starts a new
  invocation after prior custody is settled.
- Raw stages consume one extra fixed-width column and companion CPU for hashing.
- Hash correctness, mutation detection, crash recovery, and v1/v2 incompatibility
  require differential, contract, and live coverage.
- Future hash algorithms or physical proof columns require another layout
  version and explicit migration/recovery semantics.
