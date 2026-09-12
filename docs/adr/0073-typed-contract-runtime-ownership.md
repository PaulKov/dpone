# ADR 0073: Typed contracts and runtime own their validation boundaries

Status: Accepted for the private migration candidate; public integration remains gated.

Date: 2026-09-10

## Context

The retained PostgreSQL-to-MSSQL contracts contain stable wire models, scalar
framing, request projections, physical inventories and transaction execution.
Some implementation modules mixed these responsibilities or made lower-level
code import its aggregate consumers. The migration must preserve complete source
associations and existing behavior while respecting canonical dependency rules.

## Decision

- The scalar codec owns tagged framing and scalar validation. Request and receipt
  models own aggregate reconstruction through their existing classmethods. The
  codec imports neither aggregate. Decoding through a subclass continues to
  produce the same concrete model class as before.
- Receipt headers and Batch/XMin bodies share the immutable receipt owner.
  Construction from an admitted request and agreement checks belong to the
  separate receipt projection owner.
- Physical inventory owners validate their own collection membership, ordering
  and uniqueness. The descriptor aggregate retains cross-inventory agreement and
  its original validation precedence.
- Admitted query and mutation execution share one transaction execution owner
  and the existing locked UUID-to-handle/session binder. Quality queries fetch
  at most two rows.
- The prepared source boundary owns its read-only successful-completion
  assertion. Source schema authority owns selected-document decoding, catalog
  column derivation and unique canonical type-decision selection. Runtime owns
  catalog I/O, cancellation, snapshot lifetime and compatibility DTO assembly.
  Scalar renderers live in the canonical adapter package; existing runtime
  imports remain explicit aliases used by projection lookup.

Additional owner boundaries keep unbaselined module warning debt out of the
candidate. Binding validation owns post-derivation mismatch classification;
partial-authority decode recovery remains in the pack reader. Route bootstrap
owns optional correctness runtime construction with an explicitly supplied
factory. The hydrator retains activation resolution and early dependency checks.
Receipt projection owns ordered declared stage bytes and digests, without adding
a virtual model property or recomputing material.

Resource and lock-step discriminators live with their existing inventory
owners. Resource definitions precede access matrices; lock definitions precede
lock-rank constants. The lock owner depends on resource kinds and cardinalities,
never the reverse. Shared discriminators retain their existing enum owner.
Signer identity and signature intent values live with the signed permission
aggregate. Their unpublished standalone owner is retired; consumers import the
single defining class directly. The four-owner binding inventory is retained
by current evidence layout 4. Its separate protocol domain v4 binds all 15
producer, schema, test, recipe and fixture dependencies to one exact public
integration base. The original historical layouts 1, 2 and 3 remain unchanged
in the private archive. Public legacy-shape tests use newly authored synthetic
records with different identities; they do not establish historical byte
equivalence or current certification. Incompatible layouts and authority
versions are rejected.

The public RED and candidate task templates remain explicitly unbound. Zero
counts and empty arrays are placeholders, never measured outcomes. The producer
rejects them until a genuine public chain is supplied; future execution also
requires a fresh review of the task ownership. Filling hash fields alone grants
no execution authority.

Type-decision values and exact scalar derivation share the existing type
derivation owner. Policy coverage, ordering and catalog resolution remain in
type authority. Receipt observations are immutable receipt facts; projection
turns an observation into a body before the adapter builds the receipt. The
contracts layer does not import the adapter or its transaction port.

The existing PostgreSQL COPY stream module owns both file and lazy-stream
transport strategies. Their classes retain separate lifecycles: moving their
definitions does not buffer the lazy stream or combine cleanup paths. The
unpublished file-only module is retired. Released stream exports, short-write
handling, error identifiers and optional-driver import timing remain unchanged.

LoadConfig normalization owns endpoint identity and partition selection in the
existing builder-support module. Parse-trace owns its protocol and ordered
load/runtime field recording functions. Published endpoint/partition modules
and the existing builder-support trace names retain identity-preserving
reexports; the builder keeps its established injection sites. Parse-trace uses
only standard-library dependencies, and the separate scope helper keeps its
configuration-independent import behavior. Existing pickle lookup and trace
ordering remain covered by compatibility tests. Defining-module introspection
and arbitrary patches to former private globals are not promised unchanged.

## Compatibility and consequences

Constructor fields, canonical frames and golden digests remain unchanged. Moved
aggregate decoder operations preserve their concrete constructors, field order,
mode branches and exception order. Issuer model-error translation retains its
existing direct reason mapping separately from canonical constructor mapping.
No completion assertion commits, mutates a receipt or retries cleanup.

The retired V3/Binding standalone owners and migrated aggregate codec functions
were unpublished implementation paths. Their retained consumers and source
associations are mapped to the new owners. The published LoadConfig endpoint and
partition modules retain their compatibility reexports. Existing runtime renderer imports and monkeypatch
behavior remain available. No concrete provider is activated and no historical
backend implementation is claimed to have been transferred by these changes.

This decision grants no module-size, coupling or privacy exception. Architecture
fitness, local tests and source/privacy accounting retain independent outcomes.
Historical source ADR exceptions remain historical and do not waive current gates.

## Validation and references

Focused tests cover malformed canonical frames, concrete subclass decoding,
Batch/XMin goldens, inventory error precedence, concurrent handle binding,
bounded query reads, cancellation and snapshot cleanup. The integrator runs local
Docker checks and binds their results to immutable candidate snapshots privately.

- [Developer implementation map](../developer-postgres-mssql-r1-v3-provider-implementation.md)
- [Internal Binding ownership](0072-internal-binding-ownership.md)
- [Architecture](../architecture.md)
- [Engineering standards](../engineering-standards.md)
