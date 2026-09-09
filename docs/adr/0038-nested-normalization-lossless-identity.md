# ADR 0038: Nested normalization fails before mutation on lossy or ambiguous identity

- Status: Accepted
- Date: 2026-07-29
- Owners: dpone runtime and release maintainers
- Related specification:
  [v0.73.28 release-blocker remediation](../feature-design-release-blocker-remediation-v07328.md)

## Context

Nested normalization turns one source object into a root table and zero or more
child tables. The generated table name, hierarchy identifiers and configured
child unique keys are durable storage and replay identity.

The pre-0.73.28 implementation had five ambiguous states:

1. an empty list or mapping emitted no child row and was removed from its
   parent, so reverse reconstruction could not distinguish empty from missing;
2. different source paths such as `a-b` and `a_b` could resolve to one table and
   be silently merged;
3. `child_quality` duplicate/orphan policies were parseable and documented but
   were not enforced by the runtime path before target and state mutation.
4. spill retries reused flat output files in append mode, so reported counts
   could describe one attempt while the file contained rows from older attempts;
5. TSV headers were written before the complete schema was known and TSV
   readback coerced identity-bearing values to strings.
6. lineage hashing serialized non-JSON Python values with `default=str`, so a
   `date` and an equal-looking string could receive the same durable row ID.

These states can cause silent loss, duplication or non-replayable structure.
They cannot be repaired reliably after a target package commits.

## Decision

Nested normalization uses the following fail-closed boundary:

- Empty list and mapping values remain on the parent normalized row as empty
  sentinels. They do not create fake child rows or imply an empty-table schema.
- Non-empty nested values continue to split into child tables.
- Existing ASCII physical table spelling remains stable in this patch release.
  Non-ASCII generated physical identifiers fail with guidance to supply a safe
  explicit mapping.
- A separate lowercase ASCII collision key represents destination identity. A
  per-normalization registry binds it to exactly one physical table and source
  path. A second distinct path or case-only spelling resolving to the same key
  is a configuration/data error.
- Hierarchy integrity is checked with dpone row and parent-row identifiers.
  Configured child unique keys are checked per generated child table.
- `fail` aborts before child-snapshot staging, target staging, audit `STAGED`,
  source-state advancement or success evidence.
- `warn` is an explicit non-passing quality observation and `skip` is recorded
  as not evaluated. Neither is converted to `PASS`.
- Certification reconstructs and compares the complete representative payload
  and derives schema/native status from executed behavior.
- Spill publication uses a unique hidden staging directory on the configured
  output filesystem. Only a fully normalized and rendered directory is renamed
  atomically to a unique `.dpone-spill-generation-*` directory.
- Successful generations are immutable. A retry creates a new generation;
  failed staging and failed load attempts remove only their attempt-local
  files. Prior generations, legacy flat files, and unrelated user files remain
  untouched.
- `SpilledNormalizationResult.files` is the authoritative native-path mapping.
  The additive `semantic_files` mapping exposes internal JSONL with explicit
  scalar tags for quality, snapshots, and portable fallback. Every
  operator-facing spill file and its hidden semantic sidecar share one
  generation lifecycle.
- Child quality evaluates the staged semantic sidecars before atomic
  publication. Sink-native handoff remains disabled for a route until its exact
  writer/loader wire contract has retained end-to-end evidence; uncertified
  routes use typed streaming.
- TSV rendering waits for the final schema. Incompatible heterogeneous types,
  unsafe table path components, delimiter-bearing values, and literal `\N`
  strings fail before publication.
- Unkeyed child, raw, and quarantine identity includes its parent/root identity,
  and spill processing preserves the source-global root row index.
- Lineage hashing keeps the historical digest for JSON-native values and adds a
  deterministic type marker only when Python types would otherwise be erased.
  Dates, datetimes, bytes, tuples and equal-looking strings therefore remain
  distinct without rewriting existing integer/string business-key identities.
- Scalar bytes columns use the `bytes` logical type and hexadecimal native
  rendering. Bytes embedded in an unsplit JSON object fail before publication
  because ordinary JSON cannot represent them unambiguously.
- Nested package loading requires explicit stage/finalize/abort sink ports.
  ClickHouse currently satisfies that contract; MSSQL and Postgres are
  `UNVERIFIED` and fail closed before target mutation.

The root table remains under the user's explicit target identity. Canonical
normalization applies to generated child path components, including explicit
split-path table mappings when they enter the generated-table namespace.

## Consequences

### Positive

- Empty and missing values remain distinguishable.
- Ambiguous target identity is rejected before irreversible mutation.
- Duplicate/orphan policy has the same meaning in manifests, runtime and
  certification.
- Replay observes the same table and row identity for existing ASCII inputs.
- Failed retries cannot publish partial files or contaminate an earlier result.
- Memory and spill consumers evaluate the same typed hierarchy identity.
- Existing JSON-native lineage IDs remain byte-for-byte compatible while
  non-JSON type collisions are removed.

### Negative

- Payloads that previously relied on silent collision merging, case-only target
  distinctions or Unicode generated identifiers now fail and need distinct
  source keys or explicit safe table mappings.
- Empty containers remain JSON-like values on the parent even when
  `preserve_nested_json` is false; this is the minimal lossless sentinel.
- Runtime performs an additional bounded in-memory quality scan before staging.
- Returned spill paths are attempt-local rather than flat deterministic paths.
  Successful generations accumulate until the caller/operator removes them
  after all lazy or native consumers finish.

### Compatibility

Non-colliding payloads and valid child keys keep their existing table layout and
public options. The new failures are deliberate correctness enforcement, not a
breaking schema migration. `include_empty_tables` remains reserved and false;
this ADR does not introduce zero-row schema materialization.

Callers that previously constructed `<output_dir>/<table>.<suffix>` must instead
use `SpilledNormalizationResult.files[table]`. Existing flat files are not
migrated, read, overwritten, or deleted. Retention automation may remove a
completed generation only after the result is no longer in use; dpone owns
failed staging and failed-attempt generation cleanup in this patch.

## Rejected alternatives

- **Drop empty containers:** rejected because missing and empty become
  indistinguishable.
- **Emit a synthetic child row:** rejected because it invents source data and
  hierarchy identity.
- **Lowercase every physical table:** rejected because physical table name
  participates in child-row identity and requires a versioned migration, not a
  patch-release rewrite.
- **Append hashes automatically to collisions:** rejected because it silently
  changes durable target names and creates an implicit migration policy.
- **Validate after target staging:** rejected because a failure could leave
  partial target or snapshot state.

## Verification

- Focused normalization and reverse-readback tests cover empty lists/mappings.
- Collision tests cover punctuation, underscore and case; non-ASCII physical
  identifiers fail with migration guidance.
- ETL package tests prove duplicate/orphan `fail` occurs before staged mutation
  and state advancement.
- Retry/concurrency tests prove distinct immutable generations, failed-staging
  cleanup, byte-identical prior/unowned files, and no empty generation.
- TSV tests prove one final-schema header, typed semantic parity, fail-closed
  incompatible types and reserved-null handling.
- Exact-commit Docker/live certification retains hierarchy, reconciliation,
  replay and failure-ordering evidence.
