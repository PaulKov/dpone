# Nonproduction qualification plan originals

This reference is for developers implementing the approved synthetic
[nonproduction authority](nonproduction-composition-authority.md). The two
internal plan originals describe the fixtures and work items named by a
qualification grant. Their codecs compare exact bytes, declared effects and
dependencies. They do not authenticate a grant, observe a database, seed data,
reserve a budget or permit execution.

The plans add no external signed authority family. Their hashes are already
mandatory subjects of the existing qualification grant. Production/native-v2
documents, physical guards, owner and operation identities remain unchanged.
Protected acquisition, actual source bounds, source sealing and the complete
native/ordinary campaign remain required before scoped execution is available.

Use [finite fixture inputs and recipes](nonproduction-fixture-inputs.md) for the
separate expected-input original and canonical inventories. Its selected-route
writer comparison is narrower than the full plan grammar: an otherwise valid
plan can contain a source writer that the fixed recipe cannot explain. The
[scoped execution owner](composition-scoped-execution-originals.md) preserves the
full qualified scope during a future handoff without widening this plan grammar.

## Original documents

All fields below are required; unknown fields and action-field mixtures reject.
Every original uses exact canonical UTF-8 JSON, rejects duplicate keys and
nonfinite numbers, and is bounded at 1 MiB. Decoders require the expected digest;
they do not normalize incoming bytes or arrays to make them acceptable.

| Value | Exact fields |
|---|---|
| Fixture plan | `schema`, `scope_sha256`, `objects`, `fixtures` |
| Declared object | `object_id`, `connector`, `service_id`, `physical_subject_sha256`, `subject_sha256`, `object_kind`, `qualified_name` |
| Fixture | `fixture_id`, `profile`, `recipe_original`, `parameters`, `bindings` |
| Qualification plan | `schema`, `scope_sha256`, `fixture_plan_sha256`, `work_items` |
| Common work item | `work_item_id`, `action`, `fixture_id`, `predecessor_ids`, `effects`, `limits` |
| Effect | `object_id`, `access`, `purpose` |
| Original descriptor | `path`, `sha256`, `size_bytes` |
| Source-bound obligation | `source_object_id`, `accounting_profile`, `input_original`, `projection_original`, `derivation_original` |

The schemas are `dpone.nonproduction-fixture-plan.v1` and
`dpone.nonproduction-qualification-plan.v1`. A work-item digest covers
`dpone.nonproduction-qualification-work-item.v1` and every common and
action-specific field. No item contains its own hash or a parent plan/grant hash.

The identity order is:

```mermaid
flowchart LR
    U[Upstream recipe, source, toolchain and intent originals] --> S[Scope]
    S --> F[Fixture plan]
    F --> Q[Qualification plan]
    Q --> G[Qualification grant]
    G --> O[Owner and operation originals]
    O --> E[Future actual observations and evidence]
```

Scope fixture/generator hashes identify upstream originals, never either new
scope-containing plan. Future receipts, runner observations and source seals
cannot be inputs to the pre-grant plans. Staging names must already be explicit;
deriving them from a descendant hash would create a circular identity.

## Objects, descriptors and bounds

Logical IDs retain exact Unicode text, including slash and backslash, with the
existing 512-character limit. Object, fixture and work-item arrays are sorted by
their IDs; predecessor and retained-source arrays by ID; effects by
`(object_id, access, purpose)`. Duplicates or unsorted input reject.

An object is a table on PostgreSQL, MSSQL or ClickHouse, or an enum/sequence on
PostgreSQL. Its name contains separate database/schema/object parts for PG and
MSSQL, or database/object parts for ClickHouse. Each part matches
`[A-Za-z_][A-Za-z0-9_]*`, with respective limits of 63, 128 and 128 characters.
Names preserve case. SQL fragments, quoting, wildcards, ambiguous dotted strings
and search-path-relative names reject. Actual backend alias equivalence still
requires independent observation.

An effect identity is the complete
`(connector, service_id, physical_subject_sha256, subject_sha256)` tuple. Object
labels cannot split that identity. Within a domain, duplicate subjects or exact
kind/name declarations reject. The subject digest is documentary: the codec
does not compute a new physical identity formula or manufacture an observation.

Descriptors name bounded metadata originals, not raw export payloads. Their
size is a strict integer from 1 through 1 MiB. Paths are at most 512 characters
and contain slash-separated `[A-Za-z0-9_.-]+` segments, excluding `.` and `..`.
Absolute paths, backslashes, empty segments, traversal and URLs reject. A valid
descriptor has not yet been independently downloaded or checked against source.

Plans permit at most 64 fixtures, 64 total work items and 256 physical
participants. Declared objects, total effect edges and the complete participant
read/write subject count each have an independent 8,192 limit. The byte limit
also applies; cardinality ceilings do not promise that every maximum-sized
combination fits in one original. Every object has an item effect, every fixture
has work, and every reference resolves. A terminal DAG node need not be another
node's predecessor.

## Closed fixture profiles

| Profile | Exact parameters | Bindings |
|---|---|---|
| `mssql_clickhouse_bcp_wide_v1` | `row_count`: strict integer, 0–100,000 | `source`: MSSQL table; `target`: ClickHouse table |
| `postgres_mssql_wide_v1` | `watermark_key_3`: strict boolean | `source`: PG table; `target`: MSSQL table; `enum`: PG enum; `c_smallserial_sequence`, `c_serial_sequence`, `c_identity_sequence`, `c_bigserial_sequence`: distinct PG sequences |

Bindings contain exact object IDs. PG helpers must share their source's service,
physical domain and exact database/schema name parts. Two fixtures cannot bind
the same physical source identity; these profiles have no shared-source
generation variant.

BCP uses the existing 202-column recipe in
`tools/mssql_clickhouse_bcp_native_fixtures.py`. Its explicit zero-row case is an
empty fixture, never a clipped export. PG uses the existing 128-column recipe in
`tests/integration/postgres/postgres_mssql_wide_fixtures.py`, its two initial rows
and optional fixed watermark key 3. A recipe descriptor must name the matching
path and retain its exact source digest/size. The contracts neither import nor
copy the recipe implementation. Actual seed-generator and derivation originals
must also be reopened before guarded execution.

Arbitrary row IDs, column counts, providers, inline SQL and extra parameters
reject. Schema/state infrastructure must be preprovisioned. The legacy setup
functions, including CASCADE and uncontrolled helper creation, are not guarded
executors. The native dbt profile remains unavailable until its finite source
bound is independently established; supporting these two profiles does not
complete the required native dbt and generated/ordinary transfer campaign.

## Work items and declared order

| Action | Additional fields | Required effects |
|---|---|---|
| `fixture_seed` | `seed_step` | Source-write; PG initial also enum-read/write and four sequence writes; PG watermark also enum-read |
| `route_qualification` | `route`, `reviewed_case_original`, `source_bound` | Its own source-read and target-write |
| `source_seal` | `retained_source_ids`, `seal_recipe_original` | Its fixture's source-read; all participant effects are read-only |

`retained_source_ids` is exactly the singleton containing the selected fixture's
source binding. Extra retained-source IDs or a substituted source reject.

Effect access is `read` or `write`; purpose is `fixture`, `source`, `target`,
`helper`, `staging` or `state`. Purpose cannot substitute for access. Extra
declared effects participate in both full scope and selected guard comparison.
Known minimum effects do not prove complete actual producer effects.

PG inserts explicitly supply the four serial/identity values. The watermark
step does not receive a fabricated `nextval` or sequence-write requirement.
Initial sequence creation is accounted for separately.

Each fixture has exactly one initial seed. PG permits at most one
`watermark_3` item, only when enabled, with its initial seed as a transitive
predecessor. The flag permits that step; it does not mean it exists or ran.
Every route and seal depends transitively on its fixture's initial seed.

All declared writers of a retained source must be pairwise ordered in the DAG,
including writers assigned another fixture or purpose. Each route reading that
source must be ordered relative to every such writer; it cannot write its own
selected source. Read routes may run between consecutive mutations in the
declared order. Both `initial → route → watermark_3 → route` and
`initial → watermark_3 → final route` are valid; an unordered read/watermark
pair is ambiguous and rejects. Actual input generation is verified separately.

A seal follows every declared writer of its source. No seal may precede a seed
or route through any DAG path, even across fixtures: the owner cannot reopen
mutation after entering SEALING. Unordered ordinary work is closed by the later
global lifecycle barrier. Seals are optional structurally; omission establishes
no sealing, ownership-transfer or complete-campaign result. Cycles, self-edges
and missing predecessors always reject.

## Sources, limits and complete comparison

A route exactly matches a six-dimensional route in the signed scope and the
selected fixture's source/sink pair. The union of route items equals the full
signed route set. Its source-bound object is that fixture's source. Accounting
is exactly `mssql_bcp_native_file` or `postgres_copy_payload`, respectively.
There is no transport-name fallback or seal/export accounting substitution.

The obligation pins input, projection and derivation metadata. It contains no
claimed computed row/byte bound. Actual admission must independently derive a
finite immutable complete BCP-native file bound, including framing, or a typed
COPY payload bound before gzip. HTTP body bytes remain separate. Statistics,
post-read counts, truncation and TOP/LIMIT cannot establish that bound.

All items, including seeds and seals, count toward the original grant and every
item's lower `max_workloads`. The entire grant validity window must fit every
item's lower validity limit. Other structural ceilings use the grant/item
minimum. The pair receives no policy original, clock or observed usage; it
cannot infer current policy from its digest or reserve capacity.

The complete effect union must equal every signed scope participant, including
reads, writes, overlap and helper/staging/state objects. All three connectors
and both route families remain mandatory. Selecting one operation never narrows
this complete comparison.

Declared retention comes from fixture source bindings. A retained domain with
writes projects `mutation_and_retained_source`; without writes,
`retained_source`; other writable domains project `mutation`. An arbitrary read
does not imply retention. A non-source read-only domain has no supported owner
role and rejects. Compare the entire owner's role/service/domain/read/write
projection without inventing an `observation_sha256`. The original operation
still binds that exact supplied owner, including its opaque observation hashes.

Selection also compares exact grant/run/consumption/plan/scope/environment and
campaign subjects, the complete work-item hash/action and the exact selected
guard set. Current physical identity, epochs, invocation truth and ownership
state remain protected observations; positive documentary epochs prove none of
them. Campaign counters retain their existing environment/campaign/qualification
partition and exact item membership across replacement grants/plans/runs.

## Python interfaces and failure handling

| Interface | Result |
|---|---|
| `NonproductionFixturePlan` in `dpone.contracts.nonproduction_fixture_plan` | `to_dict`, `to_bytes`, `fixture_plan_sha256`, `from_bytes(raw, expected_sha256=...)` |
| `NonproductionQualificationPlan` in `dpone.contracts.nonproduction_qualification_plan` | Corresponding original codec and `qualification_plan_sha256` |
| `require_qualification_plan_originals(fixture_bytes, qualification_bytes, original_grant=...)` in `dpone.contracts.nonproduction_plan_pair` | Immutable documentary pair, compared against the exact qualification grant |
| `pair.require_operation(operation, owner=...)` | Exact selected immutable item after complete pair/owner/operation comparison |

The returned values have no `authorized` or executor-permit flag. Parsing does
not read files, call providers, authenticate, mutate counters or change state.
Original acquisition and actual execution use their separate protected paths.

New failures use value-free `NonproductionAuthorityError` reasons such as
`plan_bindings`, `plan_graph`, `plan_scope`, `plan_limits`, `plan_owner` and
`plan_operation`. Existing original owner/operation failures retain
`CompositionAdmissionError`. Diagnose the original producer's bindings, graph
or signed subjects; regenerate the affected upstream originals and obtain a
new grant when hashes change. Do not normalize a rejected signed document or
rebind a consumed run to new plans. Re-reading identical originals is only a
documentary comparison and supplies no retry or execution permission.

The required focused contract tests cover canonical identities, strict shapes and bounds,
profile effects, source-generation order, full scope/roles and selected guards.
They establish no live route result. Continue with the
[shared physical ownership algorithm](composition-shared-ownership.md#algorithm-and-failure-boundaries)
for the protected acquisition, observation, closure and transfer requirements.
