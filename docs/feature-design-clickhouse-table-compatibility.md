# Feature design: self-service ClickHouse table compatibility

- Status: APPROVED
- Implementation: [Native plan under revision](superpowers/plans/2026-09-30-clickhouse-table-compatibility.md) and [path contract](agent-tasks/clickhouse-table-compatibility.yml); structural prerequisite failed, dependent implementation not started
- Decision: [ADR 0080](adr/0080-clickhouse-table-compatibility-policy.md)
- Owner: dpone maintainers
- Issue: settings-aware amendment to PR #249, continued in PR #258
- Target release: unassigned; specification only, not route activation
- Parent: [protected publication design](feature-design-clickhouse-protected-publication.md)
- Historical design baseline inspected: `efa734b67cdf905c8502e5befa5fc686a28bc2e3`, plus the preserved uncommitted candidate-transport work
- Current integration baseline: `71180d081163455cf7ede1a40935eae512479960`; this amendment changes design documents, not runtime behavior

Approval reconciled: 2026-10-01. Source/live research below was verified on 2026-09-30.

Approved implementation amendment: [unpublished-prototype consolidation](feature-design-clickhouse-prototype-consolidation.md).
On 2026-10-01 the maintainer authorized one current execution model for the
unpublished PR #249/#258 prototypes. Published dpone contracts and immutable
historical bytes remain protected; separate old prototype execution engines and
their provisioning defaults need not be preserved. The structural gate still applies.

## Research correction: first-use existing tables

**Status: APPROVED amendment.** On 2026-10-01 the maintainer explicitly approved
admitting manually created, genuinely unmanaged tables through verified current
state, while retaining the original-operation journal requirement for recovery.
This supersedes the earlier pre-creation configuration-epoch requirement, not
operation ownership, historical readers or the required safety checks. The
normative algorithm below incorporates that decision; historical epoch research
remains preserved as research. Dependent production implementation still needs
a reviewed feasible plan and passing structural gate. Approval is not an
implementation or certification claim.

### Evidence and correction

For plain, nonreplicated MergeTree on **24.8.14.39**, the server's
[Context caches one MergeTree defaults object](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Interpreters/Context.cpp#L4542-L4555).
[system.merge_tree_settings reads that object](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/System/StorageSystemMergeTreeSettings.cpp#L35-L46).
Table construction [copies those defaults](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/MergeTree/registerStorageMergeTree.cpp#L632-L633)
and [applies persisted overrides](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/MergeTree/registerStorageMergeTree.cpp#L726-L735).
[ALTER rebuilds settings](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/MergeTree/MergeTreeData.cpp#L3719-L3728)
from [the same cached defaults](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/StorageMergeTree.cpp#L2492-L2495)
and the resulting overrides. This concerns the currently loaded configuration,
not the settings under which every historical part was written.

An owned Docker test created a table outside the epoch producer and retained its
data across restart. With `min_bytes_for_wide_part=0`, inserts before restart
produced Wide parts under the cached row threshold 0 even after config reload;
after restart, the changed threshold 1234 produced Compact parts. Explicit ALTER
to 0 produced Wide, and RESET returned to Compact. The table UUID and original
rows survived. This independently observes table behavior, not only equality
between two metadata queries. The test is
`test_pinned_restart_and_alter_resolve_preexisting_table_settings` in
`tests/integration/test_clickhouse_table_compatibility_live.py`.

The prior negative epoch test establishes only that a historical-journal
provider cannot manufacture CREATE coverage. It does **not** establish that
all safe publication methods or tables need that provider. Its remaining epoch
tests are retained as scoped research, not universal admission requirements.

### Approved algorithm amendment

1. Preserve original-authority ownership checks and the continuously held
   two-name exclusion. An externally created, genuinely unmanaged table is
   eligible for first-use inspection; retained or uncertain dpone ownership is
   not. Missing operation history cannot be replaced with a new store.
2. Require the exact certified plain-MergeTree server profile and complete
   visibility. Read current global defaults and complete persisted table
   overrides, schema, keys, UUID and actual storage topology. Resolve the seven
   managed settings as cached defaults overlaid by persisted overrides using
   the pinned algorithm, not assumed built-in literals or a caller assertion.
3. Bind the observation to the actual endpoint and one non-reconnecting native
   connection for its complete bracketed read. Cross-check metadata and defaults
   before/after; any connection break, mismatch or incomplete read invalidates
   that snapshot. A fresh pre-enrollment snapshot is allowed; an enrolled
   operation follows existing retention and source-free recovery rules.
4. Keep current registry validation and unknown-setting/global-deviation refusal.
   Do not extend this proof to Replicated/Shared/Distributed engines, a different
   server version or another node. Verify actual default-policy disks/volumes;
   the policy name alone does not establish topology.
5. Preserve the resolved target configuration by default, render every managed
   candidate setting explicitly, and independently read back candidate settings
   before source access. Re-observe configuration at seal and pre-send. No target
   ALTER, restart, setting reset or manual SQL copying is required for adoption.
6. Keep the existing conservative equal-managed-settings condition for REPLACE,
   separate physical-part checks, explicit after-states and block/warn semantics.
   Current settings do not prove historical part formats. An unknown shared
   fact still blocks; warn can exclude only an unverified method when another
   method is independently verified.

No new public flag, CLI command or manifest field is introduced. Configuration
identity still uses resolved effective values; observation provenance now names
the certified current-state resolver and captured facts instead of requiring a
pre-creation epoch. The unchanged user journey is one publish call with optional
settings, including for manually pre-created supported tables. Recovery never
reconstructs execution permission from observation evidence.

### Implementation and acceptance impact

Remove the planned deployment-epoch port/provider and bootstrap journal from
the production dependency map. Keep the resolver in the planned descriptor/
catalog/observer owners. Reconcile Task 1 and its exact path contract, then
review the structurally feasible revised plan before executing Tasks 2–7; do
not merely relabel the old epoch tests.
Preserve historical artifacts and existing version readers unless a separate
compatibility change is explicitly approved.

Required acceptance cases are: a manually created table with no dpone CREATE
receipt; default and nondefault overrides; actual reload/restart/ALTER/RESET
behavior with persistent data; independent-process observation; connection and
metadata drift; unsupported engine/version/settings; and unchanged retained
ownership, default block, verified warning alternative and zero-replay recovery.
The production resolver and full route still require exact-commit certification.

This correction removes two proposed modules and five projected cross-layer
edges, but the revised projection still fails the architecture budgets. It is
not permission to weaken those gates or claim the publisher/ODBC route ready.
Market comparison and measurable self-service targets below remain unchanged;
the new evidence corrects dpone's implementation assumption, not a vendor ranking.

## Executive summary

Audience: data engineers configuring a snapshot load, platform engineers owning
the deployment, and maintainers implementing publication and recovery.

The user asks dpone to refresh data, not to copy ClickHouse settings, calculate
fingerprints, or select SQL commands. The default must preserve the target's
supported design, prepare a compatible candidate automatically, and explain
either the publication decision or one actionable reason it cannot proceed.

The current closed parser rejects even the server-rendered
`SETTINGS index_granularity = 8192` added to a table created by dpone. Accepting
only that literal would fix one symptom. Ignoring settings would lose safety.
This design replaces both approaches with a small, versioned, typed policy:

1. Describe the actual table and the provenance of its settings.
2. Resolve the desired target design, preserving existing settings by default.
3. Check compatibility for each publication operation separately.
4. Freeze the decision and expected post-state for recovery.

There is no general ClickHouse parser, user-extensible policy language, tuning
wizard, or interactive approval for an ordinary supported load. This remains
the parent's single-operation Python capability, not a production ODBC route.

Compatibility uncertainty is configurable: `block` by default, or `warn` to
exclude an unverified method and continue through an independently verified
alternative. This is not permission to send unverified mutation SQL.

The maintainer approved this written specification with `approve` on 2026-09-30,
after reviewing commit `5075a1e50ff7d1f3d670423fe196f970ab65e873`, including
the default-block/optional-warn boundary. This amendment replaces the parent's
no-SETTINGS profile, unchanged-selector assumption and new-enrollment storage
version for the new binding; other safety rules stay. Approval is not evidence
of implementation or certification. The revised implementation plan at commit
`9553698e8374df4e4af0c5c2a4e4cc0f4cd9acda` was subsequently separately approved.
Its provenance and structural feasibility gates precede dependent production edits.
The current-state correction was separately approved on 2026-10-01. That decision
does not approve the later hypothetical compact file map or waive the failed
structural prerequisite; the revised execution plan still requires review.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Refresh a supported table | Must understand incidental DDL defaults | One publish call; zero settings copied by hand |
| Platform engineer | Establish safe operation once | Deployment assumptions mixed with per-load decisions | Reviewed deployment profile reused while valid |
| Operator | Understand and recover an interruption | Ambiguous instruction to retry or rebuild state | Exact original operation, retained resources, safe next action |
| Maintainer | Add a supported setting | One-off parser exceptions change recovery semantics | One typed rule, tests and a versioned capability profile |

### Shortest safe path

1. **Discover:** the public ClickHouse overview explains supported topology,
   types and settings, and explicitly distinguishes this staged Python backend
   from stock routes.
2. **Prepare once:** platform owner supplies the existing direct endpoint,
   separate credentials, persistent private authority location, resource limits,
   and reviewed all-writer/configuration inventory. Checks collect actual
   metadata automatically. dpone does not grant itself privileges or claim that
   catalog queries prove the absence of bypass writers.
3. **Configure:** provide the same operation identity, target, candidate and
   typed row/key design required by the parent. Settings are optional. For an
   existing supported table, omission means preserve, not reset to defaults.
   A manually created, genuinely unmanaged table needs no dpone CREATE receipt
   or reconstructed DDL history. A retained or uncertain prior dpone operation
   instead requires its original authority and source-free recovery.
4. **Execute:** call `publish_new` once. Read-only preflight is automatic and
   precedes source access. No separate plan command, hash copying, SQL selection
   or interactive confirmation is required.
5. **Observe:** receive the method, plain-language reason, settings disposition,
   publication state and diagnostic artifact location. Publication COMMITTED is
   not source-checkpoint completion, owner release, or whole-route success.
6. **Diagnose:** a rejection lists the failing field, expected/observed safe
   values, phase, whether the source was opened, and one safe next action.
7. **Recover:** call `recover_existing` with the original operation only; it
   never accepts a source. Retained ownership cannot be cleared by making a new
   store, changing IDs, or passing a force flag.
8. **Operate/upgrade:** retain original stores and readers; new profiles apply
   only to genuinely new enrollments. No recurring refresh claim is made until
   the separate owner-finalization lifecycle is implemented and certified.

One-time deployment work is counted separately from per-operation work. Fewer
manual steps must not mean hidden permission changes or automatic mutation replay.

## Scope

### In scope

- A settings-aware observation profile and canonical descriptor.
- A typed built-in settings policy with explicit provenance and capability checks.
- Automatic inheritance, deterministic planning, optional read-only preview,
  method-specific compatibility, desired post-state and actionable diagnostics.
- Version-separated intent, journal, codec and recovery behavior.
- Local Docker certification and self-service documentation requirements.

### Non-goals and retained constraints

Keep one POSIX host, one direct ClickHouse node, Atomic database, plain MergeTree,
dpone-only mutations, serialized candidate ingress and bounded scalar evidence.
Replication, Distributed/Shared engines, ON CLUSTER, TTL, row policies,
materialized views, projections, secondary indexes, arbitrary expressions,
column defaults/codecs and broader scalar types remain unsupported.

No stock route/manifest/CLI activation, MSSQL range-extraction changes, source
checkpoint advancement, owner release, cleanup, automatic rollback, v1/v2 store
migration, server reconfiguration, publication, merge or release is authorized.
This specification does not erase the existing implementation's quality-gate
failures or certify its unimplemented observer/backend.

## Public contract

### Python API and defaults

The following are proposed additions to the parent's explicit composition API,
not commands or imports already available in a released package:

```text
plan_new(request) -> CompatibilityPlan
publish_new(request, source_factory) -> PublicationRecordV3
recover_existing(operation_id) -> ProtectedPublicationStatus
```

`plan_new` is optional, read-only and source-free. It neither reserves names nor
creates an invocation capability. Publication always recomputes its plan under
the actual bound execution session; a saved preview cannot authorize execution.

The request retains a typed column/key design and adds:

- `design_change`: `preserve` by default; `replace` only for deliberate design
  replacement. This is typed intent, not a force/safety bypass.
- `settings`: an immutable typed mapping of supported overrides, empty by default.
  It accepts no SQL strings, inheritance sentinels, caller hashes or raw DDL.
- `on_unverified_compatibility`: `block` by default, or explicit `warn`.
  The mode is frozen in the enrolled plan and final intent, not a mutable
  recovery flag. Omitting it preserves fail-closed behavior.

`block` stops when compatibility for the otherwise preferred applicable method
cannot be established. `warn` excludes that method and may use an independently
verified alternative without a prompt. For example, unverified REPLACE-specific
part compatibility can select EXCHANGE if its own prerequisites and desired
post-state are all proved. This does not turn `unverified` into `compatible`.

Checks common to safe publication cannot be downgraded: unknown configuration
or setting semantics/provenance, unsupported topology, incomplete required
visibility, ownership/exclusion failure, missing seal, content mismatch and
ambiguous write outcomes block in both modes. Known incompatible methods are
never executed. If no independently verified alternative exists, `warn` also
blocks. Thus this option deliberately does not provide arbitrary SETTINGS
passthrough or blind REPLACE; that would require a different unsafe-operation
contract, not a warning-level change to this protected publisher.

Once configured, neither mode adds a manual per-operation approval. A method
that is not applicable (for example REPLACE for a multi-partition snapshot) is
`not_applicable`, not an unverified check that blocks an otherwise valid plan.
Some method-specific facts are available only after loading. In default `block`
mode, a late unverified result therefore retains the original owner/candidate;
the operation cannot then be rerun with `warn`. A preview cannot promise to
predict every such case. Choose `warn` before the original publish call when
verified alternatives are acceptable; this is a one-time request policy choice,
not a recovery switch.

For an existing target, `preserve` requires requested columns/keys to match its
normalized design. Omitted settings inherit its proven effective settings;
explicit values must be equivalent. Differences return
`design_change_requires_replace` before source access or enrollment.

With explicit `replace`, use the requested columns/keys and overlay supported
setting overrides on the existing settings. Omission still preserves settings;
it does not reset them. An actual design change may select EXCHANGE only after
candidate parity, desired-design and exchange prerequisites pass.

For an absent target, use the requested typed design and certified deployment
defaults plus explicit overrides. An empty target is present, not absent.
No per-operation setting is required in the ordinary path. No user selects
internal descriptor/profile/journal versions or computes digests.

Configuration rejection is a typed preflight result; it is not publication
UNKNOWN. Existing uncertainty exceptions retain `safe_to_retry=False`.
Concrete Python module exports and constructors must be fixed in the reviewed
implementation plan; pseudocode above is not a runnable usage example.

### CLI and manifest/schema

N/A for stock CLI and manifests: their syntax, defaults, exits and runtime
registration remain unchanged. The parent's standalone Python example gains
`plan`, alongside `publish` and source-free `recover`; it is not a new `dpone`
subcommand. It is non-interactive, sends one JSON result to stdout, redacted
diagnostics to stderr, with these outcome semantics: `plan` returns 0 for a valid
preview and 2 for a rejected preview; `publish` and `recover` return 0 only for
COMMITTED, 2 for a pre-enrollment rejection or NOT_PUBLISHED, and 3 for a
retained/unresolved outcome. JSON always distinguishes `plan`, `publish` and
`recover` and historical resolution from fresh observation. NOT_PUBLISHED does
not release ownership or authorize another publish call.

### Artifacts and evidence

New schema families:

- `dpone.clickhouse.table-descriptor.v1`: normalized supported configuration,
  setting provenance, object identity and observation context.
- `dpone.clickhouse.compatibility-plan.v1`: desired design, per-method verdicts,
  reasons and policy/profile identity, with explicit `resolved` and `selected`
  record kinds. Only the selected kind contains the final method/after-state.
- `dpone.clickhouse.observation.v2`: settings-aware evidence producer; existing
  RowBinary and typed multiset byte algorithms do not change.
- `dpone.clickhouse.guarded-publication.v3`: frozen intent and recovery evidence.
- `dpone.clickhouse.authority.v3`: explicit new-enrollment authority storage.

The composition binds descriptor/plan bytes and their digests into the original
authority transaction, request identity and seal as applicable. External JSON
sidecars are redacted projections, never publication authority. Export is UTF-8,
atomic, create-only/no-overwrite with private permissions and bounded sizes.
A failed export cannot turn an acknowledged publication into NOT_PUBLISHED;
report the publication result and diagnostic-export failure separately, with
`diagnostic_path=null` and a redacted `diagnostic_export_error`. Do not report a
nonexistent file as a recovery artifact or alter the publication outcome exit.

Human diagnostics show `phase`, `reason_code`, `message`, `operation_id`,
`source_opened`, `enrollment_state`, `publication_state`, `method`,
`settings_disposition`, `on_unverified_compatibility`, `warnings`,
`owner_retained`, `safe_to_retry`, `next_action`, and the
original recovery locator. Structured field differences are bounded and
redacted. Raw rows, credentials and SQL are not logged. Internal fingerprints
are available to support tools, never required user input.

Each compatibility warning includes stable code
`unverified_method_skipped`, excluded method, unresolved check/reason, selected
alternative and policy version. Human output begins `Completed with warnings`
only after a COMMITTED result; a plan says `Plan available with advisories` and
does not claim execution. Warnings appear in JSON and redacted stderr, never
only debug logs. A committed warning-mode run still returns exit 0, with its
warning list intact. Certification remains per method: the skipped method stays
UNVERIFIED; the actual verified alternative may commit. Warn is not evidence
that the deployment or route as a whole is certified.

Preview diagnostics instead use `compatibility_advisory`, a possibly excluded
method, conditional alternative and remaining content/partition prerequisites;
`selected_alternative` is null. They cannot assert `unverified_method_skipped`
as a completed decision. Actual selected warnings are frozen with the final
selection and PREPARED transaction, and preserved in later recovery output.

The enrolled `resolved` record is immutable and content-independent. Final
selection is a separate append-only `selected` record referencing its digest
and the acknowledged seal. Its method and expected after-state are computed
from complete observations and committed atomically with PREPARED. Neither a
preview nor enrollment fabricates future content, UUIDs or a selected method;
those fields are explicitly pending until observed. A lost ACK at this later
boundary does not authorize continuation through readback.

### Compatibility and migration

Do not reinterpret `design_digest` or regenerate historical bytes under old
identifiers. Historical decoding retains the original validation and selector
semantics; it does not rerun current policy against old records. Under the
[approved consolidation amendment](feature-design-clickhouse-prototype-consolidation.md),
preserve strict historical inspection, not every unpublished mutation-capable
engine or provisioning default. Published dpone behavior remains unchanged.

The one current composition provisions v3 explicitly for eligible unmanaged
names. It does not adopt old-owned targets or silently upgrade stores. Refuse
incompatible/unknown ownership with the original store untouched. Rollback means
stop new admissions and retain historical inspection and source availability,
not downgrade or recreate an authority file. Historical inspection never issues
new source, send or cleanup authority.

## Descriptor and settings policy

### Separate facts, identity and decisions

| Value | Purpose | Not a substitute for |
|---|---|---|
| Object identity | Deployment, database/table UUID, name and endpoint binding | Schema or content |
| Configuration digest | Canonical full configuration within the supported profile | REPLACE compatibility |
| Provenance/context digest | Explicit/inherited origin, default-resolution basis and profile | Effective-value equality |
| Content evidence | Typed multiset, count, null counts and partition inventory | Table configuration |
| Compatibility verdict | `compatible`, `incompatible`, `unverified` or `not_applicable` per operation, with reasons | Permission to send |
| Desired target descriptor | Approved effective configuration after publication | Whatever happened to be created |

Configuration identity excludes physical names, UUIDs, SQL formatting, setting
order and explicit-vs-inherited spelling when effective equivalence is proved.
Provenance is preserved separately; it is not discarded. The observation binds
both. Raw DDL may be retained privately for diagnosis, but its string hash is
not the compatibility algorithm. Unknown constructs never disappear through
normalization. Duplicate assignments, unconsumed tokens, expressions where a
literal is expected, invalid types and conflicting metadata are rejected.

Read complete server-rendered DDL, ordered columns and independent catalog
fields. Cross-check them; do not trust one parser output alone. The existing
column/type/key restrictions remain. Stable logical identity is independent of
background merge part names; part-format evidence is a separate compatibility
input when required, not a configuration hash component.

### First supported settings families

The shipped profile contains a closed, immutable registry, not arbitrary
passthrough. Each entry defines type/range, default-resolution rule, CREATE
rendering, preservation, method constraints and evidence tests.

| Family | Initial settings | Rule |
|---|---|---|
| Granularity | `index_granularity`, `index_granularity_bytes`, `enable_mixed_granularity_parts` | Typed values, joint validation, preserved for refresh; conservative equality required for REPLACE |
| Granularity validation | `min_index_granularity_bytes` | Validate byte granularity jointly; preserve the resolved value |
| Part layout | `min_rows_for_wide_part`, `min_bytes_for_wide_part` | Nonnegative bounded integers; preserve, test Compact/Wide parts |
| Storage | `storage_policy` | Only verified local `default` policy in this profile; reject remote/custom policy or disk expressions |

`index_granularity` is a positive UInt64, not a fixed 8192. The remaining listed
integer settings are nonnegative UInt64 values; reject Python bool-as-int, overflow,
negative values and coercion. Zero byte granularity is supported only through
the explicitly tested non-adaptive branch. Byte granularity must also satisfy
the pinned server's lower-bound rule. Booleans normalize only documented
literal representations. The local-default storage topology itself is verified;
its name is not proof of local disks.

The first version intentionally requires equality of all resolved managed
settings for REPLACE. This conservative sufficient condition avoids a complex
matrix of permissive exceptions. It does not claim that ClickHouse requires
equality of every tuning parameter. EXCHANGE and RENAME instead require the
candidate's configuration to equal the desired target configuration.

Other explicit table settings are `unsupported_setting`, not ignored and not
an instruction to reset them. Extending the registry requires tests, a new
policy/profile version and updated generated support reference. Users cannot
locally label unknown settings harmless. Broadening settings does not broaden
TTL, replication, materialized views or other independent restrictions.

### Resolving defaults without guessing

`system.merge_tree_settings` describes global settings, not every table's
effective configuration. A current global value alone is insufficient: the
certified resolver also needs complete persisted overrides, a supported exact
server profile and a protected, complete current observation. It establishes
currently loaded configuration, not historical part formats. Resolution follows
these rules:

1. Use explicit table metadata, including values inserted by the server into
   stored DDL. On pinned 24.8.14.39, `index_granularity` is such a value; 8192 is
   observed data, never a parser exception.
2. For the pinned plain-MergeTree profile on 24.8.14.39, resolve supported values
   from the server's cached current defaults overlaid by complete persisted
   table overrides. Capture the resolver/profile identity, actual endpoint and
   catalog facts. Do not substitute built-in constants, a user assertion, a
   configuration-file value or a CREATE-history requirement for this evidence.
   A manually created, genuinely unmanaged table is eligible for the same check.
3. The profile includes the pinned default baseline for the remaining global
   settings. Unexpected global deviations are classified by the same registry;
   unknown deviations block. A default catalog fingerprint is not permission to
   accept unreviewed settings. Verify actual disks/volumes, not only the policy
   name. Acquire the complete bracketed metadata/defaults observation on one
   non-reconnecting native connection to the actual direct endpoint. A break,
   incomplete visibility, mismatch or unsupported engine/version invalidates it.
4. If effective values or their provenance cannot be established, return
   `settings_provenance_unverified`. Do not alter/restart the server, patch the
   target, silently assume defaults, or demand users copy SQL as a workaround.
   The diagnostic identifies the concrete missing current-state fact or profile
   limitation. A fresh pre-enrollment snapshot is allowed; retained or uncertain
   operation ownership still requires the original authority, never a new store.
5. Pin the resolved supported values explicitly in the candidate CREATE, then
   read back and verify its actual configuration before opening the source.
   Recheck deployment context and table descriptors before sealing and sending.

Default resolution is an implementation certification gate, not a claim that
the required observer already exists. A profile may ship only when omitted and
explicit settings are demonstrated equivalent under its documented conditions.
User-entered timestamps or hashes are not a substitute for that demonstration.

A restart or connection loss invalidates an in-flight snapshot; it does not make
all pre-existing tables permanently ineligible. Before enrollment, collect a
fresh complete observation under the certified profile. After enrollment, keep
the frozen plan, retained ownership and existing source-free recovery rules;
do not use a fresh snapshot to reconstruct execution permission. Recheck context
and descriptors at seal and pre-send. Current settings never replace independent
historical-part compatibility evidence required by a method.

There is no production deployment-epoch provider, bootstrap journal or table
creation-history certificate in this algorithm. The earlier epoch experiments
remain an auditable decision record, not admission tests for manually created
tables. The existing operation journal is a different requirement and remains
mandatory for recovery. Neither this correction nor its tests authorize target
ALTER, server restart/reset, manual SQL copying or a settings-provenance force flag.

## Detailed algorithm

1. Validate the typed request and limits without source access. Open the original
   correct-version authority; refuse retained/conflicting ownership.
2. Acquire the existing single bound exclusion session. Verify endpoint, server,
   database, grants, mutation/topology restrictions and deployment inventory.
3. Observe target metadata and candidate absence. Resolve settings, check all
   unsupported constructs, and derive desired design. Known metadata rejection
   occurs before enrollment, CREATE or source access.
4. Compute the canonical preview: supported methods and their remaining content
   prerequisites. Do not advertise a final method before partition/content
   observation. Persist the actual resolved plan with acknowledged enrollment
   and atomic target/candidate namespace reservations.
5. Register and send fixed CREATE once; persist successful native completion.
   Observe candidate UUID/configuration and verify exact desired-design parity.
   A creation/readback failure retains the operation, with source unopened.
6. Invoke `source_factory` exactly once. Register, admit and send each bounded
   synchronous INSERT under the parent's acknowledged one-shot protocol.
7. On normal source exhaustion, close admission and join all accepted requests.
   Observe candidate content and metadata before/after under the same session.
   Verify expected typed parity, stable configuration/context and all limits.
8. Observe the target completely, using its own schema; recheck the preflight
   configuration/identity. Never interpret old rows using a changed candidate
   schema. Build and freeze operation-specific compatibility evidence and seal.
9. Select using the decision table below. Persist PREPARED with before-state,
   desired target, policy/profile and expected after-state. Obtain a fresh
   acknowledged claim, recheck protected evidence and execute at most once.
10. Close/drain the publisher before outcome classification. Verify the actual
    method-specific after-state and record the result. Keep names/owner retained;
    no source checkpoint, cleanup or route-success implication follows.

Read-only failures before enrollment may be corrected and preflight rerun;
state must be read again. Once enrollment or any write outcome is uncertain,
do not replay or give generic retry advice. Loss of an enrollment ACK, even
before source access, is retained uncertainty. No SQLite transaction spans
network I/O. Preserve bounded canonical bytes, scan rows, columns, partitions
and request deadlines; these are not a claim of bounded total process RSS.

### Method selection and expected post-state

Every row requires a supported, complete observation, sealed candidate and
desired-design parity. `unverified` never becomes `incompatible` merely to
enable EXCHANGE. Candidate divergence from its resolved plan is an error,
not a new design request.

| Condition, in order | Method | Required target result |
|---|---|---|
| Target absent | RENAME | Candidate UUID, desired configuration/content; candidate name absent |
| Target configuration equals desired and content/count/partitions equal | NOOP | Original target unchanged; no publication SQL |
| Explicit design replacement and actual configuration change | EXCHANGE | Candidate UUID/configuration/content at target; original target under candidate name |
| Empty candidate replacing nonempty target | EXCHANGE | Empty desired target; never infer a nonexistent partition to replace |
| One nonempty candidate partition, target partitions are a subset, and REPLACE compatible | REPLACE PARTITION | Original target UUID/configuration, candidate snapshot content; candidate retained unchanged |
| Multiple/different partitions with known supported same desired design | EXCHANGE | Entire desired snapshot, without leftover old partitions |
| REPLACE has a proven operation-specific incompatibility, but EXCHANGE independently passes all prerequisites and desired-design checks | EXCHANGE | Same desired configuration and complete candidate snapshot; report why REPLACE is unavailable |
| Applicable preferred REPLACE compatibility is unverified; mode is `block` | BLOCK | Default conservative behavior |
| Applicable preferred REPLACE compatibility is unverified; mode is `warn`; policy proves the missing fact irrelevant to EXCHANGE and verifies all EXCHANGE prerequisites | EXCHANGE with warning | Preserve desired configuration/content; excluded REPLACE remains UNVERIFIED |
| A shared safety fact is unverified, or no permitted method passes | BLOCK in both modes | No fallback that bypasses unknown configuration, ownership or outcome evidence |

Two empty equivalent tables select NOOP. Target absence with empty candidate
selects RENAME. One-partition replacement includes `PARTITION BY tuple()` and
also other supported partition keys when the complete-snapshot condition holds.
There is no loop of independent partition replacements masquerading as atomic
whole-table publication. Known full-snapshot EXCHANGE is an ordinary planned
method, not an error fallback.

`preserve` preserves configuration, not the target UUID across every method.
This distinction must appear in the user reference. Known REPLACE-only
incompatibility may select independently certified EXCHANGE without asking for
`design_change=replace`: the desired design has not changed. No incompatibility
may be learned by trying mutation SQL and falling back after an error; selection
precedes the single dispatch. In explicit `warn` mode, unknown facts relevant
only to a method may exclude that method only when the policy proves they are
irrelevant to the chosen one;
unknown settings, configuration or ownership always block the whole operation.

### State, identity, recovery and concurrency

```text
READ_ONLY_PREFLIGHT -> acknowledged ENROLLED -> CREATE_COMPLETED
  -> CANDIDATE_VERIFIED -> SOURCE_OPEN -> CLOSED -> SEALED
  -> PREPARED -> CLAIMED -> publication closure -> classified result

pre-enrollment rejection -> fix configuration and repeat read-only preflight
ambiguous enrollment or candidate mutation -> retained, source-free inspection
restart before PREPARED -> inspection only, including an existing seal
restart after PREPARED -> original-version recovery, never SQL/source replay
```

The preview is disposable. The enrolled plan, request frontier, seal and frozen
intent are not. Bind them to original store/inode identity, operation, profile,
configuration/provenance digests and acknowledged revisions. Persisted data
never reconstructs a volatile invocation or dispatch capability.

Recovery reads the recorded v3 method and recomputes its consistency using the
recorded policy version, not the newest runtime defaults. Its expected after-state
is explicit: REPLACE preserves target configuration/UUID; EXCHANGE swaps objects;
RENAME removes the candidate name. Current metadata is independently observed
under the recorded supported profile. Unsupported recovery versions or changed
configuration context remain unresolved, not automatically migrated.

After permanent publisher closure, the complete expected after-state means
COMMITTED only with the required acknowledged claim history. The complete
unchanged before-state means NOT_PUBLISHED when distinct from the after-state.
An unclaimed PREPARED record cannot become COMMITTED merely because content
matches. For NOOP specifically, verified unchanged evidence with acknowledged
claim history means COMMITTED; without claim history it means NOT_PUBLISHED.
Third identities, partial effects, otherwise overlapping/ambiguous classifications
or unavailable proof mean UNKNOWN. Terminal records are
reported as historical resolutions, not freshly observed current-table status.

The authority execution session covers observation through dispatch and closure.
Only dpone participates in this exclusion; it cannot fence arbitrary admin sessions.
Changes outside the reviewed deployment boundary invalidate the guarantee.
Background merges can change part layout, so no decision depends on stale part
names or assumes current settings describe every historical part. Conservative
equality does not waive the pinned part-format certification requirement.

### Actionable failure examples

Diagnostics distinguish the following cases without asking the user to infer
whether source access or enrollment happened:

| Case | Enrollment / owner | Source opened | Retry / exit | Next action |
|---|---|---|---|---|
| Proven rejection before enrollment, no conflicting original operation | `not_enrolled` / false | false | `safe_to_retry=true`, exit 2 | Correct the reported input; repeat the same request/preflight; nothing exists to recover |
| Existing operation or ownership conflict | `existing` / true | false for this invocation | `safe_to_retry=false`, exit 3 | Inspect the original operation; do not generate a new ID/store |
| Enrollment ACK lost or authority unreadable | `unknown` / null (unknown, treat as retained) | false when source has not been invoked, otherwise unknown | `safe_to_retry=false`, exit 3 | Restore access to and inspect the original authority; never reprovision |
| Failure after acknowledged enrollment, before publication resolution | `enrolled` / true | Actual observed true/false; unknown after a restart if not provable | `safe_to_retry=false`, exit 3 | Source-free recovery of original operation |
| Resolved NOT_PUBLISHED | `enrolled` / true | Actual or unknown | `safe_to_retry=false`, exit 2 | Preserve original resources; no new publish until separate finalization exists |
| Resolved COMMITTED | `enrolled` / true | Actual or unknown | `safe_to_retry=false`, exit 0 | Observe the recorded outcome; ownership is still retained |

`safe_to_retry=true` is narrowly permission to rerun corrected preflight for an
unenrolled request, never replay permission for any mutation. Unknown source
access is serialized as null, not false. Recovery of an unenrolled request
returns a source-free `operation_not_found` result without creating records.

| Reason | Explanation to user | Safe next action |
|---|---|---|
| `unsupported_setting` | This profile cannot establish the setting's publication safety | Use a certified profile supporting it; do not strip it from the target |
| `settings_provenance_unverified` | The certified resolver lacks a complete current-state fact or supported profile | Follow the reported visibility/profile diagnostic; before enrollment repeat fresh preflight, otherwise inspect the original operation; do not reconstruct CREATE history |
| `design_change_requires_replace` | Request changes the existing design, not only data | Review the structured diff and deliberately request `replace` if intended |
| `configuration_changed` | Table or configuration context changed during this operation | Inspect the retained original operation; do not start a replacement run |
| `candidate_design_mismatch` | Server-created candidate does not match the resolved plan | Inspect retained candidate and diagnostics; source remains unopened if detected after CREATE |
| `observation_limit_exceeded` | Required evidence exceeds the configured budget | Report measured/allowed limit; retained operations stay retained even if limits later increase |
| `publication_unknown` | Send/completion evidence cannot prove the outcome | Recover the original operation without reloading or resending |
| `unverified_method_compatibility` | The otherwise preferred method lacks required compatibility proof | Before enrollment, optionally select `warn` to consider independently verified alternatives; after enrollment use original-operation recovery only |

Changing the mode after enrollment never resumes, re-plans or replays the
operation. Its original frozen mode governs source-free recovery. A warning
does not justify a new store or operation over retained physical names.

## Architecture and alternatives

| Component | Responsibility | Boundary |
|---|---|---|
| Descriptor/intent contracts | Immutable typed facts, provenance and desired/after-state | No SDK, client, SQL execution or permissions |
| Pure compatibility policy | Normalize supported settings, resolve design and evaluate methods | Receives facts; no environment discovery or I/O |
| Native catalog adapter | Acquire bounded raw metadata and cross-checkable fields | Does not choose methods or declare caller-provided flags trustworthy |
| Existing candidate lifecycle | One-shot CREATE/INSERT, closure and seal | Binds resolved profile/plan; does not duplicate policy |
| Versioned authority/codec | Immutable durable decisions and historical recovery | Strict schema dispatch, no migrations or permissive field dropping |
| Composition/diagnostic renderer | Inject dependencies and explain one result | No new policy in CLI, renderers or connector shims |

Keep contracts in `dpone.contracts`, capability ports in `dpone.ports`, concrete
I/O in `dpone.adapters`, and application orchestration in the existing canonical
runtime. Pure policy must be shared through one cohesive allowed boundary; do
not make adapters import runtime implementations or scatter the settings table
across parser, selector, renderer and recovery. Use stateless functions where
appropriate, not classes solely to wrap functions.

| Alternative | Benefit | Cost/risk | Decision |
|---|---|---|---|
| Accept only 8192 | Small immediate patch | Incidental literal becomes a product limitation | Reject |
| Ignore or blindly pass all settings | Broad apparent compatibility | Unsupported semantics and false recovery confidence | Reject |
| Small typed profile plus per-method predicates | Auditable, extensible, automatic ordinary path | Explicit supported subset and certification work | Adopt |
| General SQL AST/plugin framework or user-defined safety rules | Maximum flexibility | Excess complexity and unreviewed trust boundary | Defer |

[ADR 0080](adr/0080-clickhouse-table-compatibility-policy.md) records the approved
additive decision, superseding only the relevant settings/selection/version
clauses of ADRs 0078/0079 for the new binding. Old ADRs and readers remain
meaningful. Do not relabel the whole parent implemented.

Quality planning must address the already observed strict graph-budget failures
in the unfinished candidate implementation. One integrator must revise cohesive
policy/lifecycle boundaries and the path contract across affected tasks, measure
the actual graph, and preserve behavior with characterization tests. No threshold
increase, proxy imports, artificial internal edges or baseline laundering.
Module/SLOC and edge estimates belong in the reviewed implementation plan;
[quality budgets](benchmarks/quality_budgets.yml) remain the source of truth.

## Market research and evidence basis

Sources checked 2026-09-30. Statements below are documentation/source facts;
the selected dpone policy is an architectural inference, not a vendor mandate.

- ClickHouse documents atomic partition replacement with structure/key/storage
  and index/projection prerequisites, and matching granularity for non-adaptive
  source parts. Adopt explicit compatibility checks. Do not extrapolate current
  documentation into a claim of certification on 24.8.14.39.
  [Partition operations](https://clickhouse.com/docs/reference/statements/alter/partition).
- Global and per-table settings have distinct scopes. Adopt provenance-aware
  resolution rather than treating the global table as per-table truth.
  [MergeTree settings](https://clickhouse.com/docs/reference/settings/merge-tree-settings).
- Pinned upstream source persists immutable settings in DDL and distinguishes
  granularity and part-format settings; it also validates byte-granularity
  bounds. This explains the observed server output, not all effective-default
  resolution or recovery behavior.
  [24.8.14.39 settings implementation](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/MergeTree/MergeTreeSettings.cpp).

| System/version | Relevant fact and strength | Limitation for this comparison | Adopt / reject | Source/date |
|---|---|---|---|---|
| dlt 1.30.0 docs, ClickHouse destination | Optimized replace uses atomic table exchange; strategy chosen by destination capabilities | This page does not establish dpone's retained-journal or per-setting recovery guarantees | Adopt staging and capability-aware methods; reject inferring that all refreshes must exchange | [Official destination](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse), 2026-09-30 |
| Airbyte ClickHouse connector 2.1.29 docs | Typed native loading and named full-refresh modes give users a high-level operation | Its connector contract is not proof of our settings preservation or one-shot journal | Adopt operation-oriented UX and explicit topology prerequisites; do not claim protocol equivalence | [Official connector](https://docs.airbyte.com/integrations/destinations/clickhouse), 2026-09-30 |
| Informatica | N/A | Enterprise workflow comparison is outside this table-compatibility amendment | No product ranking | Scope decision |
| Fivetran | N/A | Managed-service operation is not the local authority/default-provenance contract assessed here | No product ranking | Scope decision |
| Pentaho | N/A | No transformation-engine or workflow behavior is being replaced | No product ranking | Scope decision |
| Microsoft SSIS | N/A | MSSQL extraction and orchestration are unchanged | No product ranking | Scope decision |
| gusty | N/A | DAG authoring is outside publication | No product ranking | Scope decision |
| Astronomer Cosmos | N/A | dbt/Airflow integration is unchanged | No product ranking | Scope decision |
| Apache Beam | N/A | Distributed stream processing/checkpoint semantics are unchanged | No product ranking | Scope decision |

### Measurable differentiation

No market-superiority claim is made. Compare against dpone's current closed
profile and operator-assisted workaround, not unmeasured competitor behavior.

```yaml
axis: supported refresh with no manual settings translation
scenario: fresh enrollment of a supported existing table with nondefault granularity
baseline: current parser rejection and operator-assisted settings diagnosis
metrics:
  per_operation_manual_setting_copies: 0
  per_operation_method_selections: 0
  required_publish_calls: 1
  silent_settings_resets: 0
  source_calls_on_metadata_rejection: 0
  mutation_replays_after_uncertainty: 0
procedure: scripted baseline and new-profile journeys plus deterministic fault matrix
artifact: test_artifacts/clickhouse-table-compatibility-RUN_ID/summary.json
limitations: excludes initial deployment provisioning and later owner-finalization work
```

Targets above are acceptance criteria, not achieved benchmark results.

## Security, privacy and operations

Preserve separate publisher/observer credentials and least privilege. Never
clone arbitrary unvalidated DDL, enable async INSERT, execute user-supplied SQL,
modify global settings, broaden grants or drop retained resources automatically.
Use fixed rendering from typed values and independent actual metadata readback.

The certified direct-endpoint profile and all-writer/configuration inventory
combine deployment assumptions with observable checks; they are not cryptographic
proof that admins cannot bypass them. No pre-creation epoch certificate is required.
Collect evidence automatically where possible; invalidate cached readiness on
version, endpoint, inventory, configuration or authority-identity changes.
Never cache a successful compatibility plan as a future mutation grant.

## Test and certification plan

| Layer | Required cases | Acceptance/evidence |
|---|---|---|
| Unit | Complete parsing, order/quoting normalization, duplicates, unknown settings, bool/int/overflow boundaries | Canonical vectors and exact rejection reasons |
| Defaults | Manually created unmanaged table without CREATE receipt; explicit/omitted equivalents; cached defaults plus overrides; reload/restart/ALTER/RESET with persistent data; incomplete or drifting current observations | Equivalence only when proved; no assumed 8192 or reconstructed history; retained operation authority unchanged |
| Settings families | At least 4096 and 8192, adaptive/non-adaptive, byte lower bound, Compact/Wide, local storage validation | Positive and negative tests per registry entry |
| Planning | Preserve/default/explicit replace, absent vs empty target, same content with changed desired design, unknown capability | Deterministic method table; zero source calls on metadata rejection |
| Warning mode | Default block; warn plus verified EXCHANGE; warn without safe alternative; known incompatibility; non-applicable method; shared-safety unknown | Only proven alternative may execute; structured warning retained; no blind send or mode change on recovery |
| Lifecycle | Candidate readback before source, drift at every observation/send boundary, lost enrollment/CREATE/INSERT/seal/claim ACK, cancellation | Original owner retained, no fabricated seal/grant or replay |
| Recovery | All methods, actual UUID/configuration/content after-state, crash before/after PREPARED, older/unknown profiles | Source-free original-version interpretation |
| Compatibility | Published API/default contracts, immutable historical prototype vectors, strict read-only inspection and wrong-version stores | Published behavior and historical bytes unchanged; no migration or historical mutation authority |
| Live Docker | Exact pinned server/driver; real settings/parts, all four methods, zero-row and stale-partition cases, real drift/lost-response/restart | Exact source commit/tree, image digests, catalog/content snapshots, journal, counters, JUnit and checksums |
| UX/docs | One-call publish, optional plan, all diagnostic next actions, original-operation recovery | Executable example and scripted manual-action counts |
| Quality/security | Actual strict graph/module/import gates, full non-live suite, secrets/log privacy and bounded inputs | No weakened budget or hidden dependency |

Live environment is the already approved isolated local Docker Desktop, not
unrelated user containers or production. Pin ClickHouse 24.8.14.39 and driver
0.2.10; record actual versions and image digests. Test another positive
granularity, non-adaptive historical parts and mixed-layout histories, not only
fresh defaults. A complete profile requires every mandatory case with no skips.
Mocked tests, old native transport receipts and current docs are not live passes.
Production ingress/TLS/power-loss and the complete ODBC route remain UNVERIFIED.

Include simultaneous known incompatibility and unresolved method-only facts:
a proven incompatibility can exclude that method without evaluating irrelevant
checks, but shared safety unknowns must block before every selection branch.
Test preview advisories separately from final warnings and a late strict-mode
block that retains ownership without permitting a switch to warn on recovery.

## Documentation plan

Keep this specification under Developer navigation, linked from the parent.
At implementation, publish distinct user-facing documents:

- Overview: what the protected capability does, supported limits and no route claim.
- Tutorial/example: one-time platform preparation, one-call default publish,
  expected output and source-free recovery; no manual settings copies.
- Generated reference: supported settings, defaults/provenance rules, reason
  codes, schema versions and method predicates from the same policy producer.
- Explanation: configuration identity versus compatibility versus desired design.
- Runbook: preflight rejection versus retained uncertainty, safe actions and
  original-version recovery; no SQL snippets that bypass authority.

The tutorial/runbook must explain that preview is incomplete until content is
observed, late strict-mode blocking retains resources, and warning policy must
be chosen before enrollment rather than used to revive a retained operation.

Link overview/tutorial from the main ClickHouse guide, not only Developer docs.
Keep existing synthetic/native-only examples explicitly transport-only.
Update changelog, architecture, ADR index and implementation-state notices when
behavior lands, not now. Validate examples, links, generated references, English
language contracts, strict MkDocs and the rendered first-success navigation.

## Rollout and rollback

1. Record the approved specification and 2026-10-01 current-state correction;
   review a structurally feasible revised implementation plan and concrete path
   contract before execution. Current unfinished code stays preserved.
2. Implement one current model with strict historical readers and old golden vectors intact, characterize
   default provenance, and resolve existing quality-gate failures honestly.
3. Certify the complete profile on the exact final commit in owned Docker;
   obtain an independent fresh-context review and fix blocking findings.
4. Expose only the opt-in protected Python composition. A future route/release
   decision still requires the remaining lifecycle and route-certification gates.

On regression, stop new admissions to the new profile; retain all original
stores, data and readers. No automatic old-profile fallback, owner release,
reverse EXCHANGE, target ALTER or new operation over retained names.

## Agent execution boundaries

Root is the sole integrator/shared-file owner. The implementation plan and exact
path contract govern execution after revision review; area names below are not independent
write authorization. Its structural prerequisite has not passed, so no
dependent settings-aware production implementation is authorized to proceed.

| Role | Future owned scope after a reviewed path contract | Read-only | Forbidden |
|---|---|---|---|
| Explorer, architect, certifier, docs reviewer | Analysis/review only | Relevant specs, contracts, tests and evidence | All production writes |
| Root integrator | Versioned descriptor/policy/authority/codec/kernel seams, related tests, docs and composition | Existing evidence and immutable version vectors | Workflows, package versions, credentials, publication, production grants |
| Optional implementation worker | Only disjoint exact paths in an approved separate-worktree contract | Shared contracts and neighboring modules | Shared semantic files and unassigned paths |

The revised plan must explicitly replace the old read-only restriction where
new-version selector/codec/kernel integration needs edits. The affected areas
are observation contracts, candidate/profile/grammar, authority schema/storage,
publication codec/selection/recovery, native catalog and protected composition.
No worker may interpret this area list as a wildcard write authorization.

## Approval checklist

- [x] User problem, default journey and manual-action targets are explicit.
- [x] Settings, provenance, compatibility and desired state are distinct.
- [x] Algorithm, failures, method table and recovery identity are specified.
- [x] Published contracts and historical bytes are preserved without silent migration.
- [x] Maintainer authorizes consolidation of unpublished execution prototypes on 2026-10-01.
- [x] Alternatives and dated primary-source research are recorded.
- [x] Test, certification, documentation, rollout and rollback criteria exist.
- [x] Implementation ownership and required plan amendment are explicit.
- [x] Maintainer approves this written specification as `APPROVED`.
- [x] Historical 2026-09-30 implementation plan and exact path contract were separately reviewed.
- [x] Maintainer approves the current-state correction on 2026-10-01.
- [ ] Structurally feasible current-state execution plan and path contract are reviewed.

Approved for staged implementation subject to the plan's prerequisites. Implementation, live certification,
production readiness and release readiness are not established by this document.
