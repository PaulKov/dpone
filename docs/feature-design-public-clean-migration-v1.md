# Feature design: public-clean migration gate

Purpose: give maintainers a reproducible, non-disclosing procedure for moving
reviewed source changes into a public repository. This page describes a proposed
maintainer tool; it does not certify a route or authorize publication.

- Status: APPROVED
- Owner: maintainer
- Target release: none; repository tooling
- Last verified: 2026-09-09
- Approval: maintainer explicitly approved this specification and task contract in the current task on 2026-09-09.

Related: [agent workflow](agent-development.md),
[route documentation](source-sink/postgres-to-mssql.md), and
[feature standard](feature-design-standard.md).

## Problem, personas, and journey

Maintainers need to preserve useful historical changes without transferring
private repository objects, identities, or organization-specific examples.
Security reviewers need evidence for the exact proposed commit, including its
metadata. Operators need an honest distinction between migrated source and
current live certification.

The journey is: inventory locally, classify every candidate, compare its complete
effect with the clean snapshot, prepare a neutral patch when needed, scan the
frozen candidate, review uncertainty, commit locally, and verify the actual
commit. A rejected candidate stays in the private ledger. A failed scan reports
stable codes and opaque item identifiers; the reviewer diagnoses the input
locally, sanitizes it, and reruns. Publication requires separate authorization.

## Scope and constraints

In scope: a repository-local candidate privacy gate, tests, its operator guide,
and local history migration accounting. Reuse the existing tenant hygiene
scanner where its frozen-tree and bounded-input contracts apply.

Out of scope: runtime changes, secret discovery through network verification,
automatic proof that arbitrary text is public, importing private Git objects,
credential retention, live certification, release activation, and CI provider
configuration. Do not modify the source repository or its worktrees.

No source identifiers, private paths, private terms, emails, or raw findings may
enter tracked reports. A private external ledger retains source commit mappings
and review decisions. Credential values are never persisted, even there.
Public reports use independently assigned neutral candidate identifiers.

## Public contract

Proposed entry point: `python tools/agent_policy/public_clean_gate.py`.

`--help` and `<mode> --help` return static, non-disclosing JSON with schema
`dpone.public-clean-help.v1`, supported options and the operator guide, exit 0.
The execution summaries below use the separate scan schema. See the
[operator guide](public-clean-migration.md) for a runnable review workflow.

`candidate --root PATH --policy PATH --metadata PATH --receipt PATH [--review PATH]`
scans the exact index tree plus proposed metadata.
`history --root PATH --policy PATH --receipt PATH` scans every
reachable commit, annotated tag and unique blob from a frozen ref inventory.
`source-delta --root PATH --commit-sha SHA --policy PATH --receipt PATH`
inspects source metadata and each parent-relative complete patch before any
transfer; it never writes objects or a receipt inside that repository.
`worktree --root PATH --policy PATH --receipt PATH` inventories the full index
and working files, including ignored/generated artifacts, without following
symlinks. Discovery commands may report SCAN_COMPLETE but cannot authorize a
commit without candidate review.
`commit --root PATH --commit-sha SHA --policy PATH --candidate-receipt PATH --receipt PATH`
checks the actual local commit after creation. Neither command commits, pushes,
installs hooks, or mutates the source repository. Output is one JSON document on
stdout; no raw input, exception detail, or matched text is emitted on stderr.
Exit codes: 0 PASS or discovery-only SCAN_COMPLETE, 2 BLOCKED findings or
REVIEW_REQUIRED, 3 UNABLE_TO_CERTIFY. SCAN_COMPLETE is not privacy clearance or
commit authorization; unknown-name review remains mandatory before candidate PASS.

Metadata input contains precisely the planned message, author name/email and
committer name/email, exact author and committer Git timestamps/timezones, and
the full destination parent identity. The tree comes from the index. Commit
verification checks all of these fields and the tree against the approved
candidate. The initial implementation allows one parent only; migration of a
merge uses separately reviewed logical patches with all source-parent effects
accounted for in the private ledger. Invalid/missing fields, unknown fields, malformed UTF-8,
unsupported entries, missing policy, and resource exhaustion fail closed.

The private policy contains non-empty protected literal terms, approved public
identities, and explicit public host allowances. It is supplied by the maintainer,
stored outside the repository, and never included in output. Matching protected
terms is case-insensitive after Unicode normalization and separator folding;
private policy variants are injected and never compiled into public tests.
Matching applies NFKC, then casefold, then removes Unicode separator, punctuation
and format characters from both terms and scanned text. Raw-byte rules run as
well. Do not remove letters or digits; fuzzy similarity is a review aid only.
Retain original bytes for
content identity. Generic rules flag credentials, private network addresses,
unapproved hosts and email identities, local absolute paths, ticket-like tokens,
and suspicious database identifiers. A lexical match is a review signal, not a
claim that the value is a credential. Unknown proper names require manual review.

Do not permit blanket path exclusions. Any reviewed generic-rule exception is
bound to exact path, content digest, rule, justification and reviewer approval;
protected terms and actual credential material cannot be waived. A generic
credential-shaped lexical match requires exact synthetic-origin review or the
approved `NON_CREDENTIAL_SYNTAX` classification. The latter requires a fixed
context validator to prove a permission declaration, a value-free Python type
annotation, or a JSON Schema field-name declaration from the exact scanned Git
blob. A classification flag alone cannot clear a finding. Actual credential
values, runtime payloads and ambiguous contexts remain blocked. Exception
reviews occur privately, and reports use opaque IDs. Synthetic test fixtures
exercise the same scanner contract and require explicit exact-content review.
An exception targets one exact path or metadata field, rule and occurrence
offset in the scanned representation; it cannot clear other occurrences.

The private receipt schema `dpone.public-clean.v1` records mode, status, scanned
counts, destination parent/tree, exact timestamps, metadata digest, policy digest,
rule-set version, opaque findings, completeness and review binding. Receipts and
review inputs stay outside the repository, including policy-derived digests which
could fingerprint private terms. Stdout is a separate public summary containing
only status, scanned counts, stable codes and opaque sequential item IDs. It
contains neither policy-derived hashes nor source identities.

The external review input binds the exact candidate tree, metadata digest, parent,
policy digest and rule-set version, and records the approved reviewer identity
and an affirmative review of all content and metadata for unknown private facts.
This is a local maintainer approval record, not cryptographic proof against a
malicious local operator. The trusted policy lists permitted reviewers. Without
this matching record, a clean lexical scan yields REVIEW_REQUIRED and cannot
produce PASS. Changed input invalidates both review and receipt. Private receipts
use exclusive atomic creation with mode 0600 in a private external directory;
refuse repository-local, symlink, existing or unsafe output paths. No new runtime
CLI, Python import, manifest, checkpoint or route schema.

Structured private inputs use strict UTF-8 JSON, reject duplicate/unknown keys,
and reject non-finite numbers. Digests use SHA-256 over sorted-key, compact JSON
encoded as UTF-8 with non-ASCII characters escaped and no trailing newline;
blob identity uses SHA-256 of original bytes. Scanner identity hashes every
gate implementation module and is included in review and receipt bindings.
Actual-commit verification requires a complete PASS candidate receipt and exact
equality of scanner, policy, parent, tree and metadata identities. Read raw commit
objects with Git replacement objects disabled. Accept only one tree, parent,
author and committer header in canonical order, one blank separator and the exact
message bytes. Reject signatures, encoding headers, additional headers and
noncanonical timestamps rather than ignoring potentially identifying metadata.

History receipts bind a sorted private ref-name/target inventory, reachable
commit graph and annotated-tag chains. Scan raw tag headers/messages too. Re-read
refs before finishing; concurrent ref change invalidates completeness. Worktree
receipts bind all paths, modes, sizes and file digests, with before/after identity
checks and a second inventory to detect additions/removals. Git administration
paths are explicitly excluded from recursive worktree traversal and inspected
through Git object/ref/index acquisition instead; ignored working files remain
in scope. Source-delta receipts bind source commit and each parent, full changed
paths, modes, before/after blob bytes and complete patch. Root commits compare
against the empty tree. None of these discovery receipts can serve as a
candidate PASS receipt, and source reads never modify the source repository.

## Algorithm and failure semantics

1. Load policy and metadata through bounded regular-file reads without following
   symlinks. Capture identity before and after each read; reject replacement.
2. Read the exact index entries with NUL delimiters. Reject unmerged entries,
   submodules, symlinks and non-regular files for migration candidates. Freeze
   the tree in the destination repository only; never fetch private objects.
3. Scan paths, full candidate blobs and metadata. A patch-only scan is
   insufficient: unchanged candidate-tree material is part of the assessment.
   Inspect text as bytes and normalized Unicode. Archive inspection stays in
   memory, never extracts to disk or executes content, and inherits the existing
   hygiene member/count/ratio budgets. Inspect supported ZIP, gzip and tar member
   names and bytes; reject nested or unsupported archives, encrypted entries,
   links, traversal, duplicate names, malformed metadata and decompression limits.
   Compiled Python inputs have raw bytes and extracted printable strings scanned,
   are never imported or unmarshalled, and remain blocked for public migration
   even when no known private term matches. Other unsupported binaries block.
   Scan raw archive bytes and textual headers/comments as well as members;
   reject unparsed metadata and trailing/appended data. Aggregate expanded-byte
   and member ceilings apply to the entire scan, not separately per archive.
4. Evaluate private terms and generic rules. Record stable rule codes and opaque
   item IDs, never raw paths or substrings. Distinguish blocked findings from
   unreadable or incomplete input. Any uncertainty prevents PASS.
5. Re-read index and destination HEAD identity. If either changed, discard the
   receipt and require retry. Emit a candidate receipt only for the exact frozen
   tree and metadata; require matching explicit content review before PASS.
6. The integrator reviews all unclassified text and uses explicitly public
   identities when committing. Preserve each accepted source delta as its own
   logical destination commit unless whole-effect snapshot equivalence is proven.
7. Verify the new local commit, including metadata and parent/tree binding,
   against the candidate receipt. A mismatch blocks further migration and push.
   Keep a failed candidate visible in local accounting; never call it migrated.

```text
inventoried -> classified -> sanitized -> scanned -> reviewed -> committed
                    |           |           |           |          |
                    +-----------+-----------+-----------+----------+
                                  blocked / retry
```

Scanning is read-only and repeatable. Empty policy or an empty candidate selection
cannot certify migration; duplicate index paths, malformed records, timeouts,
resource exhaustion and concurrent mutation fail closed. Crashes before atomic publication leave no success receipt. After publication,
a complete receipt may exist even when the caller did not receive stdout. No network calls or live credentials are needed. The caller
writes reports atomically outside the input tree, without overwriting by default.
Single-integrator serialization avoids concurrent index mutation.

Resource ceilings reuse the existing hygiene budgets for tree size and blobs;
policy and metadata are each bounded to 1 MiB. Git subprocess output and execution
time are bounded. Do not reuse a reader that blocks before its timeout starts:
use deadline-aware subprocess communication and terminate the process on timeout
or output overflow. All bootstrap imports and CLI parsing are inside a redacted
failure boundary; unexpected exceptions produce only a stable unavailable code.
Bound each Git operation to 30 seconds including reads and termination, and the
complete scan to 15 minutes. Enforce normalized text limits equal to four times
the original blob limit, 10,000 entries per policy list, 100,000 exceptions per review, 100,000 findings
per complete scan, and 64 MiB per review/receipt input and serialized receipt output.
The independent protected-ticket position limit remains 10,000 per input. Overflow returns UNABLE_TO_CERTIFY, never a
truncated PASS. Accepted text is strict UTF-8 without NUL bytes; unsupported
content blocks the candidate. The inspected public baseline has only accepted
text and regular-file modes; this observation is not a privacy clearance.
History allows at most 100,000 refs, 100,000 commits and 100,000 annotated tags;
worktree inventory allows 100,000 entries. The entire scan shares the existing
1 GiB source-byte ceiling and archive expansion/member ceilings. Deduplicate
blob content reads while still scanning each path and metadata occurrence.
The capacity amendment was explicitly APPROVED on 2026-09-09. Inputs within these
bounds use one complete execution; overflow remains uncertifiable, without CLI
overrides, truncation or automatic retries with larger limits. Partial receipts
and per-input diagnostics cannot be combined into PASS.

Review matching uses an exact-key occurrence index, preserving rejection of targeted
duplicate keys and repeated exceptions, and the original order of remaining findings.
Changed scanner identity invalidates earlier candidate receipts and reviews; rerun
the full candidate with fresh filenames and matching review. Capacity supplies no approval.

Resource acceptance requires peak RSS at most 2 GiB and elapsed time below 900 seconds
on macOS arm64 with Python 3.12.11. Record exact runtime and fixture identities and
output sizes for maximum-occurrence and maximum-byte cases. This is measured acceptance,
not a runtime memory limiter; the source-byte budget remains independent. Unmeasured
acceptance is UNVERIFIED. Run complete candidate, reachable-history and worktree scans
separately; evidence from one mode cannot establish another mode or erase its failures.

Serialized credential-shaped locators or review prose cause `RECEIPT_SENSITIVE`
(exit 3) and suppress receipt publication, even for synthetic source findings.
Inspect original inputs locally, remove credential-shaped locators/prose and rerun
with fresh filenames. Never echo matched text; this failure has no private receipt
from which to retrieve a finding ordinal.

## History accounting

Inventory local branches, tags, remote-tracking refs, reflogs, worktree heads,
staged/unstaged differences and untracked draft candidates. Record coverage and
unavailable sources explicitly. Include changes selected by relevant paths and
commit messages; follow affected support paths and parent lineage to a fixed
point. Do not treat a keyword search alone as exhaustive.

For each candidate record: private source identity, parents, relevant refs,
changed paths, disposition, evidence, and destination mapping when accepted.
For each complete delta, account for additions, modifications, deletes, renames,
mode changes and merge-parent effects. Compare complete changed-path effects
against the public snapshot. A byte-equal tip file is not proof that every
historical delta is present. Sanitized equivalence requires an explicit reviewed
mapping and semantic evidence; superseded and rejected changes remain distinct
from equivalence. Missing proof is UNVERIFIED, never implicitly accepted.

Uncommitted draft assets receive their own immutable local digest inventory;
they are not treated as approved authority. Record source stability checks before
and after inspection, without changing source indexes or worktree files.

## Architecture and alternatives

The integrator owns orchestration and receipts. Separate scanner policy,
candidate acquisition and reporting only at stable responsibility boundaries.
Use explicit policy and repository parameters, no global client or network I/O.
The existing `tools/agent_policy/tenant_hygiene.py` remains backward compatible;
new candidate behavior must not weaken release scanning or reuse its PASS as
proof of metadata coverage. Follow the module and graph budgets in
`docs/benchmarks/quality_budgets.yml`. No runtime package dependency is added.

Manual grep alone lacks metadata and completeness proof. Whole-repository fetch
transfers prohibited objects. A denylist alone cannot identify unknown corporate
names. Adopt automated fail-closed checks plus exact-content human review.
An ADR is unnecessary for isolated repository tooling; any later runtime or
publication authority change requires a separate design and architecture review.

## Comparison and measurable outcome

dlt, Informatica, Airbyte, Fivetran, Pentaho, Microsoft SSIS, gusty, Astronomer
Cosmos and Apache Beam are N/A: this scope is Git migration privacy accounting,
not data integration execution. No comparative product claim is made, and no
external product behavior is relied upon.

Measure seeded sensitive content escaping across paths, blobs, messages and
identities. Target: zero escapes in the specified test matrix and zero source
candidates silently omitted from the ledger. This establishes only tested
coverage, not universal detection of private facts.

## Validation and rollout

Before implementation, approve this document and
`test_artifacts/agent-policy/public-clean-migration-v1.yml`. Run focused RED tests
for each surface; then implement and run GREEN. Cover Unicode/case variants,
rename/delete/mode changes, malformed input, empty policy, symlink/binary inputs,
bounded/redacted failures, concurrent mutation, receipt mismatch and safe cases.
Use synthetic neutral terms and explicit dummy identities only.

Generate the change-aware plan, run lint/format/type/import/layer/module checks,
non-live tests, documentation checks and strict build as applicable. Fresh
architecture, test, docs and release reviewers assess the exact candidate assets
commit. No measurement amendment is created before their GO.

The provider-attestation corrective sibling requires a separately established
public authority and exact scoped task contract. Preserve signer and catalog
rejection through independently checked authority edges; registry rejection must
change a valid definition/ref/kind binding while keeping baseline results.
For the corrective cases, change only the signer user-bound thumbprint; target
the catalog authority-owned subject/name while preserving internal consistency;
rebuild a valid registry with a changed definition/ref/kind and baseline results.
The `source_projection` metadata and reason must describe its actual
database-identity mutation. Bound/redact all probe
bootstrap imports. Live/vendor remains UNVERIFIED; activation remains blocked.

## Documentation, ownership and approval

Document the maintainer tutorial, rule/reference, recovery steps and privacy
limitations beside this specification after implementation. Link route evidence
without changing claims of live readiness. Roll back tooling through a new local
commit; never rewrite the source repository or erase rejected lineage.

The parent agent is the only integrator and shared-file owner. Initial reviewers
are read-only. Any parallel writer needs a separate destination worktree and a
validated, approved contract with disjoint paths. There is no push, PR or release
authorization in this specification.

Maintainer approval is recorded above. Migration clearance still requires complete
local accounting, a reviewed private policy and resolved findings; approval to
implement the gate does not clear source content for transfer.

## Approved non-credential syntax amendment

Approved on 2026-09-11 following the maintainer's instruction to continue in
response to the explicit amendment question. Existing six-field synthetic
exceptions preserve their meaning. A new exception adds the paired optional
`classification` and `context_evidence` fields, uses
`classification: NON_CREDENTIAL_SYNTAX`, `synthetic: false`, and must target
`CREDENTIAL_SHAPE`. Unknown fields, unknown roles, partial pairs and conflicting
flags fail closed.

Context evidence binds a complete scanner span, source and context bytes, a fixed
role, and parser/checker identities. Candidate and commit verification resolve the
source from the exact Git tree, never the mutable worktree or arbitrary evidence
paths. Fixed role validators reproduce the proof; no user-provided code executes.
Metadata and archive members are unsupported by this initial context resolver.
Derived permission reports require their own proven consumer relationship and
are not approved merely because workflow permissions are supported.

Changing the gate implementation changes scanner identity. Fresh complete scans
and matching exact review records are mandatory; historical receipts remain
immutable and do not become valid by adding a field. The amendment changes no
runtime API, route activation, publication authority or PyPI requirement.

## Approved append-only historical receipt retention amendment

Approved on 2026-09-11 after the maintainer selected append-only retention over
an LF look-alike projection or a new typed historical binary reader. The
amendment applies only to the six immutable files downloaded as the historical
`agent-pr-receipt` artifact for run `34519291252`. It removes those duplicate
copies from the current candidate tree while preserving their exact Git objects,
source commit, download manifest, and verified external archive. It does not
rewrite history, convert delimiters, alter the original receipt, or make
NUL-framed content generally admissible.

The public retention record uses schema
`dpone.public-clean-historical-receipt-retention.v1` and status `N/A`. It is an
append-only location and integrity record, never execution evidence, a current
receipt, privacy clearance, or release authorization. The existing
`hygiene-retention/index.json` remains byte-identical because it is authority for
an earlier 35-file operation. The new record binds the six original repository
paths, Git blob IDs, SHA-256 values, sizes, modes, source commit and tree, the
unchanged `download.json` digest, the complete external archive and inventory
digests, and the retained private producer identity. Public fields must not
contain a local archive path, account identity, credential, policy digest, raw
finding, or claim of a successful historical execution.

The producer is a private, reviewed, one-shot maintenance tool. It accepts only
the frozen parent `a54281906040f7e8d21e52071820c8b5f9d9608d`, the exact six
regular files, their unchanged download manifest, and the already verified
complete DDA archive. Before writing output, it performs bounded no-follow reads,
rejects symlink ancestors and leaves, verifies strict duplicate-free and
non-finite-free JSON, checks every file hash, mode and Git blob, and proves exact
archive membership. It re-reads all inputs and the candidate index after
generation. Output is written to a new temporary file, synchronized, read back,
and atomically installed without overwriting an existing record. Only after
successful final verification may the integrator stage deletion of the six
current-tree copies. Failure, timeout, mutation, partial output, archive
incompleteness, or cleanup failure cannot produce `PASS`; the operation remains
`UNVERIFIED` and the original tree is recoverable from Git.

The current-tree candidate scan is rerun after deletion and uses the ordinary
strict UTF-8-without-NUL contract. Reachable-history scanning remains
`UNVERIFIED`: the immutable historical Git blob is intentionally retained and
the generic scanner still rejects it. Candidate, history, and worktree results
remain separate; no result from this amendment can be substituted for another
mode. The worktree scan includes ignored/generated files and therefore requires
those inputs to be removed or classified explicitly.

Compatibility is data- and documentation-only. Runtime packages, CLI options,
Python APIs, manifests, connector behavior, load strategies, route activation,
state, checkpoints, and existing evidence schemas are unchanged. Consumers that
need the original artifact retrieve all six files from the pinned source commit
or verified complete external archive and validate them against the append-only
record. No active workflow or parser consumes an LF projection, so none is
created.

Required RED/GREEN coverage proves the valid six-file operation and rejects a
source/download mismatch, missing or extra archive members, altered blob or
mode, malformed/duplicate/non-finite JSON, symlink and concurrent mutation,
private locators, modification of the prior retention authority, partial or
pre-existing output, and any status stronger than `N/A`. Existing path-evidence,
receipt-source archive, merge-receipt, workflow-governance, scanner text/binary,
candidate, history, and worktree regressions remain required. Run focused host
and offline Linux Docker tests, then a fresh complete candidate scan and the
change-aware repository checks. Docker unavailability is `SKIP`; history remains
`UNVERIFIED`; neither may be reported as `PASS`.

The scoped implementation contract is
`test_artifacts/agent-policy/public-clean-historical-agent-receipt-retention-v1.yml`.
The parent integrator owns shared documentation and the six deletions. There is
no push, pull request, release, provider activation, or live/vendor authority in
this amendment.
