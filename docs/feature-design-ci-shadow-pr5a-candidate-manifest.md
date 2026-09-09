# Feature design: CI shadow PR5A immutable candidate manifest

- Status: APPROVED
- Owner: dpone maintainers
- Issue: [#512](https://github.com/PaulKov/dpone/issues/512)
- Parent specification: [CI shadow closure and exact-SHA evidence](feature-design-ci-pr-gate-exact-sha-evidence.md)
- Target release: TBD
Last verified: 2026-08-30

## Executive summary

PR5A creates the first half of exact-SHA compatibility evidence: a bounded,
immutable manifest for exactly three distribution wheels. It is a diagnostic
producer and cannot authorize a merge, release, readiness `PASS`, or change
the required contexts resolved from canonical active policy. PR5B alone will later authenticate and
execute a completed producer in a separately unprivileged runner.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
| --- | --- | --- | --- |
| Contributor | See the precise built candidate for a master commit | A directory of wheels is mutable and ambiguous | Canonical manifest binds each wheel's name, version, size and digest |
| Release engineer | Diagnose a bad candidate safely | A wrong or extra archive can look plausible | A stable code identifies the invalid entry without executing it |
| Future verifier | Select one producer artifact by identity | URLs and latest-artifact lookups are mutable navigation | Manifest/inventory identity is independent of navigation URLs |

The engineer builds the three supported wheels into an empty confined directory,
runs the CLI to a new output file, and inspects canonical JSON. A rejected
directory is repaired and rebuilt; an output is never edited or overwritten.
A producer rerun uploads a new attempt-specific artifact. The later verifier,
not this command, validates hosted provenance and may execute candidate bytes.

## Scope

### In scope

- `tools/ci/build_candidate_manifest.py --dist <path> --output <path>`.
- Closed schema `docs/schemas/cicd/compatibility-candidate-v1.schema.json` with
  schema version `dpone.compatibility-candidate.v1`.
- A pure canonical manifest service and descriptor-confined filesystem adapter.
- Exact three-wheel validation, canonical JSON/digest, create-new atomic output,
  focused tests and an explanatory CI/CD page.
- A future producer workflow contract: only a merged
  `.github/workflows/exact-sha-candidate.yml` `push` on `refs/heads/master`
  can be authoritative; PR5A does not add that workflow yet.

### Non-goals

- Candidate execution, GitHub API provenance, `workflow_run`, cache handling,
  secrets, write/OIDC authority, branch-protection or release mutation.
- Reusing PyPI candidate inventory or Airflow benchmark manifests, which have
  different cardinalities and schemas.
- ZIP extraction, wheel installation, signature/attestation verification, or
  accepting sdists, native wheels, checksums, directories, or extra files.

### Assumptions and constraints

- PR4A is merged first; the implementation task is rebased to that integration
  commit and freezes its specification blob.
- Supported distributions are exactly `dpone`, `dpone-airflow-pack`, and
  `apache-airflow-providers-dpone`; each is represented by exactly one regular
  `.whl` file. Wheel bytes are data only and are never imported or opened as ZIP.
- `dpone.manifest.confined_files` supplies descriptor-pinned reads and hashing;
  no `Path.resolve`, glob, or generic archive inventory is introduced.

## Public contract

### CLI

```text
python tools/ci/build_candidate_manifest.py --dist <directory> --output <new-file>
```

Both arguments are required. The command writes no stdout or stderr on success;
it writes one UTF-8 canonical JSON document plus newline to `--output` and exits
0. Invalid input, missing/unsafe entries, an existing output, or an I/O failure
exits 1 with one stable redacted `CANDIDATE_MANIFEST_*` code on stderr and leaves
no output. Argparse usage errors exit 2 with no stdout. There is no force,
overwrite, append, network, live, or manifest-input option.

`--dist` must be a real directory beneath a non-symlinked parent. It contains
exactly three regular wheel files and no other entries. `--output` parent must
already exist, be a real directory, and must not be the dist directory; output
is created with exclusive create, file fsync and parent fsync. Existing outputs
are never idempotently reused because a producer attempt must retain its own
artifact identity.

### Python API and schema

Canonical code lives in `dpone.manifest.compatibility_candidate`; its pure
builder receives already-confined snapshots and returns immutable typed entries
plus canonical bytes. The thin CLI composes the filesystem adapter. The closed
document fields are `schema_version`, `entries`, and `inventory_digest`.
Each entry has `filename`, `distribution`, `version`, `sha256`, and `size_bytes`.
Entries are lexicographically sorted by ASCII filename; `inventory_digest` is
`sha256:` followed by SHA-256 of canonical JSON with `inventory_digest` omitted.

Wheel names are parsed with the PEP 427 distribution-version prefix rules.
Distribution comparison normalizes runs of `-`, `_`, and `.` to `-` and lowercase;
versions are retained as the wheel's normalized PEP 440 version. The three
normalized distributions must occur exactly once and their versions must equal
the installed project metadata versions. Invalid names, versions, duplicate
names/distributions, controls, non-lowercase digest, symlinks, non-regular files,
descriptor identity changes, and unknown schema fields are rejected.

Limits are public and fixed in the schema/service: 3 entries; filename 255 bytes;
distribution 128 bytes; version 128 bytes; a wheel at most 512 MiB; aggregate
wheel bytes at most 1 GiB; manifest at most 64 KiB; JSON depth at most 4.

### Artifacts and evidence

The future producer packages exactly four root regular members—the three
manifest entries and `compatibility-candidate.json`—inside one bounded,
deterministic, uncompressed raw USTAR file. It uploads that one file directly
with `archive:false`, `overwrite:false`, and `retention-days:90`; direct upload
cannot accept four separate files. The filename and provider artifact name are
`exact-sha-candidate-<run-id>-<run-attempt>.tar`. Producer identity later includes
repository, resolved workflow ID/path, event/ref, subject SHA, run/attempt,
artifact ID/name/provider digest, and manifest inventory digest. Artifact URL is
navigation only. A rerun creates a different artifact identity even for the same
subject SHA; no prior evidence is changed or promoted.

The provider-authenticated producer `head_sha`, exact allowlisted workflow and
exact-checkout proof bind the subject SHA. PR5A v1 remains closed and does not
add a self-reported SHA field to the manifest or rewrite wheel bytes. The raw
archive is opened only by PR5B's unprivileged executor; trusted preflight and
evaluation remain data-only.

### Compatibility and migration

No existing public manifest, release candidate, branch protection, or package
format changes. Existing PyPI and Airflow benchmark inventories remain separate
contracts. PR5B may consume only this exact v1 schema; changes require a new
schema version and approved migration. Removing the future producer workflow
rolls back diagnostics only and never affects legacy authority.

## Detailed algorithm

1. Validate argument shapes and open confined non-symlinked `--dist`/output
   parent descriptors.
2. Enumerate one stable directory snapshot; require exactly three regular
   entries and reject a changed directory or entry identity before reading bytes.
3. Parse every filename; match its normalized distribution once against the
   closed expected set and validate metadata version parity.
4. Hash each descriptor-pinned wheel while counting bytes; enforce per-file and
   aggregate limits before and after reads, then revalidate identities.
5. Sort entries, canonical-encode the digest projection, add `inventory_digest`,
   schema-validate the final document, and enforce the manifest bound.
6. Create output exclusively, write bytes/newline, fsync file and parent. A
   crash/error removes only the newly-created incomplete output when safe;
   pre-existing bytes are never altered.

```text
snapshots = confined_snapshot(dist)
require_exact_regular_wheels(snapshots, expected_distributions)
entries = sort(parse_and_hash_descriptor_pinned(snapshots))
candidate = {schema_version, entries}
candidate.inventory_digest = sha256(canonical(candidate))
validate_schema(candidate)
write_create_new_fsync(output, canonical(candidate) + newline)
```

Empty/2/4-entry directories, malformed wheels, races, partial reads, output
conflicts, schema drift and fsync failures are `CANDIDATE_MANIFEST_UNVERIFIED`-style
non-successes; they never yield a partial manifest. A retry starts over with a
fresh output path. This command has no cancellation checkpoint or durable state.

## Architecture and tradeoffs

| Component | Responsibility | Dependencies |
| --- | --- | --- |
| `dpone.manifest.compatibility_candidate` | typed validation and canonical bytes | stdlib/contracts |
| confined-files adapter | descriptor-pinned directory/file snapshots | existing manifest confinement boundary |
| CLI adapter | parse/compose/render stable errors | service only |
| schema/tests/docs | public contract and recovery | no runtime vendor SDK |

No new port or GitHub adapter is needed in PR5A: hosted identity is PR5B's
boundary. No new ADR is needed because ADR 0048 already mandates the exact
candidate/trusted-executor split; a cardinality, provenance, or execution change
would require an ADR amendment.

## Market comparison

| System/version | Relevant capability | Decision | Source/date |
| --- | --- | --- | --- |
| GitHub Actions | workflow artifacts and `workflow_run` handoff | adopt immutable provider artifact IDs, not navigation URLs | [official documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows), 2026-08-28 |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos, Apache Beam | exact GitHub wheel-producer identity | N/A: data-integration products do not define this provider artifact trust boundary | official product scope, 2026-08-28 |

## Measurable differentiation

```yaml
axis: exact immutable candidate inventory
scenario: three-wheel producer directory with reordered enumeration and adversarial extra/race inputs
baseline: no PR5A manifest
metric: canonical-byte stability and unsafe-input acceptance
target: byte-identical valid manifests; 0 unsafe inputs accepted
procedure: fixture, descriptor-race, schema and workflow-contract suites
artifact: compatibility-candidate.json and exact-head CI receipts
limitations: PR5A does not authenticate hosted provenance or execute wheels
```

## Security, test, documentation, rollout and rollback

No credentials, network, archive extraction, imports, cache, secrets or writes
other than the caller-owned create-new file. Tests cover positive, boundary,
schema, symlink, race, partial-write, replay, old-contract non-regression and
workflow static cases. A controlled local wheel build is integration evidence;
only a post-merge default-branch producer artifact is live evidence, and it is
still `UNVERIFIED` until PR5B. Add `docs/cicd/exact-sha-compatibility.md`, a
focused runbook entry, navigation and fixture-backed docs contracts. Rollback
removes only the non-authoritative future producer; retained artifacts stay
historical and no legacy check changes.

## Agent execution plan

The integrator owns schema, workflow, MkDocs navigation and shared docs. Writers
receive a fresh implementation contract with disjoint paths after this spec is
approved. PR5A implementation waits for the actual PR4A integration commit.

## Approval checklist

- [x] Problem, journey, algorithm, failure semantics and compatibility are explicit.
- [x] Architecture/certification/docs reviews were reconciled.
- [x] Current primary-source research and measurable target are recorded.
- [x] Path ownership and validation are planned.
- [x] Maintainer changed status to `APPROVED` (2026-08-28).
