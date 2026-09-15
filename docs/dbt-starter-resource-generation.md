# Generate installed dbt starter resources

**Audience:** dpone package maintainers preparing the installed native starter.

This developer tool checks or regenerates the package mirror and dependency files
used by the starter. It does not expose the analyst `dpone init dbt` command,
provision SQL, qualify a runtime, or publish a release. The complete managed
package and its immutable revision are still prerequisites; current component
tests do not establish an installed end-to-end workflow.

## Prepare an immutable source

Use a local dpone checkout with the approved complete fourteen-file canonical
`packages/dbt-dpone` package committed. Supply its full lowercase forty-character
Git commit ID. The revision must already exist locally and be an ancestor of the
checkout HEAD. Canonical working bytes, executable modes and index entries must
match that revision; missing, extra, staged or unsafe package files reject.
The six authored starter templates must be present and remain unchanged.

The dependency origin is fixed to the public dpone Git repository and its
`packages/dbt-dpone` subdirectory. Generation requires the current interpreter's
certified dbt Core 1.12.3 and dbt-sqlserver 1.11.1 distributions. A version match
is a generation prerequisite, not managed-runtime qualification.

In the commands below replace `CHECKOUT` and `FULL_COMMIT_SHA` with those explicit
values. Run from the dpone tooling environment:

```bash
uv run --locked --no-sync python -m tools.dbt_self_service.generate_starter_resources --source-repo CHECKOUT --revision FULL_COMMIT_SHA --check
```

Check mode reads local Git metadata, canonical bytes, installed-resource mirrors
and dependency files. It runs no dbt process, fetch, lock creation or asset repair.
It verifies consistency, not proof that externally supplied lock bytes were
produced by dbt. Raw Git index inspection avoids invoking configured content
conversion filters.

## Generate and verify

```bash
uv run --locked --no-sync python -m tools.dbt_self_service.generate_starter_resources --source-repo CHECKOUT --revision FULL_COMMIT_SHA
```

Generation first validates source, templates, destinations and pending recovery.
It then runs the current interpreter's pinned dbt deps in an isolated temporary
project, using the fixed public dependency and captured immutable revision.
It verifies the actual returned lock and all resolved package bytes. No lock or
future commit pin is fabricated.

The tool writes exactly sixteen resources: fourteen mirrored package files plus
`packages.yml` and `package-lock.yml`. It never rewrites the canonical package or
the six authored templates. After dependency generation it rechecks captured
source/template identities and, under the existing authoring lock, destination
identities before writing. Identical output is a no-op.

Replacement is atomic per file, not across the whole set. Readers can observe
mixed resources during generation; package builds must use a successfully
verified final tree. The writer retains permissions and uses bounded durable
backups, mutation observations and identity-aware compensation. A digest match
alone does not identify an operation-owned file.

One JSON object is written to stdout. Subprocess output, ambient exception text
and file contents are not included. Help performs no filesystem or dbt work.

| Exit | Meaning | Next action |
| ---: | --- | --- |
| 0 | Requested generation or check passed; recovery inspection is clear. | Inspect changed paths and run the offline check before packaging. |
| 1 | Validation or generation failed without a known pending recovery obligation. | Correct the input/installation; inspect the returned status before retrying. |
| 2 | Arguments are invalid. | Correct the mode and required arguments using `--help`. |
| 3 | Recovery or cleanup needs inspection. | Preserve artifacts and follow the recovery procedure below. |

`--check` and `--recovery-report` are mutually exclusive. Recovery-report mode
rejects `--revision`. A successfully read pending report still returns exit 3.
If a failure occurs before writer entry, the CLI rechecks recovery against the
captured root identity. It preserves any actual retained dependency workspace;
it does not follow a replacement root to inspect someone else's files.

## Read the result and diagnose failure

A failed offline prerequisite check intentionally returns a generic envelope:

```json
{"mode": "check", "status": "FAILED"}
```

This exit-1 status does not identify which prerequisite failed. Check these in
order before retrying:

1. The selected checkout exists, its identity has not changed, and the requested
   full commit resolves locally and is an ancestor of HEAD.
2. The canonical package contains all fourteen approved files; raw working bytes,
   executable modes and index entries match the selected commit. Check for staged,
   extra, missing or unsafe files. The initial five-file package is incomplete.
3. All six authored templates are present. For check mode, both dependency files
   and the complete fourteen-file mirror must already exist and match the source.
   Check lock freshness, exact dependency coordinates and UTF-8 content.
4. For generation, the selected commit must also be available from the fixed
   public Git origin. A local-only commit is insufficient. Confirm the approved
   revision is available there, network access works, and the current interpreter
   has the required distributions. Offline check performs no remote lookup.
5. Run the recovery-report command before another generation attempt. If the
   checks above do not explain the failure, ask a maintainer to inspect the
   environment with the mode, dpone version and approved revision. Preserve any
   returned workspace; do not paste credentials or raw captured output.

Successful check returns `mode`, `status: PASS` and `revision`. Successful
creation/update returns a writer receipt with `status: APPLIED` and its
`changed_paths`; a same-byte generation uses `NOOP`. For example, this is a
representative NOOP shape, not evidence of a completed real generation;
`FULL_COMMIT_SHA` denotes the supplied revision:

```json
{
  "mode": "generate",
  "revision": "FULL_COMMIT_SHA",
  "passed": true,
  "status": "NOOP",
  "changed_paths": [],
  "recovery_required": false,
  "operation": null,
  "unpersisted_recovery_paths": []
}
```

A clean recovery inspection has a different envelope:

```json
{
  "mode": "recovery-report",
  "pending": false,
  "status": "CLEAR",
  "operation": null,
  "paths": [],
  "unresolved": [],
  "observations": [],
  "mutation_outcomes": [],
  "discovery_required": false,
  "rollback_outcomes": [],
  "inverse_outcomes": []
}
```

Pending recovery observed before an operation uses those report fields at the
top level. If recovery is discovered while handling a failure, the CLI instead
sets top-level `status: RECOVERY_REQUIRED` and includes the canonical report
under `recovery`. If the captured root cannot be verified, that nested object is
`{"pending": true, "status": "ROOT_UNAVAILABLE", "discovery_required": true}`.
Do not assume every result has the same field layout.

| Field | Interpretation |
| --- | --- |
| `changed_paths` | Writer-reported owned changes, not an instruction to remove them. |
| `operation` | Recorded operation UUID when available; null does not prove no pending work. |
| `paths`, `unresolved` | Known retained locations and unresolved operations; the list may be incomplete. |
| `observations` | Recorded path/phase/identity observations; current ownership still requires verification. |
| `mutation_outcomes` | Path, committed flag and cleanup-required flag from native replacement. |
| `rollback_outcomes`, `inverse_outcomes` | Actual recorded rollback/inverse-exchange results. |
| `unpersisted_recovery_paths` | Known obligations that could not be persisted; preserve them alongside the report. |
| `retained_workspace` | Actual dependency workspace retained because cleanup was uncertain; preserve it. |
| `recovery`, `discovery_required` | Failure-time recovery report and whether additional inspection is required. |

A clean compensated failure reports `ROLLED_BACK` with exit 1; a failed or
uncertain compensation returns exit 3. Neither is a successful generation.

## Inspect and recover

```bash
uv run --locked --no-sync python -m tools.dbt_self_service.generate_starter_resources --source-repo CHECKOUT --recovery-report
```

This mode reads the bounded report without dependency generation, source revision
resolution or file repair. The fixed operation metadata location is
`.dpone-starter-resource-transactions/<operation UUID>` under the selected
checkout. A failure-only `recovery.json` can survive removal of the main manifest
or event log. Malformed metadata, incomplete observations and unresolved native
leaf journals block further generation, including an otherwise identical no-op.

Stop competing writers before repair. Preserve the manifest, events, backups,
reported retained paths and any `retained_workspace`. Compare current file and
directory identities and digests against actual captured receipts. Restore only
objects whose ownership is established, using the canonical confined filesystem
primitives; preserve third-party or unknown files, even with identical contents.
An interrupted mutation without a durable ownership observation needs manual
inspection. `discovery_required` means reported paths may not be exhaustive.

This tool has no automatic recovery-write or resume command. Do not delete a
journal to enable retry. Remove metadata only after every target, backup and
cleanup obligation has been resolved; never recursively remove unknown entries.
Keep recovery data out of commits. If the captured root is unavailable, the
report indicates `ROOT_UNAVAILABLE` and requires discovery instead of traversing
a replacement. Cleanup uncertainty always prevents success.

[dbt self-service reference](dbt-self-service-reference.md) ·
[Starter resource error](errors/DPONE_DBT_STARTER_RESOURCES_INVALID.md) ·
[Native execution](native-generation-execution.md)
