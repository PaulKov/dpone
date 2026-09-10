# Feature design: dbt SQL Server 1.12 security toolchain upgrade

- Status: APPROVED
- Owner: Data platform
- Issue: N/A — approved security remediation for the CI/CD dependency gate
- Target release: next coordinated dpone patch release
Last verified: 2026-08-26

## Executive summary

The validation toolbox cannot receive a clean vulnerability attestation while
`dpone[dbt-mssql]` pins `dbt-core==1.10.13`: that release constrains
`sqlparse<0.6.0`, while the fixed public wheel is `sqlparse==0.6.0`.

Upgrade the certified SQL Server toolchain to `dbt-core==1.12.3` and
`dbt-sqlserver==1.11.1`.  The target pair resolves the fixed public
`sqlparse==0.6.0` wheel and has the same v12 manifest shape in the hermetic
dpone fixture.  This is a security and compatibility release, not a new dbt
product capability.

The measurable outcome is a public-PyPI-only, hash-locked runtime graph with
no fixed HIGH/CRITICAL `sqlparse` findings, followed by a clean validation
toolbox Trivy attestation.  The change is not production-certified until the
exact SQL Server compatibility and release evidence are green.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
| --- | --- | --- | --- |
| Data engineer | Submit a dbt DAG without waiting for a repeated dependency install | CI toolbox is intentionally not consumable while its scan is red | Required CI uses an attested toolbox and returns the same DAG/dbt evidence |
| Platform operator | Upgrade a vulnerable build graph safely | A forced `sqlparse` override would violate dbt metadata and hide drift | Exact toolchain identity, SBOM, scan and signature agree |
| Incident responder | Roll back a bad upgrade | An old pack could otherwise execute under a different toolchain | Old pack is diagnosed and rejected before database I/O |

Discovery: a developer reads the failed attestation receipt.  Upgrade: a
maintainer merges the coordinated dpone release after the exact candidate
checks.  Operation: source CI regenerates locks from public PyPI and attests
the toolbox.  Recovery: keep the old image/lock for audit only; publish a new
coordinated release rather than editing evidence or relaxing the scan.

## Scope

### In scope

- Exact `dbt-core==1.12.3`, `dbt-sqlserver==1.11.1` and `sqlparse==0.6.0`
  dependency resolution for the dbt-mssql and semantic-refresh extras.
- A new explicit certified toolchain identity and generated contract/evidence
  updates made through their producers.
- Hermetic manifest/selection regression, installed-wheel compatibility,
  package build, hash-lock freshness and SQL Server live certification.
- Upgrade, rollback and CI-attestation documentation.

### Non-goals

- Changing model SQL, target schemas, credentials, route semantics or data
  mutation policy.
- Publishing a private or patched `sqlparse` distribution.
- Making an EOL Airflow 2.10 image security-certified.

### Assumptions and constraints

- Public PyPI is the only runtime package index; exact wheels remain
  binary-only and SHA-256 locked.
- dbt Core 1.12.3 and dbt-sqlserver 1.11.1 are the chosen public releases.
- The existing toolchain uses manifest v12 and run-results v6; those versions
  must be re-proven, never presumed from package versions alone.
- User approval on 2026-08-26 authorizes this migration, but no production
  promotion occurs before the named certification evidence is green.

## Public contract and compatibility

The new contract id is `dbt-sqlserver-1.11-core-1.12-certified`.  New packs,
selection locks, policies, release material and runtime evidence bind this id
and its canonical SHA-256.  They keep manifest v12 and run-results v6 only if
the candidate proof confirms both.

Packs carrying `dbt-sqlserver-1.10-certified` remain parseable evidence but
are rejected before profile rendering, subprocess launch or database I/O.  A
developer regenerates branch-local lock, pack, release, deployment and
evidence through `dpone dbt compile`; hand-editing JSON is unsupported.  A
rollback is a new release bound to an already certified old image, never a
runtime version override.

No CLI flags, Python call signatures, manifest source syntax or connection
secrets change.

### Macro-authority admission

The existing SQL Server authority contains 131 framework macro records. The
candidate Core 1.12 / adapter 1.11 fixture produces 142. This difference is a
security review input, not a generated-baseline update: candidate adapter
macros include masking and full-refresh helpers, and at least one path uses
`run_query`.

The migration therefore adds an explicit macro-diff test before replacing the
baseline. `tools/dbt_self_service/generate_sqlserver_macro_authority.py
--candidate-manifest <candidate-manifest.json> --diff-output
<macro-authority-diff.json>` produces the deterministic review artifact. It
does not regenerate or approve the baseline. The artifact must prove all of
the following from the installed candidate wheels:

- the exact added, removed and body/dependency-changed records are reported;
- no selected dbt node reaches a newly added execution-capable macro unless
  that invocation has a dedicated policy and SQL Server certification;
- the artifact records a dependency path from every affected trusted root to
  every new or changed execution-capable macro, which is stricter than a
  single selection and makes the certification scope reviewable;
- the seven existing trusted roots retain their intended dispatch winners, or
  every changed winner is reviewed as a separate compatibility change;
- `dpone_publish` remains metadata-only; and
- the resulting 142-record authority is generated and committed only after
  those assertions are green.

The test fails closed on an unreviewed new macro, `run_query`, `statement`,
adapter operation or dispatch winner. This prevents a dependency-security
upgrade from silently widening database-execution authority.

### Candidate evidence (2026-08-26)

The public `dbt-core==1.12.3` and `dbt-sqlserver==1.11.1` wheels successfully
parsed the isolated demo without a database connection. Its v12 manifest has
142 framework records: 11 added, none removed and 8 changed from the approved
131-record baseline. The macro-diff reports 17 new or changed
execution-capable records and 35 dependency paths from trusted roots, including
the table and incremental materializations reaching the new mask operations.

This was release-blocking evidence until the exact SQL Server certification
completed. The final run passed on commit `d4ad99b26fc6b32f27ecb335875d6b94a8f490c3`:
[run 32999804362](https://github.com/PaulKov/dpone/actions/runs/32999804362)
executed parse, full-refresh build, incremental build and data tests against a
disposable SQL Server. Its immutable evidence artifact is
`sha256:882cb084213351c79580ffe0b612bfad3fe5dd9337ef234b84da0b97c27b1b47`.

The reviewed candidate manifest has SHA-256
`8470485ba761d5d94f556289a984ad165a6c059b39097f315a378797af2bc10e`; the
macro-diff receipt has SHA-256
`bd2baf2b444104bc2449aec4503289ad41765f2478f85e7b4019edc8100f85dc`. The
17 changed execution-capable macros and all 35 trusted-root paths are bound by
the regenerated 142-record authority baseline. No new project macro or
runtime capability was admitted.

The migration uses `dpone.dbt-publish-policy.v3`. Policy v1 stays byte-frozen
for historical evidence and v2 remains the semantic-refresh contract; neither
is silently reinterpreted as the new runtime.

The path-gated `dbt SQL Server candidate certification` GitHub workflow supplies
that reproducible disposable proof for the exact PR commit. It runs only when
toolchain, authority or demo inputs change (and is manually dispatchable after
the workflow reaches the default branch); it does not add latency to unrelated
PRs. It starts the repository's pinned SQL Server container, seeds only the
demo source, parses, builds and tests the demo twice (including its incremental
path), then uploads the manifest, macro diff and SHA-256 receipt. It never uses
production credentials.

The observer phase imports both the core and its declared lightweight
`dpone-airflow-pack` dependency from the same candidate checkout. Its source
path includes `src`, `packages/dpone-airflow-pack/src` and the repository's test
tools. Origin assertions reject an observer or resource contract resolved from
an unrelated installed package; no published candidate version is required.

Before that first merge, dispatch the existing `Connector certification`
workflow on the candidate ref with only `run_dbt_toolchain_candidate=true`.
That trusted dispatcher invokes the same reusable workflow; all of its other
inputs remain false by default.

## Detailed algorithm

1. Change the single toolchain authority and package metadata together.
2. Generate and review the candidate macro-authority diff; reject any
   execution-capable path without a separate certified policy.
3. Refresh `uv.lock` from public PyPI and prove it resolves exactly one fixed
   `sqlparse==0.6.0` wheel.
4. Regenerate contract schemas/fixtures through their producers and keep old
   pack rejection tests.
5. Run hermetic dbt parse and selection from installed candidate wheels.
6. Run isolated SQL Server compatibility/live certification on the exact
   candidate commit; a skipped or unavailable live check is `UNVERIFIED`.
7. Build and attest the four coordinated dpone distributions, publish through
   the existing trusted-release path, then regenerate example-workloads locks.
8. Build, scan, sign and only then consume the validation-toolbox digest.

Failure at any step stops the release.  Neither an incompatible resolver nor a
red Trivy finding is bypassable by an override, private index or allowlist.

## Architecture and alternatives

| Alternative | Decision | Reason |
| --- | --- | --- |
| Force `sqlparse==0.6.0` under dbt 1.10 | Rejected | Violates dbt's declared upper bound and makes `pip check` non-authoritative |
| Publish an internal patched sqlparse | Rejected | The fixed upstream wheel is public and hash-verifiable; private routing adds credentials to image builds |
| Upgrade only dbt-core | Rejected | The SQL Server adapter is an explicit certified coordinate and must move as a pair |
| Certified 1.12/1.11 pair | Adopted | Resolves the fixed wheel and passed offline manifest/selection proof |

This changes a public execution/evidence identity, so it is a compatibility
migration rather than an internal refactor.  No new module or import edge is
needed; the existing `DbtToolchainContract` remains the single authority.

## Market comparison

| System/version | Relevance | Adopt/reject | Source/date |
| --- | --- | --- | --- |
| dbt Core 1.12 | Direct dependency and artifact compatibility | Adopt its declared dependency bounds; prove our fixture and SQL Server path | [dbt Core releases](https://github.com/dbt-labs/dbt-core/releases), 2026-08-26 |
| Apache Airflow | Orchestration consumer only | N/A: does not define dbt SQL Server package compatibility | [Airflow release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html), 2026-08-26 |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos, Apache Beam | Not the owner of the dbt-core/sqlparse dependency contract | N/A | N/A |

## Security, operations and evidence

Only public PyPI wheel URLs and hashes enter locks.  The release records the
fixed sqlparse wheel SHA-256, lock digest, SBOM, Trivy receipt, Cosign
signature and provenance.  CI logs package names and versions but never index
credentials.  The existing 30-minute MR latency SLO remains unchanged until
the signed toolbox replaces job-time installation.

## Test and certification plan

| Layer | Scenario | Expected evidence |
| --- | --- | --- |
| Unit/contract | New exact toolchain and old-pack rejection | Focused dbt contract tests |
| Macro authority | Candidate 131→142 record diff and selected-node reachability | Reviewed macro-diff artifact; no unreviewed execution-capable path |
| Resolver | Public-PyPI lock contains `sqlparse==0.6.0` with its wheel hash | Byte-identical generated locks |
| Hermetic integration | Parse and select the fixture from installed wheels | v12 manifest and expected selection |
| Package | Four distributions build and install together | Twine and fresh-venv evidence |
| Live certification | Exact candidate against SQL Server | Existing live certification receipt, otherwise `UNVERIFIED` |
| Supply chain | Toolbox image after new release | SBOM, zero fixed HIGH/CRITICAL findings, signature and provenance |

## Documentation, rollout and rollback

Update compatibility/reference/runbook pages with the exact new id and the
regeneration rule.  First release the dpone candidate; second, update the
example-workloads public locks; third, bootstrap and attest the toolbox; fourth,
introduce the reviewed digest into consumers.  Stop at any red evidence.  To
roll back, pin a previously certified image with a matching old pack; do not
mix an old pack with the new runtime.

## Approval checklist

- [x] User problem and journey are explicit.
- [x] Failure and rollback semantics are fail-closed.
- [x] Public compatibility impact is stated.
- [x] Alternatives and public-source research are recorded.
- [x] Tests, evidence, rollout and ownership are defined.
- [x] Maintainer authorization received on 2026-08-26.
