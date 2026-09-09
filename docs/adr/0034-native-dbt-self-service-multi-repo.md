# ADR 0034: Native dbt self-service uses immutable multi-repo promotion

- Status: Accepted
- Date: 2026-07-27
- Amends: ADR 0006, ADR 0007, ADR 0008, ADR 0009, ADR 0024

[ADR 0052](0052-dbt-workspace-release-source-authority.md) adds an explicit
multi-project wire and complete workspace source inventory. Its implementation
and rollout remain in progress; this ADR's singleton defaults are not changed.

## Context

The first dbt publishing compiler reads resolved model metadata and emits local
Airflow artifacts, but its custom dbt pack is not executable through the strict
indexed provider. It also depends on a repository-local dbt project path and
cannot prove build-once promotion between separate Airflow dev and prod
repositories.

Cosmos can render dbt resources as Airflow tasks, but making it authoritative
would duplicate dpone selection, release/deployment identity, credentials,
failure semantics and evidence.

## Decision

Airflow dev is the only editable dbt authority. Airflow prod receives a
CI-managed byte-identical mirror for audit and operations; execution uses an
immutable content-addressed project bundle, never either DAG checkout.

One dbt project is compiled once into an environment-neutral
`dpone.release-set.v2`. It contains or inventories DAG specs, strict workload
packs, schemas, the validated manifest, exact selection lock and bounded project
bundle. Dev and prod bind the same release to separate deployment-set v2
identities. The Airflow bundle commit belongs to deployment evidence, not
release identity.

dpone generates the production DAG natively. Each publishing workflow has one
dbt build/test task, parallel dpone transfer tasks constrained by pools and a
workflow evidence outcome. Cosmos is neither installed nor called by this path.
Co-installed Cosmos DAGs are declared compatibility smoke targets. They remain
`UNVERIFIED` until exact-commit retained evidence exists and do not imply graph
integration.

The strict runtime gains a closed shell-free dbt execution shape and a bounded
init-fetch v2 payload inventory. Runtime resolves Vault credentials, creates a
temporary mode-0600 dbt profile, executes the locked selection, validates
run-results and returns the real dbt exit code.

The pinned SQL Server adapter is governed by immutable policy rather than its
implicit defaults. Authoring and runtime require the same four literal project
flags. Selection admits only the documented SQL model/test graph and binds its
policy ID/digest. The admitted boundary also closes project dispatch, adapter
macro shadowing, adapter configuration, and physical constraints; only column
`not_null` constraints remain admitted.

Execution-critical macro authority is generated, not inferred from a submitted
manifest. Seven trusted roots produce an exact 131-record dbt/dbt-sqlserver
framework closure. A distinct seven-record invocation extension pins
`is_incremental` and admitted `not_null`, `unique`, and `relationships`
generic-test calls. Identity, body digest, direct dependencies and dispatch
families are closed; selected nodes may otherwise call only the exact
metadata-only `dpone_publish` helper. Unused custom macros grant no execution
authority.

Merge keys are cross-engine identifiers, not arbitrary dbt SQL expressions.
Each effective key is an ordered, exact and case-fold-distinct tuple of
enforced contract columns with structural `not_null`; publishing metadata
cannot disagree with dbt config. ERROR-level data tests remain runtime
assurance, not compile-time nullability proof. After lineage projection and
quality gates, ClickHouse rechecks NULL key components and duplicate groups
over the exact isolated finalization table before the finalizer's target
lookup or mutation. It attempts cleanup of every attempt-local table and emits
the table identities and cleanup status for mandatory pre-retry verification.
Each handle is validated once before its target guard, and finalization consumes
immutable copies of those validated inputs; nested packages validate all
members before the first member may finalize.
Failure-path abort owns cleanup exactly once; normal cleanup runs only after a
successful finalize or a post-commit outcome. Once target invocation starts,
failure is commit-unknown (or nested partial-finalize), and attempt-local
tables are retained for reconciliation rather than aborted.

Workflow selection is a project-level ownership decision, not independent
per-workflow validation. Before immutable locks or release outputs are
published, every materialized model in every selected closure must have one
workflow owner. Cross-workflow publish dependencies and shared non-publish
materialized parents fail closed; shared result-bearing tests do not create
model ownership. Eager-selected data tests may be shared only when every model
they read belongs to the current workflow closure; a foreign dependency fails
before lock publication.

The execution pack freezes `pyodbc`, one total SQL execute attempt
(`retries: 1`, so no SQL execute retry),
15-second login timeout, query timeout at 300 seconds below the bounded dbt
process timeout, and Airflow execution timeout at 300 seconds above it. Runtime
revalidates project and graph policy before the mutating build, and evidence
records the effective runtime and policy digests.

Production compilation is fail-closed and atomic. Automatic retries default to
zero. Every rerun remains pinned to its original release, deployment and Airflow
bundle. A possible target commit without durable evidence is `COMMIT_UNKNOWN`;
staged ClickHouse loads retain exact attempt tables for reconciliation. Nested
partial finalization records finalized and retained member identities, while a
confirmed target followed by cleanup failure remains non-retryable and permits
only exact, reviewed attempt-table cleanup.

Production route authority is variant-specific. A platform publish profile
selects exactly one `transport × schema_evolution × airflow_runtime_mode`
combination. The release selection fingerprint binds those coordinates, the
capability snapshot, certification level, evidence status, and sorted evidence
digests. Aggregate connector or route maturity is insufficient.

Dev CI signs one deterministic checksum subject covering the complete release
tree. A separate bot workflow extracts the pinned project bundle into the prod
audit mirror and writes fingerprinted promotion metadata. Protected prod CI
re-verifies the signer workflow, every release byte, mirror, promotion
metadata, exact dev evidence, and expected current deployment before CAS.
Caller-provided attestation or promoter identities are not accepted.
The expected current deployment is protected environment state, never a
reusable-workflow input.

The Airflow provider is the raw dev-evidence producer. A promotion campaign
is derived from the exact release/deployment by a protected controller and is
bounded to 200 workflows under one end-to-end deadline. Airflow origin/version,
bearer token, and the shared journal root are protected environment
configuration, not caller authority. The controller create-only journals the
canonical request, uses deterministic DAG-run identities, reconciles exact
replays, observes every run, and writes one terminal campaign outcome.

The campaign request supplies one digest-shaped evidence-set identity in
DAG-run configuration.
After all declared branches complete, the terminal provider task captures the
exact KPO XCom and TaskInstance identity for every workload and installs
create-only, no-follow JSON evidence below an environment-owned shared root.
It installs evidence before publishing a passed workflow XCom.
The canonical producer format is
`dpone.dbt-airflow-attempt-evidence.v1`; the promotion verifier retains the
older `gitops.airflow_evidence_bundle` as a compatibility input. The provider
does not synthesize legacy bundle/run-spec/profile/pod-contract files that were
not emitted by the dbt runtime.

Caller orchestration identity and protected finalizer identity remain distinct.
The request binds the caller run; final evidence provenance binds
`job.workflow_*` from the reusable workflow. The finalizer requires the exact
request, terminal receipt, expected provider attempts, release, deployment,
pack, workload, and evidence-set identities before producing v2 evidence.

Dev CI also signs the exact `release-set.json` runtime subject. Its portable
GitHub Artifact Attestation bundle is stored outside the release tree and
published under a key derived from the pinned release and subject digests before
the release completion marker. A digest-pinned v2 runtime trust policy contains
the exact repository, signer workflow and workflow commit, predicate, issuer,
runner policy, bounded GitHub CLI version policy, and reviewed offline trusted
root. Init-fetch verifies that bundle without network access before it
publishes ready state or extracts runtime payloads. Production CI executes the
same verifier policy before immutable publication and CAS activation.

## Consequences

- Authors edit only dbt source and do not write Airflow Python or dpone
  manifests.
- Release bytes are identical in dev and prod; bindings and rollout policy may
  differ.
- Airflow parse remains bounded and secret-free.
- Project bundle, release v2 and init-fetch v2 require coordinated producer,
  provider and runtime compatibility.
- Production runtime images require the certified GitHub CLI verifier version;
  missing, stale or invalid offline trust material fails closed.
- Existing branch-local dbt selection locks, execution packs, releases,
  deployments, and evidence do not gain the SQL Server policy fields
  implicitly; regenerate them from source rather than repairing JSON.
- Existing preview manifests whose merge keys are expressions, outside an
  enforced contract, nullable, or sourced from a foreign eager test must be
  repaired and parsed again. Macro/body/dependency drift requires restoring the
  pinned toolchain/source, never editing generated authority data.
- Normal scheduled runs do not export promotion evidence. Requested evidence
  sets are immutable per release/deployment and fail on byte conflicts.

ADR 0045 adds an optional V2 semantic-refresh runtime beneath this product
surface. Its exact selection, journal, fencing, replacement, sealed-artifact,
and ClickHouse generation contracts are new versioned authorities. They do not
widen or reinterpret V1 execution packs, and they do not make the workflow-wide
dbt gate a workflow-wide database transaction.
- Evidence campaign replay is idempotent only for byte-identical requests and
  exact Airflow run configuration; partial or timed-out campaigns never become
  passing evidence implicitly.
- Existing release-set v1 and legacy dbt packs remain compatibility/local
  preview lanes and are not repaired in place.
- Per-dbt-node Airflow visibility is intentionally deferred. A Cosmos hybrid
  projection requires measured user demand, a separate ADR and its own
  compatibility certification.

## Related

- [Approved feature design](../feature-design-dbt-inline-self-service-v1.md)
- [SQL Server adapter guardrails design](../feature-design-dbt-mssql-adapter-guardrails-v1.md)
- [ADR 0006](0006-authority-and-canonical-ir.md)
- [ADR 0007](0007-release-set-and-deployment-set.md)
- [ADR 0024](0024-airflow-executable-init-fetch-wire-boundary.md)
- [dbt integration](../dbt.md)
