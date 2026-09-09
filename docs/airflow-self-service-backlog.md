# dpone Airflow self-service implementation backlog

This backlog turns the frozen architecture into implementation slices.

## Phase 0: Trust blockers

- Synchronize package versions for `dpone` and `dpone-airflow-pack`. (done)
- Add docs/version consistency CI. (done for docs homepage latest release)
- Publish exact Airflow/Python/provider compatibility matrix. (done in docs and Airflow compatibility workflow)
- Add parse SLO CI for current provider loader. (done with an installed-wheel,
  eight-cell Airflow/Python matrix, 100 DAG / 500 workload `init_fetch`
  fixture, 30 cold/warm samples, RSS and parse-side-effect tripwires, and one
  uploaded JSON evidence artifact per cell)
- Verify published wheels. (done through release workflow PyPI visibility/install smoke for every built distribution, with uploaded evidence)
- Normalize credential env prefix and document migration. (done: canonical `DPONE_CONN_<CONNECTION_REF>_<FIELD>`, legacy fallback documented)
- Certify one golden route such as MSSQL -> ClickHouse incremental merge through Airflow KPO. (partial: an `experimental` static planning candidate exists; current content-addressed live certification evidence remains UNVERIFIED and the production policy fails closed)

Acceptance:

- Version skew check fails CI on mismatch.
- Provider wheel imports without Airflow runtime side effects.
- Published wheel verification leaves a
  `pypi-resolver-smoke-<run-id>-<attempt>` evidence artifact for every tag
  release.
- Parse benchmark reports cold/warm p95 and additional-RSS budgets for exactly 100
  materialized DAGs / 500 workload runtime tasks and fails when required
  no-side-effect probes or JSON evidence are absent.
- Golden route certification is route-level and names source, sink, strategy, transport, schema evolution, Airflow/runtime mode, status, and proof id.

## Phase 1A: First DAG

- Add `dpone init project --airflow`. (done)
- Add `dpone init pipeline <name> --recipe <built-in> --airflow`. (done)
- Add idempotent scaffolding with change plan, conflict detection, unified diff, and rollback journal. (done)
- Add static `dpone check`. (done)
- Add `dpone airflow preview`. (done)
- Add diagnostic `dpone airflow explain`. (done)
- Emit environment-neutral `release-set`. (done for preview and public GitOps schema catalog discovery)
- Emit non-runnable preview `deployment-set`. (done with public GitOps schema catalog discovery)
- Emit `airflow-index.json`. (done with `dpone.airflow-deployment-index.v1` schema catalog discovery)
- Ship canonical provider facade under `airflow.providers.dpone`. (done, including parse-safe
  `CacheResolver` and `DponeTaskGroup.from_pack(..., index_path=...)` resolution for
  cached workload refs, Airflow provider discovery entry point, and legacy root
  deprecation warning)
- Ship `dpone.error.v1`. (done for Phase 1A self-service, static check, connection check, build, cache-plane errors, and public GitOps schema catalog discovery while keeping existing code/message fields)
- Ship 1-3 built-in immutable declarative recipes. (done)
- Write 5-minute First DAG docs. (done)

Acceptance:

- A new user can preview a DAG in 10 minutes or less.
- Golden path uses no Airflow Python authored by the user.
- Airflow parse path makes zero network, DB, Kubernetes, Vault, Variable, or Connection calls.

## Phase 1B: First safe run

- Add runnable `deployment-set`. (partial: environment deployment projection and `dpone airflow build` done, including default build text output with environment, release/deployment IDs, runnable state, artifact counts, runtime delivery, repo-relative projection paths, next cache-sync action, and command-specific failed build diagnostics)
- Add `binding-set` and `connection-registry`. (partial: dev templates, offline validation, late-binding registry refs, environment mismatch guard, unsupported resolver diagnostics, non-empty credential field mapping contract, and public GitOps schema catalog/discovery plus lightweight validator enforcement done)
- Add `credential-runtime`. (partial: dev template, offline validation, environment mismatch guard, Vault auth secret-material guard in both readiness validation and public JSON Schema, and public GitOps schema catalog/discovery plus lightweight validator enforcement done)
- Add `vault_kv` runtime-side resolver. (partial: runtime resolver adapter, lazy Vault client, and explicit rotation semantics validation done)
- Add `init_fetch` runtime artifact delivery. (partial: digest-only planner with plan-time `cache://`/`current` artifact-ref guard, verification-policy enforcement, local executor, beginner CLI local handoff wiring, connector-neutral immutable artifact-registry port, lazy local/S3/GCS/Azure adapters, exact-ID remote publication/materialization CLI, and complete public `init_fetch` delivery contracts done; approved cloud/Kubernetes init-container live evidence remains UNVERIFIED)
- Add cache materializer and current pointer integrity checks. (done locally: content-addressed release/deployment verification, exact deployment/index mirrors, confined size/SHA-256 artifact checks, immutable remote create-or-compare with markers last, bounded exact-ID remote fetch into isolated staging, immutable local install without activation, inter-process promotion lock, expected-absent/update CAS, sealed activation snapshots, read-only pinned releases, canonical relative `current`, durable control writes, exact promoter allowlist, structured mutation details, recovery/retention foundation, CLI and operator runbook; production attestation and approved cloud/Kubernetes live evidence remain UNVERIFIED)
- Add artifact pinning with release/deployment IDs. (done for release/deployment publisher, materializer, deployment projection, runtime handoff, and provider-local index; no executable path resolves remote `current` or `latest`)
- Add safe sample run. (partial: fail-closed beginner CLI, deterministic policy evaluator, explicit static route-candidate detection, certified data-copier registry gate, MSSQL -> ClickHouse bounded copier with native quoted-column `VALUES` inserts, pinned execution-plan contract, runtime-readiness report, concrete pinned init-fetch, local runnable-deployment promotion, atomic evidence, explicit platform `--enable-live-copy`, deployment-scoped automatic beginner live selection, exact manifest-to-pack verification, signed route-attestation verification, runtime policy re-authorization, binding/registry/credential-runtime fingerprint verification before I/O, workload-scoped credential reuse, physical ClickHouse create/TTL/copy/drop lifecycle, explicit execution modes, safe outcome axes, environment mismatch guard, and redacted structured errors done; approved live Sigstore/Kubernetes/Vault/MSSQL/ClickHouse evidence remains UNVERIFIED)
- Add `dpone check --connections` and `dpone check --live`. (partial: configuration-only `--connections`, schema-backed/catalog-registered `dpone.connection-check.v1`, topology-free connections text summary for both passed and failed checks with handshake/logical refs/resolved refs/bridge action/safe resolver details/manual fixes, catalog-registered fail-closed `dpone.live-preflight.v1`, `DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED` live-runner blocker, dedicated `dpone check live: FAILED|OK` text summary with target/runner/planned IO state/resolved refs/planned probes/structured error/fix lines/JSON rerun hint, injected configured-runner contract, and redaction for runner-returned structured errors done)
- Add basic retention protections. (partial: plan-first local deployment retention planner, explicit/evidence-file protected deployment IDs, schema-backed/catalog-registered JSON contracts, CLI report, topology-free retention text actions, and fail-closed current-pointer recheck before delete done)

Acceptance:

- Sample run uses temporary target, read-only source, pushdown/budget policy, and PII-safe output.
- Incomplete `dpone run --sample/--target` usage and non-positive row budgets fail before reading pipeline source with `DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID` or `DPONE_RUNTIME_SAMPLE_SIZE_INVALID`, exit code `2`, and a safe fix command.
- Safe-sample CLI argument validation emits `dpone.safe-sample-cli-argument-validation.v1` and validates against a published/catalog-registered JSON Schema.
- `dpone gitops schema list/show/validate` exposes the public GitOps schema catalog for CI/IDE/platform checks and validates safe-sample payload files, including conditional `allOf`/`if`/`then` guards, without Airflow, Vault, source IO, or runtime imports.
- `dpone docs update-gitops-schema-reference --check` keeps the public schema reference generated from the same registered contracts as the CLI catalog.
- Temporary target planning is available before execution and never reads credentials.
- Temporary target lifecycle executor emits secret-free prepare/cleanup evidence through injected adapters, redacting tokenized and nested secret-like adapter metadata before runtime reports or evidence persistence.
- Temporary target cleanup failures are promoted to top-level runtime errors and fail execution while preserving the independent data outcome.
- Beginner sample-run JSON includes catalog-registered `dpone.safe-sample-source-request.v1` when the policy and temporary target plan can be evaluated.
- Safe sample output includes catalog-registered `dpone.safe-sample-execution-plan.v1`, pins available release/deployment ids from local current deployment context, auto-materializes a local runnable development deployment when preview is current, and reports non-runnable preview or environment-mismatched deployments as structured blockers without cache refresh.
- Safe-sample execution planning rejects incomplete `init_fetch` deployment context, including nested identity/source/verification fields, with `DPONE_AIRFLOW_INDEX_DELIVERY_INVALID` before runtime handoff, so generated plans cannot be marked runnable when artifact delivery metadata is only partially published.
- The nested local deployment context also validates as standalone catalog-registered `dpone.safe-sample-airflow-deployment-context.v1`, including release/deployment ids, workload pack refs, runtime artifact delivery mode, local paths, and parse-side-effect guarantees.
- Safe sample output includes catalog-registered `dpone.safe-sample-runtime-readiness.v1`, lists available runtime contracts including bounded and credential-resolving MSSQL -> ClickHouse copy executors, and reports the remaining blockers from the evaluated plan rather than a hardcoded placeholder list.
- Runtime execution emits catalog-registered `dpone.safe-sample-runtime-execution.v1`. Generic/non-live callers remain fail-closed with `DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED` or `DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`; the explicit validated live path injects pinned init-fetch, physical target prepare/TTL/cleanup, and the bounded MSSQL -> ClickHouse copy ports.
- Runtime report and evidence writer atomically emit catalog-registered `dpone.safe-sample-runtime-execution.v1`, return catalog-registered `dpone.safe-sample-runtime-evidence-write.v1`, preserve safe diagnostic identifiers such as `vault_kv`, and redact secret-like fields plus inline secret assignments before stdout or file persistence.
- Runtime adapter/copier returned errors and evidence-writer exception messages are bounded and redacted before becoming `dpone.error.v1`, so `password=...`, `token=...`, `vault_token=...`, and similar inline secret assignments do not leak through runtime errors or nested data-copy evidence.
- Live preflight runner returned errors are recursively redacted before entering `dpone.live-preflight.v1` or top-level `dpone.error.v1`, so configured live checks cannot leak secret-like diagnostics.
- Runtime runner composes executor plus evidence writer and returns catalog-registered `dpone.safe-sample-runtime-run.v1` with execution payload, evidence write metadata, outcome axes, and structured errors.
- `dpone ops safe-sample-runtime-run --plan-json plan.json` rehydrates a `dpone.safe-sample-execution-plan.v1`, executes pinned local `init_fetch` through the runtime handoff, writes runtime evidence, emits `dpone.safe-sample-runtime-run.v1`, and returns non-zero when the fail-closed copy boundary blocks execution.
- Beginner `dpone run --sample --target temporary` writes catalog-registered `dpone.safe-sample-runtime-handoff.v1` with a saved execution-plan path, plan digest, rerun command, and explicit live-copy command so users do not have to copy JSON from stdout into a plan file manually.
- Beginner `dpone run --sample --target temporary` derives a route-authorization overlay only from the pinned deployment ID and pipeline ID. A complete overlay invokes the existing verified live assembly and shared runner; an absent overlay preserves network-free handoff; a partial, unsafe, expired, revoked, or mismatched overlay blocks before credentials or database I/O.
- `dpone ops safe-sample-runtime-run --plan-json plan.json` validates the public execution-plan contract before importing runtime handoff code, so malformed or incomplete `init_fetch` deployment context fails fast with `DPONE_SAFE_SAMPLE_EXECUTION_PLAN_INVALID`, exit code `2`, and no runtime evidence side effect.
- When `--pipeline-source pipeline.yaml` is provided, `safe-sample-runtime-run` assembles the certified MSSQL -> ClickHouse copier from the existing route registry and emits catalog-registered `dpone.safe-sample-certified-copy-request.v1` in data-copy evidence while still failing closed until live executor ports are injected.
- `safe-sample-runtime-run --enable-live-copy` requires the binding-set, connection-registry, credential-runtime, route attestation, Sigstore bundle, route-certification bundle, and verification policy. It verifies exact signed route/deployment identity plus all three environment payload fingerprints before I/O and assembles one workload-scoped `BindingCredentialResolver` for MSSQL read, ClickHouse write, and ClickHouse create/TTL/drop. Without that explicit flag no credential resolution or source/sink IO is attempted; a forged context route ID cannot authorize production.
- Production route authorization is now a content-addressed `dpone.route-attestation.v1` signed outside dpone and verified against exact certificate identity/issuer, pinned trusted-root digest, certified bundle bytes, release, deployment, environment, runtime image, validity, and revocation policy. `verified`, `invalid`, and `unverified` are evidenced separately; only `verified` reaches the existing internal capability detector. (done locally; approved end-to-end Sigstore/Kubernetes/Vault/MSSQL/ClickHouse evidence remains UNVERIFIED)
- Explicit live execution also verifies the pipeline file path and exact byte digest against the `kind: manifest` dependency in the pinned workload pack. A changed source, a missing manifest pin, or a corrupt pack fails before authorization, credentials, and database I/O; legacy direct live-copier assembly is fail-closed.
- Invalid live-copy input files fail fast with `DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID`, exit code `2`, and a bounded redacted message instead of a Python traceback.
- Beginner sample-run JSON includes `execution_mode` and a secret-free `live_selection`; runtime evidence independently records `local_handoff` or `live_copy`.
- Without an authorization overlay, beginner sample-run JSON includes a local fail-closed `dpone.safe-sample-runtime-run.v1` handoff and evidence artifact without remote artifact fetch, source IO, or physical target DDL.
- Beginner sample-run text and Markdown output show the actual mode, runtime status, data outcome, evidence path, and one retry action without platform commands; JSON retains the platform diagnostic command.
- For the development beginner path, sample-run auto-promotes a local runnable deployment when preview is current, then executes pinned local artifact fetch into `runtime-artifacts` before stopping at the source-copy boundary; non-development environment mismatches fail closed with `DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH`.
- Beginner sample-run top-level `result.errors` selects the most actionable blocker from safety policy, target planning, execution-plan blockers, and runtime-run errors before falling back to the generic not-implemented guard.
- Safe-sample runtime evidence includes digest-only deployment identity: release, deployment, binding-set, connection-registry, credential-runtime, runtime image, Airflow Bundle delivery ref, workload pack fingerprints, and runtime artifact delivery mode.
- Runtime init-fetch adapter converts deployment-context `workload_packs` into the existing `InitFetchExecutor` contract, fetches only pinned `cache://` artifacts, and never resolves `current`.
- Runtime init-fetch adapter rejects malformed verification policy before fetching artifacts: `checksums` must be `required`, and `attestations` must be `optional` or `required_for_prod`.
- Source request planning emits `dpone.safe-sample-source-request.v1` with row, byte, timeout, read-only, PII, temporary-target, and pushdown-proof fields before any source IO.
- Source sample/copy is an explicit DI boundary. The default `FailClosedSafeSampleDataCopier` emits catalog-registered `dpone.safe-sample-data-copy.v1` with zero rows/bytes, nested `source_request`, and `DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED`; certified copiers can replace that boundary later.
- Certified source copiers are registered by existing route certification id through `SafeSampleDataCopierRegistry`; unknown route ids are rejected, readiness can remove `certified_source_data_copier` only when a copier is registered for the route proven by policy, `dpone.safe-sample-data-copier-registry.v1` is catalog-registered, and `RegistryBackedSafeSampleDataCopier` injects that implementation into `SafeSampleRuntimeExecutor` while falling back closed otherwise.
- `MssqlClickHouseSafeSampleCopier` builds `dpone.safe-sample-certified-copy-request.v1` and delegates live work to an injected `MssqlClickHouseSafeSampleCopyExecutor`; the default executor is fail-closed and never reads MSSQL or writes ClickHouse. `MssqlClickHouseSafeSampleCopyConfigBuilder` derives catalog-registered `dpone.mssql-clickhouse-safe-sample-copy-config.v1` from pipeline source and rejects route mismatches with structured errors.
- `MssqlClickHouseBoundedSafeSampleCopyExecutor` implements the certified copy executor port with injected MSSQL reader and ClickHouse writer protocols, enforces read-only/row/byte/write-count invariants, and returns only secret-free metrics without serializing sampled rows.
- `MssqlSafeSampleSqlReader` builds catalog-registered `dpone.mssql-safe-sample-read-plan.v1` from the certified copy request, delegates execution to an injected MSSQL SQL client, and redacts client diagnostics before returning an in-memory batch to the bounded copy executor.
- `ClickHouseSafeSampleSqlWriter` builds catalog-registered `dpone.clickhouse-safe-sample-insert-plan.v1` from the certified copy request and in-memory batch, delegates execution to an injected ClickHouse SQL client, and returns only row/byte metrics plus redacted diagnostics.
- `CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor` resolves source/sink `connection_ref` through a runtime-side credential resolver, creates injected MSSQL reader and ClickHouse writer ports from in-memory credentials, and records only resolver-safe metadata in diagnostics.
- `build_mssql_clickhouse_safe_sample_registry_from_pipeline_source` assembles the certified registry-backed copier from pipeline source, runtime credential resolver, and concrete reader/writer factories without importing runtime infrastructure into service-layer contracts.
- `build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source` assembles the same certified copier from runtime credential resolver plus MSSQL/ClickHouse SQL-client factories, using concrete reader/writer port factories while keeping driver imports and credential objects outside the service layer.
- `dpone.runtime.safe_sample_sql_clients` provides runtime DB-API/ClickHouse connector adapters plus credentials-backed client factories that create lazy connector clients from resolved `CredentialsConfig` without opening network connections at factory creation time.
- Beginner sample-run JSON exposes a route-specific `certified_copy_request` preview for the certified MSSQL -> ClickHouse route without invoking the live copy executor.
- Production pushdown sampling must be proven by trusted external route evidence, never authoring metadata alone. (done for the signed local authorization contract: candidate detection rejects `LIMIT`/`TOP`-only strings, static catalog status, user-authored `connector_capability`, and raw context route IDs; live production certification remains UNVERIFIED)
- Production sample policy fails closed when pushdown capability is unknown or disproven.
- Development full-scan rehearsal is allowed only when `estimated_read_bytes` is present, positive, and within the environment budget; unbounded or non-positive full scans fail with `DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED`.
- Runtime resolves Vault credentials in the runtime pod, not in Airflow parse.
- `dpone airflow publish` conditionally creates release objects then release `_SUCCESS`, followed by deployment objects then deployment `_SUCCESS`; identical retries are no-op and different bytes never overwrite an existing content-addressed key.
- `dpone airflow cache-materialize` requires exact release/deployment IDs, applies per-object and total download budgets, ignores undeclared remote objects, validates downloaded SHA-256 bytes in an isolated canonical cache, installs immutable local trees, and leaves `current` absent/unchanged.
- Artifact delivery JSON validates as historical
  `dpone.airflow-artifact-publish.v1`, exact-promotion
  `dpone.airflow-artifact-publish.v2`, and
  `dpone.airflow-cache-materialize.v1`. The v2 publisher reads every immutable
  object back, validates the reconstructed projection, and binds its
  credential-free registry scope. Base/help/provider imports do not load cloud
  SDKs, Vault, Airflow Connections, or perform registry I/O.
- Evidence records release, deployment, pack, binding-set, registry, credential-runtime, runtime image, Airflow Bundle delivery ref, resolver, connection ref, credential version, `resolved_at`, `version_policy`, and `resolution_scope` metadata. (partial: safe-sample runtime evidence records digest-only deployment identity and live-copy resolver diagnostics; `workload_start` is now enforced by a per-workload resolver cache shared by target lifecycle and copy, while approved Vault live evidence remains `UNVERIFIED`)

## Phase 1C: Compatibility

- Add Airflow Connection operator bridge. (partial: registry/schema validation requires `execution_mode: operator_bridge` and logical `connection_id` values, legacy `connection_bridge` schemas and bridge-plan reports reject/redact URI-like ids and malformed `AIRFLOW_CONN_*` Secret keys, provider operators fail before reading Airflow Connections when compact-pack projections contain URI-like ids or unsafe Secret keys, `check --connections` emits a secret-free JSON bridge intent with required Airflow connection IDs, derived `AIRFLOW_CONN_*` key names, logical/resolved refs, and a `kubernetes_secret_volume` projection plan; text output shows only bridge required/not-required status, affected logical refs, and the bridge-plan command without Secret names, keys, mount paths, env names, URIs, Vault paths, or secret values; compact packs now select `AirflowConnectionSecretVolumeKubernetesPodOperator`, which resolves Airflow Connections only during task execution, derives one immutable create-only Secret per `dag_id/task_id/run_id/try_number/map_index`, stamps matching digest-only lifecycle metadata on Secret and Pod, patches and cleans up only that attempt-scoped reference, fails closed on collisions, redacts Kubernetes failures, and rejects deferrable KPO execution unless `cleanup_policy: retain` delegates cleanup to the platform; runtime parses projected Airflow URI files with `payload_format: airflow_connection_uri` and still fails fast instead of reading Airflow directly; local overlapping-run and lifecycle contracts are complete, while live Airflow/Kubernetes certification remains `UNVERIFIED`)
- Add Kubernetes Secret volume resolver. (partial: runtime file-projection resolver, static/schema non-empty mount/field path validation, runtime non-empty field guard, and restricted `kubernetes_secret_api` DI reader port with non-empty field validation done)
- Add legacy/dev `env_var` resolver policy. (partial: schema/check/runtime require `support: development_only`, validate env var names, block prod, and fail clearly on missing env vars)
- Add retained Airflow Connection Secret GC. (local implementation complete: fixed selectors, metadata-only paginated Secret/Pod inventory, active-Pod and age protection, malformed-object quarantine, plan-first actor-authorized bounded apply, UID/resourceVersion preconditions, schema-backed redacted reports, CLI/runbook/migration docs, and deterministic 10,000-item proof; approved-cluster live certification remains `UNVERIFIED`)
- Add production GC. (partial: actor-authorized confirmed local apply deletes only fresh unreferenced complete candidates and matching stale activations, blocks on pointer/current split state, emits a catalog-registered schema contract, and renders default `cache-retention-plan`/`cache-retention-apply` text summaries with current/delete/protected/deleted/skipped deployment IDs while keeping projection paths and absolute cache roots in JSON)
- Add cache recovery. (partial: local plan/apply repairs missing current pointer/path and missing/mismatched promotion audit state by explicitly re-promoting a selected complete deployment, preserves malformed historical audit lines as warnings, reports incomplete/invalid deployments as non-repairable blockers, emits catalog-registered schema contracts, renders a default `cache-recovery-plan` text summary with status/current/preferred repair/issues/candidates/confirmed apply command, and renders confirmed `cache-recovery-apply` text with recovered deployment/release/current-pointer status while keeping projection paths and absolute cache roots in JSON)
- Add extended operator diagnostics. (partial: `dpone airflow explain` now reports local index pinning, runtime delivery, Airflow Bundle versioning/reproducibility diagnostics, parse-side-effect diagnostics, checksum-verified workload operator summaries, Airflow Connection Secret-volume bridge shape with digest-only Secret reference fingerprints, deferrable cleanup policy issues, planned/invalid index summaries including top-level and nested incomplete `init_fetch` delivery contracts, aggregated check counts, user-facing next actions, actionable topology-free text output with planned-preview routing and `NEEDS_ATTENTION` invalid diagnostics, and public `dpone.airflow-explain.v1` / `dpone.airflow-operator-diagnostics.v1` schemas without reading Airflow, Vault, Kubernetes, Kubernetes Secret topology, or secret values)
- Add migration from legacy `connection_type` / `vault_path`. (partial: static check and connection check now emit explicit migration diagnostics plus catalog-registered `dpone.airflow-authoring-migration-plan.v1` and `dpone.connection-registry-migration-plan.v1` reports with proposed refs/resolver entries and redacted unified diffs instead of letting legacy keys reach runtime; legacy authoring errors include executable `dpone fix <pipeline> --plan|--apply` commands, default fix text output shows PLAN/APPLIED/NO_OP status, target, legacy/applied section counts, planned refs, redacted diffs, and normalized next check/apply actions without echoing absolute CLI input paths, the command safely applies authoring-source migration only, and connection-registry migration remains platform-owned/manual)

## Phase 2: Authoring v1

- Add one compiler boundary for `classic`, `flow`, and `folder`
  sources. (done: all modes use one canonical batch
  compiler; legacy `batch + processes` and `pipeline.v1 + processes` remain
  read-only compatibility aliases with structured markers)
- Publish `dpone.flow.v1` and make it the beginner default. (done with JSON
  Schema, CLI help/reference, semantic/source fingerprints, preview provenance,
  ordinary manifest-loader support, and exact schema/runtime `connection_ref`
  alignment)
- Add bounded folder composition. (done: explicit fragment list, confined and
  budgeted YAML adapter, semantic equivalence, scaffold/check/preview/loader,
  compact-pack dependencies, build-time parity, and safe-sample pinning)
- Add external versioned recipe catalog, profiles, and reusable components.
  (done: one trusted local catalog, exact SemVer/content pins, bounded
  declarative recipes/profiles/components, safe scalar answers, canonical
  compiler/pack/safe-sample closure parity, static Airflow/runtime separation,
  CLI discovery/validation, schemas, docs, and deterministic benchmark evidence;
  remote and signed catalogs remain Phase 4 scope)
- Add selectors with `tag:`, `domain:`, `owner:`, `source:`, `sink:`, `group:`,
  graph `+`, named selectors, state comparison, and selection explanations.
  (done: one pure workload-level engine powers selected static check, immutable
  preview and sequential safe sample; reports/state are schema-backed and the
  Airflow provider remains selector-free)
- Add hermetic `dpone.test.v1` execution with fixtures, deterministic temporary
  targets, and CI summaries. (done: every scaffold emits an executable bounded
  fixture; one credential-free service compiles all authoring modes, applies
  fail-closed full-refresh/append/merge semantics, writes schema-backed redacted
  reports, and imports no runtime, connector, Vault, Airflow, or Kubernetes code)
- Add first-class DLQ contract, reason taxonomy, retention, replay, and PII
  policy. (done: canonical `dpone.dlq.v1` records and checksummed indexes are
  create-only and metadata-first; stable reason codes, bounded PII policies,
  safe row/streaming evidence, plan-first idempotent replay, acknowledgement
  ordering, mark-and-sweep retention, schema contracts, legacy export
  compatibility, and false-replay blocking are implemented)
- Add `execution.visibility: inline|task|group`, visible-task budgets, task
  explosion warnings, and recipe/profile override policy. (done: every new
  DAG-spec node is selector-safe and explicitly resolves `inline`, `task`, or
  `group`; compact packs contain immutable selector-indexed process plans;
  inline sync/deferrable KPO execution validates the returned outcome without a
  second task; expanded hooks and outcome gates receive stable process-scoped
  IDs; TaskGroups reuse explicit group IDs; build-time warn/max budgets block
  task explosions; old unselected packs retain expanded compatibility while
  selected incompatible packs fail closed)
- Complete typed provider stubs and generated schema/reference docs for all new
  authoring contracts. (done: the canonical namespace ships PEP 561 markers,
  an explicit public stub and non-variadic facade signatures; mypy checks the
  lightweight provider, wheel smoke verifies typing files, and one generated
  reference now inventories every canonical authoring schema including
  `dpone.test.v1`)

Acceptance:

- Exactly one editable primary source exists per pipeline.
- Equivalent classic, flow, and folder sources produce one semantic
  fingerprint after canonical compilation.
- Static check, preview, build, test, and ordinary manifest loading cannot
  disagree about source validity.
- Recipe/component execution never occurs during Airflow DAG parsing.
- Selectors explain why every selected node is included.
- Hermetic tests and DLQ artifacts never expose credential or unmasked PII
  material.

## Phase 3: Airflow-native operation

- Add canonical asset inference and partition model with edge provenance,
  multi-writer ambiguity blockers, merged cycle detection, and explicit Airflow
  2 degradation. (implemented locally: one temporal partition, inheritance,
  mismatch/mixed-contract blockers, native Airflow 3.2+ timetables, runtime
  context, and explicit earlier-version downgrade; exact matrix CI and approved
  live event certification remain `UNVERIFIED`)
- Record DAG Bundle backend, versioning capability, bundle version, release,
  deployment, DAG spec, pack, image, binding, registry, and credential-runtime
  identities in run evidence. (implemented locally: provider-owned bounded
  identity is checksum-verified, propagated to KPO params/runtime env, preserved
  in XCom and evidence, and mismatch/missing release evidence fails closed;
  approved production Airflow/Kubernetes run evidence remains `UNVERIFIED`)
- Add reproducible rerun policy for independent bundle and dpone artifact
  selection. (implemented locally: local-only plan command, independent
  `original|latest` selectors, critical fail-closed bundle/retention policy,
  pinned retention refs, schemas and operator docs; versioned/non-versioned
  backend certification and approved production rerun evidence remain
  `UNVERIFIED`)
- Add bounded dynamic mapping modes `internal`, `visible`, and `summary` with
  task-count, concurrency, and pool limits. (implemented locally: static
  fingerprinted pack plans, a 200-item hard ceiling, provider
  `partial().expand()`, pool/concurrency ownership, runtime re-planning,
  PostgreSQL/MSSQL distributed chunk lease and owner-CAS completion; exact
  Airflow 2.10/2.11/3.2/3.3 matrix CI passed on Python 3.11/3.12; approved
  multi-pod live evidence remains `UNVERIFIED`)
- Add OpenLineage/OTel export with mandatory internal correlation identity
  across Airflow run, dpone run, release, deployment, pack, evidence, and pod.
  (implemented locally: strict `dpone.airflow-correlation.v1`, final evidence
  join, deterministic OpenLineage UUIDv5 and versioned custom facet, OTel
  resource/data-point projection, no synthetic spans/baggage or Prometheus
  high-cardinality labels; approved Airflow/Kubernetes/collector live evidence
  remains `UNVERIFIED`)
- Certify resolver rotation and recovery runbooks. (implemented locally:
  workload-scoped N to N+1 rotation, outage, recovery, concurrency, fail-closed
  unsupported policies, KV2 version/field evidence, stable safe errors, and a
  resolver matrix/runbook; `vault-kv-client 0.1.0` does not expose KV2 metadata,
  so approved Vault/Kubernetes/Airflow production evidence remains UNVERIFIED)

Acceptance:

- Airflow 3.2+ emits partition-aware assets when supported; earlier versions
  degrade without changing dpone evidence semantics.
- Dynamic mapping is bounded opt-in and cannot replace the dpone campaign
  ledger by accident.
- Critical reruns pin both Airflow delivery identity and dpone
  release/deployment identity.
- Parse path still performs zero network, database, secret, Variable,
  Connection, or cache-refresh calls.

## Phase 4: v1.0 standardization

- Freeze stable public CLI, Python API, schemas, provider namespace, and support
  windows. (implemented locally: reviewed machine-readable baseline, real
  parser/provider/schema/package checker, generated reference, formal
  `apache-airflow-providers-dpone` distribution, CI report artifact and seeded
  breaking-mutation tests; remote Airflow matrix and release audit remain
  `UNVERIFIED` until the exact commit completes CI)
- Publish migration tooling and enforce SemVer/deprecation policy for all
  compatibility aliases. (partial: explicit plan-first `classic`/`flow`/`folder`
  source migration now covers all six directions, proves canonical semantic
  equality, guards source bytes, publishes schema-backed redacted receipts, and
  retains user-owned fragments; the complete v1 alias migration suite remains
  pending)
- Publish route certification matrix for source x sink x strategy x transport x
  schema evolution x Airflow/runtime mode. (implemented locally: schema-backed
  `dpone certify routes` projects the explicit candidate catalog and bounded
  exact-commit vendor-live/attestation evidence into fail-closed experimental,
  route, production, and enterprise statuses; the current MSSQL -> ClickHouse
  KPO row remains experimental and external production/enterprise evidence is
  `UNVERIFIED`)
- Publish signed recipe/registry bundles and trusted catalog policy.
  (implemented locally: deterministic create-only bundles, external cosign
  verification, bounded semantic validation, six public schemas and a
  1,000-entry benchmark; approved keyless Sigstore CI evidence remains
  `UNVERIFIED`)
- Publish conformance suite for third-party providers, recipes, resolvers, and
  certified routes. (implemented locally: four closed evidence profiles with
  fail-closed `FAIL`/`UNVERIFIED` aggregation and generated contract fixtures;
  resolver and route production profiles cannot pass without live evidence)
- Produce at least two independent production reference deployments.
  (collector implemented locally: `dpone certify self-service` cross-checks
  exact release/deployment, route signer, Airflow run/correlation, pod, and
  runtime identities; real approved deployments remain `UNVERIFIED`)
- Run milestone usability studies with at least five first-time users and
  publish time-to-first-DAG/sample evidence. (privacy-safe schema, deterministic
  metric policy and report implemented locally; executable CI fixtures are not
  human evidence, so the milestone remains `UNVERIFIED`)

Acceptance:

- [x] P0 pipeline-first UX docs: glossary, What-to-commit, advanced Data Engineer
  CJM, disclaimer markers, Advanced Airflow nav demotion (docs contracts only;
  see `docs/feature-design-airflow-pipeline-first-ux-p0.md`).
- [x] P2 Source→Sink / route framing docs: glossary route/recipe/connection_ref,
  First DAG route callout + recipe↔route table, bridge marker on BRIDGE_PAGES
  (docs contracts only; see `docs/feature-design-airflow-pipeline-first-ux-p2.md`).
- First DAG preview is at most 10 minutes and first safe sample is at most 15
  minutes for at least 80% of each five-user milestone sample.
- The beginner journey uses no authored Airflow Python and no more than five
  commands.
- Conformance and route certification evidence is tied to the exact released
  commit, package set, Airflow/Python matrix, and environment.
- No skipped, mocked, stale, or unavailable live profile is reported as PASS.
