# dpone documentation

This directory is the self-service documentation entrypoint for `dpone`.

## Users

- [CLI reference](cli-reference.md) - generated command reference.
- [CLI examples](cli-examples.md) - practical command recipes.
- [Variant C manifests](manifests-variant-c.md) - batch manifests and DSL behavior.
- [Manifest sparse paths](manifest-sparse-paths.md) - GitOps sparse-checkout allowlists for manifests and manifest-owned dependencies.
- [GitOps control plane](gitops-control-plane.md) - plan, verify, bundle integrity, and JSON Schema artifacts for sparse checkout runners.
- [GitOps workload catalog](gitops-workload-catalog.md) - hierarchical workload sets, environment/source/domain overrides, compact Airflow packs, and GitLab child-pipeline rendering.
- [GitOps Airflow runner pack](gitops-airflow-runner-pack.md) - custom dpone image, KubernetesExecutor, KubernetesPodOperator, runner doctor handoff, and final evidence bundle collection for Airflow.
- [Formal Airflow provider and pack reader](airflow-pack-provider.md) - scheduler-safe `apache-airflow-providers-dpone` facade over the lightweight static reader.
- [Airflow artifact trust (fail-closed preview)](airflow-artifact-trust.md) - signed immutable deployment evidence, GitLab key custody, rotation, rollback, and fail-closed recovery; live dev/prod certification is unverified.
- [Airflow artifact attestation operations (preview)](airflow-artifact-attestation-operations.md) - exit classes, partial-package retry, incident handling, rotation, and rollback verification.
- [Airflow exact-cache operations and recovery](airflow-cache-sync.md) - exact deployment activation, loader ACK, retention authorization, diagnosis, recovery, and rollback.
- [Airflow Kubernetes cache deployment](airflow-cache-kubernetes-deployment.md) - official-chart init/watch wiring, bounded cache, fail-open startup, and fail-visible loader behavior.
- [Airflow runtime Pod retention](airflow-runtime-pod-retention.md) - metadata-only plan, conditional cleanup, least-privilege CronJob activation, evidence, rollback, and incident recovery.
- [dbt self-service platform workflows](dbt-self-service-platform-workflows.md) - immutable release, exact activation handoff, dev evidence, prod mirror, and approved production activation.
- [Industrial Airflow self-service architecture](airflow-self-service-architecture.md) - frozen release/deployment/provider contracts and the Phase 1A first-DAG preview path.
- [Airflow provider API](airflow-provider-api.md) - canonical `airflow.providers.dpone` facade and parse-safe loader contract.
- [Airflow v1 public contract](reference/airflow-public-contracts.md) - generated reference for the reviewed beginner CLI, provider, schemas and support policy.
- [Overrides, variables, merge rules](overrides.md) - deep merge, append/replace, and `depends_on` behavior.
- [Conventions](conventions.md) - presets and naming conventions.
- [Registry](registry.md) - source registry metadata (`src_system`, `src_database`, host, type).
- [DAG debugging](dag-debugging.md) - explain/report/subgraph/edge debugging.
- [VS Code](vs-code.md) - YAML schemas and editor setup.
- [Testing](testing/overview.md) - local tests, package smoke, and integration checks.
- [CI/CD](ci-cd.md) - automation map, workflow links, artifacts, and failure runbooks.
- [`dpone ops`](ops-cli.md) - operational controls for certification, data contracts, quarantine, load packages, rollback, and marketplace.
- [Operational control plane](operational-control-plane.md) - certification packs, recovery planning, reconciliation, deployment profiles, staging evidence, and catalog publication.
- [Live certification](live-certification.md) - local-live/vendor-live connector gates, benchmark/SLO evidence, and GitHub Actions workflow.
- [Route readiness](route-readiness.md) - source -> sink -> strategy readiness reports, evidence taxonomy, CLI examples, and runbook.
- [Route bootstrap and doctor](route-bootstrap-doctor.md) - credential-free connection checks, schema discovery, manifest bootstrap, and onboarding go/no-go reports.
- [Route Conformance Lab](route-conformance-lab.md) - deterministic 10,000-row, 200-column exact verification, typed hash, physical-contract, and release-gate evidence.
- [Route execution ledger](route-execution-ledger.md) - idempotent route execution, lease fencing, and commit protocol evidence for one source -> sink -> strategy route.
- [Route state promotion](route-state-promotion.md) - commit receipt protocol and safe source-state advancement for one source -> sink -> strategy route.
- [Route certification pack](route-certification-pack.md) - generated readiness-compatible evidence bundles for one source -> sink -> strategy route.
- [Route live certification](route-live-certification.md) - Docker-live/vendor-live route harness and `route_live_evidence_bundle` for release candidates.
- [Route certify](route-certify.md) - final route release certification bundle and promotion gate over refresh execute/capture/verify evidence.
- [Route certify release](route-certify-release.md) - release-level go/no-go report over first-class route certification bundles.
- [Route release finalize](route-release-finalize.md) - final route release gate with bundle discovery, freshness, provenance, regression checks, and history.
- [Route release candidate orchestrator](route-rc-orchestrator.md) - one-command route release train from certification pack to release evidence pack.
- [Route release candidate executor](route-rc-executor.md) - dry-run or opt-in execution for `route_rc_orchestration.json` with timeout, retry, redaction, and `route_rc_execution` artifacts.
- [Release RC collector](release-rc-collector.md) - collect GitHub CLI PR exports and evidence refs into finalizer-ready release inputs.
- [Release RC finalizer](release-rc-finalizer.md) - release-level merge-train, version, and evidence gate before tagging.
- [Release evidence](release-evidence.md) - exact-master provider-bound pre-tag gate, operator workflow, and fail-closed recovery.
- [Route release gate](route-release-gate.md) - final route-scoped go/no-go receipt for release candidates.
- [CDC apply certification](cdc-apply-certification.md) - credential-free CDC apply correctness, delete semantics, typed hash, and embedded handoff evidence.
- [CDC snapshot handoff](cdc-handoff.md) - snapshot-to-CDC apply evidence gate for replication-grade routes.
- [CDC observability evidence](cdc-observability-evidence.md) - lag, freshness, retention, offset, replay, and throughput SLO evidence for CDC streams.
- [CDC recovery evidence](cdc-recovery-evidence.md) - fault-injection recovery proof for restart, replay, offset ordering, partial commit, poison event, and retention pressure.
- [CDC schema evolution evidence](cdc-schema-evolution-evidence.md) - schema-change capture, compatibility, DDL dry-run, backfill, approval, and offset-ordering evidence.
- [CDC schema apply](cdc-schema-apply.md) - governed target DDL dry-run/apply reports and typed serving refresh evidence for CDC schema changes.
- [CDC promotion gate](cdc-promotion-gate.md) - final replication readiness bundle and offset promotion decision for CDC streams.
- [CDC runtime orchestrator](cdc-runtime-orchestrator.md) - bounded CDC read -> apply -> durable offset commit loop for replication-grade streams.
- [CDC poison quarantine and replay](cdc-poison-quarantine.md) - fail-closed poison-event quarantine, inspection, and replay execution without offset mutation.
- [CDC compare and repair](cdc-compare-repair.md) - source-to-ClickHouse CDC log consistency compare, bounded repair planning, and offset-safe repair execution.
- [CDC retention gap auto-resync](cdc-retention-resync.md) - source retention gap checks, bounded resync planning, and offset-safe resync execution.
- [CDC live runtime adapters](cdc-live-runtime-adapters.md) - live MSSQL -> ClickHouse readers, ClickHouse sink apply, and SQL-backed offset storage.
- [ClickHouse CDC materialization](cdc-clickhouse-materialization.md) - current-state serving tables from append-only ClickHouse CDC logs.
- [ClickHouse CDC typed materialization](cdc-clickhouse-typed-materialization.md) - typed current-state serving tables projected from append-only ClickHouse CDC logs.
- [Manual integration matrix](testing/manual-integration-matrix.md) - source -> sink x strategy certification category.
- [Testing runbooks](testing/index.md) - test taxonomy, integration matrix, local mock gates, and failure recovery.
- [Release](release.md) - OSS release process.
- [Runtime Docker image](runtime-image.md) - repeatable CLI image with MSSQL and ClickHouse native tools.
- [Source -> sink matrix](source-sink-matrix.md) - one guide per supported source/sink combination.
- [Connector overview](connectors.md) - all database, API, Kafka, and provider connector docs.
- [GitHub Pages docs site](github-pages.md)
- [Load strategies](load-strategies.md) - includes the short Postgres XMin strategy entrypoint.
- [Backfill](backfill.md) - deterministic chunks, durable resume, and atomic
  MSSQL shadow publication for large initial loads.
- [Postgres XMin](postgres-xmin.md) - same-snapshot key reconciliation,
  target-atomic MSSQL state, delete semantics, and recovery guidance.
- [Source preparation and load governance](load-governance.md) - dbt-style hooks, typed source refresh graphs, Airflow-visible tasks, quality gates and load evidence.
- [Runtime decision audit](runtime-decision-audit.md) - structured logs, JSON summaries and load-step audit rows for auto decisions and fallbacks.
- [Data Product SLO, assertions and incidents](data-product-slo.md) - data product quality assertions, SLOs, access/privacy gates, error budgets, policy waivers, authority/signing, compliance audit packages, incidents, fleet gates and release closeout artifacts.
- [Data Product Cost Governance](data-product-cost-governance.md) - cost models, budget/capacity gates, forecasts, bundle evidence and registry stages for FinOps release control.
- [Data Product Progressive Delivery](data-product-progressive-delivery.md) - release rings, shadow validation, ring gates and promotion receipts for staged data product rollouts.
- [Data Product Remediation Runbooks](data-product-remediation.md) - owner-routed repair plans, operator runbooks, controlled execution receipts, remediation gates and fresh-evidence closeout for blocked Trust Center releases.
- [Load lineage](load-lineage.md) - canonical `__dpone__*` columns, load IDs, row IDs, and `__dpone__loads`.
- [Extraction lifecycle](extraction-lifecycle.md) - truthful source snapshot timing and terminal artifact ownership.
- [Nested normalization](nested-normalization.md) - dlt-like root/child table split for nested JSON with parent/root/list lineage IDs.
- [Type inference](type-inference.md) - source metadata, sampled profiling, confidence, and empty string vs NULL behavior.
- [Schema contracts](schema-contracts.md) - explicit logical column contracts, enforcement modes, and variant-column runbooks.
- [Schema Identity](schema-identity.md) - stable semantic ids, rename aliases, deprecation windows, and `__dpone__nc__` interaction.
- [Schema impact](schema-impact.md) - downstream dependency blast-radius plans and approval gates for migration packs.
- [Physical design](physical-design.md) - target-specific DDL controls for types, indexes, partitioning, storage, compression, and ClickHouse LowCardinality.
- [Schema migration control](schema-migration-control.md) - nested `dpone schema migration` plan/baseline/apply/history/rollback evidence, shadow migration phases, environment promotion, attested review bundles, and ledger workflow.
- [Runtime data contracts](data-contract-runtime.md) - row-level enforcement, quarantine, variant columns, and evidence artifacts.
- [Streaming-safe contracts](runtime-fast-path-contracts.md) - contract wrappers for streaming, file, partitioned and native fast paths.
- [Physical DDL apply](physical-ddl-apply.md) - online-safe DDL execution, safe-window handoff, and blocking DDL runbooks.
- [Production profiles](production-profiles.md) - `production_safe` defaults for fail-closed runtime behavior.
- [Unified run evidence](unified-run-evidence.md) - checksumed run evidence packs for certification and incidents.
- [Type mapping matrix](type-mapping-matrix.md) - cross-system type conversion defaults and caveats.
- [Schema evolution](schema-evolution.md) - safe drift handling, generated columns, and runbooks.

## Data contracts and control planes

- [Developer manifest sparse paths](developer-manifest-sparse-paths.md) - sparse path taxonomy, dependency rules, extension points, and quality guardrails.
- [Developer GitOps control plane](developer-gitops-control-plane.md) - plan/verify/bundle contracts, DI boundaries, and runner extension rules.
- [Developer GitOps Airflow runner pack](developer-gitops-airflow-runner-pack.md) - Airflow runner DTOs, services, image contract, doctor policy, evidence bundle collector, and no-Airflow-import boundaries.

## Connectors and providers

- [AppsFlyer Pull API](appsflyer.md)
- [CBR XML API](cbr.md)
- [Mindbox Pull API](mindbox.md)
- [Fasttrack Pull API](fasttrack.md)
- [Google Ads API](google-ads.md)
- [Google Sheets API](google-sheets.md)
- [OpenExchangeRates API](openexchangerates.md)
- [SimilarWeb Website Keywords API](similarweb.md)
- [Yandex Webmaster API](yandex-webmaster.md)
- [ClickHouse Integration](clickhouse.md)
- [MSSQL Integration](mssql.md)
- [Generic REST API](rest-api.md)
- [Kafka batch source/sink](kafka.md)

Rollout and deployment notes that mention a specific company infrastructure are kept as historical/internal references and are intentionally not part of the public quickstart path.

## Developers

- [Architecture](architecture.md) - layers, dependency injection, import boundaries, canonical packages.
- [Module-size debt ratchet](module-size-ratchet.md) - exact-SHA checks, no-headroom debt caps, ADR exceptions, and recovery.
- [Developer integrations runbook](developer-integrations-runbook.md) - how to add a new provider or integration.
- [Developer type system](developer-type-system.md) - how to add type resolvers and physical design renderers.
- [Developer schema migration control](developer-schema-migration-control.md) - migration pack, phase ledger, shadow/cutover ports, evidence bundle facade, and future target-backed adapter taxonomy.
- [Developer data product cost governance](developer-data-product-cost-governance.md) - cost option models, estimators, probes, policy boundaries and extension rules.
- [Developer data product progressive delivery](developer-data-product-progressive-delivery.md) - rollout ring taxonomy, shadow validation, promotion receipts and no-mutation extension rules.
- [Developer data product remediation](developer-data-product-remediation.md) - failure signal taxonomy, remediation catalog, controlled execution, runbook rendering and closeout extension rules.
- [Developer schema impact](developer-schema-impact.md) - impact graph models, dependency providers, approval gate taxonomy, and extension rules.
- [Developer Schema Identity](developer-schema-identity.md) - identity resolver, alias projection, migration dialect taxonomy, and extension rules.
- [Developers guide](developers.md) - guide plus generated code metrics.
- [Developer CI/CD guide](developer-ci-cd.md) - how to add or change workflows safely.
- [Developer GitOps control plane](developer-gitops-control-plane.md) - plan/verify/bundle contracts, DI boundaries, and runner extension rules.
- [Developer GitOps Airflow runner pack](developer-gitops-airflow-runner-pack.md) - Airflow runner DTOs, services, image contract, doctor policy, evidence bundle collector, and no-Airflow-import boundaries.
- [Developer route readiness](developer-route-readiness.md) - taxonomy boundaries, extension rules, tests, and dependency constraints.
- [Developer route bootstrap and doctor](developer-route-bootstrap-doctor.md) - onboarding contracts, DI boundaries, schema discovery rules, and no-route-specific CLI logic.
- [Developer Route Conformance Lab](developer-route-conformance-lab.md) - synthetic dataset, exact verifier, release gate, DI boundaries, and extension rules.
- [Developer route execution ledger](developer-route-execution-ledger.md) - route execution stage taxonomy, idempotency, lease fencing, and commit protocol boundaries.
- [Developer route state promotion](developer-route-state-promotion.md) - commit receipt models, state-store protocol, promotion policy, and extension rules.
- [Developer route live certification](developer-route-live-certification.md) - live harness models, policy, route evidence bundle contract, and no-live-IO extension rules.
- [Developer route certify](developer-route-certify.md) - release certification bundle models, policy, DI boundaries, and extension rules.
- [Developer route certify release](developer-route-certify-release.md) - release-level route bundle aggregation, policy boundaries, DI, and workflow contract.
- [Developer route release finalize](developer-route-release-finalize.md) - finalizer taxonomy, discovery, freshness/provenance/regression policy, and history boundaries.
- [Developer route release candidate orchestrator](developer-route-rc-orchestrator.md) - route release train service, policy, JSON contract, and extension rules.
- [Developer route release candidate executor](developer-route-rc-executor.md) - command runner DI, redaction, artifact collection, and execution boundary rules.
- [Developer Release RC collector](developer-release-rc-collector.md) - release-level PR export normalization, generated finalizer inputs, and no-GitHub-API boundary.
- [Developer Release RC finalizer](developer-release-rc-finalizer.md) - release-level merge-train finalizer modules, DI, policy, and no-GitHub-API boundary.
- [Developer route release gate](developer-route-release-gate.md) - release gate models, policy, route evidence aggregation, and extension rules.
- [Developer CDC apply certification](developer-cdc-apply-certification.md) - generic CDC apply fixture, strategy, evidence, and extension rules.
- [Developer CDC handoff](developer-cdc-handoff.md) - generic CDC handoff taxonomy, interfaces, evidence policy, and extension rules.
- [Developer CDC observability evidence](developer-cdc-observability-evidence.md) - CDC telemetry models, SLO evidence factories, extension rules, and dependency constraints.
- [Developer CDC recovery evidence](developer-cdc-recovery-evidence.md) - CDC fault-injection scenario models, recovery policies, evidence factories, and extension rules.
- [Developer CDC schema evolution evidence](developer-cdc-schema-evolution-evidence.md) - CDC schema-change models, DDL governance policy, evidence factories, and extension rules.
- [Developer CDC schema apply](developer-cdc-schema-apply.md) - DDL planner boundaries, apply orchestration, typed refresh evidence, and extension rules.
- [Developer CDC promotion gate](developer-cdc-promotion-gate.md) - CDC promotion decision models, service boundaries, extension rules, and no-live-IO contract.
- [Developer CDC runtime orchestrator](developer-cdc-runtime-orchestrator.md) - runtime loop models, protocols, injected adapters, and offset-commit boundaries.
- [Developer CDC poison quarantine and replay](developer-cdc-poison-quarantine.md) - classifier, quarantine, replay executor, ClickHouse dedupe, and extension boundaries.
- [Developer CDC compare and repair](developer-cdc-compare-repair.md) - compare rows, live readers, repair plans, repair executor, and extension boundaries.
- [Developer CDC retention gap auto-resync](developer-cdc-retention-resync.md) - retention probes, gap policy, resync plans, execution boundaries, and extension rules.
- [Developer CDC live runtime adapters](developer-cdc-live-runtime-adapters.md) - live adapter factory, ClickHouse applier, SQL offset adapter, and extension rules.
- [Developer ClickHouse CDC materialization](developer-cdc-clickhouse-materialization.md) - serving-table materialization taxonomy, boundaries, and extension rules.
- [Developer ClickHouse CDC typed materialization](developer-cdc-clickhouse-typed-materialization.md) - typed projection taxonomy, boundaries, and extension rules.
- [Quality metrics](quality-metrics.md) - LOC, coupling, cohesion, layer metrics.
- [Quality tooling](quality-tooling.md) - ruff, mypy, pre-commit, CI gates.
- [Import rules](import-rules.md) - allowed layer dependencies.
- [Compatibility / deprecations](compatibility.md) - transitional shims and removal policy.
- [ADR index](adr-index.md) - architecture decisions.

## Common local checks

```bash
uv sync --all-extras
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
uv build
```

Generated docs checks:

```bash
dpone docs update-cli-reference --check
dpone docs update-dev-metrics --check
dpone docs update-deprecation-roadmap --check
dpone docs update-shim-removal-plan --check
dpone docs check-docs
dpone docs check-compatibility
dpone docs check-import-rules
dpone docs check-layer-metrics
dpone docs check-architecture-fitness
HEAD_SHA="$(git rev-parse HEAD)"
BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"
if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
dpone docs check-module-size \
  --baseline docs/module_size_baseline.json \
  --base-ref "$BASE_SHA" \
  --head-ref "$HEAD_SHA"
```

## Canonical packages

Prefer these imports for new code:

- `dpone.manifest.*`
- `dpone.dag.*`
- `dpone.runtime.*`
- `dpone.contracts.*`
- `dpone.ports.*`
- `dpone.adapters.*`

Legacy `dpone.core.*`, `dpone.lib.*`, `dpone.source.*`, and `dpone.sink.*` modules remain compatibility shims.

- [Nested normalization testing](testing/nested/index.md) - spill-to-disk, lint, certification and benchmark evidence for nested payloads.
