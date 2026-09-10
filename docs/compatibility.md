# Compatibility

## Native BCP safety corrections in 0.74.35

Upgrade core and the optional accelerator together. Native provider revision 2
is required for acceleration; older providers fall back to Python in `auto`
and fail before file access in `required`. The wire schema version is unchanged.
Re-export retained artifacts whose physical layouts no longer validate; never
edit their prefixes or hashes manually. Raw `char NOT NULL` now fails before
native export because multibyte conversion makes its unprefixed boundary
ambiguous. Use ODBC row-stream or a governed projection with accurate metadata.
Decimal precision loss and target calendar/integer overflow now fail explicitly
instead of silently truncating or clamping values. See the
[native BCP recovery and validation guide](native-bcp-docker-validation.md).

## Workload selection v1

`--select` and `--exclude` are additive project-workload options on `check`,
`airflow preview`, and bounded `run --sample --target temporary`. Existing
single-pipeline calls remain valid. Existing `run --selector` keeps its older
meaning: one process selector inside a batch manifest. It is not renamed or
reinterpreted by workload selection v1.

Selected preview output is an ordinary immutable release/deployment projection,
so published provider APIs and previously generated release ids remain valid.
State and named-selector files are build-plane inputs and are never required by
the Airflow scheduler.

`dpone` follows a compatibility-first approach for manifests, public imports, CLI commands, and runtime connector contracts.

## MSSQL run-state identity compatibility

Run-state table capability and workload policy are separate contracts. Legacy
MSSQL workloads that retain `atomicity: after_target` and runtime provisioning
continue to read and write their v1 table with the original
`(dag_id, execution_date)` identity. Installing this release does not require
adding `process_name`, hashing old rows, or changing the manifest. If that
legacy path encounters an already-v2 table, it uses the v2 capability without
rewriting history.

Process-scoped v2 is mandatory for `target_atomic` and `key_snapshot` routes.
Those routes use external provisioning and validate the exact v2 column and
unique-index shape before source I/O. A v1 table fails with
`mssql_run_state_v1_to_v2_migration_required`; operators create a separately
named v2 table and change `state.run_table.name`. Automatic v1-to-v2 backfill
is unsupported because v1 history has no trustworthy process dimension.

## Airflow cache-retention state compatibility

Historical apply evidence v1/v2 and recovery WAL v1 remain readable. New
destructive attempts use receipt v2, occurrence-keyed recovery WAL v2,
cumulative recovery acknowledgement v2, and v3 evidence containing the
approved UUIDv4 `review_id`.

Recovery v1 is normalized in memory without guessing an operation owner and is
written as v2 only on the next controlled mutation. A matching ACK v1 is
upgraded before an unrelated journal write. An older binary that does not
understand v2 must not run over that cache: stop mutation, archive the complete
receipt/WAL/ACK/activation/current-pointer state, and follow the
[retention state downgrade runbook](airflow-cache-retention-state-downgrade.md)
for certified fresh `emptyDir` reconstruction. Persistent-volume downgrade has
no public evidence contract in this release and must stop as `UNVERIFIED`.
Hand-editing state for downgrade is unsupported. See
[ADR 0043](adr/0043-airflow-retention-occurrence-review-identity.md).

## Runtime image certification evidence

`dpone.runtime-image-certification.v1` remains a closed historical contract
with exactly nine checks. dpone `0.73.28` and later produce
`dpone.runtime-image-certification.v2`, which adds the required digest-executed
Cosign check.

Both versions remain parseable for audit. Only `v2` authorizes a new runtime
image alias mutation. A completed, exact certification-SHA-bound `v1`
publication journal may be replayed to recover its already committed output
without registry I/O; an incomplete `v1` publication must be recertified as
`v2`. dpone never rewrites or upgrades historical evidence in place.

## Airflow deployment attestation compatibility

| Contract or behavior | Read compatibility | New production activation |
| --- | --- | --- |
| `dpone.runtime-artifact-trust-policy.v2` | supported GitHub/SLSA release-set authority | contract implemented; exact-environment live activation remains `UNVERIFIED` |
| `dpone.airflow-deployment-trust-policy.v1` | supported deployment-scoped Cosign authority | fail-closed preview; signed dev/prod activation remains `UNVERIFIED` |
| both authorities configured | parsed only far enough to classify the conflict | rejected before registry I/O |
| immutable deployment published before its Cosign overlay | deployment identity remains unchanged | contract-eligible only after verification; live activation still needs exact-environment evidence |
| `dpone.runtime-fetch-ready.v1` | remains readable for historical/non-deployment-attested runs | cannot represent deployment verification evidence |
| `dpone.runtime-fetch-ready.v2` | additive backend-neutral attestation evidence | required when deployment-scoped verification is selected |
| rollback deployment | exact IDs remain readable | allowed only while its attestation/key remain trusted and not revoked |
| revoked attestation or key | immutable package remains audit-readable | rejected before cache activation or source I/O |

## Project layout compatibility

Missing `layout` in `dpone.yaml` is exactly the existing `flat` behavior:
pipeline sources remain under `pipelines/`, domain orchestration remains under
`domains/`, and hermetic tests remain under top-level `tests/`.

`layout.mode: domain_first` is an explicit additive choice. It moves authoring
authority to:

```text
<layout.root>/<domain>/ownership.yaml
<layout.root>/<domain>/pipelines/<pipeline-id>/pipeline.yaml
```

Domain-first discovery is build-plane only. Airflow still parses immutable
deployment indexes and never scans authoring directories. The generated
`dpone.workload-index.v1` is a projection, not a replacement source of truth.
Its closed v1 projection binds layout root/scope, ownership, canonical source
semantics, dependencies, logical connection refs, and durable Airflow
participation. Baselines produced by development snapshots before this
contract, or baselines whose declared fingerprints do not recompute, must be
regenerated; dpone does not reinterpret them.

New scaffolds persist `metadata.airflow` as a boolean. Existing sources that
omit it retain enabled behavior. `metadata.airflow: false` remains a valid
runtime workload and produces no Airflow DAG. A first disabled preview creates
no artifacts. If a verified current local preview still advertises the same
pipeline and no other workload, dpone promotes an empty immutable compensating
projection before returning `DPONE_AIRFLOW_DISABLED`. A multi-workload current
is preserved and requires the reported bounded project refresh, preventing
unrelated DAG removal.

No automatic file migration occurs between layouts. Changing `layout.mode` or
`layout.root` without moving and validating the corresponding authoring sources
fails closed. Existing flat projects require no edits.

`dpone init pipeline` is the canonical beginner authoring command in both
layouts. Historical `dpone workload init` remains an advanced compatibility
surface for GitOps catalog workflows; it is not the domain-first golden path.

## Supported Python versions

`dpone` targets modern Python versions supported by the current packaging metadata. Use the published PyPI metadata as the source of truth for exact supported versions.

## Airflow provider compatibility

The scheduler-side provider contract is intentionally narrow:

```yaml
requires_airflow: ">=2.10,<3.4"
required_capabilities:
  - task_groups
optional_capabilities:
  - assets
  - partitioned_assets
  - dag_bundles
```

The exact CI matrix is:

| Airflow | Python | `apache-airflow-providers-cncf-kubernetes` | Support tier |
| --- | --- | --- | --- |
| `2.10.5` | `3.11` | `10.1.0` | `compatibility` |
| `2.10.5` | `3.12` | `10.1.0` | `compatibility` |
| `2.11.0` | `3.11` | `10.5.0` | `compatibility` |
| `2.11.0` | `3.12` | `10.5.0` | `compatibility` |
| `3.2.0` | `3.11` | `10.14.0` | `primary` |
| `3.2.0` | `3.12` | `10.14.0` | `primary` |
| `3.3.0` | `3.11` | `10.20.0` | `latest` |
| `3.3.0` | `3.12` | `10.20.0` | `latest` |

Airflow >= 2.11 matrix cells also install
`apache-airflow-providers-microsoft-mssql==4.7.0` for the AIP-60 Asset URI
integration cell (`tests/test_airflow_mssql_provider_asset_uri.py`). Airflow
2.10 cells intentionally omit that provider pin.

Airflow core and every dependency except the explicitly selected CNCF provider
follow the official Apache Airflow constraints file for each Airflow/Python
pair. The matrix derives a byte-preserving install constraints file by removing
exactly one CNCF provider pin and records source/derived SHA-256 evidence; the
requested provider remains an explicit installer argument. Airflow 3.3 / Python
3.11 also installs provider 10.19 from the unmodified official constraints in a
separate negative-capability environment and proves that task-state launch pins
fail closed because `KubernetesPodOperator.__init__` has no `durable` parameter.
Expanding beyond `<3.4` requires adding the new line to this table and to
`.github/workflows/airflow-pack-compat.yml` in the same PR.

Executable indexed delivery has an explicit wire boundary:

| Index | Delivery | Compatibility behavior |
| --- | --- | --- |
| `dpone.airflow-deployment-index.v1` | `local_preview` | supported non-runnable preview |
| v1 | `init_fetch` | fail closed with `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED` |
| `dpone.airflow-deployment-index.v2` | `init_fetch` | strict executable KPO path |

The v0.73.1 four-option `dpone airflow build` invocation still parses, but it
cannot safely reproduce the former incomplete v1 executable projection. It
returns exit `2` with the migration code and no durable side effects. Supply
the explicit trust tier, exact digest-pinned OCI image, and complete
registry/trust ConfigMap references documented in the
[migration guide](airflow-provider-cache-migration.md). Immutable v1 indexes
are regenerated; they are never edited in place.

The canonical `airflow.providers.dpone` namespace is a PEP 561 typed package.
Its stub exposes concrete Airflow DAG/TaskGroup returns and literal duplicate /
invalid-DAG policies across the tested matrix. Type-only compatibility imports
do not change the runtime rule: Airflow 3 materialization prefers
`airflow.sdk`, Airflow 2 uses the supported fallback, and importing the provider
without Airflow installed remains side-effect free. Legacy facade exports from
`dpone_airflow_pack` retain their existing deprecation window and delegate to
the same typed implementation.

Partition-aware assets are capability-gated independently from provider import
compatibility:

| Airflow line | Asset surface | dpone partition behavior |
| --- | --- | --- |
| `3.2.x`, `3.3.x` | Asset partitions and public SDK timetables | native `CronPartitionTimetable` / `PartitionedAssetTimetable` with identity mapping |
| `3.0.x`, `3.1.x` | Assets without partition timetables | explicit `degraded_unpartitioned` |
| `2.10.x`, `2.11.x` | Dataset-compatible downgrade | explicit `degraded_unpartitioned` |

An unavailable partition SDK is a supported downgrade. A partially available
SDK, timetable constructor failure, mixed contracts, or missing native
partition key is an error and never silently degrades. Exact Airflow-matrix CI
is required before expanding this table; live producer-to-consumer event
certification remains separate from package compatibility.

## dbt self-service compatibility

Semantic refresh V2 is additive. Its plans, dependency proofs, lifecycle
policies, execution/attempt bindings, journals, image/artifact receipts and
workflow summaries are new closed versioned contracts. V1 `+fqn:` selection,
execution packs, artifacts and runtime semantics are not widened or migrated in
place. A workflow is wholly V1 or wholly V2.

V2 uses exact model selection and rejects selected/transitive ephemeral or
unmanaged mutations. Existing V1 projects may continue to use their original
selection behavior. Version 0.74 offers V2 only as an opt-in local diagnostic
preview. Production adoption is unavailable even with a complete baseline and
exact live route certification because the exact-UUID predecessor-retention
controller is not shipped. The application and canonical MSSQL activation
boundaries fail before persistence or physical mutation with
`DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE`. A later release may
open production activation only through an additive certified guard. New V2
DagRun admission is blocked even when a pre-existing activation receipt was
preserved by schema migration; merely installing 0.74 never activates V2.

Failed-precommit replacement and completed-scope replay are not compatibility
aliases for a rerun. Replacement is a new operation with retained SQL Server
images; replay is revision `n+1` and remains upsert-only. A different DagRun
cannot resume an old operation.

The first production-certification target is intentionally exact. This is a
target matrix, not a record that live or Cosmos checks passed:

| Component | Required target | Current claim |
| --- | --- | --- |
| Python | `>=3.11,<3.13` | Package support range |
| `dbt-core` | `1.10.13` | Exact v1 MSSQL target; end-to-end production certification `UNVERIFIED` |
| `dbt-sqlserver` | `1.10.1` | Exact v1 adapter target; end-to-end production certification `UNVERIFIED` |
| dbt manifest | v12 | Production target; v10/v11 remain accepted compatibility inputs |
| `dpone` | One exact released version in dev, prod, and runtime | Compiler, release, deployment, and evidence authority |
| `apache-airflow-providers-dpone` | Same version as `dpone` | Scheduler-side `airflow.providers.dpone` namespace |
| `dpone-airflow-pack` | Same version as `dpone` | Dependency-light static pack reader used by the formal provider |
| Airflow/Python | One exact pair from the matrix above | Declared compatibility target; requires a current successful matrix run |

For a source checkout, use `uv sync --extra dbt-mssql`. After v1 ships, install
an exact release with
`python -m pip install "dpone[dbt-mssql]==<released-dpone-version>"`.
`<released-dpone-version>` is a marked placeholder. Broad `dpone[dbt]` keeps
the generic dbt Core `>=1.8,<2` range and does not install or certify a database
adapter.

The authoring demo and `dpone dbt check` do not require live route evidence.
Production `dpone dbt compile` does: capability discovery trusts only the
consuming project's bounded `dpone.yaml`
`capability_discovery.certification_evidence` authority and requires current
`PASS` evidence at `production-certified` or `enterprise-certified` level for
the exact route variant. Historical MSSQL-to-ClickHouse route evidence does not
by itself certify the new dbt-to-Airflow end-to-end journey. Its strict runtime,
live route, and environment-specific campaign/provider/finalizer gates remain
`UNVERIFIED`. The reusable evidence workflow definition and local campaign
tests are not a live pass.

The supported v1 dbt self-service API is the documented `dpone dbt` CLI plus
its JSON schemas. No Python API is supported for this feature in v1.
`dpone.dbt_publish` remains importable only as a legacy compatibility facade.
Its deprecation window begins in `0.73.21`. Removal is forbidden before
`0.75.0`, before two later minor releases have shipped, and before 12 months
have elapsed from the `0.73.21` release date; the later condition wins. The
calendar gate is 2027-07-27 because `0.73.21` shipped on 2026-07-27. Migrate
imports to the canonical
`dpone.contracts`, `dpone.manifest`, `dpone.services`, `dpone.readiness`, and
`dpone.adapters` modules named by each compatibility facade.

The compatibility flag `dpone dbt explain --model MODEL` is deprecated. Use
`dpone dbt explain MODEL`. Its compatibility window starts with the target
release `0.73.26`. The flag cannot be removed before `0.75.0` and
2027-07-29; the later condition wins.

Generated release and runtime trust contracts have separate compatibility
roles:

| Contract | Compatibility behavior | Production dbt behavior |
| --- | --- | --- |
| `dpone.release-set.v1` | Remains supported for non-dbt releases; the compact Airflow compatibility transport may include a digest-pinned non-production inventory bounded to 64 distinct payloads, 256 MiB per payload, and 512 MiB of actual release-wide payload bytes. Existing releases are not deleted or relabelled; an oversized candidate must reduce the bridge inventory or migrate to v2. | Build and runtime receipt validation reject a v1 release carrying dbt runtime payloads in the production trust tier; it cannot establish dbt selection, promotion, route-certification, or evidence authority. |
| `dpone.release-set.v2` | Additive authoritative dbt release contract. | Required for dbt compile, protected promotion, production runtime authority, and evidence. |
| `dpone.runtime-artifact-trust-policy.v1` | Parsed for existing non-production deployments. | Has no concrete verifier and cannot satisfy production offline attestation. |
| `dpone.runtime-artifact-trust-policy.v2` | Additive closed GitHub-attestation verifier policy. | Required for the stock production runtime and CI preflight. |

Module-size governance uses a deliberate v1→v2 migration. The empty v1
allowlist is accepted only as the audited bootstrap input from commit
`e1d93822b47234e940829319cac0dc9678f6906c`; a later descendant base may carry
that exact migration only while it retains the legacy ledger and every cap
remains bounded by both the audited commit and the exact descendant base. It is
not CI authority afterward; a descendant shrink followed by head regrowth is
rejected rather than absorbed into v2.
Automation must pass distinct exact base/head SHAs and consume the versioned
report `debt_model`. `--write-baseline` always exits `2`, writes only a reviewed
candidate, and never certifies the pre-write head. The legacy five-field Python
helpers remain source-compatible and advisory; authoritative callers use the
closed v2 baseline and policy. See the [module-size ratchet migration and
recovery](module-size-ratchet.md).

Dev-evidence contracts use an explicit compatibility cutover:

| Contract family | Compatibility behavior | Protected promotion behavior |
| --- | --- | --- |
| Evidence bundle/provenance/verification v1 | Readable for legacy local and compatibility verification without a campaign authority. | Insufficient for a new protected prod promotion. |
| Evidence bundle/provenance/verification v2 | Adds exact `evidence_set_id`, campaign-request digest, terminal receipt, and protected finalizer provenance. | Required by the reusable campaign/finalizer workflow. |
| Prod promotion v1 | Readable by legacy tooling during the deprecation window. | Cannot prove the v2 campaign authority and is rejected for new activation. |
| Prod promotion v2 | Binds release/deployment, source mirror, v2 evidence, attestation, and policy inputs. | Required for the stock protected prod workflow. |

The campaign request and outcome schemas are additive v1 contracts because they
are new authorities, not replacements for an older campaign schema. A missing,
partial, timed-out, conflicting, or caller-redirected campaign is
`UNVERIFIED`; no reader may downgrade it to a passing v1 bundle.

`dpone.deployment-set.v2` and
`dpone.airflow-deployment-index.v2` deliberately keep
`airflow_bundle_ref` nullable for preview and compatibility readers. The
runnable deployment materializer and protected dbt production policy require
the stricter `git:<40-hex-commit>` form. Schema readability therefore does not
grant production eligibility.

The reusable activation and prod workflows take the compare-and-swap baseline
from protected `DPONE_DBT_EXPECTED_CURRENT_DEPLOYMENT_ID` environment state.
The former caller-controlled `expected-current-deployment-id` workflow input
is not part of the supported contract. First activation still uses the
separate protected absent-pointer policy.

Raw physical-design override deprecation starts in `0.73.21`. The legacy
preview-only `physical_design.engine` and `physical_design.partition_by`
inputs remain readable for one dedicated deprecation release. The earliest removal is `0.74.0`.
Production compilation already rejects them. Migrate each
setting to a platform-owned named `physical_design.profile`; model metadata may
select that profile but cannot define an arbitrary engine or partition
expression.

The unreleased dbt v1 runtime contracts were corrected in place for hermetic
invocation, pre-build selection and target verification, strict workflow IDs,
one certified toolchain authority, and exact SQL Server adapter guardrails.
Regenerate every branch-local selection lock, execution pack, release,
deployment, and evidence artifact through `dpone dbt compile`; never patch
generated JSON. New packs require the effective adapter runtime and adapter
policy, selection locks require the graph-policy ID/digest, and evidence
requires the corresponding runtime and policy fields. An older branch-local
artifact is not upgraded or repaired in place.

Source projects must add the four literal SQL Server flags documented in the
dbt self-service reference, make incremental strategies explicit, and remove
unsupported selected-graph behavior before regeneration. Remove top-level
`dispatch`, execution-critical custom macro calls, framework macro
body/dependency/dispatch shadows, unlisted adapter configuration, model-level
physical constraints, and column constraints other than `not_null`. Convert
each merge key to exact distinct identifier columns in an enforced contract,
add structural `not_null` to every key column, and make any publishing metadata
key exactly match dbt config. An ERROR `not_null` data test remains useful but
does not replace the structural constraint or ClickHouse staging gate.

Each workflow must own a disjoint materialized model closure: move
cross-workflow publish dependencies into one workflow or replace them with a
separately governed source boundary. Eager-selected relationship and singular
tests must also read only models from that workflow. With default schema
concatenation enabled, review the resolved manifest relation identity; if the
configured target schema and model `+schema` are the same token, dbt may
resolve the combined schema (for example `pricing_pricing`). Treat that as a
source migration decision, not a runtime alias.

Use a new empty staging root; the default generated root may contain the old
immutable identity and will correctly fail with
`DPONE_DBT_PUBLISH_OUTPUT_CONFLICT`:

```bash
# Add the four flags, repair macro/key/test boundaries, make workflow model
# closures disjoint, and repair every unsupported selected node.
dbt deps  # when package declarations changed
dbt parse
dpone dbt check

# Never overwrite the previous immutable preview tree.
dpone dbt compile . --output-dir ./generated/dbt-guardrails-v1-next
```

Verify and publish the new `release-set.json`, then activate its new immutable
release/deployment identity through the normal compare-and-swap promotion
path. Retain the old tree for audit. Roll back by reactivating the previously
verified identity; if compile reports an output conflict, choose another empty
staging root instead of deleting or editing either generated tree.

The legacy `dpone.dbt_publish` facade remains an unverified, non-runnable local
preview compatibility lane and does not acquire production authority. The
author journey remains `dbt parse` followed by `dpone dbt check`. Releases
carry logical database and schema identity, while deployments bind physical
endpoints. Unsafe workflow IDs, unknown/ambiguous selectors, unsafe project
flags, and unsupported graph capabilities now fail before artifact writes.

The complete module migration map is:

| Legacy module under `dpone.dbt_publish` | Canonical module |
| --- | --- |
| `airflow_artifact_projection` | `dpone.readiness.dbt_airflow_artifact_projection` |
| `airflow_execution_pack` | `dpone.readiness.dbt_airflow_execution_pack` |
| `artifact_reader` | `dpone.adapters.dbt_publish_artifact_reader` |
| `artifact_writer` | `dpone.services.dbt_publish_artifact_writer` |
| `assembly` | `dpone.app.dbt_publish_composition` |
| `atomic_publisher` | `dpone.readiness.dbt_publish_atomic_publisher` |
| `capability_policy` | `dpone.readiness.dbt_publish_capability_policy` |
| `compiler` | `dpone.services.dbt_publish_compiler` |
| `intent_resolver` | `dpone.manifest.dbt_publish_intent_resolver` |
| `model_compiler` | `dpone.services.dbt_publish_model_compiler` |
| `models` | `dpone.contracts.dbt_publish_models` |
| `planning` | `dpone.services.dbt_publish_planning` |
| `profiles` | `dpone.manifest.dbt_publish_profiles` |
| `release_assets` | `dpone.services.dbt_release_assets` |
| `release_builder` | `dpone.services.dbt_release_builder` |
| `release_materializer` | `dpone.readiness.dbt_publish_release_materializer` |
| `schema_contract_authoring` | `dpone.contracts.dbt_publish_schema_contract_authoring` |
| `schema_contract_common` | `dpone.contracts.dbt_publish_schema_contract_common` |
| `schema_contract_policy` | `dpone.contracts.dbt_publish_schema_contract_policy` |
| `schema_contract_runtime` | `dpone.contracts.dbt_publish_schema_contract_runtime` |
| `schema_contracts` | `dpone.contracts.dbt_publish_schema_contracts` |

Astronomer Cosmos is optional co-installed software. It is not installed by
`dpone[dbt]`, `apache-airflow-providers-dpone`, or `dpone-airflow-pack`, and it
does not participate in dpone DAG topology, selection, retry, credential,
release, or evidence decisions. The repository defines these optional DagBag
coexistence targets:

| Cosmos | Airflow | Python | Current evidence status |
| --- | --- | --- | --- |
| `1.15.0` | `2.11.0` | `3.11` | `UNVERIFIED` in this documentation review |
| `1.15.0` | `3.3.0` | `3.12` | `UNVERIFIED` in this documentation review |

The workflow definition and its local contract test prove only that the probe
is configured. A compatibility pass requires a current successful exact-commit
matrix run and retained evidence. Even then, the smoke claims only that
independent Cosmos and dpone DAG modules import in one DagBag; it does not claim
graph integration, shared selection, a common retry model, or Cosmos execution
through dpone. All other combinations are also `UNVERIFIED`.

## Public compatibility surface

The stable surface includes:

- Manifest schema fields documented in this site.
- CLI commands documented in [CLI reference](cli-reference.md).
- Public imports exported by `dpone` and connector modules.
- Connector contracts used by source, sink, state, schema evolution, and quality services.

Internal modules may change between minor versions when they are not part of the documented public surface.

### Airflow authoring modes

`classic`, `flow`, and `folder` are alternative primary authoring modes. A
pipeline must use exactly one. Folder mode is additive: its root remains
`dpone.flow.v1`, while explicitly listed `dpone.flow-fragment.v1` files provide
the process list. Existing classic, flow, `dpone.batch.v1 + processes`, and
`dpone.pipeline.v1` inputs keep their current compatibility behavior.

No migration is automatic. `dpone migrate authoring --plan|--apply` performs an
explicit source-to-source rewrite and blocks unless the canonical semantic
fingerprint remains equal. Recipe-authored sources are intentionally excluded:
materializing a recipe would discard its version and provenance contract.
Folder compilation reads no undeclared files and does not alter the Airflow
parse contract; generated release, deployment, DAG-spec, and pack artifacts
remain the provider input. The additive command and
`dpone.authoring-migration.v1` receipt do not change existing source behavior.

### Declarative recipes, profiles, and components

External `dpone.recipe.v1`, `dpone.profile.v1`, and `dpone.component.v1`
artifacts are an additive `flow` authoring surface. Existing built-in recipes
and explicit classic/flow/folder sources keep their behavior. External recipe
sources require exact `id@MAJOR.MINOR.PATCH` refs plus immutable path/SHA-256
pins; version ranges and mutable aliases are not accepted.

The selected primary source remains the only editable pipeline authority. The
recipe resolver returns data-only process mappings to the existing canonical
compiler. Packs materialize generated `dpone.batch.v1` runtime IR, so neither
Airflow parse nor workload runtime executes recipe Python/Jinja/plugin code.
Changing a recipe/profile/component version is an explicit authoring change and
changes the source/release identity. Changing only artifact bytes behind an
existing pin fails closed as digest drift.

Artifact lifecycle is publication-time-only. `deprecated` artifacts continue to
resolve under their immutable pins and emit a deterministic warning. They remain
readable until the normal public deprecation window has elapsed; an immutable
artifact is never modified in place to propagate deprecation metadata. See
[Recipes, profiles, and components](airflow-recipes.md).

### Hermetic pipeline tests

`dpone.test.v1`, `dpone.test-report.v1`, and
`dpone.test-suite-report.v1` are additive public contracts. Existing pipelines
do not require test files. New Airflow self-service scaffolds add one starter
test and fixture; repeated scaffold runs remain deterministic no-ops.

Hermetic v1 supports connector-neutral `full_refresh`, `incremental_append`,
and fail-on-duplicate `incremental_merge` final-state semantics. Unsupported
SQL, transforms, connectors, or strategy behavior fails explicitly rather than
changing runtime behavior. Future optional expectations may be additive;
changing existing defaults, identity inputs, exit meanings, or report redaction
requires the normal deprecation policy. See
[Hermetic pipeline tests](testing/hermetic-pipeline-tests.md).

### Airflow process visibility and selector-safe packs

`execution.visibility` is additive. Newly compiled processes default to
`inline`; their DAG-spec nodes contain an explicit selector, visibility,
TaskGroup metadata, and visible-task estimate. Existing DAG specs that omit
`visibility` retain the former expanded `task` presentation.

Node identity and execution selection are separate compatibility contracts.
An exactly-one-process self-service `classic`, `flow`, or `folder` source keeps
the GitOps workload ID as its DAG-spec `node_id`, preserving the existing
Airflow task ID and history across authoring-mode migrations, but still carries
a non-empty selector in the DAG spec and compact process plan. Multi-process
self-service sources and explicit batches without self-service authoring
metadata use process-scoped node IDs. No newly compiled self-service process may
use the selector-less legacy execution path.

New compact schema-v3 packs may contain `runtime_selection: process_plan` and a
bounded `process_plans` mapping. The provider selects one prebuilt command and
step graph and never edits shell text during DAG parsing. A selected node that
references an older pack without a matching process plan fails closed with
`DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED`; rebuild and promote the matching
release/deployment rather than modifying an immutable pack. Selector-less
legacy nodes continue to use the old top-level runtime command.

Newly generated process plans include additive `dag_node.node_id`. New
providers compare it with the DAG-spec node and fail closed on a mismatch; old
packs that omit the field remain readable under the existing selector contract.

`DponeTaskGroup.from_pack` remains the complete-pack escape hatch. A
multi-process process-plan pack therefore uses its top-level whole-workload
command inside that explicit TaskGroup; it does not project selector plans.
The lower-level selector-less task builder keeps the same whole-pack behavior
for backward compatibility. Process-scoped plan selection is owned only by
`DponeDag.from_spec` and `load_dpone_dags` when a DAG-spec node supplies the
selector.

The normative refinement is documented in
[Airflow single-process workload identity v1](feature-design-airflow-single-process-identity-v1.md).

`inline` validates synchronous KPO results after `execute` and deferrable KPO
results after `trigger_reentry`, which is the supported CNCF Kubernetes
provider callback across the tested matrix. Provider lines that publish the
sidecar result through task XCom but return `None` are normalized by reading
that already-published value during task execution; this never happens during
DAG parsing. A missing, malformed, or non-passed XCom summary fails the Airflow
task; no visibility mode can convert a dpone
runtime failure into Airflow success.

### Bounded Airflow backfill mapping

`airflow.mapping` is additive and defaults to `internal`; existing packs and
workloads keep one concrete Airflow runtime task. New packs may carry
`dpone.airflow-mapping-plan.v1` and mapped items use
`dpone.airflow-mapping-item.v1`. Rebuild the release with a current provider
before enabling `visible` or `summary`; an older provider must not interpret a
mapped plan as an internal campaign. Generated mapped packs expose
`mapped_kpo_kwargs` instead of legacy `kpo_kwargs`; the current provider
normalizes it only after plan validation, while older providers stop on their
existing required-field check.

The provider compatibility matrix above constructs a real mapped
KubernetesPodOperator on Airflow 2.10.5, 2.11.0, 3.2.0 and 3.3.0. The contract
uses only `partial`/`expand`, a static pool and `max_active_tis_per_dag`. It does
not read `max_map_length` or any other Airflow configuration during parse.

Mapped execution is certified only for PostgreSQL and MSSQL audit-state
coordination in v1. Local-file and ClickHouse audit state remain valid for
`internal` mode but reject mapped mode with
`DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED`. Rollback is configuration-only:
change the policy to `internal`, rebuild and promote. The deterministic chunk
identities and ledger remain reusable; do not delete the campaign state.

### Canonical DLQ and legacy quarantine

`sink.options.dlq` is additive and defaults to `reference_only` when schema
enforcement is `quarantine`. Canonical `dpone.dlq.v1` artifacts contain no raw
row values by default, use stable reason codes, and are verified by checksum.
The data-contract evidence projection no longer serializes accepted target rows
or invalid `actual_value` strings. Consumers of durable evidence must use
counts, safe diagnostics, record IDs, and `data_outcome` instead.

The import `dpone.ops.quarantine.QuarantineService`, `quarantine.dir`, and raw
JSONL export remain available for the compatibility window. They are not the
default for new runtime composition. The previous
`quarantine-replay --yes` behavior was unsafe because it reported rows as
applied without invoking a sink; it now fails with exit code `4` and
`DPONE_DLQ_REPLAY_EXECUTOR_REQUIRED`. This safety correction is intentionally
immediate. Use the plan-first `DlqReplayService` with injected
`DlqRecordResolver` and `DlqReplaySink` for real replay.

## Backward compatibility policy

- Patch releases should be backward-compatible.
- Minor releases may add new optional fields, connectors, and strategies.
- Breaking changes require a migration note and should be reserved for major releases.
- Deprecated behavior should remain available for at least one minor release when practical.

### Airflow cache promotion migration

The cache integrity hardening planned for `0.72.2` is an explicitly approved
fail-closed security exception to the normal major-release rule:
`dpone airflow cache-sync` no longer promotes a deployment
projection when its pinned release-set or indexed DAG/workload artifacts are
absent. Existing projection-only caches must rematerialize their immutable
release content before upgrading. The `dpone.release-set.v1` schema remains
compatible with the legacy `artifact_ref` field when it contains a safe
release-relative path; generated releases use `path`. Release-set `cache://`
references and entries with both locator fields are rejected. See the
[Airflow cache sync and recovery runbook](airflow-cache-sync.md).

The exact-cache Kubernetes topology introduced for `0.73.32` uses
`/opt/airflow/.dpone-cache`, but new loaders and platform commands always pass
that path explicitly. A no-argument lightweight cache lookup retains the
historical `/opt/airflow/dags/.dpone-cache/airflow` default for the compatibility
window. Operators opt into the canonical root with
`DPONE_AIRFLOW_PACK_CACHE_DIR` or `--cache-dir`; the provider never scans or
silently chooses between both roots.

Promotion now also recomputes `deployment_id`, checks every environment field
mirrored into `airflow-index.json`, serializes CAS through a local promotion
lock, and rejects symlinked control files. First promotion must use
`--expect-current-absent`; updates use `--expected-current-deployment-id`.
Recovery apply now requires a promoter identity and the active-state guard from
the reviewed plan. Caches with non-content-addressed deployment directories or
a copied `current` directory must be rematerialized before promotion.

Rollout is fail-closed: materialize the release and run cache-sync in a
non-production cache before upgrading production. A validation failure happens
before activation; pointer/audit preparation failures expose whether recovery
is required. Leave the existing deployment current while the release handoff is
repaired. Rolling the binary back temporarily restores the
old behavior but also removes the new integrity gate; it requires an explicit
security exception and is not a data-plane repair. If trusted release content
cannot be recovered, stop and escalate rather than editing the published cache.

### Native-transfer target-commit guard rollout

The patch after `0.73.1` adds an optional
`target_commit_guard` inside the existing partition-checkpoint diagnostics.
Checkpoint schemas, statuses and store APIs remain compatible, and old
checkpoint records without the guard keep their previous resume behavior.

Runtime compatibility is intentionally asymmetric: new workers fail closed on
unresolved or malformed guards, while older workers do not understand them.
Do not run mixed old/new worker pools for guarded native routes. Drain older
workers or disable automatic scheduling before deployment, upgrade every worker
that can execute the route, then re-enable retries. A rollback to an older
runtime after any guarded attempt requires an explicit safety exception and
manual target/checkpoint reconciliation.

`native_transfer_manual_reconciliation_required` is not cleared by changing
query, schema or partition identity. Repair the same source/target/strategy
route deliberately. The guard prevents unsafe automatic target I/O but does not
provide target fencing, CAS or exactly-once certification; those remain in
[ADR 0022](adr/0022-target-commit-terminal-failures.md).

### Native-transfer quality row authority

The same patch family adds additive checkpoint diagnostics
`rows_loaded` and `row_count_authority=source_export_and_target_loader`.
Checkpoint schemas and store APIs remain compatible. Older committed
checkpoints without the authority marker still deserialize and participate in
resume planning, but native quality scopes treat their source and target row
counts as unavailable. Planned `estimated_rows` never certify reconciliation.
Rerun the source export (or use a separately certified live target probe) to
create current evidence; dpone does not manufacture provenance for old state.

### Production route-attestation migration

Explicit production safe-sample execution no longer accepts raw route IDs from
CLI application context as authorization. It requires the four platform-owned
route-attestation inputs documented in the
[production route-attestation runbook](airflow-route-attestation.md). This is a
fail-closed security correction: development rehearsal remains available, but
production live copy does not fall back to the old string seam.

The v1 verifier certifies `cosign >=3.0.4,<4.0.0`, exact certificate identity
and OIDC issuer matching, and a local trusted-root digest. Unsupported or
unavailable verifier/trust material is `unverified`, never success. Expanding
the version range requires current adapter tests, security review, and live
certification evidence. Route-attestation schemas are additive; safe-sample
runtime execution evidence gains an optional verification receipt.

### Beginner automatic live-selection compatibility

Starting with `0.72.7`, the existing fifth beginner command automatically
selects the signed live runtime only when platform CI has materialized all four
route-authorization files for the exact pinned deployment and pipeline. This
is an additive facade change rather than a second execution path:

- no authorization directory preserves the existing network-free local
  handoff and exit behavior;
- a complete directory delegates to the same route-attestation verifier, live
  assembly, pinned init-fetch, temporary-target lifecycle, copier, and evidence
  writer used by `dpone ops safe-sample-runtime-run`;
- an existing but incomplete or unsafe directory fails closed instead of
  silently falling back;
- the explicit `dpone ops safe-sample-runtime-run` command remains available
  as the platform diagnostic and compatibility escape hatch;
- `execution_mode` is an optional additive field in
  `dpone.safe-sample-runtime-execution.v1`, so previously written v1 evidence
  remains valid.

The overlay status field is named `authorization_overlay_profile`; it describes
the local cache-layout contract and must not be confused with the signed
route-attestation `authorization_profile`. External manifests require no
migration. Platform CI may opt in by atomically publishing the layout in the
[production route-attestation runbook](airflow-route-attestation.md).

Upgrade and rollback are explicit:

1. Upgrade without publishing an overlay to retain the `0.72.6` network-free
   behavior.
2. Verify and atomically publish a complete deployment/pipeline overlay to opt
   that deployment into automatic live selection.
3. If a partial directory is observed, replace it from staging and rerun the
   same beginner command; do not edit immutable files in place.
4. To roll back the facade selection, atomically remove the complete overlay.
   The next invocation returns to `local_handoff`; already persisted evidence
   remains immutable. The explicit platform command is unchanged.

### Airflow Connection Secret isolation

Starting with `0.72.8`, the operator-side Airflow Connection bridge treats the
pack's `connection_projection.secret_name` as a base prefix and derives one
physical Kubernetes Secret per Airflow task attempt. Existing compact packs and
schemas remain valid; no authoring migration is required.

This is an intentional fail-closed security correction. The compatibility-named
`AirflowConnectionSecretProjector.upsert()` method now means create-only
publication. A pre-existing name returns
`DPONE_AIRFLOW_CONNECTION_SECRET_CONFLICT` and is never replaced. Injected
projector implementations require a code review/migration to the same
create-only semantic contract even though the Python method signature is
unchanged. Synchronous
cleanup deletes only the derived attempt object; deferrable runs still require
`cleanup_policy: retain` and platform-managed cleanup.

Provider code that inspected the internal
`airflow_connection_projected_secret` attribute now receives only digest-safe
reference metadata rather than URI-bearing `stringData`. Use
`airflow_connection_projected_secret_ref` for new diagnostics. Rolling back
restores shared mutable Secret behavior; if rollback is unavoidable, serialize
all bridge tasks through an Airflow pool until the fixed provider is restored.

Deterministic local concurrency and lifecycle contracts are covered by the
v0.72.8 test suite. Real overlapping Airflow/Kubernetes execution remains
`UNVERIFIED` until an approved cluster produces the live certification artifact;
this does not weaken the fail-closed runtime behavior, but it is not a production
certification claim.

### Airflow Connection Secret retention GC

Starting with `0.72.9`, new attempt Secrets and their consuming Pods carry
digest-only lifecycle metadata. Existing compact packs remain valid and
ordinary pipeline owners do not gain a new command. Platform operators can use
`dpone airflow connection-secret-gc-plan` followed by an explicitly confirmed
`connection-secret-gc-apply` in one namespace.

The platform command uses the optional `dpone[kubernetes]` extra
(`kubernetes>=32.0.1,!=36.0.0,<37`). Base `dpone` and scheduler-side provider
imports remain SDK-free. Existing Airflow images may satisfy the dependency
through their CNCF Kubernetes provider; a separate restricted GC image is
preferred to avoid coupling scheduler packages and platform cleanup RBAC.

The GC never adopts pre-`0.72.9` unlabelled objects. Review and remove those
through the v0.72.8 digest-reconstruction procedure. Injected/custom Secret
projectors must preserve the provider-supplied labels and annotations; no
Python signature changes, but dropping metadata means the object remains a
manual-cleanup object. Deploy the provider first, observe plans for at least one
retention window, then enable apply.

The dedicated GC identity needs namespace-scoped `list` on Secrets and Pods and
`delete` on Secrets. Kubernetes RBAC cannot constrain `list secrets` to the
metadata representation, so use a dedicated ephemeral-credential namespace and
do not give this identity a general shell. The client itself requires
`PartialObjectMetadataList` and fails on HTTP `406`; it never falls back to a
full Secret/Pod response. Remove the obsolete `replace` permission from the
provider task identity.

Rollback is non-destructive: suspend GC apply first, then roll back the CLI or
provider. Existing labelled objects remain eligible for a later fixed GC; newly
created unlabelled objects require manual cleanup. Local fake-transport,
concurrency, redaction, and 10,000-item inventory tests are covered in v0.72.9.
Real Airflow/Kubernetes lifecycle certification remains `UNVERIFIED` until an
approved cluster produces current evidence.

### Airflow release/deployment artifact delivery

Starting with `0.72.10`, `dpone airflow publish` and
`dpone airflow cache-materialize` are the canonical platform surfaces for one
content-addressed release/deployment pair. They are additive: existing
authoring sources, `local_preview` release/deployment projections, local
`cache-sync`, provider loader APIs, and compact packs require no migration.
Executable v1 `init_fetch` is the exception: it is rejected fail-closed and must
be regenerated as a new immutable `deployment-set.v2` /
`airflow-deployment-index.v2`, then published, materialized, and promoted. Never
rename or edit the v1 projection in place.

`dpone gitops airflow publish` and `dpone-airflow-pack-sync` remain legacy
pack-only compatibility paths for at least two minor releases and 12 months.
They retain their historical generation/`latest` behavior, but that behavior is
never imported into the canonical release/deployment identity model. New
deployments must not mix a mutable legacy index with
`dpone.airflow-deployment-index.v1`.

Starting with `0.73.32`, canonical exact deployments and the mutable legacy
pack cache must use different cache roots. Each root has a versioned layout
marker, and a writer rejects a conflicting or ambiguous layout before remote
I/O or cache mutation. The legacy sync reserves bounded capacity, uses an
immutable generation plus recoverable pending state, and treats the durable
local commit receipt as its authority. Text/symlink pointers must match it.
An Airflow Variable is only a diagnostic projection and can lag after process
termination or metadata failure. Existing unmarked roots are detected once
from their historical shape and receive the matching marker. Follow the
Airflow provider cache migration runbook instead of copying `current`, a
layout marker, pending state, or a commit receipt between roots.

Starting with `0.73.21`, `dpone airflow cache-materialize` also accepts the
bounded logical `--connection-id` / `--connection-type` access mode already
supported by `dpone airflow publish`. This is additive: workload identity and
the local registry emulator remain unchanged. The logical name is resolved
only by the CLI composition root and is never stored in immutable deployment
artifacts or materialization evidence.

Before `0.73.21`, an S3 logical connection without an explicit access-key and
secret-key pair could reach the SDK ambient credential chain. Starting with
`0.73.21`, both `publish` and `cache-materialize` reject that ambiguous
configuration before object-storage I/O. Deployments that intentionally use an
ambient identity must select `--identity-mode workload_identity`; deployments
that use an Airflow, environment, or Vault logical reference must provide a
complete key pair through that provider. This fail-closed change prevents a
misspelled or empty connection from silently using the pod identity.

To migrate, validate the read-only and writer logical connections before the
upgrade, then either populate the complete key pair or switch the deployment
command to workload identity. Rollback to an older package restores the former
SDK behavior but is not recommended because it makes the effective identity
implicit.

Concrete built-in object-storage clients gain additive
`put_file_if_absent()` and `stat()` methods. The existing
`ObjectStorageClient` protocol is unchanged for third-party staging clients;
the new interface-segregated `ImmutableObjectStorageClient` capability carries
the two stronger operations. Custom clients used as the new
`ArtifactRegistry` backend must implement true conditional create; an overwrite
emulation is incompatible and fails the immutability contract.

Azure registry workload identity requires `dpone[azure]` or
`dpone[object_storage]`, which include `azure-identity>=1.13,<2`, and the
account-scoped URI `azure://<account>/<container>/<root>`. Existing injected
Azure clients may continue to use the accountless `az://` compatibility form;
the workload-identity composition rejects it because an account endpoint
cannot be derived safely.

Rollback means stop invoking the new publisher/materializer and leave the last
validated local `current` active. Do not overwrite or delete content-addressed
remote objects to roll back. Production materialization remains fail-closed
when `required_for_prod` attestation cannot be verified. Local adapter tests do
not constitute S3/GCS/Azure or Kubernetes production certification; those
profiles remain `UNVERIFIED` until current approved evidence exists.

### Formal Airflow provider and v1 public contract

The canonical `airflow.providers.dpone` namespace and Airflow discovery entry
point are owned by `apache-airflow-providers-dpone`. The
`dpone-airflow-pack` distribution is its dependency-light static reader and no
longer owns discovery metadata. Both distributions stay on the same dpone
release line.

Provider facade exports from `dpone_airflow_pack` remain compatibility aliases
for at least two minor releases and 365 days after their `0.72.0` announcement,
whichever is later. Their earliest removal is therefore both release `0.74.0`
or later and date 2027-07-13 or later. They warn at most once per process; new
code imports `airflow.providers.dpone`.

The reviewed [Airflow self-service public contract](reference/airflow-public-contracts.md)
freezes the beginner CLI, canonical provider signatures, schema majors,
package ownership and support window targeted for v1. CI checks the real parser,
provider AST, schema registry and package metadata without importing Airflow or
performing network, database, secret or cache-refresh I/O.

### Airflow self-service authoring v1

New `dpone init pipeline` calls emit `dpone.flow.v1` unless
`--authoring classic` is selected. Flow, folder, and classic modes compile into
the existing `dpone.batch.v1` canonical execution IR, so pack, release,
runtime, and provider contracts are unchanged. The incorrectly labelled legacy
`kind: dpone.batch.v1` plus `processes:` source remains readable and reports
`DPONE_LEGACY_SELF_SERVICE_BATCH_PROCESSES`; new sources must use
`kind: dpone.flow.v1`. The earlier safe-sample
`schema: dpone.pipeline.v1` shape remains readable with
`DPONE_LEGACY_SELF_SERVICE_PIPELINE_V1`. Folder composition is additive and
uses an explicit, confined, resource-bounded fragment list; it does not scan
directories or change scheduler parse behavior.

The compatibility aliases are intentionally reader-only and do not validate
against the current batch JSON Schema. Schema validation is the write contract:
rename the short source kind to `dpone.flow.v1` and set
`authoring.mode: flow`. Runtime/check compatibility prevents an emergency
break, but does not authorize new `batch + processes` files.

### Airflow composite identity and rerun plans

Index-backed provider tasks add `dpone.airflow-run-identity.v1` to task params,
runtime environment, XCom, and evidence. Existing deployment indexes without
`airflow_bundle_ref`, legacy tasks without the environment value, and older
XCom/evidence files remain readable because the new fields are additive. A
release-policy evidence bundle requires the identity; advisory/PR collection
reports its absence as a warning.

Exact cache activations do not change that strict v1 object. They add a separate
`dpone.airflow-deployment-identity.v1` value containing release, deployment and
UUIDv4 activation occurrence. Older runtimes without the separate value continue
to execute, but cannot certify a particular activation occurrence.
Deployment-bound acceptance therefore blocks on a missing or mismatched
deployment identity while ordinary legacy execution remains compatible.

`dpone airflow rerun-plan` never invents missing historical state. Critical
plans require complete release/deployment/runtime references and a versioned
Airflow bundle or a content-addressed snapshot. Git bundle refs are treated as
versioned. Local, S3, GCS, and unknown bundle refs are non-versioned unless a
snapshot is present, so selecting an original bundle fails with
`DPONE_RERUN_NOT_REPRODUCIBLE`. Selecting the latest non-versioned bundle is an
explicit warning only for a non-critical plan.

Airflow bundle selection and dpone artifact selection remain independent. An
`original/original` plan requests clearing the existing run without Airflow's
latest-version override; every mixed/latest selection requests a new pinned
rerun. The planner performs no Airflow API call. Rollback stops injecting the
optional identity and leaves existing artifacts readable; it cannot make an
expired release or non-versioned historical bundle reproducible.

### Airflow observability correlation

`dpone.airflow-correlation.v1` is additive to the final Airflow evidence bundle.
Existing evidence, OpenLineage exports, and OTel-compatible metrics exports
remain readable when no correlation input is supplied. Supplying
`--airflow-evidence-bundle` opts into strict validation: the bundle must contain
a complete correlation, otherwise the exporter exits red instead of silently
emitting partially joined telemetry.

OpenLineage keeps the historical dpone run ID when no correlation is supplied.
With correlation, `run.runId` is a deterministic UUIDv5 and the original dpone
run ID remains in the dpone facet. The custom correlation facet uses a pinned
v0.73.0 schema URL. OTel correlation attributes are additive. They are not
copied into Prometheus labels, baggage, or synthetic trace/span identifiers.
Rollback is omission of the optional evidence-bundle argument; no stored run
identity or immutable evidence is rewritten.

### Credential resolver lifecycle

The connection-registry v1 schema continues to parse `version_policy: pinned`
and `resolution_scope: dag_run_start` for diagnostic and migration
compatibility. dpone does not execute those values: readiness and runtime return
`DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED` or
`DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED` before credential backend I/O.
The supported runtime policy is `latest + workload_start`.

For explicit Vault KV v2 entries, successful evidence requires a positive
resolved version and every declared field mapping must produce a non-empty
value. Vault KV v1 remains readable with `resolved_version: null` and is not
rotation-version certified. `CredentialResolutionError` subclasses
`ValueError`, preserving callers that already catch validation failures while
adding stable `code` and `resolver` fields. See the
[credential lifecycle compatibility and recovery runbook](airflow-credential-resolver-lifecycle.md).

### Studio API v1 and capability discovery

The canonical Studio HTTP surface is `/api/v1/*`; `/healthz` and
`/openapi.json` remain unversioned protocol endpoints. Existing `/api/*`
operations remain deprecated compatibility surfaces for at least two minor
releases and 12 months, with an earliest planned removal date of 2027-07-23.
They emit `Deprecation: true`, `Sunset`, and an OpenAPI `Link` header.
`/api/manifests/draft` and `/api/plan` are direct aliases. Connector,
connection-capability, and certification views migrate to
`/api/v1/capabilities` while preserving their old response shapes on old
paths. Doctor, performance, state, run, security, audit, SLO, deployment,
schema, quality, GitOps, and reconciliation operations are retirement-only:
they have no v1 replacement and must not be adopted by new clients.

Safety corrections are not optional compatibility modes:

- empty, skipped-only, and plan-only quality checks no longer report success;
- quality outcome status is `passed`, `failed`, `skipped`, or `unverified`;
  advisory failures retain `mode: warn` instead of inventing a fifth status;
- missing, stale, failed, malformed, or foreign-commit certification evidence
  cannot promote route certification;
- mock contract certification remains behaviorally reportable but is
  `UNVERIFIED` and cannot satisfy evidence-bundle/go-live gates;
- unsupported route drafts, plans, static checks, and reconciliation fail
  before project writes or successful plan output;
- Studio plan and static-check compile one canonical process snapshot, so
  classic `schemas[].tables[].overrides` cannot bypass route support checks;
- Airflow explain reads only the requested pipeline.

Legacy certification JSON that has `passed: true` but no
`evidence_status: PASS` now fails every certification-sensitive release gate.
This is an intentional safety correction. Regenerate the artifact with a
current certifier; do not edit historical evidence. Generic non-certification
artifacts retain their existing evaluation contract. The positional
`ConnectorCatalogEntry` constructor remains source-compatible: omitted
`maturity` defaults to `experimental`, and omitted `release_phase` inherits the
legacy `status`.

`dpone connectors certify` is strict by default. `--report-only` explicitly
selects a non-gating report. The old `--fail-on-missing` option remains accepted
with one process-level deprecation warning during the same compatibility
window.

The generated [Studio OpenAPI contract](api/studio-openapi.json),
`dpone.capability-discovery.v1`, and `dpone.pipeline-summary.v1` are additive
public contracts. The bundled stdlib server remains a local development
adapter; shared-token remote opt-in is not production hosting or RBAC.

Direct Python construction through `StudioApiService(artifact_roots=...)` and
the historical `openapi()`, `record_audit()`, `certification_matrix()`, and
`capabilities()` methods remain as deprecated compatibility facades through at
least 2027-07-23. `capabilities()` preserves its historical connection DTO;
canonical `/api/v1/capabilities` uses `capability_snapshot()`. New integrations
construct the DI-first service with `build_studio_api_service()` or consume
`/api/v1`; compatibility calls emit one `DeprecationWarning` per symbol and
retain the hardened fail-closed semantics.

Historical imports of `StudioApiError`, `StudioHttpConfig`, and
`StudioAuditLog` from `dpone.readiness.studio_api` remain re-exports for the
same window. Their canonical module is
`dpone.readiness.studio_http_models`.

Legacy `connectors certify` capability rows keep their historical lowercase
`unknown` value inside the legacy DTO. Canonical capability discovery does not
reuse that field: it exposes evidence as `PASS`, `FAIL`, `SKIP`, or
`UNVERIFIED` and keeps connector maturity, release phase, route support, and
certification level separate. Mocked, missing, stale, skipped, malformed, or
foreign-commit evidence is always `UNVERIFIED`; only approved current evidence
for the exact subject and commit can certify a route.

The marketplace projection is now explicitly
`dpone.connector-marketplace.v2`. Its legacy `status` and `badge` fields map to
the connector `release_phase`; canonical `maturity` and `release_phase` are
also present and must be used by new consumers. The projection carries the
canonical `snapshot_id`, `passed`, and `issues`; marketplace CLI and evidence
bundles fail when capability authority has issues. This safety correction
removes the old ambiguous use of `certified` as a connector-wide claim without
route evidence.

Self-service and discovery result payloads remain on stdout, including
structured `passed: false` results, so existing automation can always decode
the command result from one stream. The exit code remains authoritative.
Parser/usage diagnostics and fatal Studio startup errors use stderr. Commands
may return usable discovery data together with `passed: false` and `issues[]`,
but exit non-zero until the project configuration issue is fixed.

### MSSQL BCP streaming buffer migration

`source.options.native_transfer.snapshot.streaming.read_buffer_bytes` replaces
the deprecated streaming-only field `target_chunk_bytes`. The canonical field
controls the number of bytes requested from the BCP FIFO per read, defaults to
`4MiB`, and accepts `64KiB..16MiB`. String values use whole `KiB` or `MiB`
units; integer values are bytes.

The canonical field is introduced in 0.72.4. The legacy field remains accepted
through the 0.73.x dedicated deprecation release, with 0.74.0 as the earliest
possible removal. Its value intentionally does not control the effective buffer
because previous releases always read a fixed 4 MiB. When both fields are
present, the canonical field wins and runtime decision evidence includes
`streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes`.
Values outside the canonical range fail before BCP with
`streaming_read_buffer_bytes_out_of_range`.
Malformed legacy-only values continue to fail before BCP, now with
`streaming_target_chunk_bytes_invalid`. A malformed legacy value is ignored
only when a valid canonical field is present and therefore has precedence.

Replace only the full `snapshot.streaming.target_chunk_bytes` path. Do not
rename `physical_chunking.target_chunk_bytes`, columnar/object-storage chunk
targets, or `execution.transport.stream_buffer_bytes`. Rolling back to a release
without the canonical field requires restoring the legacy streaming key;
custom buffer tuning then falls back to the historical 4 MiB behavior.

### SQL dependency symlink hardening

Canonical authoring compilation now records SQL dependencies by their lexical
project-relative path and rejects symbolic links at every path component. This
closes a check-to-use race in which a symlink could be retargeted after
compilation while the recorded resolved target remained unchanged.

Projects that previously referenced an in-repository SQL symlink must replace
it with a regular project file or generate the file before `dpone check`.
Absolute paths, traversal, symlinked parents, and final-leaf symlinks fail with
`DPONE_AUTHORING_DEPENDENCY_READ_FAILED`; dpone does not silently follow or copy
the target. Regular SQL files and their semantic fingerprints are unchanged.

### Signed catalog bundles and conformance

`dpone.catalog-bundle.v1` is an additive platform promotion format. Existing
trusted local recipe catalogs and connection registries remain readable and do
not require signatures unless a platform policy separately requires the signed
promotion path. Bundle verification does not alter recipe pins, deployment
fingerprints, Airflow parsing, or runtime credential resolution.

The existing route-attestation Python imports and route-specific error codes
remain stable. They delegate to the same generic bounded cosign blob verifier
used by catalog verification. The local HMAC supply-chain helper remains
non-production and is not accepted as catalog verification evidence.

Extension conformance is additive release evidence. `PASS` describes the exact
subject and closed profile only; it does not upgrade a connector route or
credential resolver to production-certified without the profile's required
approved live evidence.

### Deployment-cache retention constructor

New application composition uses
`dpone.app.airflow_cache_retention_composition.build_deployment_cache_retention_applier`.
The historical `dpone.runtime.deployment_cache.DeploymentCacheRetentionApplier`
constructor and runtime builder remain supported with their existing signature
and do not emit a deprecation warning. Both paths delegate to the same explicit
compatibility composition factory in
`dpone.runtime.deployment_cache_retention_compatibility`. Canonical concrete
adapter wiring belongs to
`dpone.app.airflow_cache_retention_composition`; the runtime factory accepts an
injected receipt capability. The compatibility factory only adapts the frozen
historical constructor and contains no retention policy. No removal release is scheduled. A future deprecation
requires the normal public migration window before either symbol can change.

### Semantic Refresh attempt continuation receipt

Starting with 0.74.0,
`dpone.semantic-refresh-attempt-continuation-receipt.v1` is a frozen public wire
contract. It binds only the immediate same-DagRun successor try to the original
attempt and fence after trusted termination, engine quiescence, bounded target
reconciliation, and exact after-image proof. Existing v1 bytes remain readable
and exact-replayable; v1 never authorizes a second successor, cross-DagRun
resume, changed scope/plan, or automatic SQL retry.

A future monotonic current-holder chain, detached termination-authority
binding, or broader continuation protocol must use an additive
`dpone.semantic-refresh-attempt-continuation-receipt.v2`. Its reader must keep
v1 parse compatibility while refusing to treat v1 as v2 authority. Adding
required fields or changing the v1 digest subject in place is prohibited.

## Connector maturity labels

- `certified` - covered by the required unit, contract, local integration, and
  approved current live certification artifacts for the exact subject and
  commit. Mock artifacts never establish this status.
- `experimental` - usable but still missing part of the certification matrix.
- `community` - maintained through public contribution flow and must declare its supported capabilities.

## Generated compatibility matrix

<!-- DPONE_COMPAT_MATRIX_START -->
| Deprecated path | Canonical path | Scope | Status | Removal policy |
|---|---|---|---|---|
| `dpone.core.artifacts` | `dpone.runtime.artifacts` | module | transitional-shim | after internal callers migrate to runtime.artifacts |
| `dpone.core.errors` | `dpone.contracts.errors` | module | transitional-shim | after internal callers migrate to contracts.errors |
| `dpone.core.etl_types` | `dpone.contracts.process_types` | module | transitional-shim | after internal callers migrate to contracts.process_types |
| `dpone.core.runtime` | `dpone.contracts.run_context` | module | transitional-shim | after internal callers migrate to contracts.run_context |
| `dpone.credentials` | `dpone.runtime.credentials` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.etl` | `dpone.runtime.etl` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.etl_logging` | `dpone.runtime.etl_logging` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.lib.connectors` | `dpone.runtime.connectors` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.lib.connectors.base` | `dpone.ports.db_connector` | module | transitional-shim | after internal callers migrate to ports.db_connector |
| `dpone.lib.technical_columns` | `dpone.contracts.technical_columns` | module | transitional-shim | after internal callers migrate to contracts.technical_columns |
| `dpone.lib.utils.data_type_mapper` | `dpone.runtime.support.data_type_mapper` | module | transitional-shim | after internal callers migrate to runtime.support.data_type_mapper |
| `dpone.lib.utils.timezone_converter` | `dpone.runtime.support.timezone` | module | transitional-shim | after internal callers migrate to runtime.support.timezone |
| `dpone.reconciliation` | `dpone.runtime.reconciliation` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.sink` | `dpone.runtime.sinks` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.source` | `dpone.runtime.sources` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.sql_helpers` | `dpone.runtime.sql_helpers` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.state` | `dpone.runtime.state` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.xmin` | `dpone.runtime.xmin` | package | deprecated-shim | after one dedicated deprecation release |
| `dpone.yaml_config_handler` | `dpone.dag` | package | deprecated-shim | after one dedicated deprecation release |
|  |  |  |  | _Legacy DAG package kept as re-export shim during migration._ |
<!-- DPONE_COMPAT_MATRIX_END -->
## Related docs

- [Source -> sink matrix](source-sink-matrix.md)
- [Connector certification](connector-certification.md)
- [Production readiness](production-readiness.md)
- [Release process](release.md)

## Native compact dbt workspace delivery

Compact materialization accepts a complete canonical release-set v2 with explicit
dbt wire v2. It retains producer/source/selection metadata and exact runtime
objects, but derives a new release ID for rewritten transport. Regenerate the
deployment and applicable attestations for that new identity. A native DAG subset
is rejected; legacy reconcile filtering remains unchanged.

Projection and provider preserve workload payload order. Existing legacy runtime
order handling remains supported; reordered v2 references are rejected, never
healed. Release v1 cannot declare dbt wire v2. Native singleton release roots are
not supported by this new compact mode; existing singleton delivery remains.
See [compact workspace delivery](dbt-compact-delivery.md) for migration and
troubleshooting. Production workspace activation remains separately gated.

## Verified release composition

Use [release composition](release-composition.md) to deliver one complete native
workspace and independently authored ordinary transfer packs in an explicit
`dpone.release-set.v3` parent. Native v2 authority and bytes remain intact. Upgrade
all readers before using v3. Composition activation is unavailable until physical
admission covers every constituent; see the
[contracts](release-composition-reference.md) and
[migration and recovery guide](release-composition-operations.md).

## PostgreSQL strategy-preserving refresh correction

PostgreSQL internal queries now honor the selected strategy. Omitted overwrite
mode and `truncate_insert` preserve an existing target, matching file/memory
loads. Manifest fields, Python call signatures and result fields are unchanged.
Legacy direct internal-query/file loader calls delegate to the configured sink
strategy, including the historically named exchange helpers; only configured
`overwrite_type: exchange` selects replacement. Prefer `PostgresSink.load`.

Invalid rows, missing TRUNCATE privileges and incoming foreign keys can now
correctly fail loads that previously bypassed the target contract. Upgrading
prevents this replacement defect; it cannot reconstruct constraints lost by an
earlier runtime. Restore them from approved DDL using the
[PostgreSQL recovery runbook](source-sink/postgres-to-postgres.md#recover-a-previously-replaced-target).
