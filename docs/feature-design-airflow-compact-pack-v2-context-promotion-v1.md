# Feature design: compact pack → v2 RuntimeConnectionContext promotion

- Status: APPROVED
- Owner: dpone maintainers
- Issue: retire smoke-v2 dual-load after sealed `connection_ref` cutover
- Target release: next patch after 0.73.x tip that includes #427
- Last verified: 2026-07-23
- Revision: fills the compact-pack gap left open by
  `docs/feature-design-airflow-init-fetch-connection-bridge-v0732.md`
  (which correctly kept compact-only context as a non-goal) by promoting
  compact reconcile outputs onto the existing v2 init_fetch authority
- Revision 2026-07-30: the CAS-stable compact promotion marker is an optional,
  typed top-level `dpone.release-set.v1` field. The v1 root was already open,
  so older readers ignore the marker while newer readers validate it.
- Architecture authority: ADR 0010, ADR 0027, ADR 0009 / 0024,
  `docs/airflow-self-service-architecture.md`

## Executive summary

Compact packs must be promoted to the v2 delivery path to provide a verified
`RuntimeConnectionContext`. This design removes the need for parallel loader
paths and workload-specific migration scripts. These are design requirements,
not evidence of a completed deployment.

This slice does **not** invent a second context authority inside
`RuntimeConnectionContextLoader` or the legacy `pack-index.json` lane.
It promotes compact reconcile packs into an immutable `dpone.release-set.v1`
with a closed, verified connection-context rewrite, then reuses
`AirflowDeploymentProjectionService.materialize` + `dpone airflow
build/publish/cache-sync` so the single scheduler load path is a v2 index
with binding-set / rewritten connection-registry / credential-runtime
descriptors.

Success criterion: representative synthetic DAGs run from one v2 index
without workload-specific loader exceptions or a second compact load.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Platform engineer | One publish/load path for sealed `connection_ref` | Dual-load + hardcoded pilot list | Single v2 index load |
| Pipeline author | Keep domain YAML / sealed projection | Must know smoke-v2 dual-load exists | No dual-load docs in cutover |
| Airflow operator | Keep object-storage stage green | Context missing → Airflow env fallback | Context-only S3 resolve |

Journey:

1. `dpone gitops airflow reconcile`
2. `dpone gitops airflow release-materialize` (new) → release-set
3. Existing `dpone airflow build` / `publish` / `cache-materialize` / `cache-sync`
4. Scheduler: one `load_dpone_dags(index_path=.../current/airflow-index.json)`
5. Runtime: init_fetch plan + `DPONE_RUNTIME_CONNECTION_CONTEXT` (unchanged)

## Scope

### In scope

- OSS glue: compact pack-root → immutable release-set with closed strict rewrite
- Auto-discovery of dag-specs/workloads from reconcile output (no hardcoded pilots)
- Fail-closed rewrite for sealed bridge projection + digest-pinned XCom sidecar
- Docs / changelog / consumer follow-up notes
- Focused unit tests for materialize + rewrite

### Non-goals

- Second `RuntimeConnectionContext` authority without init_fetch plan pins
- Embedding secrets or registry bodies into `pack-index.json`
- Global flip back to `unsafe_airflow_env`
- Changing object-storage stage semantics beyond using existing context preference
- Full remote pack-index deprecation for unrelated platform DAGs

### Assumptions and constraints

- Loader still requires plan env + context path (ADR 0027 fail-closed).
- Closed bridge projection remains complementary to context mounts.
- Consumer domain fleet currently covered by the former pilot set can move to
  a single v2 index without losing scheduled domain DAGs.

## Public contract

### CLI

```text
dpone gitops airflow release-materialize
  --pack-root .dpone/gitops/airflow
  --cache-root .dpone-cache
  --xcom-sidecar-image <digest-pinned OCI ref>
  [--dag-id DAG...]                 # optional subset; default = all dag-specs
  [--output PATH] [--format json|markdown]
```

Exit `0` on success, `2` on blockers. JSON report includes `release_id`,
`dag_ids`, `workload_ids`, `pack_fingerprints`,
`connection_projection_mode`, `strict_init_fetch_rewrite`.

### Python API

```python
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release

report = materialize_compact_pack_release(
    pack_root=Path(".dpone/gitops/airflow"),
    cache_root=Path(".dpone-cache"),
    xcom_sidecar_image="harbor.../alpine@sha256:...",
)
```

### Manifest/schema

Reuses `dpone.release-set.v1` and existing v2 deployment/index schemas with
`runtime_artifact_delivery.mode=init_fetch`. Compact releases may add this
backward-compatible optional top-level variant:

```json
{
  "promotion": {
    "schema": "dpone.compact-pack-release-promotion.v1",
    "profile": "compact_v2_runtime_connection_context"
  }
}
```

The marker is not an artifact and carries no credentials or mutable state. It
is part of release identity so compact promotion cannot reuse a legacy CAS key
whose release-set bytes described another delivery profile.

### Artifacts and evidence

- Release tree: `.dpone-cache/releases/sha256-<digest>/`
- Downstream: existing deployment projection writes
  `cache://runtime-connection-contexts/sha256-…/{binding-set,connection-registry,credential-runtime}.json`

### Compatibility and migration

- Legacy `gitops airflow publish` pack-index lane unchanged (no context).
- Existing `dpone.release-set.v1` readers accept the top-level marker because
  the v1 root is open; `0.73.30+` readers additionally validate its exact
  schema and profile whenever the dpone-owned compact schema or profile
  namespace is signalled. Unrelated legacy `promotion` extensions remain valid.
- Consumers replace `materialize_smoke_v2_release.py` with this CLI.
- Dual-load may remain only until the scheduler points at one v2 `current`.
- Moving the invalid pre-release `artifacts.promotion` shape to the top level
  changes `release_id`; rematerialize that local candidate instead of reusing
  its cache directory. No valid deployment could be built from the invalid
  nested shape.

## Detailed algorithm

1. Discover `pack_root/_dags/*.dag-spec.json` (or filter by `--dag-id`).
2. For each dag-spec, collect `workload_id` values from nodes; require each
   `pack_root/<workload_id>/airflow-pack.json`.
3. Rewrite each pack:
   - keep closed `connection_projection` (`kubernetes_secret_volume`,
     `payload_format=airflow_connection_uri`, `secret_values=false`);
   - preserve URI override maps (`query_overrides` / `scheme_overrides` /
     `database_overrides`);
   - strip non-closed `airflow.execution` extras;
   - pin `xcom.sidecar_image`;
   - inject bounded artifact-reader env templates into
     `provider_execution.kpo_kwargs.env_vars`;
   - recompute `pack_fingerprint` via `compute_pack_fingerprint`.
4. Rewrite each dag-spec: drop `operator_overrides`; recompute dag-spec
   fingerprint.
5. Build `dpone.release-set.v1` with the identity-bearing top-level compact
   promotion variant, compute `release_id`, and write the immutable release via
   `materialize_immutable_local_release`.
6. Fail closed on missing packs, open projection mode, fingerprint drift, or
   empty selection.
7. Consumer continues with existing `airflow build` (projection + context
   snapshots) → publish → cache-sync.
8. Both build and cache activation execute the shared public release-set
   validator; malformed compact markers can never become `current`.

### Pseudocode

```text
dags = discover_dag_specs(pack_root) filtered by dag_ids
for dag in dags:
  packs[workload] = rewrite_strict(load_pack(workload))
  specs[dag.id] = rewrite_dag_spec(dag)
release = assemble_release_set(dags, packs)
release_id = compute_release_id(release)
materialize_immutable_local_release(release_dir, files)
```

### Edge cases

| Case | Behavior |
|---|---|
| Empty pack-root / no dag-specs | blocker, exit 2 |
| Missing workload pack | blocker naming workload id |
| Projection not sealed | blocker |
| Duplicate release bytes | immutable materializer create-or-compare |
| Compact publish without this step | still no RuntimeConnectionContext |

## Architecture

### Components and responsibilities

| Component | Existing/new | Responsibility |
|---|---|---|
| `readiness.airflow_compact_pack_release` | new | Strict rewrite + release-set assembly |
| `AirflowDeploymentProjectionService` | existing | Context snapshots + v2 index |
| `RuntimeConnectionContextLoader` | existing | Unchanged fail-closed authority |
| Consumer loader | consumer | Single v2 index load |

### Alternatives and tradeoffs

| Alternative | Decision |
|---|---|
| Embed context into pack-index.json | Reject — second authority / ADR conflict |
| Teach loader to trust bare context path | Reject — breaks plan pin |
| Keep dual-load forever | Reject — operational debt |

### ADR requirement

No new ADR. This specification amends ADR 0011 to make the optional release
variant an explicit input to canonical identity. It continues to extend ADR
0009/0024 delivery and ADR 0010/0027 authority by promotion, not a parallel
model.

### Quality-budget impact

One new gitops module (<250 SLOC target), thin command/service wrappers.
No god-module split required.

## Market comparison

| System | Relevant capability | Adopt/reject | Source/date |
|---|---|---|---|
| dlt | N/A — no Airflow pack/init_fetch authority model | N/A | — |
| Airbyte | Connector config delivery ≠ verified runtime context | Reject parallel | airbyte.com docs 2026-07 |
| Fivetran | Managed creds, not OSS pack promotion | N/A | — |
| Astronomer Cosmos | dbt task generation, not connection context packs | N/A | — |
| gusty | DAG generation from YAML | Reject as context authority | gusty docs 2026-07 |
| Informatica / Pentaho / SSIS / Beam | N/A for this Airflow pack lane | N/A | — |

## Measurable differentiation

```yaml
axis: single verified connection authority at Airflow parse/runtime
scenario: sealed connection_ref DAG without smoke-v2 dual-load
baseline: dual-load compact + smoke-v2 index
metric: representative DAG SUCCESS with loader dual-load removed
target: all selected synthetic workloads succeed
procedure: build isolated fixtures → publish v2 → sync → trigger
artifact: generated isolated-run certification report (not yet verified)
limitations: platform Python DAGs outside gitops domains remain separate modules
```

## Security, privacy, and operations

- No secret values in release-set or index.
- Registry rewrite remains mount-backed `kubernetes_secret_volume`.
- Attempt Secret RBAC stays a consumer prerequisite.
- Logs/evidence expose only logical refs and digests.

## Test and certification plan

| Layer | Scenario | Expected |
|---|---|---|
| Unit | rewrite preserves URI overrides | PASS |
| Unit | release_id stable / drift fails | PASS |
| Contract | new top-level promotion marker passes the legacy open-root v1 schema | PASS |
| Contract | malformed explicitly signalled compact marker is rejected | PASS |
| Compatibility | unrelated legacy `promotion` extension remains accepted | PASS |
| Integration | `release-materialize -> airflow build -> cache-sync` | PASS |
| Unit | open projection blocked | PASS |
| Contract | release feeds projection context descriptors | PASS (reuse existing projection tests) |
| Live | Representative synthetic fixture set without dual-load | PASS or SKIP with reason |

## Documentation plan

- This feature design
- CHANGELOG
- Consumer cutover docs (consumer repo)

## Rollout and rollback

1. Land OSS CLI + pin consumer.
2. Switch publish script to OSS materialize; keep same registry URI initially.
3. Loader: single v2 index; delete dual-load.
4. Rollback: restore dual-load loader commit; repoint cache to prior deployment.

## Agent execution plan

| Agent/role | Owned paths | Forbidden |
|---|---|---|
| Integrator | feature design, gitops release module, command, tests, changelog, followups | unrelated connectors |

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research uses current official sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer instruction ("Do this") + ADR 0010/0027 / self-service gap authorize `APPROVED` for this vertical slice.

## Native workspace wire-v2 extension

The legacy reconcile input and release-v1 bridge described above remain supported.
The [approved native workspace extension](feature-design-dbt-compact-wire-v2.md)
also accepts complete canonical workspace compile output. It preserves v2 source
authority and native deployment-owned connections, regenerates transport identity,
and rejects partial workspace selection. See the
[delivery guide](dbt-compact-delivery.md) for the distinct input modes.
