# Feature design: Airflow pack storage modes (gitops / remote / hybrid)

- Status: IMPLEMENTED
- Owner: data-platform / KovalevPA01
- Issue: pack git bloat + !474-class stale git fallback; follow-up to pack-gate
- Target release: 0.74.0 (or next minor/patch after merge)
- Last verified: 2026-07-20

## Executive summary

Compiled Airflow artifacts (`airflow-pack.json`, `*.dag-spec.json`) must not be
forced into git for every consumer. dpone exposes an explicit
`artifacts.airflow_pack.storage.mode` with three values: `gitops` (default,
backward compatible), `remote` (object storage is the only runtime source;
`.dpone/gitops` is not read or committed), and `hybrid` (remote primary with
optional git fallback). Object-store `kind` covers `s3`, `gcs`, `azure_blob`,
and `filesystem`. Consumers that already used `mode: object_storage` normalize
to `remote`.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Change YAML only | Git packs bloat MRs; stale pack writes wrong DB | Remote publish → cache → correct target |
| Platform | One contract for many fleets | Dual truth (S3 + git fallback) | Mode matrix + CI gates per mode |
| SRE | Fail closed on cache miss | Silent `local_fallback` | Cache miss fails parse/run |

## Scope

### In scope

- Public mode enum + normalization (`object_storage` → `remote`)
- Resolution policy: when git fallback is allowed
- Publish index may include dag-specs
- platform-ci pack-gate remote oracle (no HEAD / no bot-push packs)

### Non-goals

- Full production GCS/Azure adapters (contracts + stubs OK)
- Removing `gitops` mode from the platform
- Ephemeral MR-only pack store (later)

## Public contract

### Manifest/schema

```yaml
gitops:
  artifacts:
    airflow_pack:
      storage:
        mode: gitops | remote | hybrid   # default gitops
        kind: s3 | gcs | azure_blob | filesystem
        uri_prefix: ...
        latest_index_uri: ...
        writer_connection_id: ...
        reader_connection_id: ...
```

Legacy alias: `mode: object_storage` ⇒ `remote`.

### Runtime

| Mode | Read packs/dag-specs | local_fallback to git |
|---|---|---|
| gitops | `.dpone/gitops` | N/A |
| remote | scheduler cache only | forbidden |
| hybrid | cache then git | allowed when policy allows |

### CI

| Mode | pack-gate oracle | bot-commit packs |
|---|---|---|
| gitops / hybrid | HEAD vs reconcile | yes (MR) |
| remote | generated exists (+ optional remote index digests) | no |

## Detailed algorithm

1. Load `storage.mode` from workload-set; normalize aliases.
2. Validate remote/hybrid require kind + URIs + connections.
3. Reconcile always writes workspace `.dpone/gitops` (ephemeral in CI).
4. If mode=gitops|hybrid: pack-gate compares to git HEAD; may auto-commit.
5. If mode=remote: pack-gate verifies generated artifacts only; publish job
   uploads packs (+ dag-specs when present) and swaps latest index.
6. Scheduler sync fills cache; loaders use cache/`cached://` only in remote.

## Architecture

- `normalize_pack_storage_mode` / `PackStoragePolicy` (pure) in dpone.gitops
- Existing `AirflowPackPublisher` / `ObjectArtifactStore` for remote put
- platform-ci reads mode from gitops.yaml
- Consumer wiring refuses fallback when mode=remote or env refuse

## Market comparison

| System | Relevant | Notes |
|---|---|---|
| dlt | N/A | Not Airflow pack artifacts |
| Astronomer Cosmos | Adopt pattern | Compiled artifacts outside source tree |
| gusty | N/A | DAG generation, not pack store |
| Airbyte/Fivetran/Informatica/SSIS/Pentaho/Beam | N/A | Different packaging |

## Measurable differentiation

```yaml
axis: git repo size / MR noise for pack artifacts
scenario: 50 workload YAML-only changes under remote mode
baseline: each change commits multi-MB airflow-pack.json
metric: committed bytes under .dpone/gitops after change
target: 0
```

## Test plan

- Unit: mode normalize + alias + validation
- pack-gate: remote skips committed_pack_missing
- Consumer: remote refuses local_fallback; gitops.yaml mode=remote
- Publish index may list dag_specs keys when provided

## Rollout

1. Ship OSS + platform-ci remote-aware gate (default gitops unchanged)
2. In an isolated example, select remote mode and validate artifact retrieval before retiring local generated files
3. Prove cache sync + one DAG; no local_fallback provenance

## Documentation and UX

- Mode matrix in load-governance / gitops Airflow docs
- MR Impact section shows storage.mode and whether pack commit is required

## Open questions

None for v1 — GCS/Azure adapters deferred.
