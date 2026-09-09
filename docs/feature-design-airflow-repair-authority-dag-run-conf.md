# Feature design: Airflow `dag_run.conf` repair-authority injection

- Status: APPROVED
- Owner: dpone maintainers
- Issue: work-item
- Target release: 0.74.10
- Last verified: 2026-08-20

Maintainer authorization: first-class scheduler compose is required. Consumer
GitOps loader overlays that rewrite pack `env_vars` after compose are not an
accepted contract.

## Executive summary

Runtime already admits one opaque repair authority from
`DPONE_REPAIR_AUTHORITY_REF` / `--repair-authority-ref`. Strict `init_fetch`
compose currently never injects that variable. Packs cannot supply it because
it is not in `ALLOWED_PACK_ENV`, and DAG `operator_overrides` are rejected.
Exceptional PostgreSQL XMin full-baseline recoveries therefore fail closed in
Airflow even after an environment owner inserts an immutable
`dpone_repair_authority` row.

This change injects a Jinja template in `compose_init_fetch_operator_kwargs`
for every workload, after the pack-env allowlist, the same way
`DPONE_DBT_EVIDENCE_SET_ID` is injected from `dag_run.conf`. Ordinary runs
render an empty string and remain a no-op.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Environment owner | One-shot full baseline / delete-guard override | Authority row exists; Airflow cannot name it | One DAG run with `dag_run.conf` consumes the exact ID |
| Data engineer | Multi-process DAG, one run | One scalar env cannot bind three `state_key`s | Per-`task_id` map `DPONE_REPAIR_AUTHORITY_REFS` |
| Pack author | Keep packs static | Temptation to bake an ID into pack env | Pack-provided `DPONE_REPAIR_AUTHORITY_REF` fail-closed |

Journey: insert immutable authority → trigger DAG with conf map or scalar →
Airflow renders env at task execute → `runtime-pack-exec` / `dpone run` reads
the env → runtime consumes once with the commit receipt.

## Scope

### In scope

- Scheduler-owned Jinja on `DPONE_REPAIR_AUTHORITY_REF` for all init-fetch
  workloads.
- Per-task map `dag_run.conf['DPONE_REPAIR_AUTHORITY_REFS']` keyed by
  `ti.task_id`, with scalar `dag_run.conf['DPONE_REPAIR_AUTHORITY_REF']`
  fallback.
- Keep the variable **out** of `ALLOWED_PACK_ENV`.
- Docs for Airflow invocation next to the existing CLI/env contract.

### Non-goals

- New CLI flags or runtime admission rules.
- Standing Kubernetes Secret / ConfigMap IDs.
- Consumer GitOps loader monkeypatches.
- Automatic authority issuance.
- Changing empty-string semantics (empty remains no authority).

### Assumptions and constraints

- Runtime 0.74.9+ already understands the env. This is pack compose only.
- Coordinated consumer pins still require the same version on `dpone`,
  `dpone-airflow-pack`, `dpone-native-accel`, and
  `apache-airflow-providers-dpone`.
- Conf map values must be a JSON object. A string is not a mapping and falls
  back to the scalar key.

## Public contract

### CLI

Unchanged. `--repair-authority-ref` and `DPONE_REPAIR_AUTHORITY_REF` remain
the only admission surfaces. Manifests must not contain `repair_authority_ref`.

### Python API

Unchanged.

### Manifest/schema

Unchanged.

### Airflow pack compose

`compose_init_fetch_operator_kwargs` always sets:

```jinja
{% set _refs = (dag_run.conf.get('DPONE_REPAIR_AUTHORITY_REFS') if dag_run is defined and dag_run else none) %}
{{ (_refs.get(ti.task_id, '') if _refs is mapping else '') or (dag_run.conf.get('DPONE_REPAIR_AUTHORITY_REF', '') if dag_run is defined and dag_run else '') }}
```

The template is copied into operator `env_vars` and the runtime base-container
env. Airflow renders it at task execute.

Pack-provided `DPONE_REPAIR_AUTHORITY_REF` raises
`DPONE_INIT_FETCH_RESERVED_COLLISION`.

### Compatibility and migration

Old behavior: missing env, empty string, or unset conf → no authority.
New behavior: same, plus conf-driven injection.
Rollback: pin the previous pack; conf keys become inert.

## Detailed algorithm

1. `provider_env` copies only `ALLOWED_PACK_ENV` keys.
2. Compose writes plan env, then the repair-authority template.
3. `_strict_pod` copies operator env onto the base container.
4. Airflow templates env at KPO execute using `dag_run` and `ti`.
5. `RunInvocationContextService` reads `DPONE_REPAIR_AUTHORITY_REF`.
6. Empty or invalid IDs fail as today; a valid ID is admitted once.

### Pseudocode

```text
env = provider_env(pack.env_vars)
env[PLAN_B64] = ...
env[PLAN_SHA256] = ...
if "DPONE_REPAIR_AUTHORITY_REF" in env:
    raise RESERVED_COLLISION
env["DPONE_REPAIR_AUTHORITY_REF"] = REPAIR_AUTHORITY_REF_TEMPLATE
```

### Failure semantics

- Pack-baked ID: compose fails closed, DAG does not parse that pack.
- Missing conf: empty string, incremental path unchanged.
- Wrong `task_id` key: that task sees empty (or the scalar fallback).
- Reuse / expiry / digest mismatch: existing runtime fail-closed codes.

## Alternatives considered

| Option | Why rejected |
|---|---|
| Add the var to `ALLOWED_PACK_ENV` | Standing pack IDs become possible |
| Loader `operator_init_kwargs` overlay | Consumer-only, not the public pack contract |
| One scalar env for a three-process DAG | Cannot bind three `state_key`s in one run |
| `runtime-pack-exec --repair-authority-ref` | Duplicates env admission; still needs compose |

## Tests

- Compose injects the template for a non-dbt workload.
- Base-container env carries the same template.
- Pack-provided `DPONE_REPAIR_AUTHORITY_REF` raises
  `DPONE_INIT_FETCH_RESERVED_COLLISION`.
- `ALLOWED_PACK_ENV` does not contain the variable.

## Documentation

- `docs/airflow-pack-provider.md`
- `docs/state.md`
- `docs/postgres-xmin.md`
- `CHANGELOG.md`
