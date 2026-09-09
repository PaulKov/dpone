# Feature design: closed init-fetch Airflow Connection bridge

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: connection_ref operator_bridge cutover (consumer blockers B1–B3)
- Target release: next patch after 0.73.8
- Last verified: 2026-07-21
- Revision: completes the already-APPROVED Airflow self-service architecture
  bridge gap called out by `init_fetch_pod_guard` and ADR 0010/0027
- Evidence: `tests/test_airflow_provider_execution_authority.py`,
  `tests/test_airflow_connection_runtime_registry.py`

## Executive summary

Strict `init_fetch` already delivers verified `RuntimeConnectionContext`
(binding-set, connection-registry, credential-runtime) and launches the
Airflow-free runtime image. Compact-pack operator-bridge projection
(`kubernetes_secret_volume` + `airflow_connection_uri`) already exists for the
legacy/local lane. The two paths were intentionally disconnected: the provider
rejected any `connection_projection` under strict init-fetch and forced the
plain KPO class, so registry `airflow_connection` entries could never
`resolve()` inside the pod.

This vertical slice closes that bridge without flipping global
`unsafe_airflow_env` and without changing legacy `connection_id` semantics:

1. allow only the closed bridge projection under strict init-fetch;
2. select `AirflowConnectionSecretVolumeKubernetesPodOperator` and pass the
   projection even when a delivery context is present;
3. rewrite published runtime registry snapshots so `airflow_connection`
   becomes `kubernetes_secret_volume` mounts that match the bridge projection;
4. keep Git/source registries on `airflow_connection` + `operator_bridge` for
   offline check and bridge intent.

Success: a v2 init-fetch pack with closed bridge projection materializes the
secret-volume operator, mounts attempt Secrets, and resolves `connection_ref`
through `RuntimeConnectionContext` without `unsafe_airflow_env`.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Platform engineer | Cut over DEV to `connection_ref` | Offline registry green; runtime fail-closed | Pilot pack runs on init-fetch + bridge |
| Pipeline author | Keep manifests environment-neutral | Forced to stay on legacy `connection_id` | Smoke domain uses `connection_ref` only |
| Airflow operator | Preserve legacy compatibility | Global projection flip would break env-based legacy | Legacy packs stay on `unsafe_airflow_env` |

Journey (pilot):

1. Author/platform keep Git registry as `airflow_connection`/`operator_bridge`.
2. Publish environment deployment with init-fetch delivery (existing v2 path).
3. Compact pack carries closed `connection_projection` (shape from
   `dpone check --connections` → `airflow_connection_bridge.projection`).
4. Provider validates projection, selects secret-volume operator, publishes
   attempt Secret, mounts URI files.
5. Runtime launcher sets `DPONE_RUNTIME_CONNECTION_CONTEXT`; resolver reads
   rewritten `kubernetes_secret_volume` mounts.
6. Legacy packs without closed projection remain unchanged.

## Scope

### In scope

- Closed-bridge validation under `validate_strict_pack_extensions`.
- Strict-lane operator selection and projection kwargs wiring.
- Deterministic runtime registry rewrite at deployment snapshot publication.
- Focused unit/contract tests, provider API note, changelog, consumer follow-ups.

### Non-goals

- Global GitOps flip away from `unsafe_airflow_env`.
- Migrating consumer manifests to `connection_ref`.
- Compact-pack-only (non-init-fetch) RuntimeConnectionContext authority.
- Environment-specific service-account policy changes.
- Mid-workload credential refresh or Vault replacement.

### Assumptions and constraints

- ADR 0010, ADR 0027, and `docs/airflow-self-service-architecture.md` remain
  authoritative; this fills their incomplete provider wiring.
- `RuntimeConnectionContextLoader` continues to require the pinned init-fetch
  plan; bare compact reconcile without v2 delivery stays out of scope.
- Attempt Secret create/delete RBAC remains a consumer-platform prerequisite.

## Public contract

### CLI

No new commands. Existing `dpone check --connections` bridge projection remains
the pack authoring source of truth.

### Python API / provider

Strict init-fetch packs may include:

```yaml
connection_projection:
  mode: kubernetes_secret_volume
  secret_name: dpone-airflow-connection-bridge
  mount_path: /run/secrets/dpone/airflow-connections
  payload_format: airflow_connection_uri
  secret_values: false
  cleanup_policy: after_execute   # or retain for deferrable
  connections:
    - connection_ref: warehouse
      registry_connection_ref: warehouse
      connection_id: warehouse
      secret_key: AIRFLOW_CONN_WAREHOUSE
      mount_path: /run/secrets/dpone/airflow-connections/warehouse
      fields: {uri: uri}
```

Invalid or `unsafe_airflow_env` projections under strict init-fetch fail with
`DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID` before operator construction.
Legacy/local packs without delivery context keep prior behavior, including
`unsafe_airflow_env`.

Published init-fetch registry snapshots rewrite each `airflow_connection`
entry to `kubernetes_secret_volume` + `payload_format: airflow_connection_uri`
using the deterministic bridge mount layout. Source registry files are not
mutated.

### Compatibility and migration

- Legacy `connection_id` + `unsafe_airflow_env`: unchanged.
- Packs without `connection_projection` on init-fetch: unchanged (Vault/K8s
  resolvers only).
- Consumers must publish via v2 init-fetch deployment projection to obtain
  `RuntimeConnectionContext`; compact reconcile alone remains insufficient.

## Detailed algorithm

1. Provider loads pack with delivery context.
2. `validate_strict_pack_extensions`:
   - empty projection → ok;
   - closed bridge shape → ok;
   - anything else → `DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID`.
3. Compose strict pod/kwargs as today.
4. If closed bridge present, select
   `AirflowConnectionSecretVolumeKubernetesPodOperator` and pass
   `airflow_connection_projection` even with `strict_runtime_image_ref`.
5. At task execute: existing attempt Secret create/mount/cleanup.
6. At deployment snapshot publish: rewrite registry `airflow_connection`
   credentials to mount-backed `kubernetes_secret_volume` using
   `airflow_connection_bridge_report(...).projection` (or an explicit matching
   pack projection when provided by the publisher).
7. Runtime loader verifies context against the init-fetch plan and resolves
   through `BindingCredentialResolver`.

### Pseudocode

```text
if pack.connection_projection configured:
    require_closed_init_fetch_connection_bridge(pack.connection_projection)
operator_pack = selection_pack(pack, strict=delivery_context is not None)
operator = operator_class(operator_pack)(..., **operator_init_kwargs(pack, ...))

runtime_registry = rewrite_airflow_connection_for_runtime(source_registry)
publish snapshots(binding_set, runtime_registry, credential_runtime)
```

## Architecture

| Component | Existing/new | Responsibility |
|---|---|---|
| `init_fetch_connection_bridge` | new (pack) | Validate closed projection |
| `pack_task_runtime` / `pack_tasks` | existing | Wire operator under delivery context |
| `airflow_connection_runtime_registry` | new (readiness) | Rewrite runtime registry snapshot |
| `AirflowConnectionSecretVolumeKubernetesPodOperator` | existing | Attempt Secret bridge |

ADR requirement: no new ADR; amends implementation of ADR 0010/0027 and the
APPROVED self-service architecture.

Quality budget: keep new modules small; do not grow
`airflow_deployment_projection.py` past the hard SLOC limit.

## Market comparison

| System | Relevant capability | Adopt/reject | Source/date |
|---|---|---|---|
| Apache Airflow 2.10+/3.x | Connections resolved at task time; KPO secret mounts | Adopt operator-side resolve | Airflow Connections howto, 2026-07 |
| dlt / Airbyte / Fivetran / Informatica / Pentaho / SSIS / gusty / Cosmos / Beam | N/A — not the Airflow KPO credential bridge surface | N/A | — |

## Measurable differentiation

```yaml
axis: fail-closed connection_ref on strict init-fetch without AIRFLOW_CONN_* env
scenario: v2 pack + closed bridge + airflow_connection registry
baseline: DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED or resolve() fail-closed
metric: operator class + rewritten registry resolve from mounted uri file
target: hermetic contract tests green; no unsafe_airflow_env
procedure: pytest focused provider/readiness modules
artifact: CI logs on the PR
limitations: live K8s Secret RBAC and consumer pilot remain out of band
```

## Test and certification plan

| Layer | Scenario | Expected |
|---|---|---|
| Unit | Incomplete/unsafe projection under strict | `DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID` |
| Unit | Closed projection under strict | secret-volume operator + kwargs |
| Unit | Registry rewrite | `airflow_connection` → mount-backed k8s volume |
| Compatibility | No projection / legacy unsafe local lane | unchanged |
| Live | Consumer pilot | UNVERIFIED; requires an explicitly approved isolated environment |

## Documentation plan

- Provider API: document strict closed-bridge allowance.
- Architecture/backlog: mark bridge wiring complete for init-fetch.

## Rollout and rollback

1. Land OSS PR; pin consumer after release.
2. Validate least-privilege Secret access and cleanup in an isolated environment.
3. Rollback: rebuild packs without closed projection; prior provider rejects
   projection again only if pin is reverted.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research uses current official sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED` through standing cutover
      authorization to close the documented init-fetch bridge gap under
      ADR 0010/0027 and the APPROVED self-service architecture.
