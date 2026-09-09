# MSSQL Airflow Asset URIs (AIP-60)

**Audience:** pipeline authors and platform operators publishing Airflow packs
with MSSQL lineage outlets / asset scheduling.

## Why this exists

`apache-airflow-providers-microsoft-mssql` validates Asset URIs on Airflow 3.3+.
Compact forms such as `mssql://schema/table` fail DAG parse. dpone therefore
emits one canonical shape from a single canonicalizer used by inferred
source/sink lineage, explicit inlets/outlets, partition bindings, and pack
runtime outlets.

## Canonical form

```text
mssql://{host}:{port}/{database}/{schema}/{table}
mssql://{host}:{port}/{INSTANCE}/{database}/{schema}/{table}
```

Rules:

1. `{host}:{port}` is deployment-owned **asset_authority** — never
   `connection_ref`.
2. Emitted URIs always include an explicit port (default `1433`) so identity
   does not drift with or without the microsoft-mssql provider.
3. Explicit authoring URIs **may omit the port**. When the active environment
   registry has exactly one approved authority for that host, pack/build adopt
   that authority's port and instance. Otherwise the URI is rejected (or, when
   no registry snapshot is available, defaults to `:1433` for syntax-only
   checks). Compact / host-less forms remain invalid.
4. Named instance path segments are normalized to lowercase so registry and
   explicit URIs share one Asset identity.
5. Path segments are percent-encoded (`urllib.parse.quote(..., safe="")`).
6. Templated / Jinja coordinates are not inferred into URIs.
7. Pack outlets are **canonical-only**: declared → canonicalize → merge
   inferred → dedupe. Never emit both `mssql://host/...` and
   `mssql://host:1433/...`. Same canonical URI with conflicting metadata is a
   structured blocker.

## Environment-bound registry

Authority and default-database indexes come from **one immutable**
`ResolvedMssqlAssetRegistry` snapshot for the active `--env` (GitOps
`pack` / `reconcile` / asset-deps / DAG specs):

```text
.dpone/registry/connection-registries/<env>.yaml
# or
platform/connection-registries/<env>.yaml
```

Exactly one source may exist for an environment; both locations together is a
blocker (`DPONE_MSSQL_REGISTRY_SOURCE_AMBIGUOUS`). Symlink components, path
escapes, and non-regular files fail closed as
`DPONE_MSSQL_REGISTRY_PATH_INVALID`.

There is no best-effort scan across `dev.yaml` → `prod.yaml`. Mixing authority
from one env with database defaults from another is forbidden. One GitOps
artifact set resolves the registry once (single-read bytes → sha256 → YAML
parse of those exact bytes), passes the same snapshot to every pack builder /
DAG spec / asset-deps path, and blocks when the on-disk registry digest drifts
mid-run. When the document declares `environment`, it must equal the requested
`--env` (`DPONE_MSSQL_REGISTRY_ENVIRONMENT_MISMATCH`).

### dbt release vs deployment (build-once)

Immutable dbt release packs are **environment-neutral**. They keep a logical
Asset reference instead of a physical URI:

```yaml
asset_ref:
  engine: mssql
  connection_ref: mssql_marts
  database: DWH
  schema: dbo
  table: orders
```

Physical `mssql://host:port[/instance]/database/schema/table` is materialized
only at the **deployment boundary** using the same binding path as credentials:

1. logical `asset_ref.connection_ref`
2. `binding_set.bindings[logical].connection_ref` (exact bound registry ref)
3. `connection_registry.connections[bound].asset_authority`
4. canonical AIP-60 URI

The closed wire document `dpone.mssql-asset-outlet-projection.v1` is attached to
**both** `deployment.json` (before `deployment_id` is computed) and
`airflow-index.json`. When a projection is present the producer emits the
closed v3 wire pair (`dpone.deployment-set.v3` /
`dpone.airflow-deployment-index.v3`) with the projection required; otherwise it
keeps exact v2 so older closed `exact_mapping` readers are unaffected.
Activation parses the deployment projection and the index projection
separately, verifies each internal `projection_sha256`, env/binding/registry
refs, then requires byte/canonical equality before accepting `deployment_id`.
Identity keys use `asset_ref_sha256 = sha256(JCS({schema, engine,
connection_ref, database, schema_name, table}))` — never `|`-joined composites.
The same release digest proven in dev is promoted to prod without rebuild; prod
outlets resolve to the prod bound authority (including normalized named
instances such as `prod01`).

URI runtime authority is the shared pack codec
(`dpone_airflow_pack.mssql_asset_uri_codec`): scheme `mssql` exactly; no
userinfo/query/fragment; explicit port `1..65535`; 3 or 4 path segments;
bounded decoded segments; percent-decode/re-encode equality; lowercase
host/instance; `canonicalize(parse(uri)) == uri`.

## Where to set asset_authority (SoT)

```yaml
schema: dpone.connection-registry.v1
environment: dev
connections:
  mssql_example:
    type: mssql
    connection:
      asset_authority:
        host: sql-prod.internal
        port: 1433
        instance: null
    credentials:
      resolver: airflow_connection
      connection_id: mssql_example
      execution_mode: operator_bridge
```

Fallback: `connection.host` + `connection.port` (+ optional `connection.instance`)
when nested `asset_authority` is omitted.

### Managed source/sink (`connection_ref`)

`lineage.asset_authority` is **not** an author override. When present it is an
assertion: after hostname normalization it must exactly match the registry
authority for that `connection_ref`. Mismatch →
`DPONE_MSSQL_ASSET_URI_INVALID`.

```yaml
sink:
  type: mssql
  connection_ref: mssql_example
  lineage:
    asset_authority:          # optional assertion only
      host: sql-prod.internal
      port: 1433
  table:
    database: dwh_example
    schema: dbo
    name: orders
```

### Explicit `mssql://` inlets/outlets

Full physical authority ``host + port + instance`` must match an approved
authority from the env registry (hostname/instance normalized). Unknown /
typo hosts, wrong ports, and missing/wrong named instances fail closed.

## Failure behavior

| Layer | Behavior |
| --- | --- |
| Build / `dpone check` / standalone compact pack | Structured `AssetUriResolution`; invalid MSSQL → blocker `DPONE_MSSQL_ASSET_URI_INVALID`; pack `passed=false` and **no output file** |
| Ordinary GitOps `pack` / `reconcile --env` | Environment-specific physical URIs (env artifact set, not a portable dbt release) |
| dbt publish transfer packs | Environment-neutral logical `asset_ref` inside release bytes; physical URI only via deployment `mssql_asset_outlet_projection` |
| Strict deployment activation | Logical `asset_ref` + missing/invalid/mismatched projection → `DPONE_MSSQL_ASSET_OUTLET_PROJECTION_REQUIRED\|INVALID\|MISMATCH` (never silent green); independently valid A in deployment + B in index → `MISMATCH` |
| Parse-time outlets | Resolve `asset_ref` through deployment projection (fail-closed if absent); projected `Asset()`/`Dataset()` `ValueError` → `DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID`; legacy authoring sanitizer `ValueError` → `DPONE_MSSQL_ASSET_URI_PARSE_REJECTED` |

Malformed ports (`:abc`, `:65536`), credentials, query strings, and fragments
are structured blockers (never uncaught `ValueError`).

See [DPONE_MSSQL_ASSET_URI_INVALID](errors/DPONE_MSSQL_ASSET_URI_INVALID.md).

## Migration

1. Add `connection.asset_authority` for every MSSQL `connection_ref` used by
   Airflow outlet / asset-scheduling workloads.
2. Ensure `table.database` or LoadConfig `source_database` /
   `target_database` is set.
3. Prefer canonical explicit inlets/outlets; portless host URIs are accepted
   and normalized to `:1433`. Compact forms remain invalid.
4. Re-run `dpone check` / pack gates, then bump the consumer dpone pin after
   this change merges.
