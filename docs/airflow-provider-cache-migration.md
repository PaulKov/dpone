# Migrate Airflow provider imports and cache loading

Purpose: move an existing dpone Airflow installation to the canonical provider,
fail-closed generated loader, and immutable release/deployment cache without
adding network or database access to DAG parsing.

Audience: Airflow platform engineers and DAG repository maintainers upgrading
legacy `dpone_airflow_pack` imports or a mutable `latest/pack-index.json` cache.

Use this guide for an upgrade. For a new installation, start with the
[lightweight provider overview](airflow-pack-provider.md) and use the canonical
loader from the beginning.

## Safe target

| Surface | Legacy compatibility path | Target |
| --- | --- | --- |
| Provider facade import | `dpone_airflow_pack` | `airflow.providers.dpone` |
| Generated loader | Return value ignored, or no fatal guard | Assign `LoadReport`, pass `index_path`, and raise with the first stable error code when `fatal` |
| Cache source | Remote mutable `latest/pack-index.json` | Local `current/airflow-index.json` pointing at one sealed immutable deployment |
| Artifact identity | Mutable generation/current lookup | Pinned `release_id`, `deployment_id`, positive `bytes`, and SHA-256 |
| DAG parse I/O | A sync sidecar may update legacy state | Provider reads bounded local files only; no network, metadata DB, Variables, Connections, Vault, or Kubernetes |
| Runtime delivery | v1 `local_preview` or incomplete v1 `init_fetch` | Keep v1 `local_preview` non-runnable; regenerate executable `init_fetch` as a complete v2 index |

`current/airflow-index.json` is a local activation path, not a mutable artifact
identity. The cache promoter switches `current` atomically to a sealed
deployment. Every artifact reference inside the index remains pinned below an
immutable release and must not contain `current` or `latest`.

## Prerequisites

- Build one scheduler image with `apache-airflow-providers-dpone` and its
  matching `dpone-airflow-pack` dependency on the same dpone release line as
  the generated runtime packs.
- Mount the new cache at `/opt/airflow/.dpone-cache`, or choose another
  stable local mount and use that exact absolute path in `index_path`.
- Retain the last known-good release and deployment until cutover verification
  and the rollback window complete.
- Run publication, remote materialization, promotion, and recovery outside the
  scheduler/DAG parse process under a platform identity.
- Stop if the old cache has no trusted immutable release source. Do not convert
  currently visible mutable bytes into a release by editing JSON or copying a
  pack in place.

## 1. Move provider facade imports

Replace provider-facade imports as one mechanical change:

| Legacy import | Canonical import |
| --- | --- |
| `from dpone_airflow_pack import load_dpone_dags` | `from airflow.providers.dpone import load_dpone_dags` |
| `from dpone_airflow_pack import DponeDag, DponeTaskGroup` | `from airflow.providers.dpone import DponeDag, DponeTaskGroup` |
| `from dpone_airflow_pack import CacheResolver, LoadReport` | `from airflow.providers.dpone import CacheResolver, LoadReport` |
| `from dpone_airflow_pack import AirflowDeploymentIndex, AirflowDeploymentIndexError, AirflowIndexArtifact` | `from airflow.providers.dpone import AirflowDeploymentIndex, AirflowDeploymentIndexError, AirflowIndexArtifact` |

The legacy root exports remain compatibility shims and emit one
`DeprecationWarning` per process. Pack-library helpers such as
`load_dpone_airflow_pack_with_provenance`,
`workload_ids_from_gitops_domain`, and runtime-adapter helpers still belong to
`dpone_airflow_pack`; they are not provider-facade imports.

Verify the scheduler image locally:

```bash
python3 -c "from airflow.providers.dpone import load_dpone_dags; print(load_dpone_dags.__module__)"
```

This import check performs no remote cache refresh or database access.

## 2. Initialize or upgrade the generated loader

From the authoring repository, run:

```bash
dpone init project --airflow
```

The command initializes `dags/dpone.py`. On a rerun, it upgrades only exact
known fingerprints of historical dpone-generated loader bytes. Canonical bytes
are an idempotent no-op. A loader with any user change, including comments or
whitespace, is a conflict and remains byte-for-byte unchanged; review and
migrate that file manually instead of forcing an overwrite.

The target loader is:

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

Keep `index_path` and the separate writable `ack_root` explicit and aligned
with the scheduler image mounts. The loader assigns the returned result before
checking `loaded.report.fatal`; it preserves
`errors[0].code` when available and uses
`DPONE_AIRFLOW_INDEX_INVALID` only as the defensive fallback. It does not fetch
the index, resolve credentials, or query the Airflow metadata database.

Review and deploy the generated file through the normal DAG repository
workflow. Do not add cache sync, object-storage, SQL, Variable, Connection,
Vault, or Kubernetes calls to it.

## 3. Replace the mutable pack index

The legacy compatibility command may read a URL such as:

```text
s3://bucket/dpone-artifacts/prod/repo/latest/pack-index.json
```

Do not point the provider loader at that object and do not fetch it during DAG
parse. Build or obtain a complete immutable release/deployment pair, then use
the platform publication and materialization workflow documented in
[Airflow cache sync and recovery](airflow-cache-sync.md#materialize-the-prerequisite).
The resulting local layout is:

```text
.dpone-cache/
  releases/sha256-<release>/
    release-set.json
    dags/...
    packs/...
  deployments/<environment>/sha256-<deployment>/
    deployment.json
    airflow-index.json
    _SUCCESS
  activations/<environment>/sha256-<deployment>/
    deployment.json
    airflow-index.json
    _SUCCESS
  current -> activations/<environment>/sha256-<deployment>
```

`current` must be exactly one relative symlink to the canonical sealed
activation path. The provider rejects a directory, an absolute symlink, a
`deployments/**` target, traversal, a symlinked intermediate component, a
non-lowercase digest, or a deployment/environment identity mismatch. Existing
caches with another layout must be rematerialized and promoted with
`dpone airflow cache-sync`; never repair immutable bytes or the symlink by hand.

Roll out the writer/materializer before the read-only scheduler/provider image.
The writer creates the metadata-only `.promotion.lock` with mode `0664`; cache
readers open that existing inode with `O_RDONLY` and never create or modify it.
Do not create the file by hand: a successful reviewed materialization proves
the writer and shared-volume ownership are correct. A cache root without the
lock fails visibly as `DPONE_CACHE_READ_LEASE_FAILED`; keep the previous
provider active until the first writer promotion succeeds.

The new `airflow-index.json` must use the wire that matches its purpose:

- `dpone.airflow-deployment-index.v1` remains the supported non-runnable
  `local_preview` compatibility wire;
- `dpone.airflow-deployment-index.v2` is required for executable
  `init_fetch`.

Both use exact lowercase SHA-256 release and deployment identities and pinned
`cache://releases/sha256-<release>/...` artifact references. Do not change only
the `schema` string: v2 is a new immutable deployment projection with a complete
image, artifact, trust, identity, and runtime-plan contract.

After the exact release and deployment have been materialized below the new
cache root, perform the first reviewed promotion:

The stock runtime now includes the concrete offline GitHub Artifact
Attestations verifier. A production `required_for_prod` projection is runnable
only when its v2 policy, trusted root, detached bundle, subject, and certified
runtime image all pass; live Kubernetes/workload-identity and route
certification are still required before calling an environment
production-certified. Use a strict-v2 non-production executable rehearsal in
dev/stage instead of weakening production policy.
A paused production parse canary remains a separate rollout lane: it proves
only scheduler/provider parsing and does not establish production
certification. Its tasks must not be triggered.

```bash
: "${DPONE_SCHEDULER_CACHE_ROOT:=/opt/airflow/.dpone-cache}"
: "${DPONE_DEPLOYMENT_DIR:?set the reviewed sha256-... deployment directory name}"

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_SCHEDULER_CACHE_ROOT}/deployments/prod/${DPONE_DEPLOYMENT_DIR}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expect-current-absent \
  --confirm-promote
```

If the new cache already has a current deployment, generate a fresh
`cache-recovery-plan` and use its `current_path_deployment_id` as
`--expected-current-deployment-id`; do not reuse the first-promotion command.
Promotion verifies local release/index/artifact identities and then activates a
sealed snapshot. It does not download remote objects.

Keep the legacy sync and new cache in separate directories during a canary.
After the new loader parses the expected DAG set from
`current/airflow-index.json`, remove the legacy `dpone-airflow-pack-sync`
sidecar or startup step. Do not let it write into the new cache root.

Current releases enforce this boundary with
`.dpone-cache-layout.json`: the compatibility watcher declares
`legacy_pack_index_v1`, while exact desired-state materialization declares
`exact_deployment_v1`. A mismatch fails before pointer or generation mutation.
Do not copy the marker, `current`, or `status/current-commit.json` between
roots. Rollback switches the reviewed exact deployment; it does not reactivate
the legacy watcher in the exact root.

## Migrate v1 init-fetch to v2

First classify the active local index without contacting Airflow or a remote
registry:

```bash
jq -r '[.schema, .runtime_artifact_delivery.mode] | @tsv' \
  /opt/airflow/.dpone-cache/current/airflow-index.json
```

Apply this decision table:

| Observed index | Action |
| --- | --- |
| v1 `local_preview` | No executable migration is required. Keep it on the non-runnable preview lane. |
| v1 `init_fetch` | Stop. The provider returns `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED` before installing a DAG. |
| v2 `init_fetch` | Continue only after validating the complete fields and compatible provider/runtime image. |
| v2 non-`init_fetch` | Stop with `DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED`. |

For v1 `init_fetch`, regenerate the immutable release/deployment projection with
a v2-capable producer. The resulting v2 index must include:

- an exact `runtime_image_ref` with the same digest as
  `runtime_image_digest`;
- matching explicit top-level and delivery-level `trust_tier`;
- positive `bytes` and SHA-256 for the exact release, deployment, and every
  selected workload pack, plus each pack fingerprint;
- `pack_identity.schema: dpone.airflow-pack-identity.v1` in every selected
  workload pack, with a fingerprint independently derived from the complete
  canonical pack rather than copied from release metadata;
- a bounded logical registry ref, a workload-identity service account and
  namespace, and a digest-pinned registry ConfigMap `{kind, name, key,
  sha256}`;
- for `production`, a digest-pinned trust-policy ConfigMap and
  `attestations: required_for_prod`.

Each selected workload pack must be rebuilt by a producer that emits the
whole-pack identity, the closed
`dpone.airflow-provider-execution.v1` projection, and a digest-pinned XCom
sidecar image. Check the structural declarations in the materialized release
without executing it:

```bash
: "${DPONE_RELEASE_DIR:?set the reviewed immutable release directory}"

find "${DPONE_RELEASE_DIR}/packs" -type f -name '*.json' -print0 |
  xargs -0 -n1 jq -e \
    '(.pack_identity.schema == "dpone.airflow-pack-identity.v1")
     and (.pack_fingerprint | test("^sha256:[0-9a-f]{64}$"))
     and (.provider_execution.schema == "dpone.airflow-provider-execution.v1")
     and (.xcom.sidecar_image | test("@sha256:[0-9a-f]{64}$"))'
```

This `jq` command is only a structural preflight. The build plane, provider and
runtime still recompute the whole-pack identity from the exact bytes.

If the installed producer still emits v1 for `init_fetch`, cannot accept these
deployment-owned inputs, or emits packs without the canonical identity, strict
projection or pinned sidecar, stop the cutover and upgrade the producer. A
missing strict execution field returns a migration-required error before DAG
installation. Never copy fields into a published index/pack, rename v1 to v2,
or calculate missing sizes or fingerprints by editing immutable JSON. Those
edits invalidate pack, release and deployment identity and do not create the
structured runtime payload required by the no-shell launcher.

### Complete v2 index example

The following document is a complete, schema-valid structural example. Its
digests, byte counts, and names are illustrative: obtain the real file from
`dpone airflow build` and immutable publication instead of editing this example
into a deployed cache.

<!-- DPONE_V2_INDEX_EXAMPLE_START -->
```json
{
  "schema": "dpone.airflow-deployment-index.v2",
  "release_id": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "deployment_id": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "trust_tier": "production",
  "dag_specs": [
    {
      "id": "orders_daily",
      "artifact_ref": "cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/dags/orders_daily.dag-spec.json",
      "sha256": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "bytes": 4096
    }
  ],
  "workload_packs": [
    {
      "id": "load_orders",
      "artifact_ref": "cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/packs/load_orders.airflow-pack.json",
      "sha256": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "bytes": 8192,
      "pack_fingerprint": "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    }
  ],
  "binding_set_ref": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
  "connection_registry_ref": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
  "credential_runtime_ref": "sha256:3333333333333333333333333333333333333333333333333333333333333333",
  "binding_set": {
    "artifact_ref": "cache://runtime-connection-contexts/sha256-9999999999999999999999999999999999999999999999999999999999999999/binding-set.json",
    "sha256": "sha256:8888888888888888888888888888888888888888888888888888888888888888",
    "bytes": 1024
  },
  "connection_registry": {
    "artifact_ref": "cache://runtime-connection-contexts/sha256-9999999999999999999999999999999999999999999999999999999999999999/connection-registry.json",
    "sha256": "sha256:7777777777777777777777777777777777777777777777777777777777777777",
    "bytes": 2048
  },
  "credential_runtime": {
    "artifact_ref": "cache://runtime-connection-contexts/sha256-9999999999999999999999999999999999999999999999999999999999999999/credential-runtime.json",
    "sha256": "sha256:6666666666666666666666666666666666666666666666666666666666666666",
    "bytes": 1536
  },
  "runtime_image_ref": "registry.example/dpone/runtime@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "runtime_image_digest": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "airflow_bundle_ref": "git:7ac31f2",
  "runtime_artifact_delivery": {
    "mode": "init_fetch",
    "trust_tier": "production",
    "artifact_registry_ref": "dpone-prod-artifacts",
    "identity": {
      "method": "kubernetes_workload_identity",
      "service_account": "dpone-runtime",
      "namespace": "airflow-example"
    },
    "registry_config_ref": {
      "kind": "kubernetes_config_map",
      "name": "dpone-artifact-registry",
      "key": "registry.json",
      "sha256": "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    },
    "trust_policy_ref": {
      "kind": "kubernetes_config_map",
      "name": "dpone-artifact-trust-policy",
      "key": "policy.json",
      "sha256": "sha256:5555555555555555555555555555555555555555555555555555555555555555"
    },
    "source": {
      "artifact_registry_ref": "dpone-prod-artifacts"
    },
    "verify": {
      "checksums": "required",
      "attestations": "required_for_prod"
    }
  },
  "release": {
    "artifact_ref": "cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/release-set.json",
    "sha256": "sha256:6666666666666666666666666666666666666666666666666666666666666666",
    "bytes": 16384
  },
  "deployment": {
    "artifact_ref": "cache://deployments/prod/sha256-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/deployment.json",
    "sha256": "sha256:7777777777777777777777777777777777777777777777777777777777777777",
    "bytes": 12288
  }
}
```
<!-- DPONE_V2_INDEX_EXAMPLE_END -->

Publish, materialize, and promote the regenerated release/deployment as new
immutable identities. Keep provider and runtime image on the same contract
line. During canary review, verify:

1. the active index is v2 `init_fetch` and its mirrored trust tiers match;
2. every selected pack has a verified whole-pack identity, the closed
   `dpone.airflow-provider-execution.v1` projection, and a digest-pinned XCom
   sidecar image;
3. the provider constructs fixed `runtime-init-fetch` and
   `runtime-pack-exec` commands with empty arguments;
4. image, service account, namespace, reserved volumes, and mounts come from
   the v2 delivery context rather than the pack projection;
5. the registry and optional trust-policy ConfigMap names/keys match the plan
   and their mounted bytes match the pinned digests;
6. production uses a v2 trust policy and the stock offline GitHub attestation
   verifier; missing, expired, mismatched, or unsupported policy/bundle/image
   inputs fail with stable `DPONE_ARTIFACT_*` security codes before ready state.

Items 1-4 can be established with local contract/model evidence. ConfigMap
projection, workload identity, registry access, and attestation require the
real approved Kubernetes environment; record them as `UNVERIFIED` when that
environment was not exercised.

## Compatibility warning: v1 missing `bytes` and strict writers

Current deployment-index writers and the public v1 JSON Schema require every
`dag_specs[]` and `workload_packs[]` entry to contain a positive `bytes` value
alongside `sha256`.

The provider reader temporarily accepts a legacy entry with missing `bytes`.
It emits one `DeprecationWarning` per process, reads the artifact through the
configured byte ceiling, verifies its SHA-256, and records the actual size in
the in-memory index. This is read compatibility only:

- new writers must remain strict and always emit positive `bytes`;
- schema validation, build, and publication reject a legacy index even though
  the provider compatibility reader can consume it;
- do not suppress the warning or hand-add sizes to a published index, because
  index and deployment identities would no longer describe the same bytes;
- regenerate the release/deployment index with the current preview/build
  producer, then publish, materialize, and promote the new immutable identity.

Treat the warning as an upgrade deadline, not certification evidence.
The v2 executable reader has no missing-`bytes` compatibility path: all exact
release, deployment, and workload descriptors are strict.

In a canary scheduler image, make the normally filtered deprecation visible
while verifying only local bytes:

```bash
PYTHONWARNINGS=default::DeprecationWarning \
python3 -c "from airflow.providers.dpone import load_airflow_deployment_index; load_airflow_deployment_index('/opt/airflow/.dpone-cache/current/airflow-index.json')"
```

## Verify cutover

1. Confirm the generated loader and scheduler mount use the same explicit
   `index_path`.
2. Run the read-only local planner:

   ```bash
   dpone airflow cache-recovery-plan \
     --cache-root /opt/airflow/.dpone-cache \
     --environment prod \
     --format json
   ```

3. Parse the DAG file in the candidate scheduler image. Verify
   `load_report.fatal` is false, the expected DAG IDs are loaded, and any
   isolated `errors[]` identify the affected `dag_id`.
4. For executable delivery, confirm the active schema is v2 `init_fetch`; a v1
   `init_fetch` must produce the migration-required error instead of a KPO.
5. With deprecation warnings enabled for the canary, confirm scheduler/DAG
   processor logs contain no legacy missing-`bytes` warning and no remote sync,
   metadata DB, Variable, Connection, Vault, or Kubernetes access from the
   loader.
6. Keep the previous complete deployment protected from retention until the
   rollback window closes.

An unavailable live Airflow cluster is not a pass. Record cluster parsing as
`UNVERIFIED` and retain the local provider/import/cache evidence separately.

## Recover a failed cutover

If the loader raises, preserve the first stable error code. A fatal
`DPONE_AIRFLOW_INDEX_*` error means no trusted deployment snapshot exists; do
not remove the guard or treat zero loaded DAGs as success.

Start with the read-only planner:

```bash
dpone airflow cache-recovery-plan \
  --cache-root /opt/airflow/.dpone-cache \
  --environment prod \
  --format json
```

Rematerialize the exact immutable release/deployment from trusted storage when
the plan reports missing or corrupt content. If the plan reports a repairable
split pointer/current state, apply only its reviewed candidate and fresh CAS
guard:

```bash
: "${DPONE_REPAIR_DEPLOYMENT_ID:?set the reviewed repair deployment sha256 digest}"
: "${DPONE_CURRENT_DEPLOYMENT_ID:?set the reviewed current deployment sha256 digest}"

dpone airflow cache-recovery-apply \
  --cache-root /opt/airflow/.dpone-cache \
  --environment prod \
  --deployment-id "${DPONE_REPAIR_DEPLOYMENT_ID}" \
  --promoted-by ci://your-platform/dpone-airflow-recovery \
  --allowed-promoter ci://your-platform/dpone-airflow-recovery \
  --expected-current-deployment-id "${DPONE_CURRENT_DEPLOYMENT_ID}" \
  --confirm-repair
```

Use `--expect-current-absent` only when the fresh plan confirms that the active
path is absent and repairable. Never edit the active index, pointer, DAG spec,
or workload pack in place.

## Rollback

Rollback means re-promoting a retained, complete immutable deployment; there is
no mutable `latest` rollback and no reason to restore a loader that can hide a
fatal index failure.

There are two distinct operating modes. Do not use a local cache promotion
while a desired-state watcher is enabled: the next watcher cycle will restore
the still-authoritative remote deployment.

### Desired-state-managed cache

Use the protected rollback job to re-verify the retained immutable release and
produce fresh passed promotion evidence. Then follow the conditional
`fetch -> prepare -> publish` procedure in
[Airflow desired state](airflow-desired-state.md#rollback). The rollback is a
new remote desired-state occurrence selecting old immutable content. Verify
the new occurrence, local activation, loader ACK and paginated Airflow REST DAG
inventory before declaring rollback complete.

### Standalone cache with watcher disabled

Only when no desired-state watcher owns this cache, use the current deployment
ID from a fresh recovery plan as the CAS guard and promote the previous
deployment:

```bash
: "${DPONE_PREVIOUS_DEPLOYMENT_DIR:?set the reviewed sha256-... deployment directory name}"
: "${DPONE_CURRENT_DEPLOYMENT_ID:?set the reviewed current deployment sha256 digest}"

dpone airflow cache-sync \
  --cache-root /opt/airflow/.dpone-cache \
  --deployment-dir "/opt/airflow/.dpone-cache/deployments/prod/${DPONE_PREVIOUS_DEPLOYMENT_DIR}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expected-current-deployment-id "${DPONE_CURRENT_DEPLOYMENT_ID}" \
  --confirm-promote
```

If pointer and active state disagree, use `cache-recovery-plan` and
`cache-recovery-apply` instead of promotion. If the scheduler image itself must
be rolled back, deploy the matching provider/pack release line together and
keep the explicit `index_path` plus fatal guard. Stop when the retained
deployment or its pinned release cannot be verified; do not reconstruct it
from mutable legacy cache bytes.

Do not use an incomplete v1 `init_fetch` index as an executable rollback
target. A retained v1 `local_preview` remains useful only for non-runnable
preview. Executable rollback must select another complete v2 `init_fetch`
deployment whose provider and runtime image understand the same wire.

After rollback, rerun the recovery plan and verify the expected DAG IDs. Keep
the failed deployment for incident evidence until retention policy explicitly
allows deletion.

## Related documentation

- [Airflow self-service overview](airflow-self-service.md)
- [Lightweight provider operations](airflow-pack-provider.md)
- [Provider API and `LoadReport`](airflow-provider-api.md)
- [Cache sync and recovery runbook](airflow-cache-sync.md)
