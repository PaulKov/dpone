# Configure Airflow workload resources

This guide is for data engineers who configure CPU, memory and temporary disk
for a workload running through strict Airflow delivery. Start with a working
[self-service pipeline](airflow-self-service.md) and a configured
[strict deployment](airflow-cache-sync-strict-v2.md). Use a compiler, provider
and digest-pinned runtime image built from the same coordinated release.

## Configure and inspect

Add this block at the root of your existing pipeline manifest. It works with
classic, flow and folder authoring; it does not belong inside a process:

```yaml
gitops:
  airflow:
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
        ephemeral-storage: "2Gi"
      limits:
        cpu: "2"
        memory: "2Gi"
        ephemeral-storage: "4Gi"
```

Run from the project root, substituting your pipeline path:

```bash
dpone check pipelines/orders_daily/pipeline.yaml --format json
dpone airflow preview pipelines/orders_daily/pipeline.yaml --format json
```

The check report's authoring details include `airflow_resources`. Preview builds
the workload pack locally. Inspect the returned pack artifact at
`provider_execution.pod_spec.spec.containers[0].resources`; each of the six values
above must be present. The same values appear in the pack's `pod_spec`.
For catalog projects, use the existing workload-set build entry point:

```bash
dpone gitops airflow pack --workload orders_daily \
  --workload-set workloads.yaml --env dev --format json
```

Catalog defaults and per-workload configuration use `airflow.resources`, without
the outer `gitops` key. Existing catalog leaf precedence applies: global defaults,
environment, source type, source, domain/catalog, manifest-local `gitops`, then
workload settings. A later CPU value overrides CPU only; other resource leaves
remain inherited. The check/preview commands for a standalone pipeline inspect
its manifest-local values; the catalog pack shows the merged effective values.

After normal release materialization, deployment and cache convergence, inspect
the Airflow KPO `dry_run()` output or the admitted Kubernetes Pod. The base
container of the runtime and every separate hook receives these resources.
Namespace admission policies can add defaults or reject the Pod. A successful
local preview proves configuration preservation, not cluster scheduling.

## Reference and defaults

| Setting | Contract |
|---|---|
| Resource names | `cpu`, `memory`, `ephemeral-storage` |
| Sections | `requests`, `limits`; at least one nonempty section |
| Quantity | Quoted nonnegative Kubernetes quantity, at most 64 characters |
| CPU precision | Whole multiples of `1m` (`0.001` CPU) |
| Supported magnitude/precision | At most `2^63-1`; no finer than `1n`; values requiring rounding are rejected |
| Units | Decimal `n`, `u`, `m`, `k`, `M`, `G`, `T`, `P`, `E`; binary `Ki` through `Ei`; decimal exponent such as `1e3` |
| Request/limit | Request must not exceed its limit when both are present; comparison uses exact arithmetic |
| Scope | Base runtime container and all separate hook base containers |
| Other containers | Init-fetch and XCom sidecar keep their existing resource behavior |
| Omitted resources | No resource field is added; existing compact-pack defaults remain unchanged |
| Missing section/resource | No value is synthesized by dpone; Kubernetes admission owns defaulting |

Memory and storage quantities denote bytes; lowercase `m` denotes a fraction,
not megabytes. Prefer `Mi`/`Gi` or `M`/`G` for readable capacity settings.
The supported range is intentionally checked before Kubernetes can round or cap
a value. Resource constraints do not open command, environment, service-account,
volume or security-context overrides.

Malformed/null quantities, unsupported resource names and request greater than
limit fail with `DPONE_AIRFLOW_RESOURCES_INVALID` and the field path. For example,
`gitops.airflow.resources.requests.cpu` identifies an invalid CPU request.
The declaration belongs at the manifest root. A declaration inside
`processes[]`, a folder fragment's process, a recipe component's process, batch
`defaults`, a schema block/defaults, or a table/its `overrides` also fails with
`DPONE_AIRFLOW_RESOURCES_INVALID`, even when root resources are present. For
example, `processes[0].gitops.airflow.resources` identifies the misplaced block.
Move it to root `gitops.airflow.resources`, then rerun check and preview.
Folder and recipe diagnostics use the expanded process index. Catalog
`reconcile` preserves this field-path diagnostic and writes no pack or DAG
artifacts when validation fails. Per-process resource overrides are unsupported.

Move `pod_template_dict`, `pod_template_file`, `full_pod_spec`,
`container_resources` or direct `resources` from `operator_overrides` into the
documented workload field. Strict release rewriting rejects those overrides
with `DPONE_COMPACT_PACK_RELEASE_OPERATOR_OVERRIDES_INVALID`; it no longer
silently discards them.

Resource changes affect the authoring semantic fingerprint, workload pack
fingerprint and downstream release/deployment identity. Rebuild and promote the
new immutable artifacts. Resource-only changes participate in `state:modified`.
Repeating the same build retains deterministic identity. Existing provider v1
packs with extended resource names retain their prior structural compatibility;
the new authoring API supports only the three names above.

## Temporary disk is not reserved free space

An `ephemeral-storage` request participates in Kubernetes scheduling; its limit
can cause eviction when tracked consumption exceeds the limit. It does not
allocate a dedicated filesystem or provision a PVC. Logs, the writable image
layer and disk-backed `emptyDir` can share node storage. A memory-backed
`emptyDir` consumes memory instead. See the
[Kubernetes storage contract](https://kubernetes.io/docs/concepts/storage/ephemeral-storage/).

A dpone `min_free_bytes` check measures free space on the actual output
filesystem at that moment. It cannot prevent another process from consuming
space afterward. Neither a request/limit nor a free-space check alone guarantees
a large export succeeds. Size both the runtime's storage policy and the
Kubernetes capacity for the intended workload; see
[native transfer runtime](native-transfer-industrial-runtime.md).

## Upgrade and recover

This capability is an Unreleased change integrated on `0.76.0`; that published
version does not contain the fix. Use all three Python distributions (`dpone`,
`dpone-airflow-pack`, `apache-airflow-providers-dpone`) from the same release and
pin the corresponding runtime image digest. The existing Airflow/Python support
matrix remains in [compatibility](compatibility.md); no additional version pair
is certified by this change.

1. Build and test the matched compiler, provider packages and runtime image.
2. Upgrade scheduler/DAG-processor provider packages and the runtime image used
   by new deployment artifacts.
3. Move previously discarded resource overrides, check the effective values,
   rebuild packs/releases/deployments and let the cache converge.
4. Inspect the admitted Pod and run a small workload before increasing volume.

Restore the prior matched package/image set and prior immutable deployment to
roll back. Retain failed-Pod diagnostics and assess hook side effects before
manual retry. For publication, writable paths and stage/errno recovery, use
[runtime startup diagnostics](airflow-runtime-startup-diagnostics.md).

The contract tests cover authoring, quantities, identity, strict delivery and
local non-root child execution. Live Kubernetes certification still requires an
approved environment and exact image/package evidence. See the
[feature design](feature-design-airflow-hooks-resources.md).
