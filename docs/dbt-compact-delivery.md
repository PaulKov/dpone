# Deliver a dbt workspace through compact Airflow packs

Use this path when `project_alpha` and `project_beta` must travel together from
workspace compilation to a pinned Airflow runtime. It preserves the complete
source inventory and each workflow's project, manifest and selection bytes.
Transfer workloads belonging to those workflows travel in the same release.

This is a delivery and preflight capability. Production workspace activation
still requires the independently certified physical-target admission and runtime
finalization described in [workspace promotion](dbt-workspace-promotion.md).
A successful compile, materialization or launcher preflight is not SQL execution
or route certification.

## Prepare and compile

Follow [workspace authoring](dbt-workspace-authoring.md) to configure both projects,
prepare their canonical manifests and resolve packages. Use compatible exact
producer/provider/runtime versions and the repository's pinned `dbt-mssql`
toolchain (dbt Core 1.12.3 and dbt-sqlserver 1.11.1). No dependency installation,
credential discovery or database execution is performed by compact materialization.

Compile the whole workspace into a new output directory:

```bash
dpone dbt workspace compile --root workspace --output-dir compiled --format json
```

The resulting tree contains `release-set.json`, `dags/`, `packs/`, canonical
schemas, `_dbt/dbt-source-snapshot.json`, `runtime/dbt/objects/`, and the generated
integrity subject. Keep them together. Do not rearrange the tree into legacy
`_dags/` paths or edit generated JSON.

## Materialize

Use the approved digest-pinned sidecar image for your environment:

```bash
dpone gitops airflow release-materialize \
  --pack-root compiled \
  --cache-root .dpone-cache \
  --xcom-sidecar-image "$XCOM_SIDECAR_IMAGE" \
  --format json
```

A successful report has `passed: true`, a derived `release_id`, all workload/DAG
IDs, and `connection_projection_mode: runtime_connection_context`. Native packs
retain deployment-owned runtime connections; they do not manufacture an Airflow
Connection bridge. The legacy reconcile path still reports its existing
`kubernetes_secret_volume` bridge mode.

The command exits `0` on success and `2` for a materialization blocker. Both JSON
and Markdown reports go to stdout; blockers are included in the report. JSON is
one object with report fields at the top level and a `meta` object for invocation
context. `--format markdown` wraps that object in a Markdown code block.

`--output report.json` optionally mirrors stdout to a UTF-8 repository-relative
file, replacing an existing report. Input/cache overlap, unsafe file aliases, path escape and invalid
file destinations are rejected before materialization with
`DPONE_COMPACT_PACK_RELEASE_OUTPUT_INVALID`, and no mirror is written. The mirror
itself uses the existing ordinary file-write semantics, not an atomic release
transaction. An I/O error while writing the report can occur after release
publication; inspect the immutable cache and retry the same inputs.

The Python entrypoint provides the same transformation:

```python
from pathlib import Path
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release

report = materialize_compact_pack_release(
    pack_root=Path("compiled"),
    cache_root=Path(".dpone-cache"),
    xcom_sidecar_image=sidecar_image,  # Your approved digest-pinned image.
)
if not report.passed:
    raise ValueError(report.blockers)
```

Omit `--dag-id` for a workspace. An explicit list must contain the complete DAG
set; a subset fails rather than silently removing another project. Identical
retries reuse the immutable release. Conflicting bytes fail without replacing it.
A canonical singleton release is not this new input mode; retain its existing
singleton delivery procedure. A malformed `release-set.json` never triggers a
legacy fallback.

## Project and verify the runtime path

Prepare the platform-owned environment files using the
[Airflow project setup](getting-started/first-airflow-dag.md). Keep logical aliases
from both projects in `environments/dev/binding-set.yaml`, with their non-secret
resolver configuration in `platform/connection-registries/dev.yaml` and
`environments/dev/credential-runtime.yaml`. The aliases must match the projects'
publishing policies; materialization does not invent missing bindings.

From that environment repository, set `RELEASE_ID` to the successful materializer
report, and use the approved runtime image and exact registry-config bytes:

```bash
dpone airflow build \
  --release-id "$RELEASE_ID" --environment dev --trust-tier non_production \
  --runtime-image-ref "$RUNTIME_IMAGE_REF" \
  --runtime-image-digest "$RUNTIME_IMAGE_DIGEST" \
  --artifact-registry-ref workspace-artifacts \
  --registry-config-map-name workspace-artifact-registry \
  --registry-config-map-key registry.json \
  --registry-config-sha256 "$REGISTRY_CONFIG_SHA256" \
  --airflow-bundle-ref "$AIRFLOW_BUNDLE_REF" --format json
```

`RUNTIME_IMAGE_REF` ends with `@` followed by `RUNTIME_IMAGE_DIGEST`;
`AIRFLOW_BUNDLE_REF` is `git:` followed by the exact 40-character bundle commit.
The ConfigMap name is a synthetic example: replace it with your approved artifact
reader configuration. This command builds a local projection; it does not activate
a workspace or execute SQL. Python callers can use
`dpone.readiness.airflow_deployment_projection.AirflowDeploymentProjectionService.materialize`
with the same named identity inputs.

Inspect `deployment_dir`, `deployment.deployment_id` and `airflow_index` in the
response. The immutable directory contains `deployment.json`, `airflow-index.json`
and the runtime connection snapshots. Supply the resulting index through the
[provider delivery contract](airflow-pack-provider.md); its normal loader composes
the init-fetch plan. Do not rebuild that plan manually or replace digest-pinned
references with mutable current pointers.

```mermaid
flowchart LR
    W[Workspace compile] --> C[Verify full source tree]
    C --> M[Rewrite compact transport]
    M --> R[Verify and publish derived release]
    R --> D[Deployment and index projection]
    D --> P[Provider selects workload trio]
    P --> F[Init-fetch verifies pinned bytes]
    F --> L[Verified launcher prepares command]
    L --> Q[Separate dbt parse and selection preflight]
```

The provider carries exactly the selected workload's ordered project, manifest,
selection references. Inventory sorting must not reorder that trio. Init-fetch
verifies release, deployment, pack and payload digests before creating READY.
The launcher rechecks the artifacts and project ownership even after READY.

For a fully synthetic developer check, including real offline parse/selection
when the pinned optional toolchain is installed, run:

```bash
uv sync --locked --extra dbt-mssql
uv run pytest tests/test_dbt_compact_wire_v2.py -q
```

The test includes both projects, mixed transfer workloads, actual provider plans,
init-fetch and `VerifiedPackLauncher.prepare`. Fixture route metadata is explicitly
synthetic. The real toolchain test performs only `parse` and `ls`; it never builds
models or proves SQL delivery. Missing optional tooling is reported as SKIP.

## Diagnose and recover

| Symptom | Meaning | Recovery |
| --- | --- | --- |
| `DPONE_COMPACT_PACK_RELEASE_CACHE_INVALID` | Cache overlaps the immutable input tree or cannot be resolved safely | Choose a separate cache directory; a retained input beneath an existing cache remains supported |
| `DPONE_COMPACT_PACK_RELEASE_WRITE_FAILED` | Storage failed during native publication | Repair storage and retry identical inputs; no success is reported |
| `DPONE_COMPACT_PACK_RELEASE_DURABILITY_UNCERTAIN` | A complete release is visible, but parent-directory durability is unproven | Retain the visible tree and retry identical inputs after storage recovery; retry verifies bytes and synchronizes the parent |
| `DPONE_COMPACT_PACK_RELEASE_WORKSPACE_INVALID` | Native metadata, full inventory, source bytes, input mode or selected DAG set failed validation | Recompile the complete workspace with compatible components; keep the whole generated tree and omit partial DAG filtering |
| `DPONE_DBT_SELECTION_DRIFT` at launch | Verified producer/wire, execution pack, ordered trio or source identity disagrees | Regenerate compile, materialize and deployment artifacts as a single chain; inspect safe identities at each boundary |
| Runtime artifact integrity failure after READY | A pinned artifact changed or no longer matches the receipt | Restore retained immutable bytes or create a new release; do not reuse altered files under the old identity |
| Workspace admission unavailable | Local delivery checks do not establish physical-target activation authority | Follow the independent workspace admission process; do not disable the gate |

Never repair v2 order by sorting references, infer v2 from filenames, relabel a
release schema, or turn off a validator. Missing producer only retains documented
legacy wire-v1 interpretation and cannot authorize v2 payloads. Unknown or malformed
producer metadata fails closed. Error reports do not echo source bytes or secrets.

## Developer identity and upgrade notes

The new builder consumes a verified captured file map through
`VerifiedWorkspaceReleaseCapture`. It applies the existing native strict pack
policy and DAG rewrite, retaining source snapshot, runtime payloads, canonical
schemas, producer and selection metadata. It recomputes changed pack/DAG descriptors
and the release ID, regenerates the checksum subject, then verifies the complete
private tree again before immutable publication. No validation callback is optional.

The compact marker is a closed optional property of release-set v2; malformed
explicit markers are rejected. Legacy release-set v1 marker behavior is unchanged.
The rewritten release needs its own deployment and applicable attestation; input
release attestations do not authorize changed bytes. Source identity remains
unchanged even when transport identity changes.

Upgrade compatible readers/providers before using native workspace delivery.
Regenerate existing artifacts through producers and retain the prior deployment
and runtime image for rollback. A deployment rollback restores code and bindings;
it does not undo already committed data. See [compatibility](compatibility.md)
and the [design contract](feature-design-dbt-compact-wire-v2.md).

## Reproduce the offline Docker matrix

Commit the tested source first. The test image receives a bundle containing only
the current commit and its public baseline. It never mounts a host worktree or
credentials. Dependency installation needs network access during image build;
the actual tests run with networking disabled. Docker Desktop must be running. The container runs as an unprivileged user.
Native publication also synchronizes newly created cache ancestor entries before
installing the release; storage failures at those boundaries cannot report PASS.

```bash
context="$(mktemp -d)"
git bundle create "$context/source.bundle" HEAD refs/remotes/origin/master
cp docker/dbt-compact-tests/Dockerfile "$context/Dockerfile"
docker build --build-arg SOURCE_COMMIT="$(git rev-parse HEAD)" \
  --build-arg BASE_COMMIT="$(git merge-base HEAD origin/master)" \
  -t dpone-dbt-compact-tests "$context"
docker run --rm --network none --cpus 2 dpone-dbt-compact-tests
```

The default matrix covers native delivery, CLI aliases, descriptors, source
closure, archive hazards, stage/publication faults and exact metadata limits.
For the complete offline suite, override the command:

```bash
docker run --rm --network none --cpus 2 dpone-dbt-compact-tests \
  uv run pytest -m "not integration_live" -n 2 --dist loadfile
```

Historical Git assertions remain fail-closed when their original commits are
absent from the public snapshot. Container PASS is not SQL certification.
