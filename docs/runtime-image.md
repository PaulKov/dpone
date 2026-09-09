# Runtime Docker image

Use the runtime image when you want a repeatable container that can run `dpone`
with the native tools required by high-throughput database paths.

The image is intentionally a CLI/runtime image, not a database server image. Use
`docker/docker-compose.integration.yml` when you need local Postgres, MSSQL,
ClickHouse, Kafka, Schema Registry and MinIO services for integration tests.

Keep the three installation lanes separate:

| Lane | Install | Do not add |
| --- | --- | --- |
| Authoring workstation/CI | Checksum-verified `dpone` candidate wheel from the frozen `0.73.32` commit; use `dpone==0.73.32` only after publication | Airflow and connector drivers unless that lane executes them |
| Airflow scheduler/DAG processor | Checksum-verified provider/pack candidate wheels from the frozen `0.73.32` commit on a tested matrix cell; use published pins only after release | `dpone[full]`, native clients, and source/sink connectors |
| KPO/runtime pod | Exact candidate image digest built from the frozen `0.73.32` commit for smoke; an executable v2 deployment always uses `@sha256:` | Airflow unless the custom target below is explicitly required |

Follow [Formal Airflow provider and lightweight pack reader](airflow-pack-provider.md#install-a-supported-scheduler-image)
for the official-constraints scheduler installation. This page covers the
runtime pod image; authoring-only users do not need to build it.

An executable v2 image contract is not automatically a production claim. Use
`trust_tier: non_production` for an executable dev/stage rehearsal. A
`production` projection is runnable only when the stock offline verifier accepts
the exact v2 policy, trusted root, detached bundle and release subject.
Production execution is certified only after that gate and exact-environment
Kubernetes, workload-identity and route evidence pass. A paused production
parse canary remains parse evidence only; do not trigger its tasks.

## Included tools

| Tool | Purpose |
| --- | --- |
| `dpone[full]` | All public Python connector extras. |
| Exact `dbt-dpone` package | Read-only platform macros at `/opt/dpone/runtime/dbt-dpone`, verified by canonical package digest during image build. |
| Microsoft ODBC Driver 18 | SQL Server connectivity through `pyodbc`. |
| `bcp` and `sqlcmd` from `mssql-tools18` | SQL Server bulk import/export and diagnostics. |
| `clickhouse-client` | Native ClickHouse TSV/client fast paths. |
| `unixODBC` headers/runtime | ODBC driver manager and build support. |
| GitHub CLI | Offline GitHub/SLSA release-set attestation verification. |
| Cosign 3.0.4 | Offline deployment-scoped signature verification. |

The Dockerfile follows the official Microsoft Linux ODBC/tooling repository
setup and the official ClickHouse Debian repository setup.

## Build from PyPI

```bash
docker build \
  -f docker/runtime/Dockerfile \
  --build-arg DPONE_PACKAGE_SPEC='dpone[full]==X.Y.Z' \
  -t dpone:X.Y.Z .
```

The default image is runtime-only: it includes dpone and native database tools,
but not `apache-airflow`.

Smoke it:

```bash
docker run --rm dpone:X.Y.Z --version
docker run --rm --entrypoint bcp dpone:X.Y.Z -v
docker run --rm --entrypoint clickhouse-client dpone:X.Y.Z --version
docker run --rm --entrypoint python dpone:X.Y.Z -c 'from pathlib import Path; from dpone.adapters.dbt_semantic_refresh_project import semantic_refresh_package_sha256; print(semantic_refresh_package_sha256(Path("/opt/dpone/runtime/dbt-dpone")))'
```

## Pull from GHCR

Release tags publish the production runtime image to GitHub Container Registry:

```bash
docker pull ghcr.io/paulkov/dpone-runtime:X.Y.Z
docker run --rm ghcr.io/paulkov/dpone-runtime:X.Y.Z --version
docker run --rm --entrypoint bcp ghcr.io/paulkov/dpone-runtime:X.Y.Z -v
docker run --rm --entrypoint sqlcmd ghcr.io/paulkov/dpone-runtime:X.Y.Z -?
docker run --rm --entrypoint clickhouse-client ghcr.io/paulkov/dpone-runtime:X.Y.Z --version
docker run --rm ghcr.io/paulkov/dpone-runtime:X.Y.Z runtime native-accel doctor --format json
```

Tag policy:

| Tag | Meaning |
| --- | --- |
| `X.Y.Z` | Immutable runtime image for a dpone package release. |
| `sha-<commit>` | Traceability tag for the exact source commit. |
| `latest` | Latest pushed SemVer release tag. Do not use it for pinned production jobs. |

Use the immutable version tag in production runtime profiles and CI. `latest`
is useful for local exploration only. The release workflow also records the
registry `sha256:` digest. Runnable deployment sets must use that digest, not
any mutable tag.

For MSSQL -> ClickHouse native-transfer acceleration, the runtime image should
install `dpone[full,accel]`. The `native-accel doctor` command is read-only and
shows whether the certified fused provider is selected or whether the route will
use the Python reference transcoder.

## Airflow strict-v2 init-fetch entry points

The `0.73.32` candidate includes two runtime-image-only commands for
`dpone.airflow-deployment-index.v2`:

```text
dpone airflow runtime-init-fetch
dpone airflow runtime-pack-exec
```

They are internal KPO entry points, not authoring commands. The provider fixes
both container commands and uses the same exact digest-pinned
`runtime_image_ref`; a workload pack cannot select another image or command.
The package/tag alone does not prove this v2 contract is present.
Verify the candidate image:

```bash
: "${DPONE_RUNTIME_IMAGE:?set the reviewed digest-pinned runtime image reference}"

docker run --rm "${DPONE_RUNTIME_IMAGE}" \
  airflow runtime-init-fetch --help
docker run --rm "${DPONE_RUNTIME_IMAGE}" \
  airflow runtime-pack-exec --help
```

These help probes prove only that the entry points exist. They do not exercise
Kubernetes, ConfigMap projection, workload identity, registry access,
attestation, or a data route.

The provider supplies the same canonical plan and SHA-256 to both containers as
`DPONE_INIT_FETCH_PLAN_B64` and `DPONE_INIT_FETCH_PLAN_SHA256`. The init
container receives:

| Path | Access | Purpose |
| --- | --- | --- |
| `/var/lib/dpone/artifacts` | RW | Private staging, verified payload, and final `runtime-fetch-ready.json`. |
| `/workspace/repo` | RW | Safely extracted runtime payload. |
| `/etc/dpone/artifact-registry/registry.json` | RO | Selected registry ConfigMap key. |
| `/etc/dpone/artifact-trust/policy.json` | RO when selected | Selected trust-policy ConfigMap key. |

The base container receives `/var/lib/dpone/artifacts` and
`/workspace/repo` read-only and `/var/lib/dpone/run` read-write. It does not
mount the registry or trust-policy ConfigMap.

`runtime-init-fetch` resolves each selected ConfigMap path under its mount
directory (including kubelet projection symlinks), opens the resolved regular
file once, reads at most 64 KiB for registry configuration and 1 MiB for the
policy with its embedded trusted root, verifies the pinned SHA-256 over those
exact bytes, and parses those same bytes before building a registry reader.
ConfigMap names are not integrity evidence. Publish changed configuration under
a new reviewed snapshot and deployment identity rather than mutating a mounted
object in place.

`trust_tier` is explicit in the v2 index and runtime plan. For `production`, a
matching trust-policy snapshot and `required_for_prod` attestation are
mandatory. The stock image pins both a checksum-verified GitHub CLI and Cosign.
`dpone.runtime-artifact-trust-policy.v2` selects the existing offline
GitHub/SLSA release-set verifier;
`dpone.airflow-deployment-trust-policy.v1` selects the deployment-scoped
Cosign verifier. Exactly one authority is accepted before registry I/O.
Runtime downloads only deterministic immutable evidence keys and verifies
without GitHub credentials or signature-service network calls. Missing,
conflicting, or invalid verifier material fails closed before ready-state
publication.

Runtime image releases from `0.73.28` emit
`dpone.runtime-image-certification.v2`, whose ten digest-bound smoke receipts
include Cosign. Historical `dpone.runtime-image-certification.v1` evidence
remains readable but contains the original nine checks and does not certify a
Cosign-capable image.

After init publishes the ready manifest, `runtime-pack-exec` revalidates the
plan, ready state, exact release/deployment/workload bytes, and structured
runtime bootstrap. Runtime (`dpone run`) selections run as a verified child
process without `shell=True`, `/bin/sh -c`, a scheduler-copied runtime command,
or an inline bootstrap fallback. Stdout/stderr are captured under
`/var/lib/dpone/run`, a JSON `gitops.airflow_xcom_summary` is written to
`/airflow/xcom/return.json`, and the base container exits `0` so the KPO xcom
sidecar can publish the outcome (pass/fail lives in XCom status). Pre-hook
and dbt selections write bounded evidence/XCom and preserve the child exit
code, so a failed prerequisite cannot become a successful Airflow task.

The launcher accepts only these pack-authored command shapes:

```text
dpone run <relative-manifest> --format json
dpone run <relative-manifest> --format json --selector <selected-workload>
dpone hooks execute <relative-manifest> --phase pre_hook --hook-id <selected-hook>
dpone hooks execute <relative-manifest> --phase pre_hook --hook-id <selected-hook> --selector <selected-process>
```

The runtime command environment must be exactly
`DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1`; a pre-hook receives no pack-authored
environment. Duplicate controls, another selector/hook ID, reordered or
unknown flags, kind confusion, absolute/traversing manifests, duplicate JSON
keys, and non-finite receipt numbers fail before the verified child starts.

The current TCB still includes the producer, promoted cache/index, Airflow
scheduler/provider, Kubernetes enforcement, this digest-pinned image, workload
identity, registry adapter, and configured verifier. Fixed entry points do not
claim compromised-scheduler/provider/cache resistance.

Live Kubernetes projected volumes, workload identity, production attestation,
Vault, MSSQL, and ClickHouse remain `UNVERIFIED` until current evidence from the
exact commit, image digest, and environment exists. See the
[runtime operator runbook](airflow-cache-sync.md#operate-strict-v2-init-fetch-pods)
and [v1-to-v2 migration guide](airflow-provider-cache-migration.md#migrate-v1-init-fetch-to-v2).

## Build from a direct wheel or private index

Use this mode when PyPI simple-index propagation is delayed but the wheel
artifact is already available through a direct `files.pythonhosted.org` URL, a
GitHub Release asset, or a private package index.

```bash
docker build \
  -f docker/runtime/Dockerfile \
  --build-arg DPONE_PACKAGE_SPEC='dpone[full] @ https://files.pythonhosted.org/.../dpone-X.Y.Z-py3-none-any.whl' \
  -t dpone:X.Y.Z-local .
```

Production images should normally pin immutable PyPI versions or a private
package index artifact. Direct wheel URLs are best kept for release smoke tests
and incident workarounds.

## Custom Airflow-in-runtime target

Most KubernetesPodOperator and KubernetesPodExecutor deployments should keep the
official runtime image runtime-only and use the `AIRFLOW_CONN_*` Kubernetes
Secret/env bridge generated by `dpone gitops airflow runtime-profile` and
`pod-contract`.

If your platform intentionally wants Airflow installed inside the runner pod,
build the explicit Docker target:

```bash
docker build \
  -f docker/runtime/Dockerfile \
  --target airflow-runtime \
  --build-arg DPONE_PACKAGE_SPEC='dpone[full]==X.Y.Z' \
  --build-arg AIRFLOW_PACKAGE_SPEC='apache-airflow==3.2.0' \
  --build-arg AIRFLOW_PROVIDER_PACKAGE_SPEC='apache-airflow-providers-cncf-kubernetes==10.14.0' \
  -t dpone-airflow-runtime:X.Y.Z .
```

This target is a custom image recipe, not the official published runtime image.
The shown Airflow/Python 3.12/Kubernetes-provider combination is a tested matrix
cell, but this target is not the formal scheduler-image installation because
the Dockerfile does not consume Airflow's official constraints file. Pin
versions explicitly, run `python -m pip check`, smoke-test `import airflow`, and
record the image digest in `runtime-profile.json`. Build scheduler/DAG-processor
images with the constrained sequence linked above.

## Run a manifest

```bash
docker run --rm \
  -v "$PWD:/workspace" \
  --env-file .env \
  ghcr.io/paulkov/dpone-runtime:X.Y.Z \
  run /workspace/examples/batch/landing_postgres_to_mssql.batch.yaml
```

For private registries, mirror the image into your internal registry and keep the
same version tag. Do not rebuild a mutable image with the same tag and different
package contents.

## CI/CD workflow

The `Runtime image` workflow has two privilege-separated paths:

- pull requests and manual dispatches are read-only, non-publishing builds;
- only a canonical tag **push** can enter the GHCR publishing job;
- a tag push still cannot mutate GHCR unless its newest eligible exact-SHA
  `Release candidate evidence` dispatch and unique artifact
  validate. Eligibility is frozen at the paired tag-run cutoff. The provider
  selects the exact-SHA dispatch with the unique maximum `created_at` inside
  that pre-cutoff set before reading its current attempt and status; an
  older-dispatch rerun cannot change dispatch order.

Pull request builds install the checked-out source tree through a BuildKit bind
mount plus the local `packages/dpone-native-accel` package, so the smoke test
never accidentally validates the latest PyPI release instead of the PR. A
manual run with no inputs behaves the same way. Manual `push_image=true` fails
closed; rerun or repair the protected tag workflow instead of publishing an
arbitrary package specification.

For a release tag, the workflow independently re-verifies the same
provider-bound pre-tag receipt required by the package release workflow. It
requires exactly one matching tag-push provider run for both `release.yml` and
`runtime-image.yml`, binds the current in-progress runtime run ID/attempt, and
uses the earlier of their GitHub `created_at` values as the cutoff. Only
exact-SHA evidence dispatches created by that instant are eligible; equal
maximum eligible timestamps fail closed and post-cutoff dispatches are
ignored. The selected evidence run, terminal check, and artifact must also
complete by the cutoff. The tagger timestamp is not authority. It then proves
the annotated tag, exact source commit,
protected-branch ancestry, package versions, live required checks, and
the complete four-distribution PyPI wheel/sdist digest set before building from
`dpone[full,accel]==X.Y.Z`, verifies the installed dpone version and `pip check`,
and smokes `dpone`, `bcp`, `sqlcmd`, `clickhouse-client`,
`runtime native-accel doctor`, `airflow runtime-init-fetch --help`, and
`airflow runtime-pack-exec --help`.

Read-only verification runs in preflight and freshly inside each GHCR mutation
block, immediately before its first irreversible write: once before digest
publication plus its attestation sequence, and once before alias promotion. No
checked-out repository or untrusted command runs between either fresh gate and
that block's first registry write. The digest block does not repeat the gate
before each attestation; its later attest writes remain under the same frozen
authority, with no intervening checked-out repository code. Runtime pull
requests and manual dispatches remain build-only and cannot enter either block.

Immediately before each GHCR mutation block, the workflow re-reads the provider
tag ref, requires an annotated tag object, and proves that it still peels to the
exact release commit. The first registry write uses only the sole OCI layout
descriptor as `${IMAGE_NAME}@sha256:<digest>`; a repository-only destination
that could default to `latest` is forbidden. Only after digest smoke and
certification does the promotion service reconcile the explicit version,
source-SHA, and `latest` aliases. It retains
`runtime-image-evidence-<run-id>-<attempt>` for 90 days with:

- release and exact-commit gate reports;
- release-candidate evidence verification bound to the exact check, provider
  run/attempt, artifact, commit, release, profile, and source digests;
- candidate distribution SHA-256 checksums and PyPI identity report;
- pushed image digest and image inspection;
- Python and OS package inventories;
- SPDX JSON SBOM;
- OIDC-backed image provenance and SBOM attestations.

Tag-run phase transport is attempt-scoped and non-overwriting. Consumers bind
the exact producer artifact ID and require its provider digest; they never
select a phase by a reconstructed name:

| Phase payload | Artifact name |
| --- | --- |
| Release preflight | `runtime-image-preflight-<run-id>-<attempt>` |
| OCI layout candidate | `runtime-image-oci-<run-id>-<attempt>` |
| Build evidence | `runtime-image-build-evidence-<run-id>-<attempt>` |
| Verifier tool bundle | `runtime-image-tools-<run-id>-<attempt>` |
| Digest push and attestations | `runtime-image-push-evidence-<run-id>-<attempt>` |
| Immutable certification | `runtime-image-certification-<run-id>-<attempt>` |
| Alias publication | `runtime-image-publication-<run-id>-<attempt>` |
| Aggregate attempt receipt | `runtime-image-evidence-<run-id>-<attempt>` |

The final collector follows producer IDs for the preflight, build, push,
certification, and publication evidence artifacts. It records all five phase
results and download outcomes and fails the attempt unless every one is
successful. For a failed tag run, preserve successful phases and use GitHub's
**Re-run failed jobs** recovery. Retried phases emit the new attempt-scoped
name and downstream jobs follow the new producer ID; never infer success from
an aggregate artifact whose `attempt.json` is `FAIL` or incomplete. After any
registry write, do not use **Re-run all jobs** to rebuild the candidate.

The Dockerfile installs dpone with a fresh PyPI resolver path:
`pip install --no-cache-dir --index-url https://pypi.org/simple --retries 10`.
This avoids stale local caches during the short window after PyPI accepts a
release but before every installer path sees the new version.

Non-publishing manual dispatch supports:

- `version`: public package version to smoke locally;
- `package_spec`: bounded single-line direct wheel/private-index override;
- `push_image`: deprecated tripwire; `true` is rejected.

Do not put credentials in `package_spec`. Use an approved package index or
runner identity. Manual inputs are transported through environment variables,
validated, and never interpolated into Bash source.

## Tuning notes

- MSSQL fast paths use `bcp`; configure `sink.options.bulk.bcp.*` in manifests.
- ClickHouse fast paths use HTTP/client TSV ingest; configure
  `source.options.partitioning.*` and the canonical native ingest settings shown by
  `dpone plan`.
- Keep database credentials in environment variables, Vault, Airflow
  connections, or mounted secrets. Do not bake credentials into the image.
- Use Linux runners for production images. macOS/Homebrew native tools are good
  for local testing, but Linux containers are easier to reproduce in CI.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `pyodbc` cannot find the driver | `docker run --rm --entrypoint odbcinst dpone:X.Y.Z -q -d` |
| `bcp: command not found` | Confirm `/opt/mssql-tools18/bin` is in `PATH`. |
| ClickHouse client missing | Rebuild and check the ClickHouse repository step. |
| `pip install dpone==X.Y.Z` fails during image build | Run `python tools/pypi_release_smoke.py --package dpone --version X.Y.Z --install-smoke` and wait for PyPI simple-index visibility, or build from a direct wheel/private index. |
| Release tag exists but PyPI/GHCR looks incomplete | Run `dpone ops release-verify --release vX.Y.Z --install-smoke --format json` and use the blocker code to decide whether to wait or use **Re-run failed jobs** on the exact runtime run. Preserve successful phases; never rebuild after a registry write or publish from an arbitrary direct wheel/private index. |
