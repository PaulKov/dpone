# Airflow loader ACK mount policy

This runbook defines who may read or write the loader acknowledgement used by
exact Airflow cache retention. It applies to the official Apache Airflow Helm
chart profiles supported by dpone:

- Airflow 2.10 with chart `1.19.0`;
- Airflow 3.2 with chart `1.22.0`.

Package compatibility and Helm-topology evidence are separate:

| Airflow | Python/provider tests | Helm ACK profile | Live Kubernetes |
| --- | --- | --- | --- |
| 2.10 | Tested | chart `1.19.0`, tested | `UNVERIFIED` until environment acceptance |
| 2.11 | Tested | `UNVERIFIED` | `UNVERIFIED` |
| 3.2 | Tested | chart `1.22.0`, tested | `UNVERIFIED` until environment acceptance |
| 3.3 | Tested | `UNVERIFIED` | `UNVERIFIED` |

Do not infer Helm or live support from a green package/DagBag matrix cell.

## Why a post-renderer is required

The official chart accepts component-level `extraVolumeMounts`. Those mounts
are reused by auxiliary containers such as migration waiters and log groomers.
Putting the ACK mount in `scheduler.extraVolumeMounts` or
`dagProcessor.extraVolumeMounts` would therefore grant write access to more
than the parser.

The checked-in values deliberately omit the parser ACK mount. Without the
post-renderer, the loader cannot write an acknowledgement and destructive
retention fails closed. The post-renderer operates on the final Helm output and
enforces this matrix:

| Container role | ACK access |
| --- | --- |
| Airflow 2 scheduler parser | read-write |
| Airflow 3 DAG processor parser | read-write |
| `dpone-cache-status-projector` | read-only |
| `dpone-cache-watch` | read-only |
| migration init containers | none |
| cache materialization init container | none |
| log groomers and other containers | none |

Exactly one rendered workload may declare the ACK volume. A render containing
both scheduler and DAG-processor ACK authorities is rejected.

## Required tooling

Install the exact `dpone-airflow-pack` version used by the Airflow image. The
package provides:

```text
dpone-airflow-pack-helm-post-renderer
```

The supported deployment examples use Helm `3.19.0` or newer in the Helm 3
release line. Helm 4 changed post-renderer discovery to a plugin contract, so
it is not a drop-in replacement for these commands.

## Render and verify

```bash
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update apache-airflow
```

Airflow 2.10:

```bash
POST_RENDERER="$(command -v dpone-airflow-pack-helm-post-renderer)"

helm template airflow apache-airflow/airflow \
  --version 1.19.0 \
  -f docs/examples/airflow-cache-values-2.10.yaml \
  --post-renderer "${POST_RENDERER}" \
  > /tmp/airflow-2.10.rendered.yaml

"${POST_RENDERER}" --verify-only \
  < /tmp/airflow-2.10.rendered.yaml \
  > /dev/null
```

Airflow 3.2:

```bash
POST_RENDERER="$(command -v dpone-airflow-pack-helm-post-renderer)"

helm template airflow apache-airflow/airflow \
  --version 1.22.0 \
  -f docs/examples/airflow-cache-values-3.2.yaml \
  --post-renderer "${POST_RENDERER}" \
  > /tmp/airflow-3.2.rendered.yaml

"${POST_RENDERER}" --verify-only \
  < /tmp/airflow-3.2.rendered.yaml \
  > /dev/null
```

Both the Helm deploy and `helm template` evidence command must use the
post-renderer. Treat a deployment path that omits it as invalid configuration.

## Deploy and observe

This page is a mount-policy reference, not deployment authority. Never pass the
checked-in `docs/examples/airflow-cache-values-*.yaml` files to `helm upgrade`:
they contain nondeployable placeholders. Copy the matching profile into the
environment repository and follow the context-bound preflight, render, diff,
deployment and evidence procedure in
[Deploy the exact Airflow cache on Kubernetes](airflow-cache-kubernetes-deployment.md).
That procedure requires explicit `KUBE_CONTEXT`, `AIRFLOW_NAMESPACE`,
`PLATFORM_VALUES`, chart version and release name before any mutation.

After the environment-owned deployment reports healthy, verify desired/current
cache identity and loader ACK convergence through
[cache diagnostics without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md).

## Failure behavior

The command returns exit code `2` and writes the stable prefix
`DPONE_AIRFLOW_LOADER_ACK_MOUNT_INVALID` when:

- no ACK-bearing parser workload exists;
- more than one ACK-bearing workload exists;
- the expected scheduler or DAG-processor container is absent or duplicated;
- any auxiliary or init container mounts the ACK volume;
- a dpone reader has write access;
- the parser ACK mount is missing, read-only, duplicated, or uses another path;
- input is empty, malformed, or exceeds the bounded 32 MiB limit.

Helm must stop on this error. Do not make the post-renderer fail-open: pod
startup is fail-open for remote cache download, but deployment authorization is
a control-plane policy and must fail closed.

## Rollback

1. Roll back the Helm release to the last chart values and post-renderer image
   digest that passed this policy.
2. Do not add the ACK mount back to component-level `extraVolumeMounts`.
3. Verify the rendered rollback manifest with `--verify-only` before applying.
4. Confirm the last-known-good exact cache remains active and retention has not
   removed its acknowledged generation.

Use the context-bound rollback procedure from the Kubernetes deployment
runbook. It requires the exact release, namespace, kube context and previous
revision, then verifies the restored manifest through this post-renderer before
cache desired-state rollback begins.

The full cache placement, sizing, startup and outage procedure is in
[Deploy the exact Airflow cache on Kubernetes](airflow-cache-kubernetes-deployment.md).
