# Deploy the exact Airflow cache on Kubernetes

**Purpose.** Choose the safe deployment task for one bounded, exact Airflow parser cache on the official Helm chart.

**Audience.** Airflow platform engineers, Kubernetes operators, SREs, and deployment reviewers.

This runbook is for platform engineers deploying dpone with the official
Apache Airflow Helm chart. It turns an immutable remote desired deployment into
one bounded local parser cache without adding network I/O to DAG parsing.

## Component placement

| Airflow and chart | Shipped profile | Parse authority | Cache init/watch | Certification |
| --- | --- | --- | --- | --- |
| Airflow 2.10, official chart `1.19.0` | `airflow-cache-values-2.10.yaml` | Scheduler-managed DAG processing | Scheduler only | Render-tested profile |
| Airflow 2.10 with standalone DAG processor | None | Standalone DAG processor | Beside that processor only | `UNVERIFIED`; render and certify a separate profile before use |
| Airflow 3.2, official chart `1.22.0` | `airflow-cache-values-3.2.yaml` | `dagProcessor` | Beside `dagProcessor` only | Render-tested profile |
| Airflow 3.3 | None | Required standalone DAG processor; bundle configuration and placement are installation-specific | Not shipped | Package matrix support does not certify a Helm topology |

Webserver/API server, triggerer, scheduler without parse authority, and
KubernetesExecutor task Pods do not mount this cache. Do not reinterpret a
package compatibility test as a deployment-profile certification.

Pin both the chart and Airflow image. The official chart dropped Airflow
versions below 2.11 after chart `1.19.0`; chart `1.22.0` defaults to Airflow
3.2.2. See the official [chart release notes](https://airflow.apache.org/docs/helm-chart/stable/release_notes.html)
and [parameter reference](https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html).

KubernetesExecutor task pods do not share this parser cache. They receive
runtime artifacts through the exact `init_fetch` contract. If a task pod must
re-import a repository loader before hand-off, give it a separate init-only,
fail-closed materialization; never mount the parse authority's `emptyDir`.


## Choose the task

| Task | Canonical guide | Outcome |
| --- | --- | --- |
| Prepare immutable inputs and reject unsafe values | [Prepare the deployment](airflow-cache-kubernetes-prerequisites.md) | Reviewed chart values, images, authority, and preflight evidence |
| Configure bounded init/watch behavior | [Configure runtime wrappers](airflow-cache-kubernetes-runtime-wrappers.md) | Fail-open Airflow startup with fail-visible dpone cache status |
| Install or roll back wrapper bytes | [Deliver the wrappers](airflow-cache-kubernetes-wrapper-delivery.md) | Reviewed ConfigMap lifecycle and parser rollout evidence |
| Deploy, observe, and roll back the release | [Operate the Helm release](airflow-cache-kubernetes-chart-operations.md) | Exact parser placement, diagnostics, verification, and rollback |

For a first deployment, follow the guides in table order. Keep parser-cache authority separate from task-Pod runtime artifacts throughout.

## Prerequisites

Start with the [pinned prerequisites and preflight](airflow-cache-kubernetes-prerequisites.md#prerequisites).

## Filesystem authority

Review the [filesystem authority boundary](airflow-cache-kubernetes-prerequisites.md#filesystem-authority).

## Required image and configuration

Prepare the [required image and configuration](airflow-cache-kubernetes-prerequisites.md#required-image-and-configuration).

## Fail-open init wrapper

Configure the [bounded init wrapper](airflow-cache-kubernetes-runtime-wrappers.md#fail-open-init-wrapper).

## Watch wrapper

Configure and approve the [continuous watch wrapper](airflow-cache-kubernetes-runtime-wrappers.md#watch-wrapper).

## Deliver the wrappers

Use the [reviewed ConfigMap delivery lifecycle](airflow-cache-kubernetes-wrapper-delivery.md#deliver-the-wrappers).

## Official chart values pattern

Apply the [official-chart placement pattern](airflow-cache-kubernetes-chart-operations.md#official-chart-values-pattern).

## Optional API diagnostics projector

Enable and verify the [optional API diagnostics projector](airflow-cache-kubernetes-chart-operations.md#optional-api-diagnostics-projector).

## Verification and rollback

Run the [deployment verification and rollback procedure](airflow-cache-kubernetes-chart-operations.md#verification-and-rollback).

For the next likely task, continue with [cache materialization, activation, and recovery](airflow-cache-sync.md).
