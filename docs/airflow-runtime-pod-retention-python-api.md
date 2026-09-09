# Embed runtime Pod retention with the Python API

**Purpose.** Compose the retention application service with explicit inventory, deletion, credential, and evidence capabilities.

**Audience.** Python integrators embedding the same retention policy behind a custom composition root.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** [deploy the Kubernetes control](airflow-runtime-pod-retention-kubernetes.md).

## Python API

The application service accepts capability-oriented ports. This read-only
example uses the bundled production Kubernetes adapter and the same request validation
as the CLI:

```python
import json
import os

from dpone_airflow_pack.provider_execution_contract import (
    RUNTIME_POD_CONTRACT_KEY,
    RUNTIME_POD_CONTRACT_VALUE,
    RUNTIME_POD_LABEL_SELECTOR,
    RUNTIME_POD_MANAGED_BY_KEY,
    RUNTIME_POD_MANAGED_BY_VALUE,
    WORKLOAD_ID_METADATA_KEY,
)
from dpone.adapters.kubernetes_airflow_runtime_pod_retention import (
    build_kubernetes_airflow_runtime_pod_retention_adapter,
)
from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionPlanRequest,
)
from dpone.contracts.airflow_runtime_pod_metadata import (
    AirflowRuntimePodOwnershipContract,
)
from dpone.services.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionService,
)

adapter = build_kubernetes_airflow_runtime_pod_retention_adapter(
    auth_mode="kubeconfig",
    kube_context="reviewed-dev",
    label_selector=RUNTIME_POD_LABEL_SELECTOR,
)
ownership = AirflowRuntimePodOwnershipContract(
    managed_by_key=RUNTIME_POD_MANAGED_BY_KEY,
    managed_by_value=RUNTIME_POD_MANAGED_BY_VALUE,
    runtime_contract_key=RUNTIME_POD_CONTRACT_KEY,
    runtime_contract_value=RUNTIME_POD_CONTRACT_VALUE,
    workload_id_key=WORKLOAD_ID_METADATA_KEY,
)
namespace = os.environ["AIRFLOW_NAMESPACE"]
service = AirflowRuntimePodRetentionService(
    inventory=adapter,
    ownership=ownership,
)
report = service.plan(
    AirflowRuntimePodRetentionPlanRequest(
        namespace=namespace,
        minimum_age_seconds=86_400,
        page_size=500,
    )
)
print(json.dumps(report, indent=2, sort_keys=True))
```

Apply also requires the delete capability, the adapter-reported credential
source and an append-only evidence publisher. The JSONL stream must be captured
by a certified acknowledged platform log backend; it is intentionally separate from the final
aggregate report:

```python
import os
import sys

from dpone.adapters.airflow_runtime_pod_retention_events import (
    JsonLinesAirflowRuntimePodRetentionEvidencePublisher,
)
from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApplyRequest,
)

namespace = os.environ["AIRFLOW_NAMESPACE"]
actor = f"serviceaccount://{namespace}/dpone-runtime-pod-retention"
apply_service = AirflowRuntimePodRetentionService(
    inventory=adapter,
    ownership=ownership,
    deletion=adapter,
    credentials=adapter,
    evidence=JsonLinesAirflowRuntimePodRetentionEvidencePublisher(sys.stderr),
)
apply_report = apply_service.apply(
    AirflowRuntimePodRetentionApplyRequest(
        namespace=namespace,
        minimum_age_seconds=86_400,
        page_size=500,
        max_delete_count=100,
        actor=actor,
        allowed_actors=(actor,),
        confirm_delete=True,
        kube_auth_mode="kubeconfig",
    )
)
print(json.dumps(apply_report, indent=2, sort_keys=True))
```

`operation_started` and each `delete_intent` are flushed before the matching
Kubernetes mutation. `delete_outcome` and `operation_completed` make a normal
cycle complete. An intent without an outcome after abrupt process death is
explicit incomplete evidence to reconcile, never a successful deletion claim.
The [manual apply procedure](airflow-runtime-pod-retention-manual.md#run-manually)
validates RBAC and seals the
aggregate report, but its stock JSONL publisher is still `process_ordered`.
It is not a production crash-durability substitute.
Unlike read-only planning, the Python apply request rejects `auto`. For
`kubeconfig`, the injected adapter must carry the exact reviewed
`kube_context`; the service validates that credential capability before
inventory I/O and includes the context in operation identity and aggregate
evidence. The request states the required credential mode without duplicating
adapter configuration.

## Durable publisher composition

Production embeddings can use the shipped acknowledgement adapter without
making core depend on S3, Kafka, a database, or one cloud SDK:

```python
from collections.abc import Mapping

from dpone.adapters.airflow_runtime_pod_retention_durable import (
    AcknowledgedAirflowRuntimePodRetentionEvidencePublisher,
)
from dpone.contracts.airflow_runtime_pod_retention_ack import (
    AirflowRuntimePodRetentionEvidenceAcknowledgement,
    runtime_pod_retention_event_sha256,
)


class PlatformDurableSink:
    def append_and_ack(
        self,
        event: Mapping[str, object],
    ) -> AirflowRuntimePodRetentionEvidenceAcknowledgement:
        # Persist the exact canonical event and wait for the platform sink ACK.
        sink_ref = platform_event_store.append_and_wait(dict(event))
        return AirflowRuntimePodRetentionEvidenceAcknowledgement(
            operation_id=str(event["operation_id"]),
            sequence=int(event["sequence"]),
            event_sha256=runtime_pod_retention_event_sha256(event),
            sink_ref=sink_ref,
        )


durable_publisher = AcknowledgedAirflowRuntimePodRetentionEvidencePublisher(
    PlatformDurableSink()
)
```

`append_and_wait` is the infrastructure variation point: it must return only
after replicated or transactional storage acknowledges the exact event. A
wrong operation, sequence, or digest becomes `evidence_unavailable`; a
`delete_intent` failure therefore prevents mutation. Returning an ACK before
durable commit violates the port contract and cannot be certified.
