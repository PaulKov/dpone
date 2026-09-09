# Embed Airflow cache sync with the Python API

**Purpose.** Use the public runtime services for local materialization and promotion while keeping trust handoff explicit.

**Audience.** Python integrators embedding cache materialization or promotion in a platform service.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [review the Airflow self-service architecture](airflow-self-service-architecture.md).

## Python integration

The CLI is the recommended platform surface. An embedded materializer can use
the same public runtime service and structured exception:

```python
from pathlib import Path

from dpone.runtime.deployment_cache import (
    DeploymentCacheError,
    DeploymentCacheMaterializer,
)

cache_root = Path("/opt/airflow/.dpone-cache")
deployment_id = "sha256:" + "a" * 64
deployment_dir = cache_root / "deployments" / "prod" / deployment_id.replace(":", "-")
materializer = DeploymentCacheMaterializer(
    cache_root,
    allowed_promoters=("ci://your-platform/dpone-airflow",),
    max_artifact_bytes=64 * 1024 * 1024,
)
try:
    current = materializer.promote(
        deployment_dir,
        environment="prod",
        promoted_by="ci://your-platform/dpone-airflow",
        expect_current_absent=True,
        activation_id="12345678-1234-4234-9234-123456789abc",
    )
except DeploymentCacheError as exc:
    # exc.code and exc.path are stable inputs for platform error handling.
    raise
```

The keyword-only `activation_id` is the authoritative UUIDv4 occurrence from
the trusted desired-state publisher. Every pod-local cache receiving that
occurrence must pass the same value. Omit it only for standalone local
promotion, where the injected factory generates a new UUIDv4. The value is
validated before the service creates a snapshot or mutates permissions; a
replay preserves the supplied occurrence while deployment CAS still protects
the local `current` switch.

The service performs local reads and writes only. Callers must still provide
the trusted release handoff, explicit promoter policy, and recovery operation.

Continue with the [Airflow self-service architecture](airflow-self-service-architecture.md),
the [approved cache promotion feature design](feature-design-airflow-cache-promotion-integrity.md),
the generated [`cache-sync` CLI reference](cli-reference.md#dpone-airflow-cache-sync),
or the [GitOps schema catalog](reference/gitops-schema-catalog.md).
