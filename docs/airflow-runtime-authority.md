# Protected Airflow runtime-authority inputs

Protected development tasks call one image-installed authority adapter in both
init-fetch and base before sensitive work. The adapter reads its external input
from the fixed path `/run/secrets/dpone/runtime-authority/authority` selected by
`DPONE_RUNTIME_AUTHORITY_PATH`.

Choose the source by data classification:

| Source | Use it for | Persistence and confidentiality |
|---|---|---|
| Immutable payload (v5) | Small safe-to-persist opaque configuration or ciphertext | Bytes are visible in deployment/index/runtime-plan artifacts, scheduler metadata, and Pod specs. SHA-256 provides integrity, **not confidentiality**. |
| Kubernetes Secret (v4) | Plaintext credentials, tokens, private endpoints, tenant identifiers, personal data, or private policy | Only Secret coordinates enter dpone artifacts; Kubernetes projects the confidential value. |

## Build with an immutable non-secret payload

Prepare 1..4,096 exact binary bytes and compute the digest without text
transcoding or newline normalization:

```bash
PAYLOAD_SHA256="sha256:$(sha256sum authority-public.json | cut -d' ' -f1)"

dpone airflow build \
  --release-id "$RELEASE_ID" \
  --environment development \
  --trust-tier non_production \
  --runtime-image-ref "$RUNTIME_IMAGE_REF" \
  --runtime-image-digest "$RUNTIME_IMAGE_DIGEST" \
  --artifact-registry-ref artifacts \
  --registry-config-map-name dpone-artifact-registry \
  --registry-config-sha256 "$REGISTRY_CONFIG_SHA256" \
  --runtime-authority-payload-file authority-public.json \
  --runtime-authority-payload-sha256 "$PAYLOAD_SHA256"
```

The CLI rejects symlinks, non-regular/unreadable files, empty input, more than
4,096 bytes, and a digest mismatch before deployment artifacts are written. The
v5 provider validates canonical Base64, byte count, digest, and the unchanged
16 KiB total runtime-plan limit before constructing a task. Init-fetch writes a
verified file atomically to an 8 KiB memory-backed Pod volume; base mounts it
read-only and verifies it again before its independent authority call. This mode
creates or reads no deployment-specific Secret or ConfigMap.

Inspect `runtime_artifact_delivery.runtime_authority.mode`, `bytes`, and
`sha256` in the generated deployment/index. Do not print `payload_b64` in shared
diagnostics. A successful task has a completed init-fetch container and a base
container that passes runtime authority before registry/ready/connection access.

## Keep the Kubernetes Secret source

Use Secret mode whenever the file contains confidential plaintext:

```bash
dpone airflow build \
  --release-id "$RELEASE_ID" \
  --environment development \
  --trust-tier non_production \
  --runtime-image-ref "$RUNTIME_IMAGE_REF" \
  --runtime-image-digest "$RUNTIME_IMAGE_DIGEST" \
  --artifact-registry-ref artifacts \
  --registry-config-map-name dpone-artifact-registry \
  --registry-config-sha256 "$REGISTRY_CONFIG_SHA256" \
  --runtime-authority-secret-name dpone-runtime-authority \
  --runtime-authority-secret-key authority.json
```

Secret mode continues to emit the frozen v4 wire and unchanged read-only Secret
volume. The source modes are mutually exclusive. Upgrade core, Airflow pack,
formal provider, and runtime image together before using v5. See the
[operations runbook](runbooks/airflow-runtime-authority.md) for recovery and
rollback.

Live Kubernetes behavior is `UNVERIFIED` until separately exercised in an
explicitly approved environment.
