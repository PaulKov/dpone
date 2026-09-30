# MSSQL SqlClient synthetic certification

Certification runs deterministic narrow and 100-column fixtures at 10,000 and
1,000,000 rows, layouts 1 and 2, serial and parallel import, plus force-kill
recovery. It proves the exact clean commit in an immutable Linux x86-64 image.
It does not certify a deployment's private source, target, network, capacity,
or SLO.

## Preconditions

- clean Git worktree at the intended commit;
- Docker with Linux amd64 execution support;
- a dedicated Docker VM with at least 8 GiB assigned and no unrelated heavy
  workloads; the repository SQL Server profile defaults to a 2 GiB engine cap;
- a reachable synthetic SQL Server on a named Docker network;
- an empty access-controlled output directory;
- the six `DPONE_IT_MSSQL_*` variables required by the live fixture;
- database permission to create, bulk-load, lock, inspect, and drop synthetic
  stages plus use the external transaction catalog.

Verify the network and target container before the run:

```bash
docker network inspect certification-network >/dev/null
docker ps --format '{{.Names}} {{.Networks}}'
```

## Exact campaign

```bash
CERT_ROOT="$(mktemp -d)"
HEAD_SHA="$(git rev-parse HEAD)"
SHORT_SHA="$(git rev-parse --short=8 HEAD)"
DOCKER_BIN="${DOCKER_BIN:-docker}"

uv run python tools/mssql_sqlclient_certification_image.py \
  --root . --docker "$DOCKER_BIN" \
  --tag "dpone-mssql-sqlclient-cert:$SHORT_SHA" \
  --output "$CERT_ROOT/image.json"

uv run python tools/mssql_sqlclient_certification_runner.py \
  --docker "$DOCKER_BIN" \
  --image "dpone-mssql-sqlclient-cert:$SHORT_SHA" \
  --image-receipt "$CERT_ROOT/image.json" \
  --network certification-network \
  --evidence-dir "$CERT_ROOT/evidence" \
  --output "$CERT_ROOT/runner.json" \
  --pass-env DPONE_IT_MSSQL_HOST --pass-env DPONE_IT_MSSQL_PORT \
  --pass-env DPONE_IT_MSSQL_DATABASE --pass-env DPONE_IT_MSSQL_USER \
  --pass-env DPONE_IT_MSSQL_PASSWORD \
  --pass-env DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE

uv run python tools/mssql_sqlclient_transport_campaign.py \
  --evidence-dir "$CERT_ROOT/evidence" \
  --source-commit-sha "$HEAD_SHA" \
  --runner-receipt "$CERT_ROOT/runner.json" \
  --package-version 0.88.0 \
  --secret-env DPONE_IT_MSSQL_PASSWORD \
  --output "$CERT_ROOT/campaign.json"
```

Success exits `0`. The tools emit Docker/pytest progress to inherited stdout and
stderr; the authoritative outputs are UTF-8 JSON files. They are created once,
atomically, with mode `0600`. An existing file or symlink is rejected.

Each fixture performs a bounded SQL readiness handshake before starting its
measurement. This absorbs transient connection establishment while a new
container network namespace settles; readiness time is excluded from transport
phase timings. Exhausting the readiness deadline fails the cell and therefore
the complete campaign.

The versioned campaign uses a 48 MiB encoded/IPC frame ceiling, one extra
pending slot, two encoders, and up to two TDS writers. The wide profile adds an
8,192-row ceiling: one million rows require at most 123 raw stages plus the one
prepared-stage reservation under the 128-table limit. Each runner container has
a 2 GiB cgroup limit with swap disabled. Before execution, the runner verifies
Docker's applied memory, memory-plus-swap, and OOM-killer settings. During the
cell it samples Docker CLI's cache-adjusted memory usage and records the largest
sample with the applied settings in its v3 receipt. Missing observations,
setting drift, or an OOM-killed cell fails certification. This sampled value is
not a cgroup high-water mark. These limits apply to the synthetic campaign;
normal manifests keep their existing configurable defaults.

| Failed phase | Exit | Files retained |
|---|---:|---|
| image build or identity check | non-zero | Docker may retain build cache/image; `image.json` is absent |
| runner cell | non-zero | completed cell JSON files remain; `runner.json` is absent |
| runner closure | non-zero | all produced cell files remain; `runner.json` is absent |
| campaign validation/privacy scan | non-zero | image, runner, and cell inputs remain; `campaign.json` is absent |

Never append to a failed evidence directory. Preserve it for diagnosis, then
start with a new empty directory. A successful close contains seven immutable
container executions and seven cell artifacts, exact commit/tree/image identities,
`runner_platform: linux/amd64`, and `privacy_scan_status: PASS`.

Each fixture runs in a fresh container and Python process. This prevents ODBC,
multiprocessing, or companion-process state from one fixture affecting another
fixture's result.

The image, transport evidence, and campaign schemas remain v2. The runner
receipt is v3 because it binds the inspected resource policy and sampled
cache-adjusted container memory to every execution. Earlier runner receipts are rejected; rerun the
producers rather than editing or translating evidence by hand.
