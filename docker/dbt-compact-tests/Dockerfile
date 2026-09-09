# Public-source offline test image. Build context contains only this file and
# source.bundle, created from the current repository's reachable public commits.
FROM python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254
RUN apt-get update && apt-get install -y --no-install-recommends git jq unixodbc ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir uv==0.12.10
COPY source.bundle /tmp/source.bundle
ARG SOURCE_COMMIT
ARG BASE_COMMIT
RUN git clone --no-checkout /tmp/source.bundle /workspace \
    && git -C /workspace checkout --detach "$SOURCE_COMMIT" \
    && git -C /workspace update-ref refs/remotes/origin/master "$BASE_COMMIT" \
    && rm /tmp/source.bundle
WORKDIR /workspace
RUN --mount=type=cache,id=dpone-dbt-compact-uv,target=/root/.cache/uv uv sync --locked --extra dbt-mssql --extra postgres --extra gcp --extra columnar --extra accel
RUN useradd --create-home --uid 10001 dpone && chown -R dpone:dpone /workspace
USER dpone
ENV UV_NO_SYNC=1 UV_OFFLINE=1 PYTHONDONTWRITEBYTECODE=1
# Runtime is launched with --network none. No credentials or host directories
# are mounted, and no SQL/database service is required by this test matrix.
CMD ["uv", "run", "pytest", "tests/test_dbt_compact_wire_v2.py", "tests/test_dbt_compact_wire_v2_cli.py", "tests/test_dbt_compact_wire_v2_boundaries.py", "tests/test_dbt_compact_wire_v2_failures.py", "tests/test_dbt_release_wire_dispatch.py", "tests/test_dbt_runtime_payload_contract.py", "tests/test_dbt_release_workload_binding.py", "tests/test_dbt_release_artifact_index.py", "tests/test_dbt_workspace_cache_installation.py", "tests/test_dbt_source_inventory_binding.py", "tests/test_dbt_publish_atomicity.py", "tests/test_dbt_release_integrity.py", "tests/test_airflow_compact_pack_runtime_payload_limits.py", "-q", "-o", "addopts="]
