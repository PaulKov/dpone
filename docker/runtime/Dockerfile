# syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bookworm AS runtime-base

ARG DPONE_PACKAGE_SPEC="dpone[full]"
ARG DPONE_ACCEL_PACKAGE_SPEC=""
ARG CLICKHOUSE_REPO_CHANNEL="stable"
ARG DPONE_IMAGE_VERSION="dev"
ARG DPONE_IMAGE_REVISION="unknown"
ARG DPONE_IMAGE_CREATED=""
ARG AIRFLOW_PACKAGE_SPEC=""
ARG AIRFLOW_PROVIDER_PACKAGE_SPEC=""
ARG GH_CLI_VERSION="2.93.0"
ARG GH_CLI_AMD64_SHA256="5371f77155e5dd81b6e897c379431f734949b99b99ae1f01c5e3ab6d3795cf4d"
ARG GH_CLI_ARM64_SHA256="ac30768545cc31bbef9ffef27d2f2218b2739375019883b8095acf2f734e7b7f"
ARG COSIGN_VERSION="3.0.4"
ARG COSIGN_AMD64_SHA256="10dab2fd2170b5aa0d5c0673a9a2793304960220b314f6a873bf39c2f08287aa"
ARG COSIGN_ARM64_SHA256="c12fc6150195758ec0b1aeb1aade3381a1d3a299584982b66543f22bab04535b"

LABEL org.opencontainers.image.title="dpone runtime" \
      org.opencontainers.image.description="dpone runtime with ODBC Driver 18, bcp, sqlcmd, ClickHouse, GitHub CLI, and Cosign" \
      org.opencontainers.image.source="https://github.com/PaulKov/dpone" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${DPONE_IMAGE_VERSION}" \
      org.opencontainers.image.revision="${DPONE_IMAGE_REVISION}" \
      org.opencontainers.image.created="${DPONE_IMAGE_CREATED}"

ENV DEBIAN_FRONTEND=noninteractive \
    ACCEPT_EULA=Y \
    PATH="/opt/mssql-tools18/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      apt-transport-https \
      ca-certificates \
      curl \
      git \
      gnupg \
      build-essential \
      unixodbc \
      unixodbc-dev \
    && curl -sSL -O "https://packages.microsoft.com/config/debian/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)/packages-microsoft-prod.deb" \
    && dpkg -i packages-microsoft-prod.deb \
    && rm -f packages-microsoft-prod.deb \
    && install -m 0755 -d /usr/share/keyrings \
    && curl -fsSL "https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key" | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && arch="$(dpkg --print-architecture)" \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${arch}] https://packages.clickhouse.com/deb ${CLICKHOUSE_REPO_CHANNEL} main" > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql18 \
      mssql-tools18 \
      clickhouse-client \
    && architecture="$(dpkg --print-architecture)" \
    && case "${architecture}" in \
      amd64) gh_sha256="${GH_CLI_AMD64_SHA256}"; gh_asset_arch="amd64"; cosign_sha256="${COSIGN_AMD64_SHA256}" ;; \
      arm64) gh_sha256="${GH_CLI_ARM64_SHA256}"; gh_asset_arch="arm64"; cosign_sha256="${COSIGN_ARM64_SHA256}" ;; \
      *) echo "unsupported GitHub CLI architecture: ${architecture}" >&2; exit 1 ;; \
    esac \
    && gh_deb="gh_${GH_CLI_VERSION}_linux_${gh_asset_arch}.deb" \
    && curl -fsSLo "${gh_deb}" \
      "https://github.com/cli/cli/releases/download/v${GH_CLI_VERSION}/${gh_deb}" \
    && echo "${gh_sha256}  ${gh_deb}" | sha256sum --check --strict \
    && dpkg -i "${gh_deb}" \
    && rm -f "${gh_deb}" \
    && curl -fsSL --retry 5 \
      "https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-linux-${architecture}" \
      -o /tmp/cosign \
    && echo "${cosign_sha256}  /tmp/cosign" | sha256sum --check --strict \
    && install -m 0755 /tmp/cosign /usr/local/bin/cosign \
    && rm -f /tmp/cosign \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=bind,source=.,target=/opt/dpone-src,readonly \
    python -m pip install --upgrade pip \
    && if [[ "${DPONE_PACKAGE_SPEC}" == /opt/dpone-src* ]]; then \
      python -m pip install \
        --no-cache-dir \
        --retries 10 \
        --timeout 60 \
        "/opt/dpone-src/packages/dpone-native-accel" \
        "/opt/dpone-src/packages/dpone-airflow-pack" \
        "/opt/dpone-src/packages/apache-airflow-providers-dpone"; \
    elif [[ -n "${DPONE_ACCEL_PACKAGE_SPEC}" ]]; then \
      python -m pip install \
        --no-cache-dir \
        --index-url https://pypi.org/simple \
        --retries 10 \
        --timeout 60 \
        "${DPONE_ACCEL_PACKAGE_SPEC}"; \
    fi \
    && python -m pip install \
      --no-cache-dir \
      --index-url https://pypi.org/simple \
      --retries 10 \
      --timeout 60 \
      "${DPONE_PACKAGE_SPEC}" \
    && apt-get purge -y --auto-remove build-essential unixodbc-dev \
    && rm -rf /var/lib/apt/lists/* \
    && dpone --version \
    && bcp -v \
    && sqlcmd -? >/dev/null \
    && clickhouse-client --version \
    && gh version \
    && cosign version --json

COPY --chmod=0444 packages/dbt-dpone/dbt_project.yml /opt/dpone/runtime/dbt-dpone/dbt_project.yml
COPY --chmod=0444 packages/dbt-dpone/macros/dpone_publish.sql /opt/dpone/runtime/dbt-dpone/macros/dpone_publish.sql
COPY --chmod=0444 packages/dbt-dpone/macros/semantic_refresh_restore.sql /opt/dpone/runtime/dbt-dpone/macros/semantic_refresh_restore.sql
COPY --chmod=0444 packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql /opt/dpone/runtime/dbt-dpone/macros/semantic_refresh_scope_merge.sql

RUN python - <<'PY'
from pathlib import Path

from dpone.adapters.dbt_semantic_refresh_project import semantic_refresh_package_sha256

digest = semantic_refresh_package_sha256(Path("/opt/dpone/runtime/dbt-dpone"))
print(f"dbt-dpone {digest}")
PY

WORKDIR /workspace
ENTRYPOINT ["dpone"]
CMD ["--help"]

FROM runtime-base AS airflow-runtime

ARG AIRFLOW_PACKAGE_SPEC=""
ARG AIRFLOW_PROVIDER_PACKAGE_SPEC=""

RUN test -n "${AIRFLOW_PACKAGE_SPEC}" \
    && python -m pip install "${AIRFLOW_PACKAGE_SPEC}" ${AIRFLOW_PROVIDER_PACKAGE_SPEC:+${AIRFLOW_PROVIDER_PACKAGE_SPEC}} \
    && python - <<'PY'
import airflow
print(f"apache-airflow {airflow.__version__}")
PY

FROM runtime-base AS runtime
