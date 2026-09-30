FROM mcr.microsoft.com/dotnet/sdk:10.0@sha256:35d40304542c8689331f8cab17c65926cdf48fe711e289321d71924b230a7d29

ENV DEBIAN_FRONTEND=noninteractive \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    PATH="/venv/bin:/opt/mssql-tools18/bin:${PATH}" \
    PYTHONUNBUFFERED=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg python3 python3-pip python3-venv unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/ubuntu/24.04/prod.list \
      -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 \
    && python3 -m venv /venv \
    && rm -rf /var/lib/apt/lists/*

ARG SOURCE_COMMIT
ARG SOURCE_TREE_OID
LABEL org.opencontainers.image.source="https://github.com/PaulKov/dpone" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}" \
      dev.dpone.certification.source-tree="${SOURCE_TREE_OID}" \
      dev.dpone.certification.route="clickhouse-mssql-sqlclient-v1"

RUN [[ "${SOURCE_COMMIT}" =~ ^[0-9a-f]{40}$ ]] \
    && [[ "${SOURCE_TREE_OID}" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] \
    && install -d -m 0755 /etc/dpone \
    && printf '%s\n' "${SOURCE_COMMIT}" > /etc/dpone/source-commit \
    && printf '%s\n' "${SOURCE_TREE_OID}" > /etc/dpone/source-tree \
    && chmod 0444 /etc/dpone/source-commit /etc/dpone/source-tree

COPY . /opt/dpone-src
WORKDIR /opt/dpone-src

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip \
    && python -m pip install ./packages/dpone-airflow-pack \
    && python -m pip install ".[clickhouse,mssql]" pytest==9.0.3 \
    && python -m pip install --no-deps ./packages/dpone-mssql-sqlclient \
    && python - <<'PY'
from importlib.metadata import version

import dpone_mssql_sqlclient

print(version("dpone"))
print(dpone_mssql_sqlclient.capabilities()["protocol"])
PY

ENTRYPOINT ["python", "-m", "pytest"]
