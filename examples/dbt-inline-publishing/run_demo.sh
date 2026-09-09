#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "${DEMO_DIR}/../.." && pwd)"
PUBLISH_PROFILES="${DEMO_DIR}/dpone/dbt-publish-profiles.yml"
FIXTURE_MANIFEST="${DEMO_DIR}/fixtures/manifest.v12.json"
DEMO_MODE="${DPONE_DBT_DEMO_MODE:-auto}"
MANIFEST_OUTPUT="${DPONE_DBT_DEMO_MANIFEST_OUTPUT:-}"
DEMO_WORK_DIR="$(mktemp -d)"

cleanup() {
  rm -rf -- "${DEMO_WORK_DIR}"
}
trap cleanup EXIT

case "${DEMO_MODE}" in
  auto | parse | fixture) ;;
  *)
    echo "DPONE_DBT_DEMO_MODE must be auto, parse, or fixture." >&2
    exit 2
    ;;
esac

if [[ -x "${REPOSITORY_ROOT}/.venv/bin/dpone" ]]; then
  DPONE_COMMAND=("${REPOSITORY_ROOT}/.venv/bin/dpone")
elif command -v dpone >/dev/null 2>&1; then
  DPONE_COMMAND=(dpone)
elif command -v uv >/dev/null 2>&1; then
  DPONE_COMMAND=(uv run dpone)
else
  echo "Install dpone, or run this script from a uv-managed dpone checkout." >&2
  exit 2
fi

DBT_COMMAND=()
DBT_VERSION=""
if [[ -x "${REPOSITORY_ROOT}/.venv/bin/dbt" ]]; then
  DBT_COMMAND=("${REPOSITORY_ROOT}/.venv/bin/dbt")
elif command -v dbt >/dev/null 2>&1; then
  DBT_COMMAND=(dbt)
elif command -v uv >/dev/null 2>&1; then
  DBT_COMMAND=(uv run dbt)
fi
if [[ "${#DBT_COMMAND[@]}" -gt 0 ]]; then
  DBT_VERSION="$("${DBT_COMMAND[@]}" --no-send-anonymous-usage-stats --version 2>/dev/null || true)"
fi

DBT_PARSE_READY=false
if [[ -n "${DBT_VERSION}" && "${DBT_VERSION}" == *"sqlserver"* ]]; then
  DBT_PARSE_READY=true
fi

MANIFEST="${FIXTURE_MANIFEST}"
MANIFEST_SOURCE="checked-in deterministic fixture"
PROJECT_DIR="${DEMO_DIR}"

if [[ "${DEMO_MODE}" == "parse" && "${DBT_PARSE_READY}" != true ]]; then
  echo "Real parse mode requires dbt Core and the dbt-sqlserver adapter." >&2
  exit 2
fi
if [[ -n "${MANIFEST_OUTPUT}" && "${DEMO_MODE}" != "parse" ]]; then
  echo "DPONE_DBT_DEMO_MANIFEST_OUTPUT requires DPONE_DBT_DEMO_MODE=parse." >&2
  exit 2
fi

if [[ "${DEMO_MODE}" != "fixture" && "${DBT_PARSE_READY}" == true ]]; then
  mkdir -p "${DEMO_WORK_DIR}/examples" "${DEMO_WORK_DIR}/packages"
  cp -R "${DEMO_DIR}" "${DEMO_WORK_DIR}/examples/dbt-inline-publishing"
  cp -R "${REPOSITORY_ROOT}/packages/dbt-dpone" "${DEMO_WORK_DIR}/packages/dbt-dpone"

  TEMP_PROJECT="${DEMO_WORK_DIR}/examples/dbt-inline-publishing"
  cat >"${TEMP_PROJECT}/packages.yml" <<'YAML'
packages:
  - local: ../../packages/dbt-dpone
YAML
  TEMP_LOGS="${DEMO_WORK_DIR}/logs"
  TEMP_TARGET="${DEMO_WORK_DIR}/target"

  "${DBT_COMMAND[@]}" --no-send-anonymous-usage-stats \
    --no-use-colors --log-path "${TEMP_LOGS}" deps \
    --project-dir "${TEMP_PROJECT}"
  DPONE_DBT_DEMO_SQLSERVER_USER=parse_only \
    DPONE_DBT_DEMO_SQLSERVER_PASSWORD=parse_only \
    "${DBT_COMMAND[@]}" --no-send-anonymous-usage-stats \
      --no-use-colors --log-path "${TEMP_LOGS}" parse \
      --project-dir "${TEMP_PROJECT}" \
      --profiles-dir "${TEMP_PROJECT}/profiles" \
      --target-path "${TEMP_TARGET}" \
      --no-partial-parse

  MANIFEST="${TEMP_TARGET}/manifest.json"
  MANIFEST_SOURCE="real dbt parse in a temporary project copy"
  PROJECT_DIR="${TEMP_PROJECT}"
  if [[ -n "${MANIFEST_OUTPUT}" ]]; then
    if [[ -L "${MANIFEST_OUTPUT}" ]]; then
      echo "Refusing to replace a symlinked manifest output." >&2
      exit 2
    fi
    mkdir -p -- "$(dirname -- "${MANIFEST_OUTPUT}")"
    install -m 0644 -- "${MANIFEST}" "${MANIFEST_OUTPUT}"
    echo "Generated manifest: ${MANIFEST_OUTPUT}"
  fi
elif [[ "${DEMO_MODE}" == "auto" ]]; then
  echo "dbt-sqlserver is unavailable; using the deterministic fixture."
fi

echo "Manifest source: ${MANIFEST_SOURCE}"
"${DPONE_COMMAND[@]}" dbt check \
  --project-dir "${PROJECT_DIR}" \
  --manifest "${MANIFEST}" \
  --profiles "${PUBLISH_PROFILES}"
echo "Demo PASS: validated 2 publish-enabled models in 1 workflow."
