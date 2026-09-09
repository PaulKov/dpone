"""Reserved names and allowlists for strict init-fetch pod composition."""

from dpone_airflow_pack.deployment_identity import AIRFLOW_DEPLOYMENT_IDENTITY_ENV

PLAN_B64_ENV = "DPONE_INIT_FETCH_PLAN_B64"
PLAN_SHA256_ENV = "DPONE_INIT_FETCH_PLAN_SHA256"
PLAN_SHA256_ANNOTATION = "dpone.io/init-fetch-plan-sha256"

FETCHED_VOLUME = "dpone-fetched-artifacts"
WORKTREE_VOLUME = "dpone-worktree"
RUN_OUTPUT_VOLUME = "dpone-run-output"
DEV_EVIDENCE_VOLUME = "dpone-dev-evidence"
REGISTRY_CONFIG_VOLUME = "dpone-artifact-registry-config"
TRUST_POLICY_VOLUME = "dpone-artifact-trust-policy"
INIT_CONTAINER_NAME = "dpone-runtime-init-fetch"

ARTIFACT_ROOT = "/var/lib/dpone/artifacts"
WORKTREE_ROOT = "/workspace/repo"
RUN_OUTPUT_ROOT = "/var/lib/dpone/run"
DEV_EVIDENCE_ROOT = "/var/lib/dpone/dev-evidence"
DEV_DBT_EVIDENCE_ROOT = "/var/lib/dpone/dev-evidence-dbt"
DEV_DBT_EVIDENCE_SUBPATH = "dbt-spool"
DEV_EVIDENCE_BOOTSTRAP_ROOT = "/var/lib/dpone/dev-evidence-bootstrap"
DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV = "DPONE_DBT_EVIDENCE_BOOTSTRAP_ROOT"
REGISTRY_CONFIG_DIRECTORY = "/etc/dpone/artifact-registry"
REGISTRY_CONFIG_PATH = f"{REGISTRY_CONFIG_DIRECTORY}/registry.json"
TRUST_POLICY_DIRECTORY = "/etc/dpone/artifact-trust"
TRUST_POLICY_PATH = f"{TRUST_POLICY_DIRECTORY}/policy.json"

RESERVED_VOLUMES = frozenset(
    {
        FETCHED_VOLUME,
        WORKTREE_VOLUME,
        RUN_OUTPUT_VOLUME,
        DEV_EVIDENCE_VOLUME,
        REGISTRY_CONFIG_VOLUME,
        TRUST_POLICY_VOLUME,
    }
)
RESERVED_PATHS = frozenset(
    {
        ARTIFACT_ROOT,
        WORKTREE_ROOT,
        RUN_OUTPUT_ROOT,
        DEV_EVIDENCE_ROOT,
        DEV_DBT_EVIDENCE_ROOT,
        DEV_EVIDENCE_BOOTSTRAP_ROOT,
        REGISTRY_CONFIG_DIRECTORY,
        REGISTRY_CONFIG_PATH,
        TRUST_POLICY_DIRECTORY,
        TRUST_POLICY_PATH,
    }
)
ALLOWED_PACK_ENV = frozenset(
    {
        "DPONE_DAG_ID",
        "DPONE_DAG_RUN_ID",
        "DPONE_TRY_NUMBER",
        "DPONE_LOGICAL_DATE",
        "DPONE_INTERVAL_START",
        "DPONE_INTERVAL_END",
        "DPONE_PARTITION_KEY",
        "DPONE_PARTITION_DIMENSION",
        "DPONE_PARTITION_MODE",
        "DPONE_AIRFLOW_RUN_IDENTITY",
        AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
        "DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF",
        "DPONE_AIRFLOW_MAPPING_ITEM",
        # Non-production object-storage registry access via boto3 default chain.
        # Used when the platform has no Kubernetes workload identity for S3.
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        # DPONE_REPAIR_AUTHORITY_REF is scheduler-owned (compose after this
        # filter from dag_run.conf). Packs must not bake a standing ID.
    }
)
FORBIDDEN_KPO_FIELDS = frozenset(
    {
        "security_context",
        "container_security_context",
        "hostnetwork",
        "host_network",
        "host_aliases",
        "dnspolicy",
        "dns_policy",
        "dns_config",
        "image_pull_secrets",
        "secrets",
        "env_from",
        "volumes",
        "volume_mounts",
        "init_containers",
        "pod_template_file",
        "pod_template_dict",
    }
)
FORBIDDEN_POD_SPEC_FIELDS = frozenset(
    {
        "securityContext",
        "hostNetwork",
        "hostPID",
        "hostIPC",
        "hostUsers",
        "shareProcessNamespace",
        "hostAliases",
        "dnsConfig",
        "automountServiceAccountToken",
    }
)

__all__ = [
    "ALLOWED_PACK_ENV",
    "ARTIFACT_ROOT",
    "FETCHED_VOLUME",
    "DEV_EVIDENCE_ROOT",
    "DEV_DBT_EVIDENCE_ROOT",
    "DEV_DBT_EVIDENCE_SUBPATH",
    "DEV_EVIDENCE_BOOTSTRAP_ROOT",
    "DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV",
    "DEV_EVIDENCE_VOLUME",
    "FORBIDDEN_KPO_FIELDS",
    "FORBIDDEN_POD_SPEC_FIELDS",
    "INIT_CONTAINER_NAME",
    "PLAN_B64_ENV",
    "PLAN_SHA256_ANNOTATION",
    "PLAN_SHA256_ENV",
    "REGISTRY_CONFIG_DIRECTORY",
    "REGISTRY_CONFIG_PATH",
    "REGISTRY_CONFIG_VOLUME",
    "RESERVED_PATHS",
    "RESERVED_VOLUMES",
    "RUN_OUTPUT_ROOT",
    "RUN_OUTPUT_VOLUME",
    "TRUST_POLICY_DIRECTORY",
    "TRUST_POLICY_PATH",
    "TRUST_POLICY_VOLUME",
    "WORKTREE_ROOT",
    "WORKTREE_VOLUME",
]
