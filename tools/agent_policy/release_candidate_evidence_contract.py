"""Closed field sets for provider-bound release-candidate evidence."""

MANIFEST_KEYS = frozenset(
    {
        "schema",
        "repository",
        "commit_sha",
        "release",
        "profile",
        "run_id",
        "run_attempt",
        "policy_sha256",
        "entries",
    }
)
PACK_KEYS = frozenset(
    {
        "schema",
        "status",
        "decision",
        "repository",
        "commit_sha",
        "release",
        "profile",
        "policy_sha256",
        "source_chain_sha256",
        "required_roles",
        "source_digests",
        "observations",
        "checklist",
        "package_versions",
        "blockers",
    }
)
RECEIPT_KEYS = frozenset(
    {
        "schema",
        "status",
        "repository",
        "commit_sha",
        "release",
        "profile",
        "workflow_path",
        "workflow_name",
        "job_name",
        "run_id",
        "run_attempt",
        "policy_sha256",
        "manifest_sha256",
        "pack_sha256",
        "source_chain_sha256",
        "binding_id",
    }
)
ENTRY_KEYS = frozenset({"role", "path", "size_bytes", "sha256"})
PAIRED_PUBLICATION_RUN_KEYS = frozenset(
    {
        "workflow_path",
        "run_id",
        "run_attempt",
        "created_at",
    }
)

__all__ = [
    "ENTRY_KEYS",
    "MANIFEST_KEYS",
    "PACK_KEYS",
    "PAIRED_PUBLICATION_RUN_KEYS",
    "RECEIPT_KEYS",
]
