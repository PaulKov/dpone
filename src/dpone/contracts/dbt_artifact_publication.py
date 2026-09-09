"""Stable failure semantics of immutable dbt artifact publication.

These exceptions describe the capability, not its filesystem implementation.
An uncertain durable outcome must not be reported as a validation conflict:
the requested tree may already be visible and must be verified on retry.
"""


class DbtArtifactOutputConflict(RuntimeError):
    """The destination exists but does not contain the requested immutable tree."""


class DbtArtifactPublicationError(RuntimeError):
    """The immutable tree could not be published with a provable outcome."""
