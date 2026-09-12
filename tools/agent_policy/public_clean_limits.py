"""Shared bounded capacities for the approved local migration review contract.

These are fixed safety limits, not operator overrides. Source/archive budgets
remain owned by tenant_hygiene; measured RSS acceptance is not a runtime limiter.
"""

MAX_FINDINGS = 100_000
MAX_REVIEW_EXCEPTIONS = 100_000
MAX_PRIVATE_JSON_BYTES = 64 * 1024**2
MAX_POLICY_ENTRIES = 10_000
MAX_TICKET_POSITIONS = 10_000
