"""Compatibility imports for native invocation authentication.

Acquisition and filesystem observations belong to the adapter; immutable identity
policy belongs to contracts. The historical imports retain object identity.
"""

from dpone.adapters.native_generation_invocation_auth import (
    InvocationOriginalReader as InvocationOriginalReader,
)
from dpone.adapters.native_generation_invocation_auth import (
    authenticate_invocation as authenticate_invocation,
)
from dpone.adapters.native_generation_invocation_auth import (
    verify_invocation_paths as verify_invocation_paths,
)
from dpone.contracts.native_generation_invocation import AuthenticatedInvocationPlan as AuthenticatedInvocationPlan
