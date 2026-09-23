"""Compatibility facade for API parallel execution helpers."""

from dpone.runtime.connectors.api.parallel_execution import (
    BoundedParallelStreamExecutor as BoundedParallelStreamExecutor,
)
from dpone.runtime.connectors.api.parallel_execution import (
    ParallelTaskExecutor as ParallelTaskExecutor,
)
from dpone.runtime.connectors.api.parallel_execution import (
    TaskResult as TaskResult,
)

__all__ = ["BoundedParallelStreamExecutor", "ParallelTaskExecutor", "TaskResult"]
