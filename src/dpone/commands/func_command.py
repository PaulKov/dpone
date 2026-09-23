"""Compatibility facade for functional command adapters."""

from dpone.commands.func_command_impl import (
    CommandGroup as CommandGroup,
)
from dpone.commands.func_command_impl import (
    FuncCommand as FuncCommand,
)

__all__ = ["CommandGroup", "FuncCommand"]
