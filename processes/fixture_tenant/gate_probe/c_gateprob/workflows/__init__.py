"""Workflow classes for this process."""

from ..workflow import (
    GateProbe_gateprobWorkflow,
)

# Workflow list for worker registration (used by auto-discovery)
WORKFLOWS = [
    GateProbe_gateprobWorkflow,
]

__all__ = [
    "WORKFLOWS",
    "GateProbe_gateprobWorkflow",
]
