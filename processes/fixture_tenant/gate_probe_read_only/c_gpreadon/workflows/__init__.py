"""Workflow classes for this process."""

from ..workflow import (
    GateProbeReadOnly_gpreadonWorkflow,
)

# Workflow list for worker registration (used by auto-discovery)
WORKFLOWS = [
    GateProbeReadOnly_gpreadonWorkflow,
]

__all__ = [
    "WORKFLOWS",
    "GateProbeReadOnly_gpreadonWorkflow",
]
