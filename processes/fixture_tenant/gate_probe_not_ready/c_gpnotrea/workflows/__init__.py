"""Workflow classes for this process."""

from ..workflow import (
    GateProbeNotReady_gpnotreaWorkflow,
)

# Workflow list for worker registration (used by auto-discovery)
WORKFLOWS = [
    GateProbeNotReady_gpnotreaWorkflow,
]

__all__ = [
    "WORKFLOWS",
    "GateProbeNotReady_gpnotreaWorkflow",
]
