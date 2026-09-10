"""Workflow classes for this process."""

from ..workflow import (
    GateProbeFlip_gpflip1Workflow,
)

# Workflow list for worker registration (used by auto-discovery)
WORKFLOWS = [
    GateProbeFlip_gpflip1Workflow,
]

__all__ = [
    "WORKFLOWS",
    "GateProbeFlip_gpflip1Workflow",
]
