"""Activity functions for this process."""

from .resolve_read_workspace_stepreso import resolve_read_workspace_stepreso, ResolveReadWorkspaceStepresoInput
from .create_feature_branch_stepbran import create_feature_branch_stepbran, CreateFeatureBranchStepbranInput
from .gate_probe_code_stepcode import gate_probe_code_stepcode, GateProbeCodeStepcodeInput

# Activity list for worker registration (used by auto-discovery)
ACTIVITIES = [
    resolve_read_workspace_stepreso,
    create_feature_branch_stepbran,
    gate_probe_code_stepcode,
]

# Agent-queue activities — dispatched to {tenant}--agent for dedicated
# infrastructure. Only execution_type=agent steps.
AGENT_QUEUE_ACTIVITIES = [
    gate_probe_code_stepcode,
]

__all__ = [
    "ACTIVITIES",
    "AGENT_QUEUE_ACTIVITIES",
    "resolve_read_workspace_stepreso",
    "create_feature_branch_stepbran",
    "gate_probe_code_stepcode",
    "ResolveReadWorkspaceStepresoInput",
    "CreateFeatureBranchStepbranInput",
    "GateProbeCodeStepcodeInput",
]
