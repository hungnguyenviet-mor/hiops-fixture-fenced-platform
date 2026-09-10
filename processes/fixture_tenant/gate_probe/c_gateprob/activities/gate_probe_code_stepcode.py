# @scaffold:activity gate_probe_code_stepcode.py
# @scaffold:step_id step-code
# @scaffold:step_name "Gate Probe Code"
# @scaffold:executor AI
# @scaffold:implementation CATALOG_DISPATCHER
# @scaffold:file_hash sha256:628516a285563e9bb22b56922bdfead0aa82c1be0366cc4b535004198af5e86c
#
# PURPOSE: One mutating agent step so the readiness gate runs
#
# INPUTS:
# - Gate Probe Code - Task (art-code-task) -> task
# - Create Feature Branch - Working Directory (art-branch-wd) -> working_directory
# - Create Feature Branch - Branch Name (art-branch-name) -> branch_name
# - Resolve Read Workspace - Base Branch (art-resolve-base-branch) -> base_branch
#
# OUTPUTS:
# - Gate Probe Code - Success (art-code-success) -> success
# - Gate Probe Code - Output (art-code-output) -> output
# - Gate Probe Code - Commit SHA (art-code-commit) -> commit_sha
# - Gate Probe Code - Is Stuck (art-code-is-stuck) -> is_stuck
#
# EXECUTION MODE: Catalog Dispatcher
# catalog_activity_key: op_anthropic_develop_code
# This step uses a pre-configured catalog activity.
# The execute_catalog_activity dispatcher handles the actual execution.
"""
Step: Gate Probe Code (AI)

Lineage ID: step-code
Executor Type: AI
Exit Point: Yes
Catalog Activity: op_anthropic_develop_code

Uses LLM with Instructor for structured output.
"""

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from temporalio import activity

from hiops_platform import platform, Artifact

from processes.fixture_tenant.gate_probe.c_gateprob.constants import STEP_GATE_PROBE_CODE
from hiops_platform import (
    CatalogActivityInput,
    CatalogActivityResult,
    dispatch_catalog,
)
from processes.fixture_tenant.gate_probe.c_gateprob.models.artifacts import (
    GateProbeCodeTask,
    CreateFeatureBranchWorkingDirectory,
    CreateFeatureBranchBranchName,
    ResolveReadWorkspaceBaseBranch,
    GateProbeCodeSuccess,
    GateProbeCodeOutput,
    GateProbeCodeCommitSHA,
    GateProbeCodeIsStuck,
)

logger = logging.getLogger(__name__)


@dataclass
class GateProbeCodeStepcodeInput:
    """Input for the Gate Probe Code AI activity."""
    instance_id: str
    step_execution_id: str
    step_lineage_id: str
    tenant_code: str
    task: Optional[GateProbeCodeTask] = None
    working_directory: Optional[CreateFeatureBranchWorkingDirectory] = None
    branch_name: Optional[CreateFeatureBranchBranchName] = None
    base_branch: Optional[ResolveReadWorkspaceBaseBranch] = None
    resolved_connections: Optional[dict[str, str]] = None  # catalog_activity_key -> connection_id mapping
    platform_context: Optional[dict] = None  # Injected by workflow runtime for attempt-aware execution


@dataclass
class GateProbeCodeStepcodeOutput:
    """Output from the Gate Probe Code AI activity."""
    result: Optional[GateProbeCodeSuccess] = None


@activity.defn(name="gate_probe_code_stepcode_c_gateprob")
async def gate_probe_code_stepcode(
    input: GateProbeCodeStepcodeInput,
) -> GateProbeCodeStepcodeOutput:
    """
    One mutating agent step so the readiness gate runs

    Uses catalog activity: op_anthropic_develop_code
    """
    start_time = time.time()
    logger.info(
        f"[{STEP_GATE_PROBE_CODE}] Starting AI processing",
    )

    async with platform.for_tenant(input.tenant_code) as p:
        # ─── Step Execution Tracking ─────────────────────────────────────────
        await p.steps.start(
            step_execution_id=uuid.UUID(input.step_execution_id),
            instance_id=uuid.UUID(input.instance_id),
            step_lineage_id="step-code",
            executor_type="AI",
        )
        # Commit step execution so it's visible to UI before long-running activity
        await p.commit()
        # ─────────────────────────────────────────────────────────────────────

        # Exceptions propagate directly — StepExecutionInterceptor
        # handles failure recording in its own session (AFP).
        # ─── Catalog Dispatcher Execution ───────────────────────────────────
        # Resolve connection_id from workflow's resolved_connections (if needed)
        connection_id = None
        if input.resolved_connections:
            connection_id = input.resolved_connections.get("op_anthropic_develop_code")

        # Build config from step configuration
        config: dict[str, Any] = {"allowed_tools": ["Read", "Write", "Edit", "Bash", "Glob", "Grep"], "max_turns": 40, "timeout_seconds": 900}
        if connection_id:
            config["connection_id"] = connection_id

        # Build payload from input artifacts (non-expression inputs)
        payload: dict[str, Any] = {
            "working_directory": Artifact.unwrap(input.working_directory),
            "branch_name": Artifact.unwrap(input.branch_name),
            "base_branch": Artifact.unwrap(input.base_branch),
        }
        # Inject platform_context for attempt-aware agent execution
        if getattr(input, "platform_context", None) is not None:
            payload["platform_context"] = input.platform_context

        # Expression definitions for runtime evaluation by dispatcher.
        # If the workflow already resolved the value (e.g., split scope),
        # use it directly in the payload and skip the expression.
        expressions: dict[str, Any] = {}
        _expr_val_task = Artifact.unwrap(getattr(input, "task", None))
        if _expr_val_task is not None:
            payload["task"] = _expr_val_task
        else:
            expressions["task"] = "Do exactly these three things and nothing else. 1) Write a file named GATE_PROBE.md at the repository root containing the single line: gate probe. 2) Commit only that file with the message: gate probe. 3) Immediately call the complete_step tool with a one-sentence summary and stop. Do not read other files, do not run the build or tests, do not inspect the repository."

        # Execute via catalog activity dispatcher
        result: CatalogActivityResult = await dispatch_catalog(
            CatalogActivityInput(
                tenant_slug=input.tenant_code,
                instance_id=input.instance_id,
                step_execution_id=input.step_execution_id,
                step_lineage_id="step-code",
                workflow_id=activity.info().workflow_id,
                config=config,
                payload=payload,
                expressions=expressions,
                catalog_activity_key="op_anthropic_develop_code",
            )
        )

        if not result.success:
            raise RuntimeError(f"Catalog activity failed: {result.error}")

        # Map outputs from result.output to artifact
        # Extract nested 'data' field if present (some executors wrap data)
        _result_data = result.output.get("data", result.output) if isinstance(result.output, dict) else result.output
        output = GateProbeCodeSuccess(
            id=input.step_execution_id,
            data=_result_data,
        )
        # ─────────────────────────────────────────────────────────────────────

        # ─── Complete Step Tracking ──────────────────────────────────────────
        await p.steps.complete(
            step_execution_id=uuid.UUID(input.step_execution_id),
            output_snapshot=output.__dict__ if hasattr(output, '__dict__') else None,
        )

        duration = time.time() - start_time
        logger.info(
            f"[{STEP_GATE_PROBE_CODE}] AI processing completed",
            extra={"duration_seconds": round(duration, 2)},
        )

        return GateProbeCodeStepcodeOutput(result=output)
        # ─────────────────────────────────────────────────────────────────────
