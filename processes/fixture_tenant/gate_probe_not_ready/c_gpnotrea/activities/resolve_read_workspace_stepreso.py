# @scaffold:activity resolve_read_workspace_stepreso.py
# @scaffold:step_id step-resolve
# @scaffold:step_name "Resolve Read Workspace"
# @scaffold:executor SYSTEM
# @scaffold:implementation CATALOG_DISPATCHER
# @scaffold:file_hash sha256:14fe26dc496e7f6b322dda4782212412706e1edf2feb35a0bf1a0556baddad96
#
# PURPOSE: Session worktree for petclinic at main
#
# INPUTS:
# - Resolve Read Workspace - Repository Key (art-resolve-repo-key) -> repo_key
# - Resolve Read Workspace - Base Branch Override (art-resolve-branch-base) -> branch_base
#
# OUTPUTS:
# - Resolve Read Workspace - Working Directory (art-resolve-wd) -> working_directory
# - Resolve Read Workspace - Repository Key (out) (art-resolve-repo-key-out) -> repo_key
# - Resolve Read Workspace - Base Branch (art-resolve-base-branch) -> base_branch
# - Resolve Read Workspace - Base SHA (art-resolve-base-sha) -> base_sha
#
# EXECUTION MODE: Catalog Dispatcher
# catalog_activity_key: resolve_read_workspace
# This step uses a pre-configured catalog activity.
# The execute_catalog_activity dispatcher handles the actual execution.
"""
Step: Resolve Read Workspace (SYSTEM)

Lineage ID: step-resolve
Executor Type: SYSTEM
Catalog Activity: resolve_read_workspace
Entry Point: Yes

Session worktree for petclinic at main
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Optional, Any

from temporalio import activity

from hiops_platform import platform, Artifact

from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.constants import STEP_RESOLVE_READ_WORKSPACE
from hiops_platform import (
    CatalogActivityInput,
    CatalogActivityResult,
    dispatch_catalog,
)
from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.models.artifacts import (
    ResolveReadWorkspaceRepositoryKey,
    ResolveReadWorkspaceBaseBranchOverride,
    ResolveReadWorkspaceWorkingDirectory,
    ResolveReadWorkspaceRepositoryKey,
    ResolveReadWorkspaceBaseBranch,
    ResolveReadWorkspaceBaseSHA,
)

logger = logging.getLogger(__name__)


@dataclass
class ResolveReadWorkspaceStepresoInput:
    """Input for resolve_read_workspace_stepreso activity."""
    instance_id: str
    step_execution_id: str
    step_lineage_id: str
    tenant_code: str
    repo_key: Optional[ResolveReadWorkspaceRepositoryKey] = None
    branch_base: Optional[ResolveReadWorkspaceBaseBranchOverride] = None
    resolved_connections: Optional[dict[str, str]] = None  # catalog_activity_key -> connection_id mapping


@dataclass
class ResolveReadWorkspaceStepresoOutput:
    """Output from resolve_read_workspace_stepreso activity."""
    result: Optional[ResolveReadWorkspaceWorkingDirectory] = None


@activity.defn(name="resolve_read_workspace_stepreso_c_gpnotrea")
async def resolve_read_workspace_stepreso(
    input: ResolveReadWorkspaceStepresoInput,
) -> ResolveReadWorkspaceStepresoOutput:
    """
    Session worktree for petclinic at main

    Uses catalog activity: resolve_read_workspace
    """
    logger.info(
        f"[{STEP_RESOLVE_READ_WORKSPACE}] Starting system processing",
        extra={
            "instance_id": input.instance_id,
            "step_execution_id": input.step_execution_id,
            "step_lineage_id": input.step_lineage_id,
        },
    )

    async with platform.for_tenant(input.tenant_code) as p:
        # ─── Step Execution Tracking ─────────────────────────────────────────
        await p.steps.start(
            step_execution_id=uuid.UUID(input.step_execution_id),
            instance_id=uuid.UUID(input.instance_id),
            step_lineage_id="step-resolve",
            executor_type="SYSTEM",
        )
        # ─────────────────────────────────────────────────────────────────────

        # ─── Catalog Dispatcher Execution ───────────────────────────────────
        # Build config from step configuration
        config: dict[str, Any] = {}

        # Resolve connection_id from workflow's resolved_connections (legacy)
        if input.resolved_connections:
            connection_id = input.resolved_connections.get("resolve_read_workspace")
            if connection_id:
                config["connection_id"] = connection_id

        # Build payload from input artifacts (non-expression inputs)
        payload: dict[str, Any] = {
        }

        # Expression definitions for runtime evaluation by dispatcher.
        # If the workflow already resolved the value (e.g., cross-step references),
        # use it directly in the payload and skip the expression.
        expressions: dict[str, Any] = {}
        _expr_val_repo_key = Artifact.unwrap(getattr(input, "repo_key", None))
        if _expr_val_repo_key is not None:
            payload["repo_key"] = _expr_val_repo_key
        else:
            expressions["repo_key"] = "hello-world"
        _expr_val_branch_base = Artifact.unwrap(getattr(input, "branch_base", None))
        if _expr_val_branch_base is not None:
            payload["branch_base"] = _expr_val_branch_base
        else:
            expressions["branch_base"] = "master"

        # Execute via catalog activity dispatcher
        result: CatalogActivityResult = await dispatch_catalog(
            CatalogActivityInput(
                tenant_slug=input.tenant_code,
                instance_id=input.instance_id,
                step_execution_id=input.step_execution_id,
                step_lineage_id=input.step_lineage_id,
                workflow_id=activity.info().workflow_id,
                config=config,
                payload=payload,
                expressions=expressions,
                catalog_activity_key="resolve_read_workspace",
            )
        )

        if not result.success:
            raise RuntimeError(f"Catalog activity failed: {result.error}")

        # Check inner success status (all execution types return success: bool)
        if isinstance(result.output, dict) and result.output.get("success") is False:
            error_msg = result.output.get("error") or result.output.get("message") or "Catalog activity failed"
            raise RuntimeError(f"Catalog activity failed: {error_msg}")

        # Map outputs from result.output to artifact
        # Extract nested 'data' field if present (integration results wrap data)
        _result_data = result.output.get("data", result.output) if isinstance(result.output, dict) else result.output
        output = ResolveReadWorkspaceWorkingDirectory(
            id=input.step_execution_id,
            data=_result_data,
        )
        # ─────────────────────────────────────────────────────────────────────

        # ─── Complete Step Tracking ──────────────────────────────────────────
        await p.steps.complete(
            step_execution_id=uuid.UUID(input.step_execution_id),
            output_snapshot=output.__dict__ if hasattr(output, '__dict__') else None,
        )

        return ResolveReadWorkspaceStepresoOutput(result=output)
        # ─────────────────────────────────────────────────────────────────────

        # Exceptions propagate directly — StepExecutionInterceptor
        # handles failure recording in its own session (AFP).