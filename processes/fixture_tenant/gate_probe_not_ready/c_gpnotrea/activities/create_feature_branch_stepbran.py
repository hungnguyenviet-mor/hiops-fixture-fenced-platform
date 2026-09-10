# @scaffold:activity create_feature_branch_stepbran.py
# @scaffold:step_id step-branch
# @scaffold:step_name "Create Feature Branch"
# @scaffold:executor SYSTEM
# @scaffold:implementation CATALOG_DISPATCHER
# @scaffold:file_hash sha256:8b88d7e0182c85f26545f7e1d816b42e098b400b2ce1c23d6fc8b6b1e177ca80
#
# PURPOSE: Feature branch gate-probe/<probe id>
#
# INPUTS:
# - Resolve Read Workspace - Working Directory (art-resolve-wd) -> working_directory
# - Resolve Read Workspace - Repository Key (out) (art-resolve-repo-key-out) -> repo_key
# - Create Feature Branch - Context Label (art-branch-label) -> context_label
# - Create Feature Branch - Context ID (art-branch-ctx-id) -> context_id
# - Create Feature Branch - Allow Branch Reuse (art-branch-allow-reuse) -> allow_reuse
#
# OUTPUTS:
# - Create Feature Branch - Working Directory (art-branch-wd) -> working_directory
# - Create Feature Branch - Branch Name (art-branch-name) -> branch_name
# - Create Feature Branch - Repository Key (art-branch-repo-key) -> repo_key
#
# EXECUTION MODE: Catalog Dispatcher
# catalog_activity_key: create_feature_branch
# This step uses a pre-configured catalog activity.
# The execute_catalog_activity dispatcher handles the actual execution.
"""
Step: Create Feature Branch (SYSTEM)

Lineage ID: step-branch
Executor Type: SYSTEM
Catalog Activity: create_feature_branch

Feature branch gate-probe/<probe id>
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Optional, Any

from temporalio import activity

from hiops_platform import platform, Artifact

from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.constants import STEP_CREATE_FEATURE_BRANCH
from hiops_platform import (
    CatalogActivityInput,
    CatalogActivityResult,
    dispatch_catalog,
)
from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.models.artifacts import (
    ResolveReadWorkspaceWorkingDirectory,
    ResolveReadWorkspaceRepositoryKey,
    CreateFeatureBranchContextLabel,
    CreateFeatureBranchContextID,
    CreateFeatureBranchAllowBranchReuse,
    CreateFeatureBranchWorkingDirectory,
    CreateFeatureBranchBranchName,
    CreateFeatureBranchRepositoryKey,
)

logger = logging.getLogger(__name__)


@dataclass
class CreateFeatureBranchStepbranInput:
    """Input for create_feature_branch_stepbran activity."""
    instance_id: str
    step_execution_id: str
    step_lineage_id: str
    tenant_code: str
    working_directory: Optional[ResolveReadWorkspaceWorkingDirectory] = None
    repo_key: Optional[ResolveReadWorkspaceRepositoryKey] = None
    context_label: Optional[CreateFeatureBranchContextLabel] = None
    context_id: Optional[CreateFeatureBranchContextID] = None
    allow_reuse: Optional[CreateFeatureBranchAllowBranchReuse] = None
    resolved_connections: Optional[dict[str, str]] = None  # catalog_activity_key -> connection_id mapping


@dataclass
class CreateFeatureBranchStepbranOutput:
    """Output from create_feature_branch_stepbran activity."""
    result: Optional[CreateFeatureBranchWorkingDirectory] = None


@activity.defn(name="create_feature_branch_stepbran_c_gpnotrea")
async def create_feature_branch_stepbran(
    input: CreateFeatureBranchStepbranInput,
) -> CreateFeatureBranchStepbranOutput:
    """
    Feature branch gate-probe/<probe id>

    Uses catalog activity: create_feature_branch
    """
    logger.info(
        f"[{STEP_CREATE_FEATURE_BRANCH}] Starting system processing",
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
            step_lineage_id="step-branch",
            executor_type="SYSTEM",
        )
        # ─────────────────────────────────────────────────────────────────────

        # ─── Catalog Dispatcher Execution ───────────────────────────────────
        # Build config from step configuration
        config: dict[str, Any] = {}

        # Resolve connection_id from workflow's resolved_connections (legacy)
        if input.resolved_connections:
            connection_id = input.resolved_connections.get("create_feature_branch")
            if connection_id:
                config["connection_id"] = connection_id

        # Build payload from input artifacts (non-expression inputs)
        payload: dict[str, Any] = {
            "working_directory": Artifact.unwrap(input.working_directory),
            "repo_key": Artifact.unwrap(input.repo_key),
        }

        # Expression definitions for runtime evaluation by dispatcher.
        # If the workflow already resolved the value (e.g., cross-step references),
        # use it directly in the payload and skip the expression.
        expressions: dict[str, Any] = {}
        _expr_val_context_label = Artifact.unwrap(getattr(input, "context_label", None))
        if _expr_val_context_label is not None:
            payload["context_label"] = _expr_val_context_label
        else:
            expressions["context_label"] = "gate-probe"
        _expr_val_context_id = Artifact.unwrap(getattr(input, "context_id", None))
        if _expr_val_context_id is not None:
            payload["context_id"] = _expr_val_context_id
        else:
            expressions["context_id"] = "probe-nr1"
        _expr_val_allow_reuse = Artifact.unwrap(getattr(input, "allow_reuse", None))
        if _expr_val_allow_reuse is not None:
            payload["allow_reuse"] = _expr_val_allow_reuse
        else:
            expressions["allow_reuse"] = True

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
                catalog_activity_key="create_feature_branch",
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
        output = CreateFeatureBranchWorkingDirectory(
            id=input.step_execution_id,
            data=_result_data,
        )
        # ─────────────────────────────────────────────────────────────────────

        # ─── Complete Step Tracking ──────────────────────────────────────────
        await p.steps.complete(
            step_execution_id=uuid.UUID(input.step_execution_id),
            output_snapshot=output.__dict__ if hasattr(output, '__dict__') else None,
        )

        return CreateFeatureBranchStepbranOutput(result=output)
        # ─────────────────────────────────────────────────────────────────────

        # Exceptions propagate directly — StepExecutionInterceptor
        # handles failure recording in its own session (AFP).