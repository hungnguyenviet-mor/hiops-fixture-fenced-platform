# @scaffold:workflow workflow.py
# @scaffold:process_id gate-probe-notready
# @scaffold:process_name "Gate Probe Not Ready"
# @scaffold:file_hash sha256:ffc016217855a86cb532cb9db11f902b2c2e348c83e89d39442b061d5a647df1
#
# PURPOSE: Temporal workflow that orchestrates the Gate Probe Not Ready process.
#
# PROCESS FLOW (ASCII DAG — topological order):
# +-------------------------------------------------------------------------+
# |  [ENTRY] Resolve Read Workspace (SYSTEM)
# |             |
# |             v
# |          Create Feature Branch (SYSTEM)
# |             |
# |             v
# |  [EXIT]  Gate Probe Code (AI)
# +-------------------------------------------------------------------------+
#
#
# SIGNALS:
# Human Task signals (standardized — same 3 signals regardless of step count):
# - hi_human_task_complete: Task completed with result payload
# - hi_human_task_cancel: Task cancelled by admin/user
# - hi_human_task_reassign: Task reassigned (audit trail)
#
# MANAGEMENT SIGNALS:
# - pause: Pause workflow before next step
# - resume: Resume paused workflow
# - force_complete_step: Force complete a step with manual output
"""
Gate Probe Not Ready Workflow

Temporal workflow that orchestrates the Gate Probe Not Ready process.

resolve_read_workspace -> create_feature_branch -> op_anthropic_develop_code
"""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from json_logic import jsonLogic
from hiops_platform.router.evaluator import wrap_routing_value

from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.constants import (
    STEP_RESOLVE_READ_WORKSPACE,
    STEP_CREATE_FEATURE_BRANCH,
    STEP_GATE_PROBE_CODE,
)

# Import activities using workflow.unsafe to handle Temporal's sandboxing
# IMPORTANT: All activity imports MUST be in this block, not inside workflow methods.
# Temporal's sandbox blocks os.environ access, and SQLAlchemy (imported transitively)
# tries to access os.environ during import, causing RestrictedWorkflowAccessError.
# NOTE: Use absolute imports (not relative) for Temporal sandbox compatibility.
with workflow.unsafe.imports_passed_through():
    from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.activities import (
        resolve_read_workspace_stepreso,
        create_feature_branch_stepbran,
        gate_probe_code_stepcode,
        ResolveReadWorkspaceStepresoInput,
        CreateFeatureBranchStepbranInput,
        GateProbeCodeStepcodeInput,
    )
    from processes.fixture_tenant.gate_probe_not_ready.c_gpnotrea.models.artifacts import (
        HumanTaskResult,
        ResolveReadWorkspaceRepositoryKey,
        ResolveReadWorkspaceBaseBranchOverride,
        ResolveReadWorkspaceWorkingDirectory,
        ResolveReadWorkspaceRepositoryKey,
        ResolveReadWorkspaceBaseBranch,
        ResolveReadWorkspaceBaseSHA,
        CreateFeatureBranchContextLabel,
        CreateFeatureBranchContextID,
        CreateFeatureBranchAllowBranchReuse,
        CreateFeatureBranchWorkingDirectory,
        CreateFeatureBranchBranchName,
        CreateFeatureBranchRepositoryKey,
        GateProbeCodeTask,
        GateProbeCodeSuccess,
        GateProbeCodeOutput,
        GateProbeCodeCommitSHA,
        GateProbeCodeIsStuck,
    )
    # Artifact utilities for consistent wrapping/unwrapping
    from hiops_platform import Artifact
    # Catalog activity dispatch — Temporal string-name for cross-worker routing.
    from hiops_platform import (
        CatalogActivityInput,
        CatalogActivityResult,
        EXECUTE_CATALOG_ACTIVITY_NAME,
    )
    # Human Task primitives
    from hiops_platform import build_task_instance_id
    from hiops_platform.exceptions import (
        HumanTaskTimeoutError,
        HumanTaskCancelledError,
    )
    # Management activities for step failure, SLA breach, timeout
    from hiops_platform.management import (
        handle_step_failure,
        handle_sla_breach,
        handle_step_timeout,
        StepFailureInput,
        SLABreachInput,
        StepTimeoutInput,
        # Instance lifecycle activities
        complete_process_instance_activity,
        CompleteProcessInstanceInput,
        update_instance_status_activity,
        UpdateInstanceStatusInput,
        # Step lifecycle tracking
        mark_step_started_activity,
        MarkStepStartedInput,
        mark_step_complete_activity,
        MarkStepCompleteInput,
        # Human Task activities
        create_human_task_record,
        CreateHumanTaskInput,
        mark_human_task_timed_out,
        MarkHumanTaskTimedOutInput,
        emit_sla_breach as emit_sla_breach_activity,
        EmitSLABreachInput,
        # Compensation activities (P11)
        mark_step_compensated,
        MarkStepCompensatedInput,
        # Sub-project B Tier 1.5: defensive saga-walker cleanup
        terminalize_compensation_step_activity,
        TerminalizeCompensationStepInput,
        # Sub-project B Tier 3.4: audit row when the saga walker's
        # stack-push guard rejects a forward step (no result captured).
        record_compensation_skip_activity,
        RecordCompensationSkipInput,
        fetch_step_output_snapshot,
        FetchStepOutputSnapshotInput,
        # P12: Skip step activity
        mark_step_skipped,
        MarkStepSkippedInput,
    )
    # Sub-project A (hi_platform_v6): per-step branch SHA capture.
    # Contracts live in the library so backend + scaffolds don't drift.
    # Activities dispatched by string name; result_type= pins deserialization.
    from hiops_platform import (
        CapturePreStepShaInput,
        CapturePreStepShaResult,
        CreatePauseChatTaskInput,
        PAUSE_REASONS,
    )


# =============================================================================
# VERSION CONSTANTS
# =============================================================================
PROCESS_CHANGE_ID = "gpnotready1"  # Full change UUID
PLATFORM_PATCH_VERSION = 17



@dataclass
class GateProbeNotReady_gpnotreaInput:
    """Input for the Gate Probe Not Ready workflow (version gpnotrea)."""

    instance_id: str
    process_def_id: str
    snapshot_id: str
    tenant_code: str  # Required for tenant isolation in activities
    resolved_connections: Optional[dict[str, str]] = None  # catalog_id -> connection_id for integrations
    trigger_payload: Optional[dict[str, Any]] = None  # Raw webhook/trigger payload for entry point inputs


@dataclass
class GateProbeNotReady_gpnotreaOutput:
    """Output from the Gate Probe Not Ready workflow (version gpnotrea)."""

    success: bool
    outcome: str  # Final outcome of the workflow


# Default retry policy for activities
DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=3,
)

# Maximum iterations for LOOP connections to prevent infinite loops
MAX_LOOP_ITERATIONS = 100


def _get_referenced_vars(expr: Any) -> set[str]:
    """Extract all referenced variable names from a JSONLogic expression."""
    if isinstance(expr, dict):
        if "var" in expr and isinstance(expr["var"], str):
            return {expr["var"]}
        res = set()
        for v in expr.values():
            res.update(_get_referenced_vars(v))
        return res
    elif isinstance(expr, list):
        res = set()
        for item in expr:
            res.update(_get_referenced_vars(item))
        return res
    return set()


def _get_nested(
    data: Optional[dict],
    path: str,
    default: Any = None,
) -> Any:
    """
    Extract a value from a nested dict using dot-notation path.

    Examples:
        _get_nested({'issue_key': 'HIOPS-123'}, 'issue_key')
        -> 'HIOPS-123'

        _get_nested({'issue': {'key': 'HIOPS-123'}}, 'issue.key')
        -> 'HIOPS-123'

        _get_nested(None, 'issue.key')
        -> None
    """
    if not data:
        return default
    keys = path.split('.')
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current




def _system_context(instance_id: str) -> dict:
    """Build the process.* context for system variable expressions.

    workflow.info() is a metadata read, not a command, so calling this on every
    step dispatch adds no history and is replay-safe.
    """
    from hiops_platform.expressions.system_vars import build_system_context
    return build_system_context(
        instance_id=instance_id,
        started_at=workflow.info().start_time,
    )



# ============================================================================
# COMPENSATION SUPPORT (P11 Saga generation)
# ============================================================================

@dataclass
class _CompensationEntry:
    """Stack entry for the Saga compensation walk.

    Pushed after each compensatable step completes successfully.
    Walked in reverse order by _run_compensations() when a step fails.
    """
    forward_step_execution_id: str
    compensation_activity_key: str
    step_output: dict
    step_id: str
    step_name: str


class _CompensateAndCancelSentinel(Exception):
    """Raised after compensation walk to signal clean workflow termination.

    Caught by the top-level except in run() and converted to a terminal
    WorkflowOutput. Not a real error — this is control flow to break
    out of the dispatch/loop structure after compensate_and_cancel.
    """
    pass


# ============================================================================
# STEP ID CONSTANTS (for routing lookup)
# ============================================================================
_STEP_RESOLVE_READ_WORKSPACE_ID = "step-resolve"
_STEP_CREATE_FEATURE_BRANCH_ID = "step-branch"
_STEP_GATE_PROBE_CODE_ID = "step-code"

# Step ID -> display name (for error messages and hold state logging)
_STEP_NAMES: dict[str, str] = {
    "step-resolve": "Resolve Read Workspace",
    "step-branch": "Create Feature Branch",
    "step-code": "Gate Probe Code",
}

# ============================================================================
# ROUTING TABLE (generated from connections)
# ============================================================================
# Maps: source_step_id -> connection_type -> list of (target_step_id, outcome)
# For BRANCH connections triggered by ROUTER steps: outcome is the router outcome
# For SEQUENCE connections: outcome is None
# For PARALLEL connections: multiple targets execute concurrently

_ROUTING_TABLE: dict[str, dict[str, list[tuple[str, Optional[str]]]]] = {
    # Resolve Read Workspace
    "step-resolve": {
        "SEQUENCE": [
            ("step-branch", None),  # -> Create Feature Branch
        ],
    },
    # Create Feature Branch
    "step-branch": {
        "SEQUENCE": [
            ("step-code", None),  # -> Gate Probe Code
        ],
    },
}



# ============================================================================
# COMPENSATION POLICIES (P11 — generated from activity catalog)
# ============================================================================
# Maps step_id -> on_failure policy for steps with compensation-aware handling.
# Steps not in this dict use the existing _handle_step_failure path.

_STEP_COMPENSATION_POLICIES: dict[str, str] = {
    "step-resolve": "pause",  # Resolve Read Workspace
    "step-branch": "pause",  # Create Feature Branch
    "step-code": "pause_if_committed",  # Gate Probe Code
}

_EXCEPTION_TARGETS: set[str] = {
}

# ============================================================================
# P13 REGISTRIES (compensate_then_retry)
# ============================================================================
# Step lookup tables for the structured-failure dispatch path and for
# the inflight-compensation helpers. Mirrors _STEP_COMPENSATION_POLICIES
# but exposes per-step compensation keys, agent classification, workspace
# dependency, and the workspace artifact's lineage ID for cleanup.

# Maps step_id -> the forward activity's declared compensation_activity_key
# (if any). Used by the structured-failure path and by inflight cleanup.
_STEP_COMPENSATION_KEYS: dict[str, str] = {
    "step-code": "op_revert_branch_inflight",
}

# Agent steps need `attempt_history` appended on each failed retry; the
# helper that does the appending needs to know which steps are agents.
_STEP_IS_AGENT: set[str] = {
    "step-code",
}

# Steps whose inputs transitively depend on a workspace resolver
# (resolve_workspace / resolve_read_workspace / create_feature_branch).
# The per-step compensate_then_retry path fires a workspace-cleanup
# activity for these steps; non-workspace steps skip it.
_STEP_REQUIRES_WORKSPACE: set[str] = {
    "step-branch",
    "step-code",
}

# Maps step_id -> the artifact_lineage_id carrying the workspace's
# working_directory. Used by the workspace-cleanup hook to find the
# worktree path in self._artifacts.
_STEP_WORKSPACE_ARTIFACT: dict[str, str] = {
    "step-branch": "art-resolve-wd",
    "step-code": "art-branch-wd",
}

# Maps artifact_lineage_id -> activity_field_key (output declaration's
# field name). Used by _build_artifact_context to render the operator-
# facing pause description against named placeholders rather than
# UUID-keyed artifacts.
_ARTIFACT_FIELD_KEYS: dict[str, str] = {
    "art-resolve-wd": "working_directory",
    "art-resolve-repo-key-out": "repo_key",
    "art-resolve-base-branch": "base_branch",
    "art-resolve-base-sha": "base_sha",
    "art-branch-wd": "working_directory",
    "art-branch-name": "branch_name",
    "art-branch-repo-key": "repo_key",
    "art-code-success": "success",
    "art-code-output": "output",
    "art-code-commit": "commit_sha",
    "art-code-is-stuck": "is_stuck",
}

# Maps compensation activity_key -> human_description_template string.
# Used by _pause_on_failure to render the operator-facing cleanup copy
# for the compensate_then_retry confirmation dialog.
_COMPENSATION_DESCRIPTION_TEMPLATES: dict[str, str] = {
    "op_revert_branch_inflight": "discard any partial work on branch \u0027{branch_name}\u0027 from this step (predecessor commits preserved)",
}

# P13 sentinel: matches PAUSE_META_UNCHANGED in hiops_platform.instances.
# Distinguishes "leave pause metadata column unchanged" from "set to NULL".
# UpdateInstanceStatusInput fields default to this; explicit None clears.
_PAUSE_META_UNCHANGED = "__P13_PAUSE_META_UNCHANGED__"

@workflow.defn(name="gate_probe_not_ready_gpnotready1")
class GateProbeNotReady_gpnotreaWorkflow:
    """
    Gate Probe Not Ready workflow with signal-based human tasks.

    Version: gpnotready1
    Change Name: Initial version

    Orchestrates the process flow:
    1. Resolve Read Workspace (SYSTEM)
    2. Create Feature Branch (SYSTEM)
    3. Gate Probe Code (AI)
    """

    # -------------------------------------------------------------------------
    # TRIGGER CONFIGURATION
    # -------------------------------------------------------------------------
    # Entry point: Resolve Read Workspace
    # Trigger: Manual
    # -------------------------------------------------------------------------

    def __init__(self):
        # Management signal state
        self._is_paused = False
        self._paused_on_failure = False  # True when auto-paused due to step failure
        self._force_complete_data: Optional[dict] = None

        # Saga compensation stack (P11). Pushed after each compensatable
        # step succeeds; walked in reverse on failure.
        self._compensation_stack: list[_CompensationEntry] = []
        # Which step's failure triggered the current pause (P12 reads this)
        self._paused_failure_step_id: Optional[str] = None
        # Step execution UUID captured at pause time; consumed by the
        # _CompensateAndCancelSentinel handler to finalize the failing row.
        self._paused_failure_step_execution_id: Optional[str] = None
        # Maps step_id -> step_execution_id (UUID string) for the current
        # execution. Populated at step start; consumed by the structured
        # failure dispatch in run() which needs the UUID after the step
        # handler returns.
        self._step_execution_ids: dict[str, str] = {}

        # Artifacts dictionary for data flow between steps
        self._artifacts: dict[str, Any] = {}

        # Step error tracking for exception routing
        self._step_errors: dict[str, dict] = {}

        # Iteration counts for LOOP protection
        self._iteration_counts: dict[str, int] = {}

        # Routing context from the last router that executed
        self._routing_contexts: dict[str, dict] = {}
        # Named outputs from router steps, keyed by output_name
        self._named_routing_outputs: dict[str, Any] = {}
        # The most recent routing context (for the next downstream step)
        self._last_routing_context: dict | None = None
        # Attempt history per step (keyed by step_id). Each entry captures
        # the previous attempt's outcome so the next iteration's agent sees
        # what already failed. Populated by _record_attempt() at the end
        # of each step's dispatch.
        self._attempt_histories: dict[str, list[dict]] = {}
        # Most recent human-resolve outcome (decision/diagnosis/instructions/
        # chat_summary). Attached to the NEXT attempt's history entry so the
        # agent sees "human said X" alongside "previous attempt did Y". Cleared
        # after the next step consumes it.
        self._pending_human_resolution: dict | None = None
        # Most recent test-step outcome (success, tests_run/failed, output_tail,
        # …). Threaded into platform_context for downstream code-gen steps so
        # the agent has the actual test output to reason about. Replaced each
        # time a test step runs.
        self._last_test_run: dict | None = None
        # Platform catch-all state: which step is the failure that the catchall
        # is currently handling. Set when __exception__ routing falls through
        # to __platform_catchall_resolve__; consumed by the catchall handler
        # to build the human task's context. PLATFORM CATCH-ALL v1.
        self._catchall_origin_step_id: str | None = None

        # P12: Resume action from failure-pause. Set by
        # hi_resume_with_action signal handler; consumed by
        # _handle_activity_failure after _pause_on_failure returns.
        self._resume_action: Optional[str] = None
        self._resume_action_params: dict = {}

        # P13: rendered human_description_template for the operator UI.
        # Set at pause time from the failing step's compensation activity's
        # template; written to mp_process_instances.pause_description by
        # the same update_instance_status_activity that flips the status.
        # Cleared on resume.
        self._pending_pause_description: Optional[str] = None

        # Sub-project A (hi_platform_v6): per-step pre-dispatch branch
        # SHA cache. Populated by capture_pre_step_sha_activity before
        # dispatching steps whose compensation activity declares
        # needs_pre_sha=true. Consumed by _run_inflight_compensation to
        # scope op_revert_branch_inflight's reset to the captured SHA
        # (revert_target_sha) so we don't wipe prior committed work.
        if workflow.patched("hi_platform_v6"):
            self._pre_step_sha: dict[str, str] = {}


        # Standardized Human Task state (keyed by task_instance_id).
        # UNCONDITIONAL since #264 D5: emit_step_helpers emits _watch_sla
        # into every class and it reads _human_task_states/_human_task_uuids;
        # a non-HUMAN child without these dicts is one helper call away from
        # AttributeError. Five empty dicts cost nothing.
        self._human_task_completions: dict[str, dict] = {}
        self._human_task_cancellations: dict[str, str] = {}
        self._human_task_reassignments: dict[str, str] = {}
        self._human_task_states: dict[str, str] = {}
        # task_instance_id -> mp_human_tasks.id (UUID string). Populated when
        # the human task record is created so the SLA watcher can pass the
        # real DB UUID to handle_sla_breach (which looks up mp_human_tasks.id).
        self._human_task_uuids: dict[str, str] = {}

    # -------------------------------------------------------------------------
    # MANAGEMENT SIGNALS
    # -------------------------------------------------------------------------

    @workflow.signal
    async def pause(self) -> None:
        """Pause workflow before the next step."""
        workflow.logger.info("Received pause signal")
        self._is_paused = True

    @workflow.signal
    async def resume(self) -> None:
        """Resume paused workflow."""
        workflow.logger.info("Received resume signal")
        # A child's pause is ALWAYS a failure-pause (plain `pause` is never
        # forwarded), and releasing _pause_on_failure with no action re-raises
        # the original cause — the item's work would be destroyed. Only
        # hi_resume_with_action may clear a child's failure-pause; the bare
        # resume is deliberately NOT forwarded to children for the same
        # reason (#264 D5 review gate).
        if self._paused_on_failure and workflow.info().parent is not None:
            workflow.logger.warning(
                "bare resume ignored: this child is failure-paused and can "
                "only be released with an explicit recovery action"
            )
            return
        self._is_paused = False

    @workflow.signal
    async def force_complete_step(self, data: dict) -> None:
        """Force complete current step with manual output."""
        workflow.logger.info(
            "Received force_complete_step signal",
            extra={"data": data},
        )
        self._force_complete_data = data

    @workflow.signal(name="hi_resume_with_action")
    async def on_resume_with_action(self, payload: dict) -> None:
        """Resume from failure-pause with a specific recovery action.

        P12: Sent by the resume-with-action API endpoint. Carries the
        operator's chosen action and action-specific parameters. Unblocks
        _pause_on_failure's wait_condition.

        Idempotency (secondary defense): if the workflow is not paused,
        log and discard. The API provides primary defense (status check).
        """
        if not self._is_paused:
            workflow.logger.warning(
                "hi_resume_with_action received but workflow is not paused; "
                "discarding",
                extra={"payload": payload},
            )
            return

        # Targeted consume (#264 D5 review gate): the payload names the step
        # the operator acted on. With two children paused on DIFFERENT steps,
        # an untargeted action would be consumed by both — silently skipping
        # or retrying a step nobody looked at. A mismatch leaves this
        # workflow paused; the matching workflow consumes it. (Sits after the
        # forwarding loop so children always receive the fan-out.)
        _target_step = payload.get("step_id")
        # Fail CLOSED in a child: paused_failure_step_id is nullable, and a
        # falsy target would short-circuit this gate so every paused child
        # consumes the same action. A child that cannot confirm the target is
        # for it stays paused (recoverable). The root still consumes an
        # untargeted action, so legacy instances with a null column resume.
        _is_child = workflow.info().parent is not None
        if (_target_step or _is_child) and _target_step != self._paused_failure_step_id:
            workflow.logger.info(
                f"resume_with_action targets step {_target_step}; this "
                f"workflow is paused on {self._paused_failure_step_id} — "
                "leaving it for the target"
            )
            return

        self._resume_action = payload.get("action")
        self._resume_action_params = payload.get("params") or {}
        self._is_paused = False
        workflow.logger.info(
            f"Received resume_with_action: action={self._resume_action}",
            extra={"params": self._resume_action_params},
        )

    async def _check_paused(self, input: "GateProbeNotReady_gpnotreaInput") -> None:
        """Wait if workflow is paused. Call before each step."""
        if self._is_paused:
            # Update DB status so the UI shows the correct state
            status = "paused_on_failure" if self._paused_on_failure else "paused"
            try:
                await workflow.execute_activity(
                    update_instance_status_activity,
                    UpdateInstanceStatusInput(
                        tenant_code=input.tenant_code,
                        instance_id=input.instance_id,
                        status=status,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            except Exception:
                workflow.logger.warning("Failed to update instance status to paused")

            workflow.logger.info(f"Workflow {status}, waiting for resume signal...")
            await workflow.wait_condition(lambda: not self._is_paused)

            # Restore running status after resume
            self._paused_on_failure = False
            try:
                await workflow.execute_activity(
                    update_instance_status_activity,
                    UpdateInstanceStatusInput(
                        tenant_code=input.tenant_code,
                        instance_id=input.instance_id,
                        status="running",
                        error_context=None,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            except Exception:
                workflow.logger.warning("Failed to update instance status to running")

            workflow.logger.info("Workflow resumed")

    def _record_attempt(
        self,
        step_id: str,
        step_name: str,
        result_obj: Any,
    ) -> None:
        """Append an attempt entry to this step's history.

        Captures the outcome of THIS attempt so the next iteration's agent
        (or anything reading platform_context.attempt_history) can see what
        already failed. Also attaches the most recent human_resolution
        (if any), then clears it — it belongs to THIS attempt only.
        """
        if step_id not in self._attempt_histories:
            self._attempt_histories[step_id] = []

        agent_outcome: dict = {}
        if result_obj is not None and hasattr(result_obj, "result"):
            unwrapped = Artifact.unwrap(result_obj.result) if result_obj.result else None
            if isinstance(unwrapped, dict):
                agent_outcome = {
                    "summary": unwrapped.get("output") or unwrapped.get("summary"),
                    "confidence": unwrapped.get("confidence"),
                    "is_stuck": unwrapped.get("is_stuck"),
                    "stuck_category": unwrapped.get("stuck_category"),
                    "auto_synthesized": unwrapped.get("auto_synthesized"),
                }

        self._attempt_histories[step_id].append({
            "attempt": self._iteration_counts.get(step_id, 1),
            "step_name": step_name,
            "agent_outcome": agent_outcome,
            "routing": self._last_routing_context,
            "human_resolution": self._pending_human_resolution,
        })
        # Human resolution is consumed once per attempt — clear so it
        # doesn't bleed into a later unrelated attempt.
        self._pending_human_resolution = None

    def _handle_step_failure(
        self,
        step_id: str,
        cause: BaseException,
        on_permanent_failure: str = "EXCEPTION",
        force_permanent: bool = False,
    ) -> str:
        """Handle permanent step failure (retries exhausted or non-retryable error).

        Records the error and either raises (FAIL_WORKFLOW) or returns
        the __exception__ sentinel for exception-connection routing.
        """
        self._step_errors[step_id] = {
            "type": type(cause).__name__,
            "message": str(cause),
            "timestamp": workflow.now().isoformat(),
        }
        if on_permanent_failure == "FAIL_WORKFLOW" or force_permanent:
            raise cause
        # EXCEPTION routing: return sentinel that the routing table recognizes
        return "__exception__"

    # -------------------------------------------------------------------------
    # COMPENSATION HELPERS (P11 Saga generation)
    # -------------------------------------------------------------------------

    async def _handle_activity_failure(
        self,
        step_id: str,
        step_execution_id: str,
        step_name: str,
        cause: BaseException,
        on_failure: str,
        input: "GateProbeNotReady_gpnotreaInput",
        output_for_sha_lookup: Optional[dict] = None,
    ) -> tuple[str, dict]:
        """Dispatch on failure policy after ActivityError (retries exhausted).

        P11: Always raised — workflow terminated or paused then raised.
        P12: Returns an (action, params) tuple after failure-pause so the
             caller can route (retry/skip/accept/compensate). Raises only
             for 'fail', 'compensate' (auto), and legacy resume (no action).

        Returns:
            (action, params) tuple. Action: "retry", "skip",
            "accept_partial_work", or "compensate_and_cancel".
            Params: action-specific data (empty dict if none).

        Raises:
            Original cause for 'fail', 'compensate' (auto), and
            legacy resume (no action payload).
        """
        self._step_errors[step_id] = {
            "type": type(cause).__name__,
            "message": str(cause),
            "timestamp": workflow.now().isoformat(),
        }

        if on_failure == "fail":
            raise cause

        if on_failure == "compensate":
            await self._run_compensations(input, step_execution_id)
            raise cause

        # --- Pause paths (pause / pause_if_committed) ---
        # Both paths pause, then dispatch based on operator's chosen action.

        if on_failure == "pause":
            await self._pause_on_failure(
                input, step_id, step_name, cause,
                reason_key="activity_failure",
                step_exec_id=step_execution_id,
            )
            return self._consume_resume_action(input, step_execution_id, cause)

        if on_failure == "pause_if_committed":
            if output_for_sha_lookup is not None:
                # Structured failure path — output is in workflow state
                _pre = output_for_sha_lookup.get("pre_commit_sha")
                _post = output_for_sha_lookup.get("post_commit_sha")
            else:
                # ActivityError path — fetch from DB
                _snapshot = await workflow.execute_activity(
                    fetch_step_output_snapshot,
                    FetchStepOutputSnapshotInput(
                        tenant_code=input.tenant_code,
                        step_execution_id=step_execution_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                _pre = (_snapshot or {}).get("pre_commit_sha")
                _post = (_snapshot or {}).get("post_commit_sha")

            if _pre is None or _post is None:
                # P10.7 defensive fallthrough: missing SHAs -> pause
                await self._pause_on_failure(
                    input, step_id, step_name, cause,
                    reason_key="missing_sha_capture",
                    step_exec_id=step_execution_id,
                )
                return self._consume_resume_action(
                    input, step_execution_id, cause,
                )
            elif _pre != _post:
                # Committed work exists — pause for human review
                await self._pause_on_failure(
                    input, step_id, step_name, cause,
                    reason_key="committed_work_detected",
                    step_exec_id=step_execution_id,
                )
                return self._consume_resume_action(
                    input, step_execution_id, cause,
                )
            else:
                # No commits — safe to auto-compensate
                await self._run_compensations(input, step_execution_id)
                raise cause

        # Unknown policy — defensive fallthrough
        raise cause

    def _consume_resume_action(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        step_execution_id: str,
        cause: BaseException,
    ) -> tuple[str, dict]:
        """Read and clear the resume action set by the signal handler.

        Returns:
            (action, params) tuple. Action is the operator's chosen
            recovery action; params is action-specific data (may be
            empty dict).

        Raises:
            Original cause if no action was set (legacy resume signal).
        """
        action = self._resume_action
        params = self._resume_action_params
        self._resume_action = None
        self._resume_action_params = {}

        # P13: clear pause metadata on the DB row. We do this in a
        # fire-and-forget pattern (best-effort) — a failure to clear
        # the column shouldn't block the chosen recovery action from
        # proceeding. The retry / compensation paths will overwrite
        # the status to "running" via their own dispatch.
        self._pending_pause_description = None

        if action is None:
            # Legacy resume signal (no payload) — P11 behavior.
            # Workflow terminates with the original exception.
            raise cause

        return action, params

    def _build_artifact_context(self) -> dict[str, Any]:
        """Build name → value dict from artifacts for template rendering.

        Templates (human_description_template) reference artifacts by
        their ``activity_field_key`` (e.g., ``{branch_name}``), not by
        lineage ID. This helper resolves each lineage ID to its field
        name via _ARTIFACT_FIELD_KEYS and produces a dict suitable for
        ``str.format(**ctx)``.

        Artifacts without a known field_key are skipped (they're not
        referenceable by templates anyway).
        """
        ctx: dict[str, Any] = {}
        for lineage_id, artifact in self._artifacts.items():
            if artifact is None:
                continue
            field_name = _ARTIFACT_FIELD_KEYS.get(lineage_id)
            if field_name:
                ctx[field_name] = Artifact.unwrap(artifact)
        return ctx

    def _prepare_retry_state(
        self,
        step_id: str,
        is_agent_step: bool,
    ) -> str:
        """Prepare state for a retry attempt — shared by retry_step
        and compensate_then_retry.

        Increments the iteration counter (which drives
        platform_context.attempt_number) and, for agent steps, appends
        the failed attempt to attempt_histories so the retried agent
        sees what happened before. Returns a fresh step_execution_id.
        """
        # Bump iteration counter. The per-step while-True retry loop
        # bypasses run()'s outer-loop counter increment, so we do it here.
        self._iteration_counts[step_id] = (
            self._iteration_counts.get(step_id, 0) + 1
        )

        if is_agent_step:
            # _record_attempt fires only on success; for failures, the
            # retry path must record explicitly so the next agent
            # invocation sees what happened on the previous attempt.
            self._attempt_histories.setdefault(step_id, []).append({
                "attempt": self._iteration_counts[step_id] - 1,
                "outcome": "failure",
                "error": self._step_errors.get(step_id, {}),
            })

        new_step_execution_id = str(workflow.uuid4())
        self._step_execution_ids[step_id] = new_step_execution_id
        return new_step_execution_id

    async def _run_inflight_compensation(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        step_id: str,
        step_execution_id: str,
        step_name: str,
        compensation_activity_key: str,
    ) -> tuple[bool, Optional[str]]:
        """Run inflight compensation for the failed step (P13).

        Distinct from _run_compensations — that walks the stack of
        SUCCESSFUL predecessor steps. The failed step is never on the
        stack (push happens after success). This helper compensates
        the failed step's partial work using the same
        compensation_activity_key, with input data drawn from the
        workflow's artifact dict.

        The compensation activity must support partial-state cleanup
        (catalog flag ``supports_inflight_cleanup``) — the API
        pre-flight validation rejects compensate_then_retry for any
        step whose compensation activity doesn't.

        For Phase C+D scope the full artifacts dict is passed to every
        inflight compensation. Future contributors can scope payloads
        per-activity if performance or sensitive-data concerns emerge.

        Returns:
            (success, error_message) — on failure, the caller re-pauses
            with error_message surfaced in the pause reason.
        """
        comp_step_exec_id = str(workflow.uuid4())

        # Compose the inflight payload. The catalog dispatcher's
        # P13 routing branch recognizes ``_inflight: True`` and
        # extracts declared input field_keys from the artifacts dict.
        # Sub-project A (hi_platform_v6): also threads explicit intent
        # flags for op_revert_branch_inflight. revert_target_sha is the
        # cached pre-step SHA (None if capture failed — the activity
        # raises in that case, loud failure per spec Section 6.1);
        # allow_wipe_to_base=False forbids the saga-only path.
        inflight_payload = {
            "_inflight": True,
            "failed_step_id": step_id,
            "failed_step_execution_id": step_execution_id,
            "artifacts": self._build_artifact_context(),
            "revert_target_sha": (
                self._pre_step_sha.get(step_execution_id)
                if workflow.patched("hi_platform_v6")
                else None
            ),
            "allow_wipe_to_base": False,
        }

        # 1. Record compensation step start
        try:
            await workflow.execute_activity(
                mark_step_started_activity,
                MarkStepStartedInput(
                    tenant_code=input.tenant_code,
                    step_execution_id=comp_step_exec_id,
                    instance_id=input.instance_id,
                    step_lineage_id=f"compensation/{compensation_activity_key}",
                    executor_type="SYSTEM",
                    input_snapshot=inflight_payload,
                    execution_mode="compensation",
                    compensates_execution_id=None,
                    triggered_by_failure_id=None,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception as mark_err:
            workflow.logger.warning(
                f"Failed to record inflight compensation step start "
                f"for {compensation_activity_key}: {mark_err}",
            )

        # 2. Run the compensation activity
        comp_error_message: Optional[str] = None
        result_status = "compensated"
        try:
            await workflow.execute_activity(
                EXECUTE_CATALOG_ACTIVITY_NAME,
                CatalogActivityInput(
                    tenant_slug=input.tenant_code,
                    instance_id=input.instance_id,
                    step_execution_id=comp_step_exec_id,
                    config={},
                    payload=inflight_payload,
                    catalog_activity_key=compensation_activity_key,
                ),
                result_type=CatalogActivityResult,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except ActivityError as comp_err:
            _cause = comp_err.cause if hasattr(comp_err, 'cause') else comp_err
            comp_error_message = (
                f"{type(_cause).__name__}: {str(_cause)[:300]}"
            )
            workflow.logger.error(
                f"Inflight compensation {compensation_activity_key} "
                f"failed for step '{step_name}': {comp_error_message}",
            )
            result_status = "failed"

        # 3. Mark the compensation step itself complete (or failed)
        try:
            await workflow.execute_activity(
                mark_step_complete_activity,
                MarkStepCompleteInput(
                    tenant_code=input.tenant_code,
                    step_execution_id=comp_step_exec_id,
                    outcome="success" if result_status == "compensated" else "failure",
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception:
            pass

        return (result_status == "compensated", comp_error_message)

    async def _cleanup_workspace_for_step(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        workspace_artifact_lineage_id: str,
    ) -> None:
        """Reset the worktree to clean state for the retry path (P13).

        Called by compensate_then_retry after the inflight compensation
        succeeds, gated on _STEP_REQUIRES_WORKSPACE.

        Sub-project A follow-up (2026-06-05): uses ``reset_workspace``
        (git reset --hard + git clean -fd) instead of ``cleanup_workspace``
        (which destroys the directory). The retry path needs the worktree
        to still exist at the same path so the unchanged
        ``working_directory`` artifact remains valid — destroying the
        directory caused the retried agent activity to fail with
        "Working directory does not exist".

        Non-fatal on failure — the canonical case (branch revert) is what
        actually matters; the operator can manually clean the worktree if
        this fails.
        """
        _wd_artifact = self._artifacts.get(workspace_artifact_lineage_id)
        if _wd_artifact is None:
            return
        _wd = Artifact.unwrap(_wd_artifact)
        if not _wd:
            return
        try:
            _cleanup_exec_id = str(workflow.uuid4())
            await workflow.execute_activity(
                EXECUTE_CATALOG_ACTIVITY_NAME,
                CatalogActivityInput(
                    tenant_slug=input.tenant_code,
                    instance_id=input.instance_id,
                    step_execution_id=_cleanup_exec_id,
                    config={"resolver": "reset_workspace"},
                    payload={"working_directory": _wd},
                    catalog_activity_key="reset_workspace",
                ),
                result_type=CatalogActivityResult,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception as _ws_err:
            workflow.logger.warning(
                f"Workspace reset failed (non-fatal): {_ws_err}",
            )

    async def _dispatch_pause_action(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        step_id: str,
        step_name: str,
        step_execution_id: str,
        cause: BaseException,
        reason_key: str = "activity_failure",
        reason_args: Optional[dict] = None,
    ) -> tuple[str, dict]:
        """Pause then consume — used by the inner dispatch loop when
        compensation itself fails and we need a fresh action choice
        from the operator without re-executing the failing activity.

        Encapsulates the pause→wait→consume cycle so the caller doesn't
        manage signal state directly across loop iterations.
        """
        await self._pause_on_failure(
            input, step_id, step_name, cause,
            reason_key=reason_key,
            reason_args=reason_args,
            step_exec_id=step_execution_id,
        )
        return self._consume_resume_action(input, step_execution_id, cause)

    async def _handle_structured_failure(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        workflow_id: str,
        step_id: str,
        step_exec_id: str,
        step_output: dict,
        effective_policy: str,
    ) -> Optional[str]:
        """Run the recovery action loop for a step that returned
        __exception__ (structured failure variant).

        Mirrors the per-step ActivityError inner dispatch loop (B1.3)
        but tailored to run()'s structured-failure path: re-dispatches
        the step via _dispatch_step (not by rebuilding activity_input
        inline) and returns the final outcome for the outer loop's
        routing. Per Phase B addendum Revision 1, an inner while-True
        captures the operator's next action after a compensation
        failure without re-executing the step.

        Returns the outcome to feed into ``_get_next_step_ids``:
            - new outcome (e.g., None) for retry / compensate_then_retry
              followed by a successful re-dispatch;
            - None for skip / accept_partial_work;
            - __exception__ propagates (terminal: legacy / unknown).

        Raises ``_CompensateAndCancelSentinel`` on compensate_and_cancel.
        """
        step_name = _STEP_NAMES.get(step_id, step_id)
        cause = RuntimeError(
            self._step_errors.get(step_id, {}).get("message", "Step failure"),
        )
        _action, _params = await self._handle_activity_failure(
            step_id=step_id,
            step_execution_id=step_exec_id,
            step_name=step_name,
            cause=cause,
            on_failure=effective_policy,
            input=input,
            output_for_sha_lookup=step_output,
        )

        while True:
            # API → workflow token dispatch. Backend's resume-with-action
            # route now translates API tokens (retry_step / skip_step) to
            # the workflow tokens (retry / skip) before signaling, so
            # generated workflows only need to handle the short forms.
            # We accept both for belt-and-suspenders — if a caller ever
            # bypasses the backend translation (direct signal in a test,
            # manual Temporal CLI, etc.), the workflow still works.
            if _action in ("retry", "retry_step"):
                self._prepare_retry_state(
                    step_id=step_id,
                    is_agent_step=step_id in _STEP_IS_AGENT,
                )
                return await self._dispatch_step(step_id, input, workflow_id)

            if _action == "compensate_then_retry":
                _comp_key = _STEP_COMPENSATION_KEYS.get(step_id, "")
                _comp_ok, _comp_err = await self._run_inflight_compensation(
                    input=input,
                    step_id=step_id,
                    step_execution_id=step_exec_id,
                    step_name=step_name,
                    compensation_activity_key=_comp_key,
                )

                if not _comp_ok:
                    # Re-pause; loop with the operator's new action choice.
                    _action, _params = await self._dispatch_pause_action(
                        input=input,
                        step_id=step_id,
                        step_name=step_name,
                        step_execution_id=step_exec_id,
                        cause=cause,
                        reason_key="compensation_failed",
                        reason_args={"compensation_error": str(_comp_err)[:500]},
                    )
                    continue

                # Workspace cleanup (runtime check; structured-failure
                # path doesn't know step shape statically).
                if step_id in _STEP_REQUIRES_WORKSPACE:
                    ws_artifact = _STEP_WORKSPACE_ARTIFACT.get(step_id, "")
                    if ws_artifact:
                        await self._cleanup_workspace_for_step(input, ws_artifact)

                self._paused_failure_step_execution_id = None
                self._paused_failure_step_id = None
                self._prepare_retry_state(
                    step_id=step_id,
                    is_agent_step=step_id in _STEP_IS_AGENT,
                )
                return await self._dispatch_step(step_id, input, workflow_id)

            if _action in ("skip", "skip_step"):
                await workflow.execute_activity(
                    mark_step_skipped,
                    MarkStepSkippedInput(
                        tenant_code=input.tenant_code,
                        step_execution_id=step_exec_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                return None

            if _action == "accept_partial_work":
                # Structured-failure path: artifact already stored from
                # the prior dispatch; just advance routing with outcome=None.
                return None

            if _action == "compensate_and_cancel":
                await self._run_compensations(input, step_exec_id)
                self._compensation_stack = []
                raise _CompensateAndCancelSentinel()

            # Unknown / legacy terminate.
            raise cause

    async def _run_compensations(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        failed_step_execution_id: str,
    ) -> None:
        """Walk the compensation stack in reverse, best-effort.

        Each compensation activity runs in its own try/except. Failures
        are logged and recorded (compensation_status='failed') but do
        not halt the walk — remaining compensations continue.
        """
        if not self._compensation_stack:
            workflow.logger.info("Compensation stack empty — nothing to undo")
            return

        workflow.logger.info(
            f"Running compensations: {len(self._compensation_stack)} entries, "
            f"reverse order",
        )

        for entry in reversed(self._compensation_stack):
            comp_step_exec_id = str(workflow.uuid4())

            try:
                # 1. Record compensation step start
                try:
                    await workflow.execute_activity(
                        mark_step_started_activity,
                        MarkStepStartedInput(
                            tenant_code=input.tenant_code,
                            step_execution_id=comp_step_exec_id,
                            instance_id=input.instance_id,
                            step_lineage_id=f"compensation/{entry.compensation_activity_key}",
                            executor_type="SYSTEM",
                            input_snapshot=entry.step_output,
                            execution_mode="compensation",
                            compensates_execution_id=entry.forward_step_execution_id,
                            triggered_by_failure_id=failed_step_execution_id,
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except Exception as mark_err:
                    workflow.logger.warning(
                        f"Failed to record compensation step start for "
                        f"{entry.compensation_activity_key}: {mark_err}",
                    )

                # 2. Run the compensation activity
                # Sub-project A (hi_platform_v6): for op_revert_branch_inflight
                # the saga walker MUST explicitly consent to the wipe path
                # (revert_target_sha=None, allow_wipe_to_base=True). Other
                # compensation activities don't declare these fields and the
                # catalog dispatcher silently drops them — safe no-op.
                # Copy so we can overlay Sub-project A intent flags below
                # without mutating the saga entry's recorded payload.
                _comp_payload = dict(entry.step_output)
                if (
                    workflow.patched("hi_platform_v6")
                    and entry.compensation_activity_key == "op_revert_branch_inflight"
                ):
                    _comp_payload["_inflight"] = True
                    _comp_payload["revert_target_sha"] = None
                    _comp_payload["allow_wipe_to_base"] = True
                result_status = "compensated"
                try:
                    await workflow.execute_activity(
                        EXECUTE_CATALOG_ACTIVITY_NAME,
                        CatalogActivityInput(
                            tenant_slug=input.tenant_code,
                            instance_id=input.instance_id,
                            step_execution_id=comp_step_exec_id,
                            config={},
                            payload=_comp_payload,
                            catalog_activity_key=entry.compensation_activity_key,
                        ),
                        result_type=CatalogActivityResult,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except ActivityError as comp_err:
                    workflow.logger.error(
                        f"Compensation activity {entry.compensation_activity_key} "
                        f"failed for step {entry.step_name}: {comp_err}. "
                        f"Continuing best-effort.",
                    )
                    result_status = "failed"

                # 3. Record outcome (link forward row to compensation row)
                try:
                    await workflow.execute_activity(
                        mark_step_compensated,
                        MarkStepCompensatedInput(
                            tenant_code=input.tenant_code,
                            forward_step_execution_id=entry.forward_step_execution_id,
                            compensation_step_execution_id=comp_step_exec_id,
                            result_status=result_status,
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except Exception as mark_err:
                    workflow.logger.warning(
                        f"Failed to record compensation outcome for "
                        f"{entry.compensation_activity_key}: {mark_err}",
                    )
            finally:
                # Sub-project B Tier 1.5: defensive cleanup — ensure the
                # compensation step row reaches a terminal status even if
                # the compensation activity raised, timed out, or returned
                # without calling p.steps.complete(). Idempotent (the
                # WHERE status='running' predicate makes this a no-op if
                # the row was already transitioned by the activity itself
                # or by mark_step_compensated).
                # Inner try/except prevents the cleanup itself from
                # raising and breaking the saga walk loop.
                try:
                    await workflow.execute_activity(
                        terminalize_compensation_step_activity,
                        TerminalizeCompensationStepInput(
                            tenant_code=input.tenant_code,
                            comp_step_execution_id=comp_step_exec_id,
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except Exception as _terminalize_err:
                    workflow.logger.warning(
                        f"Failed to terminalize compensation step "
                        f"{comp_step_exec_id}: {_terminalize_err}",
                    )

        workflow.logger.info("Compensation walk complete")

    async def _pause_on_failure(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        step_id: str,
        step_name: str,
        cause: BaseException,
        reason_key: str,
        reason_args: Optional[dict] = None,
        step_exec_id: Optional[str] = None,
    ) -> None:
        """Pause workflow for operator review after step failure.

        Updates ProcessInstance.status to 'paused_on_failure' and waits
        for a resume signal. The rendered PAUSE_REASONS[reason_key]
        string is persisted as error_context for the frontend banner.

        Approach B per
        docs/superpowers/specs/2026-06-02-compensation-wire-up-design.md
        Section 2.3.
        """
        # Render the operator-facing pause reason from PAUSE_REASONS.
        # step_name is always provided; reason_args carries extras
        # like compensation_error for compensation_failed.
        args = {"step_name": step_name, **(reason_args or {})}
        template = PAUSE_REASONS.get(reason_key)
        if template:
            try:
                rendered_reason = template.format(**args)
            except (KeyError, IndexError, ValueError) as _tmpl_err:
                workflow.logger.warning(
                    f"Failed to render PAUSE_REASONS[{reason_key!r}]: "
                    f"{_tmpl_err}. Falling back to generic copy.",
                )
                rendered_reason = (
                    f"Step '{step_name}' is paused. "
                    "Choose a recovery action."
                )
        else:
            workflow.logger.warning(
                f"Unknown reason_key {reason_key!r}; using generic copy.",
            )
            rendered_reason = (
                f"Step '{step_name}' is paused. "
                "Choose a recovery action."
            )

        # P13: render the operator-facing cleanup description for
        # compensate_then_retry. Looks up the failing step's
        # compensation activity's human_description_template and
        # interpolates with field-keyed artifact context. Defends
        # against missing fields / bad templates with a generic
        # fallback so a template typo can never block the pause itself.
        self._pending_pause_description = None
        _comp_key = _STEP_COMPENSATION_KEYS.get(step_id)
        _comp_template = (
            _COMPENSATION_DESCRIPTION_TEMPLATES.get(_comp_key)
            if _comp_key else None
        )
        if _comp_template:
            try:
                _artifact_ctx = self._build_artifact_context()
                self._pending_pause_description = _comp_template.format(
                    **_artifact_ctx,
                )
            except (KeyError, IndexError, ValueError) as _tmpl_err:
                workflow.logger.warning(
                    f"Failed to render cleanup description template "
                    f"for {_comp_key!r}: {_tmpl_err}. Using generic copy.",
                )
                self._pending_pause_description = (
                    "undo the partial work from this attempt"
                )

        await workflow.execute_activity(
            update_instance_status_activity,
            UpdateInstanceStatusInput(
                tenant_code=input.tenant_code,
                instance_id=input.instance_id,
                status="paused_on_failure",
                error_context=rendered_reason,
                # P13: persist pause metadata so the API pre-flight
                # validation (compensate_then_retry) and the UI can
                # serve them without a Temporal query.
                paused_failure_step_id=step_id,
                pause_description=self._pending_pause_description,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=10),
        )

        # Create the diagnostic chat task. Fire-and-forget — the workflow
        # does NOT wait on this; it continues to wait on the resume signal
        # below. The Task Inbox surfaces it so the operator can chat with
        # an agent about what failed. ProcessManagementService.resume_with_action
        # cancels the task when the operator picks a recovery action via
        # the pause banner.
        _pause_chat_task_instance_id = (
            f"{workflow.info().workflow_id}/__pause_chat__/{step_id}"
        )

        # Resolve the failing step's worktree path so the diagnose-pause
        # chat agent's op_git resolves cwd automatically and its Bash
        # isn't sandboxed to /tmp. None for non-workspace steps.
        _pause_chat_wd = None
        _pause_chat_ws_artifact_id = _STEP_WORKSPACE_ARTIFACT.get(step_id)
        if _pause_chat_ws_artifact_id:
            _wd_artifact = self._artifacts.get(_pause_chat_ws_artifact_id)
            if _wd_artifact is not None:
                _pause_chat_wd = Artifact.unwrap(_wd_artifact) or None

        try:
            await workflow.execute_activity(
                "create_pause_chat_task",
                CreatePauseChatTaskInput(
                    tenant_slug=input.tenant_code,
                    task_instance_id=_pause_chat_task_instance_id,
                    instance_id=str(input.instance_id),
                    process_def_id=str(input.process_def_id),
                    step_id=step_id,
                    step_name=step_name,
                    workflow_id=workflow.info().workflow_id,
                    rendered_pause_reason=rendered_reason,
                    pause_description=self._pending_pause_description,
                    working_directory=_pause_chat_wd,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception as _chat_task_err:
            workflow.logger.warning(
                f"Failed to create pause chat task: {_chat_task_err}. "
                "Pause continues; operator can still resume via banner.",
            )

        self._is_paused = True
        self._paused_on_failure = True
        self._paused_failure_step_id = step_id
        self._paused_failure_step_execution_id = step_exec_id

        workflow.logger.info(
            f"Workflow paused on failure: {step_name}. "
            "Awaiting operator resume signal.",
        )

        await workflow.wait_condition(lambda: not self._is_paused)

        # After resume — P12 determines next action.
        # P11: caller raises the original exception (workflow terminates).
        self._paused_on_failure = False
        self._paused_failure_step_id = None
        self._pending_pause_description = None


    async def _watch_sla(
        self,
        task_instance_id: str,
        sla_minutes: int,
        input: "GateProbeNotReady_gpnotreaInput",
        sla_policy: dict | None = None,
        step_id: str = "",
    ) -> None:
        """Fire SLA breach audit event and escalation on SLA expiry."""
        try:
            await asyncio.sleep(sla_minutes * 60)
            if self._human_task_states.get(task_instance_id) == "pending":
                step_display_name = _STEP_NAMES.get(step_id, task_instance_id)
                # Audit trail — permanent record of breach
                await workflow.execute_activity(
                    emit_sla_breach_activity,
                    EmitSLABreachInput(
                        tenant_code=input.tenant_code,
                        task_instance_id=task_instance_id,
                        instance_id=input.instance_id,
                        step_name=step_display_name,
                        sla_minutes=sla_minutes,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                # Escalation actions (notify, create_escalation_task, reassign).
                # handle_sla_breach expects the mp_human_tasks.id UUID, not
                # the workflow-internal task_instance_id ("{wf_id}/{step_id}").
                # If we did not record the UUID (creation failed or older
                # workflow), skip escalation rather than crash on uuid.UUID().
                _task_uuid = self._human_task_uuids.get(task_instance_id)
                if sla_policy and _task_uuid:
                    await workflow.execute_activity(
                        handle_sla_breach,
                        SLABreachInput(
                            tenant_code=input.tenant_code,
                            task_id=_task_uuid,
                            instance_id=input.instance_id,
                            step_name=step_display_name,
                            sla_policy=sla_policy,
                        ),
                        start_to_close_timeout=timedelta(minutes=2),
                    )
                elif sla_policy and not _task_uuid:
                    workflow.logger.warning(
                        "SLA breach: missing task UUID for %s; "
                        "skipping handle_sla_breach escalation",
                        task_instance_id,
                    )
        except asyncio.CancelledError:
            pass  # Task completed before SLA; normal
    @workflow.signal(name="hi_human_task_complete")
    async def on_task_complete(self, payload: dict) -> None:
        """Handle human task completion signal."""
        tid = payload.get("task_instance_id", "")
        self._human_task_completions[tid] = payload
        self._human_task_states[tid] = "complete"
        workflow.logger.info(f"Human task complete: {tid}")

    @workflow.signal(name="hi_human_task_cancel")
    async def on_task_cancel(self, payload: dict) -> None:
        """Handle human task cancellation signal."""
        tid = payload.get("task_instance_id", "")
        self._human_task_cancellations[tid] = payload.get("reason", "")
        self._human_task_states[tid] = "cancelled"
        workflow.logger.info(f"Human task cancelled: {tid}")

    @workflow.signal(name="hi_human_task_reassign")
    async def on_task_reassign(self, payload: dict) -> None:
        """Handle human task reassignment signal (audit trail)."""
        tid = payload.get("task_instance_id", "")
        self._human_task_reassignments[tid] = payload.get("assignee_id", "")
        workflow.logger.info(f"Human task reassigned: {tid}")

    @workflow.query(name="hi_pending_human_tasks")
    def get_pending_human_tasks(self) -> list[dict]:
        """Return list of currently pending human tasks."""
        return [
            {"task_instance_id": tid, "state": state}
            for tid, state in self._human_task_states.items()
            if state == "pending"
        ]

    async def _execute_human_task(
        self,
        task_instance_id: str,
        step_id: str,
        config: dict,
        input: "GateProbeNotReady_gpnotreaInput",
    ) -> dict:
        """Execute a human task: create record, wait for signal, handle timeout/cancel.

        Args:
            task_instance_id: Unique ID for this task instance (from build_task_instance_id)
            step_id: Step lineage ID
            config: Task configuration (sla_minutes, hard_timeout_minutes, task_type, etc.)
            input: Workflow input for tenant context

        Returns:
            The completion result dict from the signal payload.

        Raises:
            HumanTaskTimeoutError: If hard timeout expires.
            HumanTaskCancelledError: If task is cancelled via signal.
        """
        # Clear any stale signal state from a prior iteration of this same
        # human step. task_instance_id is deterministic (workflow_id + step_id),
        # so when a HUMAN step lives inside a LOOP — or any control flow that
        # re-enters the same step — completions / cancellations from the
        # previous iteration must be cleared. Otherwise wait_condition below
        # fires immediately on the stale entry and the workflow short-circuits
        # without ever waiting for the new iteration's human signal.
        self._human_task_completions.pop(task_instance_id, None)
        self._human_task_cancellations.pop(task_instance_id, None)
        self._human_task_states[task_instance_id] = "pending"

        step_execution_id = str(workflow.uuid4())

        # Evaluate required_when and assignee_role_when dynamically (Phase 1 Change Governance)
        _eval_data = {}
        for _art_id, _art_val in self._artifacts.items():
            _eval_data[_art_id] = Artifact.unwrap(_art_val)
        _eval_data.update(self._named_routing_outputs)

        _form_fields = config.get("form_fields")
        _required_field_decisions = []
        if _form_fields and isinstance(_form_fields, list):
            for _field in _form_fields:
                _req_when = _field.get("required_when")
                if _req_when:
                    _ref_vars = _get_referenced_vars(_req_when)
                    # Missing-variable semantics: if any referenced variable is missing, skip override (falls back to static)
                    if _ref_vars and not all(_var_name in _eval_data for _var_name in _ref_vars):
                        _required_field_decisions.append({
                            "field_key": _field.get("field_key"),
                            "expression": _req_when,
                            "result": "skipped (missing variable)",
                            "static_fallback": _field.get("is_required"),
                        })
                        continue

                    try:
                        _eval_res = bool(jsonLogic(_req_when, _eval_data))
                        _field["is_required"] = _eval_res
                        _required_field_decisions.append({
                            "field_key": _field.get("field_key"),
                            "expression": _req_when,
                            "result": _eval_res,
                        })
                    except Exception as _e:
                        workflow.logger.warning(
                            f"Failed to evaluate required_when for field {_field.get('field_key')}: {_e}. Using static default."
                        )
                        _required_field_decisions.append({
                            "field_key": _field.get("field_key"),
                            "expression": _req_when,
                            "result": f"error: {_e}",
                            "static_fallback": _field.get("is_required"),
                        })

        _assignee_role_when = config.get("assignee_role_when")
        _assignee_role_decisions = []
        if _assignee_role_when and isinstance(_assignee_role_when, list):
            _matched_role = None
            for _idx, _clause in enumerate(_assignee_role_when):
                _when = _clause.get("when")
                _role = _clause.get("role")
                _ref_vars = _get_referenced_vars(_when)
                
                # Missing-variable semantics: clause evaluates to false (skips)
                if _ref_vars and not all(_var_name in _eval_data for _var_name in _ref_vars):
                    _assignee_role_decisions.append({
                        "clause_index": _idx,
                        "expression": _when,
                        "role": _role,
                        "result": "skipped (missing variable)",
                    })
                    continue

                try:
                    _eval_res = bool(jsonLogic(_when, _eval_data))
                    _assignee_role_decisions.append({
                        "clause_index": _idx,
                        "expression": _when,
                        "role": _role,
                        "result": _eval_res,
                    })
                    if _eval_res:
                        _matched_role = _role
                        break
                except Exception as _e:
                    workflow.logger.warning(
                        f"Failed to evaluate assignee_role_when clause {_idx}: {_e}."
                    )
                    _assignee_role_decisions.append({
                        "clause_index": _idx,
                        "expression": _when,
                        "role": _role,
                        "result": f"error: {_e}",
                    })

            if _matched_role is not None:
                config["actor_role"] = _matched_role
            else:
                _assignee_role_decisions.append({
                    "result": "fallback to static default",
                    "role": config.get("actor_role"),
                })

        # Propagate decisions to payload (so they save in context_payload)
        _payload = config.get("payload")
        if _payload is None:
            _payload = {}
            config["payload"] = _payload
        if _required_field_decisions:
            _payload["_required_field_decisions"] = _required_field_decisions
        if _assignee_role_decisions:
            _payload["_assignee_role_decisions"] = _assignee_role_decisions

        # Create the human task record.
        # If a catalog_activity_key is set, route through the catalog dispatcher
        # (handles catalog-specific setup like integration config).
        # Otherwise, use create_human_task_record directly — all the config
        # (form_fields, chat, SLA, skills) is already in the config dict.
        _signal_name = f"hi_human_task_complete"
        _catalog_key = config.get("catalog_activity_key")
        if _catalog_key:
            _catalog_result = await workflow.execute_activity(
                EXECUTE_CATALOG_ACTIVITY_NAME,
                CatalogActivityInput(
                    tenant_slug=input.tenant_code,
                    instance_id=input.instance_id,
                    step_execution_id=step_execution_id,
                    process_def_id=input.process_def_id,
                    config=config.get("activity_config", {}),
                    payload=config.get("payload", {}),
                    catalog_activity_key=_catalog_key,
                    workflow_id=workflow.info().workflow_id,
                    signal_name=_signal_name,
                    step_lineage_id=step_id,
                    actor_role=config.get("actor_role"),
                    chat_prompt_key=config.get("chat_prompt_key"),
                    step_prompt=config.get("step_prompt"),
                    skills=config.get("skills"),
                    chat_agent_activity_key=config.get("chat_agent_activity_key"),
                    chat_allowed_platform_actions=config.get("chat_allowed_platform_actions"),
                    chat_repo_key=config.get("chat_repo_key"),
                    sla_policy=config.get("sla_policy"),
                    task_instance_id=task_instance_id,
                ),
                result_type=CatalogActivityResult,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            # The catalog dispatcher (HUMAN execution path) returns
            # {"task_id": "..."} inside CatalogActivityResult.output. Store
            # it so _watch_sla can pass the real UUID to handle_sla_breach.
            _catalog_output = (_catalog_result.output if _catalog_result else None) or {}
            _catalog_task_id = _catalog_output.get("task_id") or (
                _catalog_output.get("data", {}).get("task_id")
                if isinstance(_catalog_output.get("data"), dict)
                else None
            )
            if _catalog_task_id:
                self._human_task_uuids[task_instance_id] = str(_catalog_task_id)
        else:
            # Inject workflow_id so the activity can build temporal_signal_id
            config["workflow_id"] = workflow.info().workflow_id
            _create_result = await workflow.execute_activity(
                create_human_task_record,
                CreateHumanTaskInput(
                    tenant_code=input.tenant_code,
                    task_instance_id=task_instance_id,
                    instance_id=input.instance_id,
                    process_def_id=input.process_def_id,
                    step_id=step_id,
                    step_execution_id=step_execution_id,
                    config=config,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            if _create_result and _create_result.task_id:
                self._human_task_uuids[task_instance_id] = str(_create_result.task_id)

        # SLA watcher (runs concurrently, doesn't block completion)
        sla_minutes = config.get("sla_minutes")
        sla_policy = config.get("sla_policy")
        sla_task = None
        if sla_minutes:
            sla_task = asyncio.create_task(
                self._watch_sla(
                    task_instance_id,
                    sla_minutes,
                    input,
                    sla_policy,
                    step_id,
                )
            )

        hard_timeout = config.get("hard_timeout_minutes", 43200)  # 30 days default

        try:
            await workflow.wait_condition(
                lambda: (
                    task_instance_id in self._human_task_completions
                    or task_instance_id in self._human_task_cancellations
                ),
                timeout=timedelta(minutes=hard_timeout),
            )
        except asyncio.TimeoutError:
            self._human_task_states[task_instance_id] = "timed_out"
            await workflow.execute_activity(
                mark_human_task_timed_out,
                MarkHumanTaskTimedOutInput(
                    tenant_code=input.tenant_code,
                    task_instance_id=task_instance_id,
                    instance_id=input.instance_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )
            raise HumanTaskTimeoutError(task_instance_id)
        finally:
            if sla_task:
                sla_task.cancel()

        if task_instance_id in self._human_task_cancellations:
            self._human_task_states[task_instance_id] = "cancelled"
            raise HumanTaskCancelledError(
                task_instance_id,
                self._human_task_cancellations[task_instance_id],
            )

        self._human_task_states[task_instance_id] = "complete"

        # Mark step complete
        result = self._human_task_completions[task_instance_id]
        await workflow.execute_activity(
            mark_step_complete_activity,
            MarkStepCompleteInput(
                tenant_code=input.tenant_code,
                step_execution_id=step_execution_id,
                outcome=result.get("outcome", "success"),
                output_snapshot=result,
                completed_by_actor_id=result.get("completed_by"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        return result

    async def _execute_platform_catchall_resolve(
        self,
        input: "GateProbeNotReady_gpnotreaInput",
        workflow_id: str,
    ) -> Optional[str]:
        """Platform catch-all: create a chat human task on structured failure.

        Fires when a non-agent step returned a structured failure
        (success=False or is_stuck=True) AND no designer-wired exception
        connection caught it. Reuses _execute_human_task to create a chat
        task with platform-defined config (form fields: decision /
        diagnosis / instructions / chat_summary; chat agent:
        op_anthropic_resolve_implementation_stuck). After the human
        completes the task, the workflow terminates — operator uses
        restart-from-step to retry the failing step.

        PLATFORM CATCH-ALL v1 — supersede with compensation epic. The
        structured-failure detection (in each non-agent step's dispatch
        body) is keep-forever; the routing-to-human default here is the
        disposable piece compensation will replace.
        """
        failing_step_id = self._catchall_origin_step_id or "unknown"
        failing_step_name = _STEP_NAMES.get(failing_step_id, failing_step_id)
        error_info = self._step_errors.get(failing_step_id, {})

        # Deterministic task_instance_id so restart-from-step recreates
        # cleanly without duplicates. Includes failing_step_id so each
        # distinct failure within a workflow gets its own catchall task.
        task_instance_id = f"{workflow_id}/__platform_catchall__/{failing_step_id}"

        workflow.logger.warning(
            f"Platform catch-all firing for '{failing_step_name}' "
            f"({failing_step_id}). Creating chat human task. "
            f"Error: {error_info.get('type', 'StructuredFailure')}: "
            f"{error_info.get('message', '(no message)')[:200]}"
        )

        config = {
            "sla_minutes": 60,
            "hard_timeout_minutes": 43200,  # 30 days
            "task_type": "chat",
            "assignment_mode": None,
            "actor_role": "Software Engineer",
            "title": f"Resolve failure: {failing_step_name}",
            "description": (
                f"Step '{failing_step_name}' failed with no designer-wired "
                f"error path. Platform catch-all created this task for "
                f"manual resolution. Error type: "
                f"{error_info.get('type', 'StructuredFailure')}. "
                f"Error message: "
                f"{error_info.get('message', '(no message)')[:500]}"
            ),
            "outcomes": ["complete"],
            "form_fields": [
                {"field_key": "decision", "display_name": "Decision", "data_type": "string", "is_required": True, "description": "What should happen next? (retry / fail / other)", "sort_order": 0},
                {"field_key": "diagnosis", "display_name": "Diagnosis", "data_type": "string", "is_required": True, "description": "What was the root cause?", "sort_order": 1},
                {"field_key": "instructions", "display_name": "Instructions", "data_type": "string", "is_required": True, "description": "What needs to change before retrying?", "sort_order": 2},
                {"field_key": "chat_summary", "display_name": "Chat Summary", "data_type": "string", "is_required": True, "description": "", "sort_order": 3},
            ],
            "catalog_activity_key": None,
            "chat_prompt_key": None,
            "step_prompt": None,
            "skills": None,
            # Reuse the implementation-stuck agent as the chat partner.
            # It already knows how to investigate a failed code-gen scenario;
            # works for non-code-gen failures too (it has Read/Grep/Bash).
            "chat_agent_activity_key": "op_anthropic_resolve_implementation_stuck",
            "chat_allowed_platform_actions": None,
            "sla_policy": None,
            "activity_config": {},
            "payload": {
                "subject": f"Resolve failure: {failing_step_name}",
                "failing_step_name": failing_step_name,
                "failing_step_id": failing_step_id,
                "stuck_category": error_info.get("type", "StructuredFailure"),
                "stuck_reason": error_info.get("message", "(no message)"),
                "routing_metadata": self._last_routing_context,
            },
        }

        try:
            result = await self._execute_human_task(
                task_instance_id=task_instance_id,
                step_id=f"__platform_catchall__/{failing_step_id}",
                config=config,
                input=input,
            )
        except (HumanTaskCancelledError, HumanTaskTimeoutError) as e:
            workflow.logger.warning(
                f"Platform catch-all task ended without resolution: {type(e).__name__}. "
                "Workflow will terminate. Operator can use restart-from-step "
                "to retry."
            )
            self._catchall_origin_step_id = None
            return None  # terminate

        # Capture human resolution into _pending_human_resolution so any
        # subsequent agent step (e.g. on restart-from-step) sees the
        # diagnosis/instructions in its platform_context.
        _edited = result.get("edited_content") or {}
        if isinstance(_edited, dict):
            _hr = {
                "decision": _edited.get("decision"),
                "diagnosis": _edited.get("diagnosis"),
                "instructions": _edited.get("instructions"),
                "chat_summary": _edited.get("chat_summary"),
            }
            self._pending_human_resolution = {k: v for k, v in _hr.items() if v} or None

        workflow.logger.info(
            f"Platform catch-all task completed. Workflow terminating; "
            f"operator can restart-from-step to retry '{failing_step_name}'."
        )

        # Clear catchall state for any future failures in the same workflow.
        self._catchall_origin_step_id = None

        # Return None to indicate no next step — workflow terminates after
        # the catch-all. Operator uses restart-from-step to retry.
        return None

    # -------------------------------------------------------------------------
    # STEP EXECUTION HELPERS
    # -------------------------------------------------------------------------

    async def _execute_step_resolve(
        self,
        input: GateProbeNotReady_gpnotreaInput,
        workflow_id: str,
    ) -> Optional[str]:
        """
        Execute step: Resolve Read Workspace (SYSTEM)
        Entry Point: Yes

        Returns:
            None for non-routing steps.
        """
        step_execution_id = str(workflow.uuid4())
        self._step_execution_ids["step-resolve"] = step_execution_id

        workflow.logger.info(
            f"Executing step: Resolve Read Workspace",
            extra={
                "step_lineage_id": STEP_RESOLVE_READ_WORKSPACE,
                "step_execution_id": step_execution_id,
                "instance_id": input.instance_id,
            },
        )

        # Check for pause before step
        await self._check_paused(input)

        # =========================================================
        # SYSTEM/AI STEP: Execute activity and store outputs
        # =========================================================
        # Build expression context: self._artifacts + aliased input names
        # Expressions reference sibling inputs by alias (e.g., _expr_92b6b1ad_pr_title)
        # which exist in the activity payload but not in self._artifacts (keyed by UUID).
        _expr_ctx = dict(self._artifacts)
        # Applied AFTER the input aliases above, not before: the alias loop
        # writes activity_field_key values into _expr_ctx, so a step input
        # whose field key is literally `process` would otherwise clobber the
        # whole system namespace and make process.* resolve to that input.
        _expr_ctx.update(_system_context(input.instance_id))

        activity_input = ResolveReadWorkspaceStepresoInput(
            instance_id=input.instance_id,
            step_execution_id=step_execution_id,
            step_lineage_id=STEP_RESOLVE_READ_WORKSPACE,
            tenant_code=input.tenant_code,
            repo_key=Artifact.wrap(str(workflow.uuid4()), "hello-world"),
            branch_base=Artifact.wrap(str(workflow.uuid4()), "master"),
            resolved_connections=input.resolved_connections,
        )

        # Compensation-aware retry loop (P12). Loops on retry_step;
        # breaks on success, skip, or accept_partial_work.
        while True:
            try:
                result = await workflow.execute_activity(
                    resolve_read_workspace_stepreso,
                    args=[activity_input],
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=RetryPolicy(
                        maximum_attempts=1,
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=100),
                        backoff_coefficient=2.0,
                        non_retryable_error_types=["EditConflict", "HumanTaskCancelledError", "HumanTaskTimeoutError", "PermissionDeniedError", "ProcessModelError", "ResourceNotFoundError", "RetryUnsafeError", "RouterEvaluationError", "ValidationError"],
                    ),
                )
                break  # Activity succeeded — exit retry loop

            except ActivityError as e:
                _action, _params = await self._handle_activity_failure(
                    step_id="step-resolve",
                    step_execution_id=step_execution_id,
                    step_name="Resolve Read Workspace",
                    cause=e.cause if hasattr(e, 'cause') else e,
                    on_failure="pause",
                    input=input,
                )

                # P13: inner action-dispatch loop. Most branches break
                # out (retry / skip / accept exit and the outer loop
                # either re-executes or moves on). compensate_then_retry's
                # compensation-failure path re-pauses and *continues*
                # this inner loop with the operator's new action choice.
                #
                # API → workflow token dispatch. Backend translates
                # API tokens (retry_step / skip_step) to workflow tokens
                # (retry / skip) before signaling; we accept both for
                # belt-and-suspenders against callers that bypass the
                # backend translation (tests, manual Temporal CLI).
                while True:
                    if _action in ("retry", "retry_step"):
                        step_execution_id = self._prepare_retry_state(
                            step_id="step-resolve",
                            is_agent_step=False,
                        )
                        # Rebuild activity_input with the fresh step_execution_id.
                        activity_input = ResolveReadWorkspaceStepresoInput(
                            instance_id=input.instance_id,
                            step_execution_id=step_execution_id,
                            step_lineage_id=STEP_RESOLVE_READ_WORKSPACE,
                            tenant_code=input.tenant_code,
                            repo_key=Artifact.wrap(str(workflow.uuid4()), "hello-world"),
                            branch_base=Artifact.wrap(str(workflow.uuid4()), "master"),
                            resolved_connections=input.resolved_connections,
                        )
                        break  # exit inner; outer continue re-executes


                    elif _action in ("skip", "skip_step"):
                        # Mark the failed step as skipped in DB
                        await workflow.execute_activity(
                            mark_step_skipped,
                            MarkStepSkippedInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "accept_partial_work":
                        # Preserve whatever partial artifacts exist.
                        await workflow.execute_activity(
                            mark_step_complete_activity,
                            MarkStepCompleteInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                                outcome="operator_accepted_partial",
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "compensate_and_cancel":
                        await self._run_compensations(input, step_execution_id)
                        self._compensation_stack = []
                        raise _CompensateAndCancelSentinel()

                    else:
                        # Unknown action — defensive: legacy terminate.
                        raise e.cause if hasattr(e, 'cause') else e
                # End of inner action-dispatch loop.

                # If skip/accept exited inner with result=None, exit
                # the outer activity-retry loop too. Otherwise (retry,
                # compensate_then_retry) the outer loop's implicit
                # `continue` re-executes the activity with the new
                # step_execution_id.
                if _action in ("skip", "accept_partial_work"):
                    break  # exit outer
                # else: fall through; outer loop re-executes the activity.

        # Store outputs in artifacts
        # Extract specific fields from result.output using activity_field_key
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'working_directory' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("working_directory") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("working_directory")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("working_directory")
            self._artifacts["art-resolve-wd"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'repo_key' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("repo_key") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("repo_key")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("repo_key")
            self._artifacts["art-resolve-repo-key-out"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'base_branch' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("base_branch") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("base_branch")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("base_branch")
            self._artifacts["art-resolve-base-branch"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'base_sha' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("base_sha") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("base_sha")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("base_sha")
            self._artifacts["art-resolve-base-sha"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )


        # Universal catch-all for STRUCTURED failures (success=False or
        # is_stuck=True). Platform-service / integration / system steps
        # don't raise on failure — they return a structured result.
        # Without this check, the workflow would advance to the next
        # step regardless, cascading the failure silently.
        #
        # Agent steps are explicitly excluded — they have their own
        # success/stuck semantics via complete_step + designer-wired
        # routing predicates (is_stuck_low_confidence, etc.). Firing
        # __exception__ on agent results would bypass those routers.
        _structured_failure = None
        if result is not None and hasattr(result, "result") and result.result is not None:
            _failure_data = Artifact.unwrap(result.result)
            if isinstance(_failure_data, dict):
                if _failure_data.get("success") is False or _failure_data.get("is_stuck") is True:
                    _structured_failure = _failure_data
        if _structured_failure is not None:
            workflow.logger.warning(
                "Step Resolve Read Workspace returned structured failure: "
                f"success={_structured_failure.get('success')}, "
                f"is_stuck={_structured_failure.get('is_stuck')}, "
                f"stuck_category={_structured_failure.get('stuck_category')}."
            )
            self._step_errors["step-resolve"] = {
                "type": "StructuredFailure",
                "message": (
                    _structured_failure.get("stuck_reason")
                    or _structured_failure.get("failure_summary")
                    or _structured_failure.get("output_summary")
                    or f"success={_structured_failure.get('success')}, is_stuck={_structured_failure.get('is_stuck')}"
                ),
                "timestamp": workflow.now().isoformat(),
            }

            # Consult the on-failure policy table (Bug 3 fix, 2026-06-18).
            # The outer-loop dispatch at run() only fires when the step
            # has BOTH a compensation policy AND no _EXCEPTION_TARGETS
            # wiring. A step with default_on_failure=pause but no
            # designer-wired exception target would otherwise silently
            # route to the platform catch-all instead of pausing.
            _on_failure_policy = _STEP_COMPENSATION_POLICIES.get(
                "step-resolve", "fail",
            )
            if _on_failure_policy in ("pause", "pause_if_committed"):
                _sf_cause = RuntimeError(
                    self._step_errors["step-resolve"]["message"],
                )
                _action, _params = await self._handle_activity_failure(
                    step_id="step-resolve",
                    step_execution_id=self._step_execution_ids.get(
                        "step-resolve", "",
                    ),
                    step_name="Resolve Read Workspace",
                    cause=_sf_cause,
                    on_failure=_on_failure_policy,
                    input=input,
                    output_for_sha_lookup=_structured_failure,
                )
                # After pause + resume, _handle_activity_failure returns
                # the operator's chosen action. Handle each known action
                # explicitly:
                #
                #   retry / retry_step    → re-dispatch this step.
                #   skip / skip_step      → record skip + advance.
                #   accept_partial_work   → advance (treat partial as ok).
                #   compensate            → fall through to __exception__
                #                           for the outer loop's
                #                           compensation-aware dispatch.
                #
                # compensate_then_retry is rejected at the operator API
                # boundary for any step without a compensation_activity_key
                # (see ProcessManagementService._validate_compensate_then_retry).
                # The per-step branch fires only for non-agent steps with
                # default_on_failure in (pause, pause_if_committed) — these
                # may or may not have a compensation_activity_key. When
                # they do, the existing outer-loop dispatch handles the
                # compensate-then-retry flow; when they don't, the API
                # gate prevents the action from ever reaching the workflow.
                if _action in ("retry", "retry_step"):
                    self._prepare_retry_state(
                        step_id="step-resolve",
                        is_agent_step=False,
                    )
                    return await self._dispatch_step(
                        "step-resolve", input, workflow_id,
                    )
                if _action in ("skip", "skip_step"):
                    await workflow.execute_activity(
                        mark_step_skipped,
                        MarkStepSkippedInput(
                            tenant_code=input.tenant_code,
                            step_execution_id=self._step_execution_ids.get(
                                "step-resolve", "",
                            ),
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                    return None
                if _action == "accept_partial_work":
                    # Structured-failure path: artifact already stored from
                    # the prior dispatch; just advance routing with outcome=None.
                    return None
                # compensate / unknown → __exception__
                workflow.logger.warning(
                    f"Routing pause-action {_action} via __exception__.",
                )
                return "__exception__"

            # Default path (fail / compensate / unknown): route via
            # __exception__ for the outer loop's compensation-aware
            # dispatch (or the platform catch-all if no compensation
            # policy is configured).
            workflow.logger.warning("Routing via __exception__.")
            return "__exception__"


        # Record this attempt's outcome in the step's attempt history,
        # so the NEXT attempt's agent (if this step is re-entered) sees
        # what already happened.
        self._record_attempt("step-resolve", "Resolve Read Workspace", result)

        workflow.logger.info(
            f"Step complete: Resolve Read Workspace",
            extra={"has_result": result is not None},
        )

        return None

    async def _execute_step_branch(
        self,
        input: GateProbeNotReady_gpnotreaInput,
        workflow_id: str,
    ) -> Optional[str]:
        """
        Execute step: Create Feature Branch (SYSTEM)

        Returns:
            None for non-routing steps.
        """
        step_execution_id = str(workflow.uuid4())
        self._step_execution_ids["step-branch"] = step_execution_id

        workflow.logger.info(
            f"Executing step: Create Feature Branch",
            extra={
                "step_lineage_id": STEP_CREATE_FEATURE_BRANCH,
                "step_execution_id": step_execution_id,
                "instance_id": input.instance_id,
            },
        )

        # Check for pause before step
        await self._check_paused(input)

        # =========================================================
        # SYSTEM/AI STEP: Execute activity and store outputs
        # =========================================================
        # Build expression context: self._artifacts + aliased input names
        # Expressions reference sibling inputs by alias (e.g., _expr_92b6b1ad_pr_title)
        # which exist in the activity payload but not in self._artifacts (keyed by UUID).
        _expr_ctx = dict(self._artifacts)
        _expr_ctx["working_directory"] = Artifact.unwrap(self._artifacts.get("art-resolve-wd"))
        _expr_ctx["repo_key"] = Artifact.unwrap(self._artifacts.get("art-resolve-repo-key-out"))
        # Applied AFTER the input aliases above, not before: the alias loop
        # writes activity_field_key values into _expr_ctx, so a step input
        # whose field key is literally `process` would otherwise clobber the
        # whole system namespace and make process.* resolve to that input.
        _expr_ctx.update(_system_context(input.instance_id))

        activity_input = CreateFeatureBranchStepbranInput(
            instance_id=input.instance_id,
            step_execution_id=step_execution_id,
            step_lineage_id=STEP_CREATE_FEATURE_BRANCH,
            tenant_code=input.tenant_code,
            working_directory=self._artifacts.get("art-resolve-wd"),
            repo_key=self._artifacts.get("art-resolve-repo-key-out"),
            context_label=Artifact.wrap(str(workflow.uuid4()), "gate-probe"),
            context_id=Artifact.wrap(str(workflow.uuid4()), "probe-nr1"),
            allow_reuse=Artifact.wrap(str(workflow.uuid4()), True),
            resolved_connections=input.resolved_connections,
        )

        # Compensation-aware retry loop (P12). Loops on retry_step;
        # breaks on success, skip, or accept_partial_work.
        while True:
            try:
                result = await workflow.execute_activity(
                    create_feature_branch_stepbran,
                    args=[activity_input],
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=RetryPolicy(
                        maximum_attempts=1,
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=100),
                        backoff_coefficient=2.0,
                        non_retryable_error_types=["EditConflict", "HumanTaskCancelledError", "HumanTaskTimeoutError", "PermissionDeniedError", "ProcessModelError", "ResourceNotFoundError", "RetryUnsafeError", "RouterEvaluationError", "ValidationError"],
                    ),
                )
                break  # Activity succeeded — exit retry loop

            except ActivityError as e:
                _action, _params = await self._handle_activity_failure(
                    step_id="step-branch",
                    step_execution_id=step_execution_id,
                    step_name="Create Feature Branch",
                    cause=e.cause if hasattr(e, 'cause') else e,
                    on_failure="pause",
                    input=input,
                )

                # P13: inner action-dispatch loop. Most branches break
                # out (retry / skip / accept exit and the outer loop
                # either re-executes or moves on). compensate_then_retry's
                # compensation-failure path re-pauses and *continues*
                # this inner loop with the operator's new action choice.
                #
                # API → workflow token dispatch. Backend translates
                # API tokens (retry_step / skip_step) to workflow tokens
                # (retry / skip) before signaling; we accept both for
                # belt-and-suspenders against callers that bypass the
                # backend translation (tests, manual Temporal CLI).
                while True:
                    if _action in ("retry", "retry_step"):
                        step_execution_id = self._prepare_retry_state(
                            step_id="step-branch",
                            is_agent_step=False,
                        )
                        # Rebuild activity_input with the fresh step_execution_id.
                        activity_input = CreateFeatureBranchStepbranInput(
                            instance_id=input.instance_id,
                            step_execution_id=step_execution_id,
                            step_lineage_id=STEP_CREATE_FEATURE_BRANCH,
                            tenant_code=input.tenant_code,
                            working_directory=self._artifacts.get("art-resolve-wd"),
                            repo_key=self._artifacts.get("art-resolve-repo-key-out"),
                            context_label=Artifact.wrap(str(workflow.uuid4()), "gate-probe"),
                            context_id=Artifact.wrap(str(workflow.uuid4()), "probe-nr1"),
                            allow_reuse=Artifact.wrap(str(workflow.uuid4()), True),
                            resolved_connections=input.resolved_connections,
                        )
                        break  # exit inner; outer continue re-executes


                    elif _action in ("skip", "skip_step"):
                        # Mark the failed step as skipped in DB
                        await workflow.execute_activity(
                            mark_step_skipped,
                            MarkStepSkippedInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "accept_partial_work":
                        # Preserve whatever partial artifacts exist.
                        await workflow.execute_activity(
                            mark_step_complete_activity,
                            MarkStepCompleteInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                                outcome="operator_accepted_partial",
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "compensate_and_cancel":
                        await self._run_compensations(input, step_execution_id)
                        self._compensation_stack = []
                        raise _CompensateAndCancelSentinel()

                    else:
                        # Unknown action — defensive: legacy terminate.
                        raise e.cause if hasattr(e, 'cause') else e
                # End of inner action-dispatch loop.

                # If skip/accept exited inner with result=None, exit
                # the outer activity-retry loop too. Otherwise (retry,
                # compensate_then_retry) the outer loop's implicit
                # `continue` re-executes the activity with the new
                # step_execution_id.
                if _action in ("skip", "accept_partial_work"):
                    break  # exit outer
                # else: fall through; outer loop re-executes the activity.

        # Store outputs in artifacts
        # Extract specific fields from result.output using activity_field_key
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'working_directory' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("working_directory") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("working_directory")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("working_directory")
            self._artifacts["art-branch-wd"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'branch_name' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("branch_name") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("branch_name")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("branch_name")
            self._artifacts["art-branch-name"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'repo_key' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("repo_key") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("repo_key")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("repo_key")
            self._artifacts["art-branch-repo-key"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )


        # Universal catch-all for STRUCTURED failures (success=False or
        # is_stuck=True). Platform-service / integration / system steps
        # don't raise on failure — they return a structured result.
        # Without this check, the workflow would advance to the next
        # step regardless, cascading the failure silently.
        #
        # Agent steps are explicitly excluded — they have their own
        # success/stuck semantics via complete_step + designer-wired
        # routing predicates (is_stuck_low_confidence, etc.). Firing
        # __exception__ on agent results would bypass those routers.
        _structured_failure = None
        if result is not None and hasattr(result, "result") and result.result is not None:
            _failure_data = Artifact.unwrap(result.result)
            if isinstance(_failure_data, dict):
                if _failure_data.get("success") is False or _failure_data.get("is_stuck") is True:
                    _structured_failure = _failure_data
        if _structured_failure is not None:
            workflow.logger.warning(
                "Step Create Feature Branch returned structured failure: "
                f"success={_structured_failure.get('success')}, "
                f"is_stuck={_structured_failure.get('is_stuck')}, "
                f"stuck_category={_structured_failure.get('stuck_category')}."
            )
            self._step_errors["step-branch"] = {
                "type": "StructuredFailure",
                "message": (
                    _structured_failure.get("stuck_reason")
                    or _structured_failure.get("failure_summary")
                    or _structured_failure.get("output_summary")
                    or f"success={_structured_failure.get('success')}, is_stuck={_structured_failure.get('is_stuck')}"
                ),
                "timestamp": workflow.now().isoformat(),
            }

            # Consult the on-failure policy table (Bug 3 fix, 2026-06-18).
            # The outer-loop dispatch at run() only fires when the step
            # has BOTH a compensation policy AND no _EXCEPTION_TARGETS
            # wiring. A step with default_on_failure=pause but no
            # designer-wired exception target would otherwise silently
            # route to the platform catch-all instead of pausing.
            _on_failure_policy = _STEP_COMPENSATION_POLICIES.get(
                "step-branch", "fail",
            )
            if _on_failure_policy in ("pause", "pause_if_committed"):
                _sf_cause = RuntimeError(
                    self._step_errors["step-branch"]["message"],
                )
                _action, _params = await self._handle_activity_failure(
                    step_id="step-branch",
                    step_execution_id=self._step_execution_ids.get(
                        "step-branch", "",
                    ),
                    step_name="Create Feature Branch",
                    cause=_sf_cause,
                    on_failure=_on_failure_policy,
                    input=input,
                    output_for_sha_lookup=_structured_failure,
                )
                # After pause + resume, _handle_activity_failure returns
                # the operator's chosen action. Handle each known action
                # explicitly:
                #
                #   retry / retry_step    → re-dispatch this step.
                #   skip / skip_step      → record skip + advance.
                #   accept_partial_work   → advance (treat partial as ok).
                #   compensate            → fall through to __exception__
                #                           for the outer loop's
                #                           compensation-aware dispatch.
                #
                # compensate_then_retry is rejected at the operator API
                # boundary for any step without a compensation_activity_key
                # (see ProcessManagementService._validate_compensate_then_retry).
                # The per-step branch fires only for non-agent steps with
                # default_on_failure in (pause, pause_if_committed) — these
                # may or may not have a compensation_activity_key. When
                # they do, the existing outer-loop dispatch handles the
                # compensate-then-retry flow; when they don't, the API
                # gate prevents the action from ever reaching the workflow.
                if _action in ("retry", "retry_step"):
                    self._prepare_retry_state(
                        step_id="step-branch",
                        is_agent_step=False,
                    )
                    return await self._dispatch_step(
                        "step-branch", input, workflow_id,
                    )
                if _action in ("skip", "skip_step"):
                    await workflow.execute_activity(
                        mark_step_skipped,
                        MarkStepSkippedInput(
                            tenant_code=input.tenant_code,
                            step_execution_id=self._step_execution_ids.get(
                                "step-branch", "",
                            ),
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                    return None
                if _action == "accept_partial_work":
                    # Structured-failure path: artifact already stored from
                    # the prior dispatch; just advance routing with outcome=None.
                    return None
                # compensate / unknown → __exception__
                workflow.logger.warning(
                    f"Routing pause-action {_action} via __exception__.",
                )
                return "__exception__"

            # Default path (fail / compensate / unknown): route via
            # __exception__ for the outer loop's compensation-aware
            # dispatch (or the platform catch-all if no compensation
            # policy is configured).
            workflow.logger.warning("Routing via __exception__.")
            return "__exception__"


        # Record this attempt's outcome in the step's attempt history,
        # so the NEXT attempt's agent (if this step is re-entered) sees
        # what already happened.
        self._record_attempt("step-branch", "Create Feature Branch", result)

        workflow.logger.info(
            f"Step complete: Create Feature Branch",
            extra={"has_result": result is not None},
        )

        return None

    async def _execute_step_code(
        self,
        input: GateProbeNotReady_gpnotreaInput,
        workflow_id: str,
    ) -> Optional[str]:
        """
        Execute step: Gate Probe Code (AI)
        Exit Point: Yes

        Returns:
            None for non-routing steps.
        """
        step_execution_id = str(workflow.uuid4())
        self._step_execution_ids["step-code"] = step_execution_id

        workflow.logger.info(
            f"Executing step: Gate Probe Code",
            extra={
                "step_lineage_id": STEP_GATE_PROBE_CODE,
                "step_execution_id": step_execution_id,
                "instance_id": input.instance_id,
            },
        )

        # Check for pause before step
        await self._check_paused(input)

        # =========================================================
        # SYSTEM/AI STEP: Execute activity and store outputs
        # =========================================================
        # Build expression context: self._artifacts + aliased input names
        # Expressions reference sibling inputs by alias (e.g., _expr_92b6b1ad_pr_title)
        # which exist in the activity payload but not in self._artifacts (keyed by UUID).
        _expr_ctx = dict(self._artifacts)
        _expr_ctx["working_directory"] = Artifact.unwrap(self._artifacts.get("art-branch-wd"))
        _expr_ctx["branch_name"] = Artifact.unwrap(self._artifacts.get("art-branch-name"))
        _expr_ctx["base_branch"] = Artifact.unwrap(self._artifacts.get("art-resolve-base-branch"))
        # Applied AFTER the input aliases above, not before: the alias loop
        # writes activity_field_key values into _expr_ctx, so a step input
        # whose field key is literally `process` would otherwise clobber the
        # whole system namespace and make process.* resolve to that input.
        _expr_ctx.update(_system_context(input.instance_id))
        # Sub-project A (hi_platform_v6): capture branch HEAD before dispatch.
        # Failure is non-fatal — the workflow continues; if this step
        # later fails and the operator picks compensate_then_retry, the
        # missing cache entry surfaces as a loud ValueError from
        # op_revert_branch_inflight (no silent over-revert).
        if workflow.patched("hi_platform_v6"):
            try:
                _pre_sha_result = await workflow.execute_activity(
                    "capture_pre_step_sha_activity",
                    CapturePreStepShaInput(
                        tenant_code=input.tenant_code,
                        step_execution_id=step_execution_id,
                        working_directory=(
                            Artifact.unwrap(self._artifacts.get("art-branch-wd"))
                            or ""
                        ),
                        branch_name=(
                            Artifact.unwrap(self._artifacts.get("art-branch-name"))
                            or ""
                        ),
                        # 2026-06-05 follow-up: lets _persist_pre_step_sha
                        # do a race-safe upsert if the parent step's row
                        # doesn't yet exist when the capture writes.
                        instance_id=input.instance_id,
                        external_lineage_id="step-code",
                    ),
                    result_type=CapturePreStepShaResult,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )
                if _pre_sha_result.success and _pre_sha_result.pre_sha:
                    self._pre_step_sha[step_execution_id] = _pre_sha_result.pre_sha
            except Exception as _cap_err:
                workflow.logger.warning(
                    f"pre_step_sha capture failed for {step_execution_id}: "
                    f"{_cap_err}. Inflight compensation will fail loudly "
                    "rather than silently over-revert."
                )

        activity_input = GateProbeCodeStepcodeInput(
            instance_id=input.instance_id,
            step_execution_id=step_execution_id,
            step_lineage_id=STEP_GATE_PROBE_CODE,
            tenant_code=input.tenant_code,
            task=Artifact.wrap(str(workflow.uuid4()), "Do exactly these three things and nothing else. 1) Write a file named GATE_PROBE.md at the repository root containing the single line: gate probe. 2) Commit only that file with the message: gate probe. 3) Immediately call the complete_step tool with a one-sentence summary and stop."),
            working_directory=self._artifacts.get("art-branch-wd"),
            branch_name=self._artifacts.get("art-branch-name"),
            base_branch=self._artifacts.get("art-resolve-base-branch"),
            resolved_connections=input.resolved_connections,
            platform_context={
                "attempt_number": self._iteration_counts.get("step-code", 1),
                "attempt_history": self._attempt_histories.get("step-code", []),
                "routing_metadata": self._last_routing_context,
                # Carry the most recent human-resolve outcome (decision +
                # instructions) into this attempt. Cleared after consumption
                # so it doesn't bleed into a later unrelated attempt.
                "human_resolution": self._pending_human_resolution,
                # Carry the most recent test-step output into the prompt so
                # the agent can read the actual failure (output_tail) instead
                # of just a one-line summary.
                "last_test_run": self._last_test_run,
            } if self._iteration_counts.get("step-code", 0) > 0 else None,
        )

        # Compensation-aware retry loop (P12). Loops on retry_step;
        # breaks on success, skip, or accept_partial_work.
        while True:
            try:
                result = await workflow.execute_activity(
                    gate_probe_code_stepcode,
                    args=[activity_input],
                    start_to_close_timeout=timedelta(minutes=120),
                    retry_policy=RetryPolicy(
                        maximum_attempts=1,
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=100),
                        backoff_coefficient=2.0,
                        non_retryable_error_types=["EditConflict", "HumanTaskCancelledError", "HumanTaskTimeoutError", "PermissionDeniedError", "ProcessModelError", "ResourceNotFoundError", "RetryUnsafeError", "RouterEvaluationError", "ValidationError"],
                    ),
                    task_queue=f"{input.tenant_code}--agent",
                )
                break  # Activity succeeded — exit retry loop

            except ActivityError as e:
                _action, _params = await self._handle_activity_failure(
                    step_id="step-code",
                    step_execution_id=step_execution_id,
                    step_name="Gate Probe Code",
                    cause=e.cause if hasattr(e, 'cause') else e,
                    on_failure="pause_if_committed",
                    input=input,
                )

                # P13: inner action-dispatch loop. Most branches break
                # out (retry / skip / accept exit and the outer loop
                # either re-executes or moves on). compensate_then_retry's
                # compensation-failure path re-pauses and *continues*
                # this inner loop with the operator's new action choice.
                #
                # API → workflow token dispatch. Backend translates
                # API tokens (retry_step / skip_step) to workflow tokens
                # (retry / skip) before signaling; we accept both for
                # belt-and-suspenders against callers that bypass the
                # backend translation (tests, manual Temporal CLI).
                while True:
                    if _action in ("retry", "retry_step"):
                        step_execution_id = self._prepare_retry_state(
                            step_id="step-code",
                            is_agent_step=True,
                        )
                        # Rebuild activity_input with the fresh step_execution_id.
                        activity_input = GateProbeCodeStepcodeInput(
                            instance_id=input.instance_id,
                            step_execution_id=step_execution_id,
                            step_lineage_id=STEP_GATE_PROBE_CODE,
                            tenant_code=input.tenant_code,
                            task=Artifact.wrap(str(workflow.uuid4()), "Do exactly these three things and nothing else. 1) Write a file named GATE_PROBE.md at the repository root containing the single line: gate probe. 2) Commit only that file with the message: gate probe. 3) Immediately call the complete_step tool with a one-sentence summary and stop."),
                            working_directory=self._artifacts.get("art-branch-wd"),
                            branch_name=self._artifacts.get("art-branch-name"),
                            base_branch=self._artifacts.get("art-resolve-base-branch"),
                            resolved_connections=input.resolved_connections,
                            platform_context={
                                "attempt_number": self._iteration_counts.get("step-code", 1),
                                "attempt_history": self._attempt_histories.get("step-code", []),
                                "routing_metadata": self._last_routing_context,
                                "human_resolution": self._pending_human_resolution,
                                "last_test_run": self._last_test_run,
                            } if self._iteration_counts.get("step-code", 0) > 0 else None,
                        )
                        break  # exit inner; outer continue re-executes

                    elif _action == "compensate_then_retry":
                        # P13: clean up the failed step's partial work,
                        # then re-execute as a fresh attempt.
                        _comp_ok, _comp_err = await self._run_inflight_compensation(
                            input=input,
                            step_id="step-code",
                            step_execution_id=step_execution_id,
                            step_name="Gate Probe Code",
                            compensation_activity_key="op_revert_branch_inflight",
                        )

                        if not _comp_ok:
                            # Re-pause with the compensation error in the
                            # pause reason. The dispatch helper consumes
                            # the operator's NEW action choice; the
                            # inner-loop `continue` re-handles it without
                            # re-executing the failing activity.
                            _action, _params = await self._dispatch_pause_action(
                                input=input,
                                step_id="step-code",
                                step_name="Gate Probe Code",
                                step_execution_id=step_execution_id,
                                cause=e.cause if hasattr(e, 'cause') else e,
                                reason_key="compensation_failed",
                                reason_args={"compensation_error": str(_comp_err)[:500]},
                            )
                            continue  # inner loop — handle the new action

                        # Workspace cleanup (gated on the step's
                        # transitive dependency on a workspace resolver).
                        await self._cleanup_workspace_for_step(
                            input, "art-branch-wd",
                        )

                        self._paused_failure_step_execution_id = None
                        self._paused_failure_step_id = None
                        step_execution_id = self._prepare_retry_state(
                            step_id="step-code",
                            is_agent_step=True,
                        )
                        activity_input = GateProbeCodeStepcodeInput(
                            instance_id=input.instance_id,
                            step_execution_id=step_execution_id,
                            step_lineage_id=STEP_GATE_PROBE_CODE,
                            tenant_code=input.tenant_code,
                            task=Artifact.wrap(str(workflow.uuid4()), "Do exactly these three things and nothing else. 1) Write a file named GATE_PROBE.md at the repository root containing the single line: gate probe. 2) Commit only that file with the message: gate probe. 3) Immediately call the complete_step tool with a one-sentence summary and stop."),
                            working_directory=self._artifacts.get("art-branch-wd"),
                            branch_name=self._artifacts.get("art-branch-name"),
                            base_branch=self._artifacts.get("art-resolve-base-branch"),
                            resolved_connections=input.resolved_connections,
                            platform_context={
                                "attempt_number": self._iteration_counts.get("step-code", 1),
                                "attempt_history": self._attempt_histories.get("step-code", []),
                                "routing_metadata": self._last_routing_context,
                                "human_resolution": self._pending_human_resolution,
                                "last_test_run": self._last_test_run,
                            } if self._iteration_counts.get("step-code", 0) > 0 else None,
                        )
                        break  # exit inner; outer continue re-executes

                    elif _action in ("skip", "skip_step"):
                        # Mark the failed step as skipped in DB
                        await workflow.execute_activity(
                            mark_step_skipped,
                            MarkStepSkippedInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "accept_partial_work":
                        # Preserve whatever partial artifacts exist.
                        await workflow.execute_activity(
                            mark_step_complete_activity,
                            MarkStepCompleteInput(
                                tenant_code=input.tenant_code,
                                step_execution_id=step_execution_id,
                                outcome="operator_accepted_partial",
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        result = None
                        break  # exit inner

                    elif _action == "compensate_and_cancel":
                        await self._run_compensations(input, step_execution_id)
                        self._compensation_stack = []
                        raise _CompensateAndCancelSentinel()

                    else:
                        # Unknown action — defensive: legacy terminate.
                        raise e.cause if hasattr(e, 'cause') else e
                # End of inner action-dispatch loop.

                # If skip/accept exited inner with result=None, exit
                # the outer activity-retry loop too. Otherwise (retry,
                # compensate_then_retry) the outer loop's implicit
                # `continue` re-executes the activity with the new
                # step_execution_id.
                if _action in ("skip", "accept_partial_work"):
                    break  # exit outer
                # else: fall through; outer loop re-executes the activity.

        # Store outputs in artifacts
        # Extract specific fields from result.output using activity_field_key
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'success' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("success") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("success")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("success")
            self._artifacts["art-code-success"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'output' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("output") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("output")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("output")
            self._artifacts["art-code-output"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'commit_sha' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("commit_sha") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("commit_sha")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("commit_sha")
            self._artifacts["art-code-commit"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )
        if result is not None and hasattr(result, 'result') and result.result is not None:
            # Extract specific field 'is_stuck' from output
            _output_data = Artifact.unwrap(result.result)
            _field_value = _output_data.get("is_stuck") if isinstance(_output_data, dict) else _output_data
            if _field_value is None and isinstance(_output_data, dict):
                _so = _output_data.get("structured_output") or {}
                _field_value = _so.get("is_stuck")
                if _field_value is None:
                    _field_value = (_so.get("_meta") or {}).get("is_stuck")
            self._artifacts["art-code-is-stuck"] = Artifact.wrap(
                result.result.id if hasattr(result.result, 'id') else str(workflow.uuid4()),
                _field_value
            )

        # Saga: push compensatable step onto stack (P11)
        if result is not None and hasattr(result, 'result') and result.result is not None:
            _comp_output = Artifact.unwrap(result.result) if result.result else {}
            self._compensation_stack.append(_CompensationEntry(
                forward_step_execution_id=step_execution_id,
                compensation_activity_key="op_revert_branch_inflight",
                step_output=_comp_output if isinstance(_comp_output, dict) else {},
                step_id="step-code",
                step_name="Gate Probe Code",
            ))
        else:
            # Tier 3.4: record the skip in management events for operator audit.
            # The guard rejected this entry; the saga walker won't try to
            # compensate it. Without this audit row, operators have no record
            # of why a step that succeeded forward was not compensated.
            _skip_reason = (
                "null_result" if result is None or not hasattr(result, "result")
                else "null_result_attr"
            )
            try:
                await workflow.execute_activity(
                    record_compensation_skip_activity,
                    RecordCompensationSkipInput(
                        tenant_code=input.tenant_code,
                        instance_id=input.instance_id,
                        forward_step_execution_id=step_execution_id,
                        forward_step_lineage_id="step-code",
                        compensation_activity_key="op_revert_branch_inflight",
                        reason=_skip_reason,
                    ),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            except Exception as _skip_audit_err:
                # Audit-only failure must not break the workflow's main path.
                workflow.logger.warning(
                    f"Failed to record compensation skip audit: {_skip_audit_err}",
                )



        # Record this attempt's outcome in the step's attempt history,
        # so the NEXT attempt's agent (if this step is re-entered) sees
        # what already happened.
        self._record_attempt("step-code", "Gate Probe Code", result)

        workflow.logger.info(
            f"Step complete: Gate Probe Code",
            extra={"has_result": result is not None},
        )

        return None

    # -------------------------------------------------------------------------
    # ROUTING HELPERS
    # -------------------------------------------------------------------------

    def _get_entry_step_id(self) -> str:
        """Get the entry step ID for starting the workflow."""
        return "step-resolve"

    def _get_next_step_ids(self, current_step_id: str, outcome: Optional[str]) -> list[str]:
        """
        Get the next step ID(s) based on current step and outcome.

        Handles:
        - SEQUENCE: Returns single target (no outcome needed)
        - BRANCH: Uses outcome to find correct target
        - LOOP: Returns target (iteration guard checked separately)
        - PARALLEL: Returns multiple targets for concurrent execution
        - __exception__: Routes to EXCEPTION connection or __fail_workflow__

        Returns empty list for exit points.
        """
        # Platform catch-all terminates the workflow after the human task
        # completes. Operator uses restart-from-step to retry. PLATFORM
        # CATCH-ALL v1 — supersede with compensation epic.
        if current_step_id == "__platform_catchall_resolve__":
            return []

        # Handle __exception__ sentinel from _handle_step_failure
        if outcome == "__exception__":
            _exception_route = {
            }.get(current_step_id, "__fail_workflow__")
            if _exception_route == "__fail_workflow__":
                # No designer-wired EXCEPTION connection — fall through to
                # the platform catch-all. _execute_platform_catchall_resolve
                # creates a chat human task with the failure context (using
                # op_anthropic_resolve_implementation_stuck as the chat
                # agent), waits for the operator to complete it, then
                # terminates the workflow. Operator can use restart-from-step
                # to retry after fixing the underlying issue.
                #
                # PLATFORM CATCH-ALL v1 — supersede with compensation epic.
                error = self._step_errors.get(current_step_id, {})
                step_name = _STEP_NAMES.get(current_step_id, current_step_id)
                workflow.logger.error(
                    f"Step {step_name} failed with no designer-wired EXCEPTION "
                    f"route — routing to platform catch-all. "
                    f"Error: {error.get('type', 'unknown')}: {error.get('message', 'unknown')}",
                    extra={"step_id": current_step_id},
                )
                # Stash the failing step's identity so the catchall handler
                # can include it in the task context.
                self._catchall_origin_step_id = current_step_id
                return ["__platform_catchall_resolve__"]
            return [_exception_route]

        if current_step_id == "step-code":
            return []  # Exit point

        routing = _ROUTING_TABLE.get(current_step_id, {})

        # Check for PARALLEL first (fork to multiple concurrent steps)
        if "PARALLEL" in routing:
            return [target for target, _ in routing["PARALLEL"]]

        # Check for BRANCH (conditional routing based on outcome)
        if "BRANCH" in routing:
            for target, branch_outcome in routing["BRANCH"]:
                if branch_outcome == outcome:
                    return [target]
            # No matching branch - check LOOP (router outcome may loop back)
            if "LOOP" in routing:
                for target, loop_outcome in routing["LOOP"]:
                    if loop_outcome == outcome:
                        return [target]
            # Still no match - check for SEQUENCE fallback
            if "SEQUENCE" in routing:
                return [routing["SEQUENCE"][0][0]]
            workflow.logger.warning(
                f"No matching branch for outcome: {outcome}",
                extra={"step_id": current_step_id},
            )
            return []

        # Check for SEQUENCE (linear flow)
        if "SEQUENCE" in routing:
            return [routing["SEQUENCE"][0][0]]

        # Check for LOOP (backwards connection)
        if "LOOP" in routing:
            return [routing["LOOP"][0][0]]

        return []  # No outgoing connections

    def _resolve_parallel_join(self, parallel_step_ids: list[str]) -> list[str]:
        """
        Find the convergence point after parallel execution.

        All parallel branches should connect to a common step.
        If branches have different targets, we take the union (continue all paths).
        """
        all_next_steps: set[str] = set()

        for step_id in parallel_step_ids:
            next_ids = self._get_next_step_ids(step_id, None)
            all_next_steps.update(next_ids)

        return list(all_next_steps)

    # -------------------------------------------------------------------------
    # MAIN WORKFLOW EXECUTION
    # -------------------------------------------------------------------------

    @workflow.run
    async def run(self, input: GateProbeNotReady_gpnotreaInput) -> GateProbeNotReady_gpnotreaOutput:
        """Execute the Gate Probe Not Ready workflow."""
        workflow_id = workflow.info().workflow_id

        workflow.logger.info(
            "Starting GateProbeNotReady_gpnotreaWorkflow",
            extra={
                "instance_id": input.instance_id,
                "process_def_id": input.process_def_id,
                "snapshot_id": input.snapshot_id,
                "workflow_id": workflow_id,
            },
        )

        try:
            # Initialize artifacts from trigger payload if present
            if input.trigger_payload:
                self._artifacts["_trigger_payload"] = Artifact.wrap(
                    str(workflow.uuid4()),
                    input.trigger_payload
                )

            # Start from entry point
            next_step_ids = [self._get_entry_step_id()]

            # Main execution loop
            while next_step_ids:
                if len(next_step_ids) == 1:
                    # Sequential execution (SEQUENCE, BRANCH, or LOOP)
                    step_id = next_step_ids[0]

                    # LOOP iteration guard
                    self._iteration_counts[step_id] = self._iteration_counts.get(step_id, 0) + 1
                    if self._iteration_counts[step_id] > MAX_LOOP_ITERATIONS:
                        raise RuntimeError(
                            f"Max loop iterations ({MAX_LOOP_ITERATIONS}) exceeded for step {step_id}"
                        )

                    # Track attempt history for platform_context (captures context before re-execution)
                    if step_id not in self._attempt_histories:
                        self._attempt_histories[step_id] = []

                    # Execute the step
                    outcome = await self._dispatch_step(step_id, input, workflow_id)

                    # P11: Structured failure compensation dispatch.
                    # When a step returns "__exception__" (structured failure)
                    # and has no designer-wired exception connection, dispatch
                    # through compensation-aware failure handling instead of
                    # the platform catch-all.
                    if outcome == "__exception__":
                        _comp_policy = _STEP_COMPENSATION_POLICIES.get(step_id)
                        if _comp_policy and step_id not in _EXCEPTION_TARGETS:
                            _step_exec_id = self._step_execution_ids.get(step_id, "")
                            if not _step_exec_id:
                                # Defensive: no UUID means we can't trace the
                                # failure. Force pause regardless of policy.
                                _effective_policy = "pause"
                            else:
                                _effective_policy = _comp_policy

                            _step_artifact = self._artifacts.get(step_id)
                            _step_output = (
                                Artifact.unwrap(_step_artifact)
                                if _step_artifact else {}
                            )
                            _step_output = (
                                _step_output
                                if isinstance(_step_output, dict) else {}
                            )

                            # P13: structured-failure dispatch lives in
                            # _handle_structured_failure. The helper runs
                            # the recovery-action loop (retry, skip,
                            # accept, compensate_and_cancel,
                            # compensate_then_retry) and returns the
                            # outcome to feed into _get_next_step_ids.
                            outcome = await self._handle_structured_failure(
                                input=input,
                                workflow_id=workflow_id,
                                step_id=step_id,
                                step_exec_id=_step_exec_id,
                                step_output=_step_output,
                                effective_policy=_effective_policy,
                            )

                    # Get next step(s)
                    next_step_ids = self._get_next_step_ids(step_id, outcome)


                else:
                    # PARALLEL execution - multiple concurrent paths
                    workflow.logger.info(
                        "Executing parallel steps",
                        extra={"step_ids": next_step_ids},
                    )

                    # Execute all parallel steps concurrently
                    await asyncio.gather(*[
                        self._dispatch_step(sid, input, workflow_id)
                        for sid in next_step_ids
                    ])

                    # Find convergence point
                    next_step_ids = self._resolve_parallel_join(next_step_ids)


            # Workflow completed successfully
            await workflow.execute_activity(
                complete_process_instance_activity,
                CompleteProcessInstanceInput(
                    tenant_code=input.tenant_code,
                    instance_id=input.instance_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=DEFAULT_RETRY_POLICY,
            )

            workflow.logger.info(
                "Workflow completed successfully",
                extra={"instance_id": input.instance_id},
            )

            return GateProbeNotReady_gpnotreaOutput(
                success=True,
                outcome="completed",
            )

        except _CompensateAndCancelSentinel:
            # Operator chose compensate_and_cancel. Compensation walk
            # already completed. Update instance status and return
            # terminal output.
            await workflow.execute_activity(
                update_instance_status_activity,
                UpdateInstanceStatusInput(
                    tenant_code=input.tenant_code,
                    instance_id=input.instance_id,
                    status="cancelled",
                    finalize_step_execution_id=self._paused_failure_step_execution_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=10),
            )

            workflow.logger.info(
                "Workflow terminated via compensate_and_cancel",
                extra={"instance_id": input.instance_id},
            )

            return GateProbeNotReady_gpnotreaOutput(
                success=False,
                outcome="compensated_and_cancelled",
            )

        except ActivityError:
            # Activity-level retry policies have already been honored.
            # If we reach here, a persistent activity failure (e.g., the
            # update_instance_status_activity DB schema mismatch in spec
            # bug 1.8) deserves a visible workflow failure in Temporal UI
            # rather than a silent outcome="failed" return.
            workflow.logger.error(
                "Activity failed persistently after retries; surfacing as workflow failure",
                extra={"instance_id": input.instance_id},
            )
            raise

        except Exception as e:
            # Re-raise so Temporal records the run as FAILED (matches the
            # ActivityError handler above). Surfacing the failure in
            # Temporal's view means:
            #   * UI / alerts / metrics on workflow failures all fire.
            #   * Reconcile sweep maps Temporal FAILED → DB status='failed'
            #     directly, without depending on the PR-4 COMPLETED→failed
            #     mapper-side downgrade. The mapper-side downgrade stays
            #     in place as belt-and-suspenders for older committed
            #     workflows that still return the success=False payload.
            # The finally block below still runs the cleanup activities.
            import traceback
            tb = traceback.format_exc()
            workflow.logger.error(
                f"Workflow failed: {type(e).__name__}: {e}\n{tb}",
                extra={
                    "instance_id": input.instance_id,
                },
            )
            raise

    async def _dispatch_step(
        self,
        step_id: str,
        input: GateProbeNotReady_gpnotreaInput,
        workflow_id: str,
    ) -> Optional[str]:
        """Dispatch to the correct step execution method based on step ID."""
        # Platform catch-all sentinel — dispatched when a step failed with
        # a structured failure (success=False / is_stuck=True) AND no
        # designer-wired exception connection handled it. Creates a chat
        # human task with the failure context so the operator can review
        # and decide. After the human completes the task, the workflow
        # terminates (operator uses restart-from-step to retry, if desired).
        #
        # PLATFORM CATCH-ALL v1 — supersede with compensation epic.
        if step_id == "__platform_catchall_resolve__":
            return await self._execute_platform_catchall_resolve(input, workflow_id)
        if step_id == "step-resolve":
            return await self._execute_step_resolve(input, workflow_id)
        elif step_id == "step-branch":
            return await self._execute_step_branch(input, workflow_id)
        elif step_id == "step-code":
            return await self._execute_step_code(input, workflow_id)
        else:
            raise RuntimeError(f"Unknown step ID: {step_id}")