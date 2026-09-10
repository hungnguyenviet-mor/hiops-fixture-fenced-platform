# @scaffold:artifacts artifacts.py
# @scaffold:process_id gate-probe-notready
# @scaffold:process_name "Gate Probe Not Ready"
# @scaffold:file_hash sha256:a9314e27a225ba683dde6cc536e95f3a363eec09cc4d42ae7e8b0184fa91c703
#
# PURPOSE: Data models for artifacts that flow between steps in the
# Gate Probe Not Ready process.
#
# CONTENTS:
# - Resolve Read Workspace - Repository Key (SIMPLE)
# - Resolve Read Workspace - Base Branch Override (SIMPLE)
# - Resolve Read Workspace - Working Directory (SIMPLE)
# - Resolve Read Workspace - Repository Key (out) (SIMPLE)
# - Resolve Read Workspace - Base Branch (SIMPLE)
# - Resolve Read Workspace - Base SHA (SIMPLE)
# - Create Feature Branch - Context Label (SIMPLE)
# - Create Feature Branch - Context ID (SIMPLE)
# - Create Feature Branch - Allow Branch Reuse (SIMPLE)
# - Create Feature Branch - Working Directory (SIMPLE)
# - Create Feature Branch - Branch Name (SIMPLE)
# - Create Feature Branch - Repository Key (SIMPLE)
# - Gate Probe Code - Task (SIMPLE)
# - Gate Probe Code - Success (SIMPLE)
# - Gate Probe Code - Output (SIMPLE)
# - Gate Probe Code - Commit SHA (SIMPLE)
# - Gate Probe Code - Is Stuck (SIMPLE)
#
# SIMPLE artifacts use Python dataclasses.
# SCHEMA artifacts use Pydantic models with the schema from export JSON.
#
# This file is FULLY MANAGED by the scaffold generator for SCHEMA artifacts.
# You may add fields to SIMPLE artifact dataclasses as needed.
"""
Artifact models for the Gate Probe Not Ready process.

These models correspond to the artifact definitions in the process export JSON:
- art-resolve-repo-key (SIMPLE)
- art-resolve-branch-base (SIMPLE)
- art-resolve-wd (SIMPLE)
- art-resolve-repo-key-out (SIMPLE)
- art-resolve-base-branch (SIMPLE)
- art-resolve-base-sha (SIMPLE)
- art-branch-label (SIMPLE)
- art-branch-ctx-id (SIMPLE)
- art-branch-allow-reuse (SIMPLE)
- art-branch-wd (SIMPLE)
- art-branch-name (SIMPLE)
- art-branch-repo-key (SIMPLE)
- art-code-task (SIMPLE)
- art-code-success (SIMPLE)
- art-code-output (SIMPLE)
- art-code-commit (SIMPLE)
- art-code-is-stuck (SIMPLE)
"""

from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel, Field


@dataclass
class HumanTaskResult:
    """
    Return type for HUMAN step activities.

    HUMAN steps create a task in the database and return immediately.
    The workflow waits for a signal when the human completes the task.
    """
    task_id: str
    temporal_signal_id: str


@dataclass
class ResolveReadWorkspaceRepositoryKey:
    """
    Resolve Read Workspace - Repository Key artifact (art-resolve-repo-key).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class ResolveReadWorkspaceBaseBranchOverride:
    """
    Resolve Read Workspace - Base Branch Override artifact (art-resolve-branch-base).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class ResolveReadWorkspaceWorkingDirectory:
    """
    Resolve Read Workspace - Working Directory artifact (art-resolve-wd).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class ResolveReadWorkspaceRepositoryKey:
    """
    Resolve Read Workspace - Repository Key (out) artifact (art-resolve-repo-key-out).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class ResolveReadWorkspaceBaseBranch:
    """
    Resolve Read Workspace - Base Branch artifact (art-resolve-base-branch).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class ResolveReadWorkspaceBaseSHA:
    """
    Resolve Read Workspace - Base SHA artifact (art-resolve-base-sha).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchContextLabel:
    """
    Create Feature Branch - Context Label artifact (art-branch-label).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchContextID:
    """
    Create Feature Branch - Context ID artifact (art-branch-ctx-id).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchAllowBranchReuse:
    """
    Create Feature Branch - Allow Branch Reuse artifact (art-branch-allow-reuse).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchWorkingDirectory:
    """
    Create Feature Branch - Working Directory artifact (art-branch-wd).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchBranchName:
    """
    Create Feature Branch - Branch Name artifact (art-branch-name).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class CreateFeatureBranchRepositoryKey:
    """
    Create Feature Branch - Repository Key artifact (art-branch-repo-key).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class GateProbeCodeTask:
    """
    Gate Probe Code - Task artifact (art-code-task).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class GateProbeCodeSuccess:
    """
    Gate Probe Code - Success artifact (art-code-success).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class GateProbeCodeOutput:
    """
    Gate Probe Code - Output artifact (art-code-output).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class GateProbeCodeCommitSHA:
    """
    Gate Probe Code - Commit SHA artifact (art-code-commit).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None


@dataclass
class GateProbeCodeIsStuck:
    """
    Gate Probe Code - Is Stuck artifact (art-code-is-stuck).

    Definition type: SIMPLE

    Add fields as needed for your implementation.
    """

    # TODO: Add fields for this artifact
    # Example fields (customize based on your needs):
    id: str
    data: Any = None

