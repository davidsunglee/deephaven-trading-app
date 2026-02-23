"""
Workflow orchestration — durable workflows with pluggable backends.

Users import only WorkflowEngine, WorkflowHandle, and WorkflowStatus.
The concrete backend (e.g. DBOS) is an implementation detail.
"""

from workflow.engine import WorkflowEngine, WorkflowHandle, WorkflowStatus

__all__ = ["WorkflowEngine", "WorkflowHandle", "WorkflowStatus"]
