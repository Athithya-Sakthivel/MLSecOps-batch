# src/workflows/ELT/launch_plans.py
from __future__ import annotations

from flytekit import CronSchedule, LaunchPlan

from workflows.ELT.workflows.elt_workflow import elt_workflow
from workflows.ELT.workflows.iceberg_maintenance_workflow import iceberg_maintenance_workflow

# Default manual entrypoint for the ELT workflow.
# Flyte already generates a default launch plan for each registered workflow;
# this explicit constant keeps the codebase readable and importable.
ELT_WORKFLOW_LP = LaunchPlan.get_or_create(workflow=elt_workflow)

# Daily Iceberg maintenance: snapshot expiration + orphan-file cleanup.
# Schedule changes should be treated as a new launch-plan version/name.
ICEBERG_MAINTENANCE_DAILY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_daily_lp",
    schedule=CronSchedule(schedule="30 2 * * *"),
)

# Weekly Iceberg maintenance: optional rewrite/compaction for selected tables.
ICEBERG_MAINTENANCE_WEEKLY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_weekly_lp",
    schedule=CronSchedule(schedule="30 3 * * 0"),
)

__all__ = [
    "ELT_WORKFLOW_LP",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
]