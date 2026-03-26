from __future__ import annotations

from flytekit import CronSchedule, LaunchPlan

from workflows.ELT.workflows.elt_workflow import elt_workflow
from workflows.ELT.workflows.iceberg_maintenance_workflow import iceberg_maintenance_workflow

# Manual / ad-hoc launch plan for the ELT workflow.
# This is the entry point for one-off executions of Bronze -> Silver -> Gold.
# No schedule is attached here; it is intended to be triggered explicitly.
ELT_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=elt_workflow,
    name="elt_workflow_lp",
)

# Daily Iceberg maintenance launch plan.
# Runs snapshot expiration and orphan-file cleanup on a low-cost cadence.
# The schedule is intentionally separate from the ELT workflow so maintenance
# never becomes part of the ingestion critical path.
ICEBERG_MAINTENANCE_DAILY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_daily_lp",
    schedule=CronSchedule(schedule="30 2 * * *"),
)

# Weekly Iceberg maintenance launch plan.
# Use this for heavier storage hygiene such as optional compaction / rewrite
# operations, usually on the Gold table only.
# Keeping this separate lets you tune the cadence without affecting ELT latency.
ICEBERG_MAINTENANCE_WEEKLY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_weekly_lp",
    schedule=CronSchedule(schedule="30 3 * * 0"),
)

# Explicit exports for registration / import clarity.
__all__ = [
    "ELT_WORKFLOW_LP",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
]
