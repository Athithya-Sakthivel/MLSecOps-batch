from __future__ import annotations

from flytekit import CronSchedule, LaunchPlan, workflow

from workflows.ELT.tasks.maintenance_optimize import MaintenanceResult, maintenance_optimize
from workflows.ELT.workflows.elt_workflow import elt_workflow

__all__ = [
    "ELT_WORKFLOW_LP",
    "ELT_WORKFLOW_LP_NAME",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_DAILY_LP_NAME",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
    "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
    "iceberg_maintenance_workflow",
]


@workflow
def iceberg_maintenance_workflow() -> MaintenanceResult:
    return maintenance_optimize()

# Default manual entrypoint for ELT.

ELT_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=elt_workflow,
    name="elt_workflow_lp",
)
ELT_WORKFLOW_LP_NAME = ELT_WORKFLOW_LP.name

# Daily maintenance: cleanup only.
ICEBERG_MAINTENANCE_DAILY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_daily_lp",
    schedule=CronSchedule(schedule="30 2 * * *"),
)
ICEBERG_MAINTENANCE_DAILY_LP_NAME = ICEBERG_MAINTENANCE_DAILY_LP.name

# Weekly maintenance: cleanup only for now; keep the schedule separate.
ICEBERG_MAINTENANCE_WEEKLY_LP = LaunchPlan.get_or_create(
    workflow=iceberg_maintenance_workflow,
    name="iceberg_maintenance_weekly_lp",
    schedule=CronSchedule(schedule="30 3 * * 0"),
)
ICEBERG_MAINTENANCE_WEEKLY_LP_NAME = ICEBERG_MAINTENANCE_WEEKLY_LP.name