from __future__ import annotations

from flytekit import workflow

from workflows.ELT.tasks.maintenance_optimize import MaintenanceResult, maintenance_optimize


@workflow
def iceberg_maintenance_workflow() -> MaintenanceResult:
    return maintenance_optimize()