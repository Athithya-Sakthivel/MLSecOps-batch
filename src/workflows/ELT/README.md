# Medallion Lakehouse (Bronze → Silver → Gold) + ML Feature Store–like contract + Flyte orchestration

## End-to-end runtime model

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Spark)   ───────► all data movement, joins, feature generation, and Iceberg maintenance
Storage (Iceberg)    ───────► table state, snapshots, transactions
Object store (S3)    ───────► immutable data files
Catalog DB           ───────► Iceberg metadata/state
```

Flyte tasks run in Kubernetes pods, and the Spark task plugin provides the Spark session and Spark runtime needed by each task. The code is organized around registered workflows and launch plans: Flyte generates a default launch plan for each workflow when it is registered, and launch plans are the invocation mechanism for executions. Launch plans can also define fixed inputs, schedules, and other execution-time settings. ([Flyte][1])

## 1) Core contracts

### Single runtime image

One shared runtime image is used by all task files.

It contains:

* Python runtime
* Flytekit
* Spark
* Iceberg jars
* Hadoop S3 support
* Python libraries needed by the bronze extractor and downstream Spark tasks

### Versioning

* Image version is separate from code version.
* Code lineage is tracked by git SHA.
* Execution lineage is tracked through Flyte execution IDs and registered workflow/launch-plan versions.

## 2) Final file-by-file plan

| File                                                          | Final responsibility                                                  | Must contain                                                                                                                                 | Must not contain                                                  |
| ------------------------------------------------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `src/workflows/ELT/launch_plans.py`                           | Single source of truth for Flyte execution entrypoints and schedules. | `LaunchPlan` definitions for manual ELT and scheduled maintenance; fixed defaults/schedules; exports.                                        | Task code, workflow orchestration logic, operational shell logic. |
| `src/workflows/ELT/tasks/bronze_ingest.py`                    | Bronze landing task.                                                  | Source validation, extraction, minimal normalization, idempotent Bronze writes, lineage metadata.                                            | Joins, feature engineering, maintenance, workflow orchestration.  |
| `src/workflows/ELT/tasks/silver_transform.py`                 | Silver canonicalization task.                                         | Bronze-to-Silver joins, deterministic trip-level canonical row construction, stable schema writes.                                           | External source access, feature contract freezing, maintenance.   |
| `src/workflows/ELT/tasks/gold_features.py`                    | Gold feature-contract task.                                           | Frozen model-ready features, fixed schema/order/dtypes/null policy, point-in-time-safe aggregates, categorical encoding, contract artifacts. | Raw ingestion, ad hoc training logic, maintenance.                |
| `src/workflows/ELT/tasks/maintenance_optimize.py`             | Iceberg housekeeping task.                                            | Snapshot expiration, orphan cleanup, optional compaction/rewrite operations.                                                                 | Business-data transforms, ELT orchestration, feature creation.    |
| `src/workflows/ELT/workflows/elt_workflow.py`                 | Thin ELT orchestration DAG.                                           | The sequence `bronze_ingest -> silver_transform -> gold_features`.                                                                           | Business logic, source validation, maintenance, scheduling.       |
| `src/workflows/ELT/workflows/iceberg_maintenance_workflow.py` | Thin maintenance orchestration DAG.                                   | The maintenance task invocation only.                                                                                                        | ELT steps, feature logic, external source ingestion.              |
| `src/workflows/ELT/ops/register_elt_setup.sh`                 | Operator command for cluster bootstrap and registration.              | Namespace bootstrap, Spark ServiceAccount/RBAC, registration of tasks/workflows/launch plans, activation of scheduled launch plans.          | Running data jobs, debugging executions, cleanup logic.           |
| `src/workflows/ELT/ops/execute_launch_plans.sh`               | Operator command for triggering executions from launch plans.         | Launch-plan-based execution submission.                                                                                                      | Registration, code changes, task internals.                       |
| `src/workflows/ELT/ops/elt_lifecycle_manage.sh`               | Operator command for lifecycle management.                            | Diagnose, inspect, cleanup, zombie execution / SparkApplication handling.                                                                    | Business logic, registration, workflow definition.                |

### Canonical ownership

* Tasks own data work.
* Workflows own orchestration only.
* Launch plans own execution contracts and schedules.
* Ops scripts own bootstrap, registration, execution triggering, diagnosis, and cleanup.

### Final rule

If a file changes **data behavior**, it belongs in `tasks/`.

If a file changes **step ordering**, it belongs in `workflows/`.

If a file changes **how/when Flyte runs things**, it belongs in `launch_plans.py`.

If a file changes **how operators interact with the system**, it belongs in `ops/`.

## 3) Data contract for the source datasets

The source datasets are treated as a fact/dimension pair:

* trips table contains `PULocationID` and `DOLocationID`
* taxi zone lookup contains `LocationID`

Join rules:

* `trips.PULocationID -> zones.LocationID`
* `trips.DOLocationID -> zones.LocationID`

The Silver table must be a trip-level canonical table, not an analytics aggregate.

## 4) Exact behavior of each task

### `bronze_ingest.py`

Purpose: land raw data with minimal transformation.

Rules:

* validate the source URIs before Spark work begins
* extract the two datasets once
* write them to Iceberg Bronze tables with idempotent semantics
* preserve lineage metadata such as `run_id`, `ingestion_ts`, `source_uri`, `source_revision`, `source_kind`, and `source_file`
* only normalize column names and do the smallest required type cleanup

This task should fail fast if the raw source is inaccessible. It must not contain joins, feature logic, or maintenance logic.

### `silver_transform.py`

Purpose: produce the canonical trip dataset.

Rules:

* read only from Bronze Iceberg tables
* join the trips table to the taxi zone lookup twice:

  * pickup enrichment via `PULocationID`
  * dropoff enrichment via `DOLocationID`
* produce stable canonical columns such as:

  * trip duration
  * pickup hour
  * day-of-week
  * distance
  * fare/tip/total amounts
  * pickup/dropoff borough and zone fields
* write deterministically so retries do not duplicate or corrupt the table

This is the canonical MLOps dataset. It should stay narrow, reproducible, and schema-stable.

### `gold_features.py`

Purpose: produce the frozen model-ready matrix and the contract artifacts used by training and ONNX inference.

Rules:

* read only from Silver Iceberg tables
* enforce a fixed column order, dtypes, and null policy
* use point-in-time-safe aggregates only
* convert categorical values to stable integer IDs
* keep the label explicit and singular for the dataset version
* write both the training matrix and the contract table
* include schema hash, encoding mappings, feature spec, and label spec

Gold must be the exact feature matrix that training and ONNX inference consume identically.

### `maintenance_optimize.py`

Purpose: keep Iceberg healthy.

Iceberg snapshots accumulate with each write/update/delete/compaction. Regular snapshot expiration removes unneeded data files and keeps metadata small. `remove_orphan_files` cleans up files left behind by failed tasks or aborted jobs. `rewrite_data_files` compacts small files into larger ones and can remove dangling deletes.

Use these maintenance actions:

* `CALL ... system.expire_snapshots(...)`
* `CALL ... system.remove_orphan_files(...)`
* `CALL ... system.rewrite_data_files(...)`

Compaction is opt-in per table and should only run when a table-specific predicate is supplied.

### `elt_workflow.py`

Purpose: be the thin orchestration layer for the ELT hot path.

Recommended flow:

1. `bronze_ingest`
2. `silver_transform`
3. `gold_features`

If maintenance needs to run on a separate cadence, it should not be embedded in this workflow.

### `iceberg_maintenance_workflow.py`

Purpose: be the thin orchestration layer for Iceberg housekeeping only.

Recommended flow:

1. `maintenance_optimize`

This should be scheduled independently from ELT so compaction never becomes part of the ingestion critical path.

### `launch_plans.py`

Purpose: centralize launch-plan definitions.

Recommended launch plans:

* one manual launch plan for `elt_workflow`
* one daily maintenance launch plan for snapshot expiration and orphan cleanup
* one weekly maintenance launch plan for selected compaction jobs

## 5) Runtime and execution model

Use the Spark task pattern Flyte documents:

* task config carries Spark settings
* `hadoop_conf` injects S3 access details for Spark-side I/O
* the Spark plugin provides the Spark infrastructure the task needs by the time the task function runs

That gives this runtime shape:

```text
Flyte workflow
  -> Bronze task pod
  -> Spark work for bronze
  -> Silver task pod
  -> Spark work for silver
  -> Gold task pod
  -> Spark work for training matrix generation
```

Maintenance runs separately:

```text
Flyte maintenance workflow
  -> Maintenance task pod
  -> Spark work for Iceberg lifecycle operations
```

## 6) Bottleneck rules

* Only `bronze_ingest.py` talks to external raw datasets.
* Only `silver_transform.py` does dataset joins and canonicalization.
* Only `gold_features.py` does ML feature engineering and contract freezing.
* Only `maintenance_optimize.py` does table hygiene.
* The workflow files must stay thin.
* Rebuild the shared image only when task dependencies change, not for workflow wiring changes.
* Keep Spark jobs bounded and task-scoped.
* Do not move long-running table cleanup into the ingestion path.
* Do not use maintenance as a dependency of the ELT workflow if it can be scheduled separately.

## 7) Final invariants

These should always hold:

* Flyte orchestrates.
* Spark executes.
* Iceberg owns table state.
* S3 stores immutable files.
* Bronze is raw landing.
* Silver is canonical curation.
* Gold is the frozen model matrix.
* Maintenance is file/snapshot hygiene.
* The runtime image is shared.
* Application code is external and versioned by git SHA.

## 8) Local operator model

The current local operator model is split across three scripts:

* `register_elt_setup.sh` for bootstrap and registration
* `execute_launch_plans.sh` for execution submission
* `elt_lifecycle_manage.sh` for diagnosis and cleanup

That is the current replacement for the old `run.sh` pattern.

## 9) Two separate execution modes:

* ELT workflow

  * manual or on a business schedule
  * Bronze → Silver → Gold

* Maintenance workflow

  * separate daily and weekly launch plans
  * daily: snapshot expiration + orphan cleanup
  * weekly: optional compaction for selected tables only

This keeps the ELT path fast and stable while preserving Iceberg health on a separate cadence.

[1]: https://docs-legacy.flyte.org/en/latest/user_guide/basics/launch_plans.html "Launch plans"
