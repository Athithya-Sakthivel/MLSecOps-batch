# Medallion Lakehouse (Bronze → Silver → Gold) + ML Feature Store–like contract + Flyte orchestration
## End-to-end runtime model

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Spark)   ───────► all data movement, joins, feature generation, and Iceberg maintenance
Storage (Iceberg)    ───────► table state, snapshots, transactions
Object store (S3)    ───────► immutable data files
Catalog DB           ───────► Iceberg metadata/state
```

Flyte tasks run in Kubernetes pods. The Spark task plugin provides the Spark session and Spark runtime the task needs. `pyflyte run --remote` packages the workflow code locally, registers it, and launches a remote execution on the Flyte backend. Task code is versioned separately from the runtime image, and execution names are derived deterministically from the git SHA plus a UTC timestamp.

## 1) Core contracts

### Single runtime image
One shared runtime image is used by all task files.

It contains:
- Python runtime
- Flytekit
- Spark
- Iceberg jars
- Hadoop S3 support
- Python libraries needed by the bronze extractor and downstream Spark tasks

It does not contain application source code.

### Code delivery
Workflow code is shipped by Flyte at execution time, not baked into the image.

The image is the runtime; the repository snapshot is the code.

### Versioning
- Image version is separate from code version.
- Code lineage is tracked by git SHA.
- Execution names are generated from git SHA + timestamp for traceability.

## 2) Final file-by-file plan

| File | Responsibility | Inputs | Outputs | Bottleneck rule |
|---|---|---|---|---|
| `bronze_ingest.py` | Pull validated sources, normalize minimally, and land raw Bronze tables in Iceberg | Remote trip dataset + taxi zone lookup | `iceberg.bronze.*` tables | This is the only stage that touches external datasets |
| `silver_transform.py` | Build the canonical, cleaned trip fact table by joining Bronze facts and dimensions | Bronze tables | `iceberg.silver.trip_canonical` | All enrichment happens in Spark, not in Flyte control logic |
| `gold_features.py` | Build the frozen, model-ready training matrix and contract artifacts for LightGBM / ONNX | Silver table | `iceberg.gold.trip_training_matrix` and `iceberg.gold.trip_training_contracts` | Features must be point-in-time safe and deterministic |
| `maintenance_optimize.py` | Maintain Iceberg health with snapshot expiration, orphan cleanup, and optional compaction | Bronze, Silver, Gold tables | No business-data output | Runs on its own cadence; never blocks ingestion |
| `elt_workflow.py` | Orchestrate Bronze → Silver → Gold | Task outputs | Execution result | Keep it thin; no transformation logic here |
| `iceberg_maintenance_workflow.py` | Orchestrate Iceberg maintenance only | None or table config | Maintenance result | Keep it detached from the ELT hot path |
| `launch_plans.py` | Define launch plans for manual and scheduled execution | Workflows | Launch plans | Keep schedules and fixed inputs in one place |

## 3) Data contract for the source datasets

The source datasets are treated as a fact/dimension pair:

- trips table contains `PULocationID` and `DOLocationID`
- taxi zone lookup contains `LocationID`

Join rules:
- `trips.PULocationID -> zones.LocationID`
- `trips.DOLocationID -> zones.LocationID`

The Silver table must be a **trip-level canonical table**, not an analytics aggregate.

## 4) Exact behavior of each task

### `bronze_ingest.py`
Purpose: land raw data with minimal transformation.

Rules:
- validate the source URIs before Spark work begins
- extract the two datasets once
- write them to Iceberg Bronze tables with idempotent semantics
- preserve lineage metadata such as `run_id`, `ingestion_ts`, `source_uri`, `source_revision`, `source_kind`, and `source_file`
- only normalize column names and do the smallest required type cleanup

This task should fail fast if the raw source is inaccessible. It must not contain joins, feature logic, or maintenance logic.

### `silver_transform.py`
Purpose: produce the canonical trip dataset.

Rules:
- read only from Bronze Iceberg tables
- join the trips table to the taxi zone lookup twice:
  - pickup enrichment via `PULocationID`
  - dropoff enrichment via `DOLocationID`
- produce stable canonical columns such as:
  - trip duration
  - pickup hour
  - day-of-week
  - distance
  - fare/tip/total amounts
  - pickup/dropoff borough and zone fields
- write deterministically so retries do not duplicate or corrupt the table

This is the canonical MLOps dataset. It should stay narrow, reproducible, and schema-stable.

### `gold_features.py`
Purpose: produce the frozen model-ready matrix and the contract artifacts used by training and ONNX inference.

Rules:
- read only from Silver Iceberg tables
- enforce a fixed column order, dtypes, and null policy
- use point-in-time-safe aggregates only
- convert categorical values to stable integer IDs
- keep the label explicit and singular for the dataset version
- write both the training matrix and the contract table
- include schema hash, encoding mappings, feature spec, and label spec

Gold must be the exact feature matrix that training and ONNX inference consume identically.

### `maintenance_optimize.py`
Purpose: keep Iceberg healthy.

Iceberg snapshots accumulate with each write/update/delete/compaction. Regular snapshot expiration removes unneeded data files and keeps metadata small. `remove_orphan_files` cleans up files left behind by failed tasks or aborted jobs. `rewrite_data_files` compacts small files into larger ones and can remove dangling deletes.

Use these maintenance actions:
- `CALL ... system.expire_snapshots(...)`
- `CALL ... system.remove_orphan_files(...)`
- `CALL ... system.rewrite_data_files(...)`

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
- one manual launch plan for `elt_workflow`
- one daily maintenance launch plan for snapshot expiration and orphan cleanup
- one weekly maintenance launch plan for selected compaction jobs

## 5) Runtime and execution model

Use the Spark task pattern Flyte documents:
- task config carries Spark settings
- `hadoop_conf` injects S3 access details for Spark-side I/O
- the Spark plugin provides the Spark infrastructure the task needs by the time the task function runs

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

- Only `bronze_ingest.py` talks to external raw datasets.
- Only `silver_transform.py` does dataset joins and canonicalization.
- Only `gold_features.py` does ML feature engineering and contract freezing.
- Only `maintenance_optimize.py` does table hygiene.
- The workflow files must stay thin.
- Rebuild the shared image only when task dependencies change, not for workflow wiring changes.
- Keep Spark jobs bounded and task-scoped.
- Do not move long-running table cleanup into the ingestion path.
- Do not use maintenance as a dependency of the ELT workflow if it can be scheduled separately.

## 7) Final invariants

These should always hold:

- Flyte orchestrates.
- Spark executes.
- Iceberg owns table state.
- S3 stores immutable files.
- Bronze is raw landing.
- Silver is canonical curation.
- Gold is the frozen model matrix.
- Maintenance is file/snapshot hygiene.
- The runtime image is shared.
- Application code is external and versioned by git SHA.

## 8) Local submission model

`run.sh` is a local operator script.

It should:
- activate `.venv_elt`
- lint the ELT tree with Ruff before submit
- start the Flyte Admin port-forward
- initialize `flytectl`
- submit the selected workflow remotely with `pyflyte run --remote`

The local virtual environment is used only for submission-time packaging and linting. The runtime dependencies needed inside Flyte task pods must be present in the task image.

## 9) Operational recommendation

Use two separate execution modes:

- **ELT workflow**
  - manual or on a business schedule
  - Bronze → Silver → Gold

- **Maintenance workflow**
  - separate daily and weekly launch plans
  - daily: snapshot expiration + orphan cleanup
  - weekly: optional compaction for selected tables only

This keeps the ELT path fast and stable while preserving Iceberg health on a separate cadence.
