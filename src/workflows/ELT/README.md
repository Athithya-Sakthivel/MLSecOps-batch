# Medallion Lakehouse (Bronze → Silver → Gold) + ML Feature Store–like contract + Flyte orchestration

This package implements a three-stage ELT pipeline with a separate Iceberg maintenance path.

- **Flyte** provides orchestration and execution contracts.
- **Spark** performs all data processing.
- **Iceberg** stores Bronze, Silver, Gold, and contract tables.
- **S3** stores immutable data files.
- **Launch plans** define execution entrypoints and schedules.

## 1) System model

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Spark)   ───────► extraction, joins, feature generation, maintenance
Storage (Iceberg)    ───────► table state, snapshots, transactions
Object store (S3)    ───────► immutable data files
Catalog DB           ───────► Iceberg metadata/state
````

Flyte tasks run in Kubernetes pods. The Spark task plugin supplies the Spark session and runtime context used by each task.

The implementation is organized around:

* `workflows/elt_workflow.py`
* `workflows/iceberg_maintenance_workflow.py`
* `tasks/bronze_ingest.py`
* `tasks/silver_transform.py`
* `tasks/gold_features.py`
* `tasks/maintenance_optimize.py`
* `launch_plans.py`

## 2) Implemented file responsibilities

### `tasks/bronze_ingest.py`

Bronze ingestion is the raw landing stage.

Implemented behavior:

* validates the Iceberg REST endpoint before data extraction
* validates the Spark Iceberg catalog configuration
* validates the HTTP source for the taxi zone lookup
* streams the trips source and taxi zone source once
* normalizes column names
* converts values to the Bronze schema with minimal type cleanup
* preserves lineage metadata:

  * `run_id`
  * `ingestion_ts`
  * `source_uri`
  * `source_revision`
  * `source_kind`
  * `source_file`
* writes the trips source to a Bronze Iceberg table in batches
* writes the taxi zone source to a Bronze Iceberg table
* deletes any existing Bronze trip rows for the current `run_id` before rewriting them, so reruns are idempotent for that execution identifier
* creates Bronze/Silver/Gold namespaces if they do not already exist

The task exposes a `BronzeIngestResult` containing:

* `run_id`
* Bronze trips table identifier
* Bronze taxi zone table identifier
* row counts
* source references
* write modes

### `tasks/silver_transform.py`

Silver transform produces the canonical trip-level dataset.

Implemented behavior:

* reads only from Bronze Iceberg tables
* filters Bronze trips to the current `run_id`
* reads the taxi zone lookup from Bronze
* validates the Bronze schemas
* enriches trips twice:

  * pickup enrichment through `pickup_location_id`
  * dropoff enrichment through `dropoff_location_id`
* derives a canonical trip schema with:

  * `trip_id`
  * `pickup_date`
  * `pickup_ts`
  * `dropoff_ts`
  * `pickup_hour`
  * `pickup_dow`
  * `pickup_month`
  * `pickup_is_weekend`
  * `trip_duration_seconds`
  * `trip_duration_minutes`
  * pickup/dropoff borough, zone, and service-zone fields
  * fare and distance fields
  * lineage fields from Bronze
  * `bronze_run_id`
  * `silver_run_id`
* computes `trip_id` as a stable SHA-256 hash over the core trip attributes
* filters invalid rows:

  * null timestamps
  * null location IDs
  * non-positive trip duration
  * negative distance
  * negative fare
  * negative total amount
* deduplicates canonical rows on `trip_id`
* writes the Silver table partitioned by `pickup_date`
* creates the Silver namespace if required

The task exposes a `SilverTransformResult` containing:

* `run_id`
* Silver table identifier
* upstream Bronze table identifiers
* write mode
* status

### `tasks/gold_features.py`

Gold feature generation produces the frozen ML training matrix and the associated contract table.

Implemented behavior:

* reads only from the Silver Iceberg table
* filters Silver rows to the current `run_id`
* carries lineage fields into the training matrix:

  * `trip_id`
  * `pickup_ts`
  * `as_of_ts`
  * `as_of_date`
  * `schema_version`
  * `feature_version`
* converts the Silver table into a fixed model-ready schema
* encodes borough categories with a fixed integer mapping
* encodes service-zone categories with a versioned lookup derived from the observed Silver values
* encodes route pairs into a fixed hash bucket space
* computes point-in-time-safe aggregates only:

  * `avg_duration_7d_zone_hour`
  * `avg_fare_30d_zone`
  * `trip_count_90d_zone_hour`
* uses `pickup_ts` as the feature as-of timestamp
* derives the final ordered output column set used for training and inference
* enforces label and schema validity
* validates:

  * column presence
  * `pickup_ts == as_of_ts`
  * `as_of_date == date(pickup_ts)`
  * uniqueness of `trip_id`
  * categorical and count ranges
  * label range
* writes the training matrix to the Gold table partitioned by `as_of_date`
* writes the contract table partitioned by `feature_version`
* records contract metadata including:

  * `schema_hash`
  * feature specification
  * encoding specification
  * aggregate specification
  * label specification
  * source Silver table
  * source Silver snapshot ID
  * training row count
* validates contract stability for an existing feature version before writing a new contract row

The task exposes a `GoldFeatureResult` containing:

* `run_id`
* Gold training table identifier
* Gold contract table identifier
* upstream Silver table identifier
* upstream Silver snapshot ID
* training row count
* schema hash
* feature version
* schema version
* write mode
* status

### `tasks/maintenance_optimize.py`

Maintenance is isolated from the ELT hot path.

Implemented behavior:

* validates the Iceberg catalog before running maintenance actions
* targets a configurable set of tables for:

  * snapshot expiration
  * orphan file cleanup
  * optional data-file rewrite
* uses UTC cutoffs for maintenance windows
* expires snapshots with:

  * `CALL ... system.expire_snapshots(...)`
* removes orphan files with:

  * `CALL ... system.remove_orphan_files(...)`
* rewrites data files with:

  * `CALL ... system.rewrite_data_files(...)`
* defaults compaction to cold partitions by using a rewrite predicate that excludes recent dates
* keeps compaction opt-in
* skips rewrite when a table lacks `as_of_date`
* records per-table operation results
* fails fast unless `MAINTENANCE_CONTINUE_ON_ERROR` is enabled

The task exposes a `MaintenanceResult` containing:

* `run_id`
* overall status
* expired table list
* rewritten table list
* skipped table list
* failed table list
* JSON-encoded table results
* retention windows

## 3) Workflow behavior

### `workflows/elt_workflow.py`

This workflow is the ELT hot path.

Implemented sequence:

1. `bronze_ingest`
2. `silver_transform`
3. `gold_features`

The workflow is thin. It contains orchestration only.

### `workflows/iceberg_maintenance_workflow.py`

This workflow is the Iceberg housekeeping path.

Implemented sequence:

1. `maintenance_optimize`

The workflow is thin. It contains orchestration only.

## 4) Launch plans

`launch_plans.py` centralizes Flyte execution entrypoints.

Implemented launch-plan responsibilities:

* manual ELT execution entrypoint
* scheduled maintenance execution entrypoints
* separation of ELT and maintenance schedules

Launch plans are the execution contract layer. They are not data-processing logic.

## 5) Runtime and execution boundaries

Implemented runtime boundaries:

* **Flyte** starts and schedules tasks and workflows
* **Spark** executes all data transformations and Iceberg maintenance
* **Iceberg** owns table state and snapshot history
* **S3** stores immutable data files
* **The shared runtime image** supplies Python, Flytekit, Spark, Iceberg jars, Hadoop S3 support, and task dependencies

The task configuration supplies:

* Spark driver and executor settings
* Spark Hadoop configuration for S3 access
* task-level resource limits
* execution environment variables

## 6) Data layout and contract behavior

### Bronze

Bronze stores raw source data with minimal normalization and explicit lineage fields.

### Silver

Silver stores a canonical trip-level dataset with deterministic joins and stable columns.

### Gold

Gold stores the frozen model-ready matrix and the contract artifacts that describe it.

### Contract table

The contract table records:

* feature version
* schema version
* schema hash
* model family
* inference runtime
* Gold table identifier
* source Silver table identifier
* source Silver snapshot ID
* training row count
* output column order
* feature specification
* encoding specification
* aggregate specification
* label specification
* creation timestamp

## 7) Invariants

The implementation is organized around the following invariants:

* Flyte orchestrates.
* Spark computes.
* Iceberg stores table state.
* S3 stores immutable files.
* Bronze is raw landing.
* Silver is canonical curation.
* Gold is the frozen training matrix.
* Maintenance is separated from ingestion and feature generation.
* Launch plans define execution entrypoints.
* Workflows define ordering only.
* Tasks own data behavior.

## 8) Operational separation

The current implementation does not use a single monolithic runner. Execution is split across:

* task modules
* workflow modules
* launch-plan definitions

This keeps runtime behavior, orchestration, and data logic separated at the file boundary.
