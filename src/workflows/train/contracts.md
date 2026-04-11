# One contract across ELT, train, eval, register, and inference

The system should use one frozen matrix contract end to end.

ELT materializes the matrix, training consumes the matrix, evaluation consumes the same matrix, registration records the matrix bundle, and deployment loads that bundle and only post-processes model outputs.

The 14 matrix feature columns are the only feature interface that should flow through training and inference:

`pickup_hour, pickup_dow, pickup_month, pickup_is_weekend, pickup_borough_id, pickup_zone_id, pickup_service_zone_id, dropoff_borough_id, dropoff_zone_id, dropoff_service_zone_id, route_pair_id, avg_duration_7d_zone_hour, avg_fare_30d_zone, trip_count_90d_zone_hour`

The ELT provenance table, `trip_training_contracts`, is not a feature-engineering source for inference. It is the provenance source for metadata, contract validation, and bundle lineage.

That means the old “raw business features on the serving path” idea is not part of this system. The serving path should receive the same 14-column frozen matrix the gold training table already contains.

# 0) What ELT already gives us

Two ELT outputs matter:

`trip_training_matrix`
This is the frozen matrix table. It already contains exactly the 21 output columns that training should consume:

* metadata columns: `trip_id`, `pickup_ts`, `as_of_ts`, `as_of_date`, `schema_version`, `feature_version`
* 14 matrix columns: the exact 14 listed above
* label: `label_trip_duration_seconds`

`trip_training_contracts`
This is the provenance and schema-definition table. It contains:

* `run_id`
* `feature_version`
* `schema_version`
* `schema_hash`
* `model_family`
* `inference_runtime`
* `gold_table`
* `source_silver_table`
* `source_silver_snapshot_id`
* `training_row_count`
* `output_columns_json`
* `feature_spec_json`
* `encoding_spec_json`
* `aggregate_spec_json`
* `label_spec_json`
* `created_ts`

The training code should read this row, validate it, and mirror it into bundle metadata. It should not reinterpret it into a second serving schema.

# 1) `src/workflows/train/shared_utils.py`

This file should become the canonical contract module for training, evaluation, registration, and bundle construction.

## What should be defined here

Keep the matrix contract explicit and central:

* `MATRIX_FEATURE_COLUMNS`
* `REQUEST_FEATURE_COLUMNS` as an alias to the same 14-column list
* `ENGINEERED_FEATURE_COLUMNS` as an alias to the same 14-column list
* `MODEL_FEATURE_COLUMNS` as an alias to the same 14-column list for compatibility
* `OUTPUT_COLUMNS` for the 21 ELT columns in `trip_training_matrix`
* `TARGET_COLUMN = "label_trip_duration_seconds"`
* `TARGET_TRANSFORM = "log1p"`
* `MAX_PREDICTION_SECONDS = 24.0 * 3600.0`
* `PREPROCESSING_VERSION`, which in this contract is not a feature-engineering pipeline but a matrix-validation contract version

Keep the current validation and metrics functions, but make them matrix-native:

* `prepare_model_features(...)` should only verify the 14 matrix columns exist, coerce them to numeric, validate finiteness, and return them in exact order.
* It should not build derived features.
* It should not encode categories.
* It should not invent any second feature contract.

That is the key contract correction.

## Dataclasses to keep or add

The file should carry these dataclasses:

### `ELTContract`

This represents the provenance row mirrored from `trip_training_contracts`. It should include:

* `run_id`
* `feature_version`
* `schema_version`
* `schema_hash`
* `model_family`
* `inference_runtime`
* `gold_table`
* `source_silver_table`
* `source_silver_snapshot_id`
* `training_row_count`
* `output_columns_json`
* `feature_spec_json`
* `encoding_spec_json`
* `aggregate_spec_json`
* `label_spec_json`
* `created_ts`

### `BundleArtifactPlan`

This is the deployable bundle layout:

* `artifact_root_s3_uri`
* `model_s3_uri` → `.../model.onnx`
* `schema_s3_uri` → `.../schema.json`
* `metadata_s3_uri` → `.../metadata.json`
* `manifest_s3_uri` → `.../manifest.json`

### `BundleContract`

This is the serving contract for the matrix bundle:

* `schema_version`
* `feature_version`
* `target_transform`
* `feature_order`
* `input_name`
* `output_names`
* `allow_extra_features`

### `BundleMetadata`

This should hold the full contract and lineage payload, including:

* ELT contract values
* lineage
* bundle URIs
* training and evaluation metrics
* `label_cap_seconds`
* `train_label_p50_seconds`
* `best_iteration_inner`
* `final_num_boost_round`
* `train_rows`
* `test_rows`
* `model_name`
* `model_version`
* matrix feature order
* output column order
* any provenance JSON that must survive registration

## Helpers that should exist here

Add canonical serialization and hashing helpers:

* a stable JSON dump with sorted keys
* `sha256_text(...)`
* `sha256_file(...)`
* maybe `ordered_columns_hash(...)` for feature-order checks

Keep these helpers in this file so train, eval, and deploy all compute hashes the same way.

## Existing training utilities to keep

These remain valid and should stay here:

* `load_iceberg_table(...)`
* `table_snapshot_lineage(...)`
* `read_table_as_dataframe(...)`
* `validate_raw_dataframe(...)`
* `split_train_test_by_date(...)`
* `split_by_date_fraction(...)`
* `evenly_spaced_sample(...)`
* `build_category_levels(...)` if you want it purely for provenance
* `to_log_target(...)`
* `from_log_target(...)`
* `clip_seconds(...)`
* `compute_metrics(...)`
* `compute_baseline_metrics(...)`
* `export_onnx_model(...)`
* `build_artifact_plan(...)`
* `build_training_result(...)`

The important point is that any function touching model features must remain matrix-only. The old derived-feature expansion is not part of the contract anymore.

# 2) `src/workflows/train/tasks/train_model_task.py`

This task trains the model on the frozen matrix and publishes the bundle.

## Resource envelope

Use:

* requests: `cpu="2"`, `mem="3Gi"`
* limits: `cpu="3"`, `mem="3Gi"`

Also enforce in code:

* `train_num_threads` defaults to `2`
* `train_num_threads > 3` is rejected

That keeps the task under the quota that was causing problems without starving LightGBM to the point that it becomes unstable or impractically slow.

## What the task must do

### Step 1: load and validate ELT provenance

Before training, read `trip_training_contracts` and validate:

* `schema_version`
* `feature_version`
* `schema_hash`
* `model_family`
* `inference_runtime`

If any of those differ from the expected contract, fail early.

This is where the ELT contract gets mirrored into training metadata.

### Step 2: read the matrix table

Load `trip_training_matrix` and validate it exactly.

The dataframe should match the ELT output columns and row semantics. This means:

* exact column order
* non-null checks on required columns
* date consistency checks
* label positivity checks
* no unexpected nulls in the matrix columns
* numeric bounds checks on the matrix fields

### Step 3: train only on the 14 matrix columns

Do not rebuild derived features. Do not add a raw feature encoder. Do not re-derive ELT aggregates.

Train directly on the frozen matrix columns from `trip_training_matrix`.

That keeps train aligned with ELT.

### Step 4: preserve the log target contract

Keep the current target transform:

* train on `log1p(label_trip_duration_seconds)`
* retain `label_cap_seconds`
* retain `train_label_p50_seconds`
* evaluate metrics in seconds after inverse transform

The exported ONNX model should remain a log-target model, because deployment will post-process it back to seconds.

### Step 5: export ONNX from the final LightGBM model

The ONNX export should accept a single fixed input tensor with width 14.

The input name should be stable, ideally `"input"`, and the output should reflect log-space predictions.

### Step 6: emit the deployable bundle

The publishable unit should be a directory root containing exactly:

```text
model.onnx
schema.json
metadata.json
manifest.json
```

No separate runtime training summary file belongs inside the deployment bundle.

### What each file in the bundle means

#### `schema.json`

This is the serving contract for the frozen matrix.

It should contain:

* `schema_version`
* `feature_version`
* `target_transform = "log1p"`
* `input_name = "input"` unless ONNX requires something else
* `feature_order` = the exact 14 matrix columns
* `output_names`
* `allow_extra_features = false`

#### `metadata.json`

This is provenance, training context, and deployability metadata.

It should include:

* ELT contract values
* lineage
* `artifact_root_s3_uri`
* `model_name`
* `model_version`
* `schema_version`
* `feature_version`
* `target_transform`
* `label_cap_seconds`
* `train_rows`
* `test_rows`
* `best_iteration_inner`
* `final_num_boost_round`
* final holdout metrics
* baseline holdout metrics
* `feature_spec_json`
* `encoding_spec_json`
* `aggregate_spec_json`
* `label_spec_json`

This file should be rich enough for deployment to validate the bundle, but it should not encode a second feature-engineering path.

#### `manifest.json`

This is the integrity record.

It should contain:

* `format_version`
* `source_uri`
* `model_version`
* `model_sha256`
* `schema_sha256`
* `metadata_sha256`

### Step 7: validate locally before upload

Before any upload, verify:

* the ONNX file hash matches the declared digest
* `schema.json` is canonical and consistent with the model
* `metadata.json` contains the mirrored ELT contract
* the manifest hashes match the written files

### Step 8: parity check

Run a parity test before upload:

* native LightGBM prediction
* exported ONNX prediction

After inverse transform back to seconds, the values should match within a tight tolerance.

That is the final export correctness gate.

# 3) `src/workflows/train/tasks/evaluate_and_register_task.py`

This task evaluates the held-out matrix, logs to MLflow, and records registration metadata.

## Resource envelope

Use:

* requests: `cpu="1"`, `mem="2Gi"`
* limits: `cpu="2"`, `mem="3Gi"`

## What this task must consume

It must consume the same bundle root published by training:

* `model.onnx`
* `schema.json`
* `metadata.json`
* `manifest.json`

It should not reconstruct artifacts from separate paths or from any old summary file shape.

## Input and evaluation shape

The evaluation dataset should use the frozen matrix columns, exactly as training does.

So the MLflow signature input should be based on:

* `input_example = test_df[MATRIX_FEATURE_COLUMNS].head(...)`
* target = `label_trip_duration_seconds`

The evaluation dataset is the same matrix table; it is not a raw event table.

## PyFunc wrapper contract

`Log1pFrozenMatrixPyFuncModel` should:

* load the bundle root files
* validate schema, metadata, and manifest
* validate checksums
* open the ONNX session
* accept the frozen matrix input
* run ONNX inference
* inverse-transform predictions back to seconds
* clip to `MAX_PREDICTION_SECONDS`
* return a single prediction column

No derived-feature reconstruction belongs here.

No pandas-heavy feature engineering belongs here.

No `summary` file should be treated as the authoritative contract source.

The bundle metadata and schema are the contract source.

## Logging expectations

Log these MLflow params:

* bundle root URI
* model URI
* schema URI
* metadata URI
* manifest URI
* schema version
* feature version
* target transform
* request feature order hash
* engineered feature order hash
* ELT `schema_hash`
* `source_silver_snapshot_id`

Also log the bundle files as MLflow artifacts.

## Evaluation expectations

Keep `mlflow.models.evaluate(...)`, but evaluate on the frozen matrix input.

Log the holdout metrics and the baseline metrics.

The evaluation task should be the place where training output is recorded and lineage becomes discoverable in MLflow, but it should not alter the contract.

## Return value

The task should return JSON containing:

* `run_id`
* `model_uri`
* `artifact_plan`
* `evaluation_metrics`

Make sure the return path is valid serialization and not a broken placeholder.

# 4) `src/workflows/train/workflows/training_workflow.py`

This file should stay thin.

It should do only orchestration:

1. call `train_model_task(...)`
2. pass the returned `TrainingResult` to `evaluate_and_register_task(...)`
3. return the final MLflow run payload

It should not inspect artifact structure.
It should not apply any feature logic.
It should not do any bundle validation itself.

The contract belongs in `shared_utils.py`, training in `train_model_task.py`, and evaluation/registration in `evaluate_and_register_task.py`.

# 5) `src/workflows/train/launch_plans.py`

This file should only set conservative defaults.

Recommended defaults:

* `train_num_threads = 2`
* `tuning_sample_rows = 100_000`
* `max_boost_rounds = 5_000`
* `max_eval_rows = 100_000`
* `mlflow_experiment_name = "trip_eta_lgbm"`

The launch plan should not imply a 4+ CPU workflow, because that was the source of the quota issue.

The point of the launch plan is operational safety, not model design.

# 6) `src/workflows/deploy/model_store.py`

This is the matching deployment file and it should remain bundle-aware only.

## Bundle loading

`load_model_bundle(...)` should accept a directory root and validate the presence of:

* `model.onnx`
* `schema.json`
* `metadata.json`
* `manifest.json`

It should fail fast if anything is missing or mismatched.

## Manifest validation

It should validate:

* `model_sha256`
* `schema_sha256`
* `metadata_sha256`

It should also verify:

* `source_uri` matches the deployed bundle root
* `model_version` matches the expected version

## Session wrapper contract

This module should not create a raw ONNX session and hand it straight to `service.py`.

Instead, it should return a wrapper object that exposes the same session interface used by the service:

* `get_inputs()`
* `get_outputs()`
* `run(output_names, inputs)`

The wrapper should:

1. accept the frozen matrix input from FastAPI
2. pass it into the ONNX model
3. convert log-space predictions back to seconds
4. clip predictions to `MAX_PREDICTION_SECONDS`
5. return outputs that `service.py` can format directly

That keeps the service thin while still ensuring the deployed predictions are in the correct target space.

## Dependency discipline

Keep this module deployment-thin.

Do not pull in:

* pandas
* lightgbm
* pyiceberg

Use minimal runtime dependencies only, ideally `numpy` plus ONNX runtime.

## Important alignment

The deployment bundle schema must define the same 14 matrix columns in the same order used by ELT and training.

There should not be a second serving schema.

# 7) `src/workflows/deploy/service.py`

This file should remain a thin FastAPI ingress.

It should:

* accept the request body
* coerce it into rows
* validate it against the matrix contract
* build a dense numeric matrix
* trace the request
* call the wrapped model session
* split and return outputs

It should not define feature engineering.
It should not recreate ELT aggregates.
It should not interpret raw business features.

The request validation should use the 14 matrix columns exactly as the bundle schema defines them.

The service should stay operationally simple and should not own any contract logic beyond row validation, tracing, and response formatting.

# 8) `src/workflows/deploy/schemas.py`

This file should remain the request-shape validator.

It should ensure:

* every instance is an object
* every required matrix column exists
* no unknown columns are accepted unless explicitly allowed
* values are numeric
* values are finite
* booleans are rejected as numeric input

It should not do derived-feature engineering.

It should not transform the input into a different schema.

It should not re-create ELT logic.

The file’s job is input validation and matrix assembly only.

# 9) `src/workflows/deploy/config.py`

This file should stay as environment and deployment configuration.

It should continue to own:

* service identity
* deployment profile
* Ray Serve scaling defaults
* ONNX Runtime thread settings
* OTLP exporter settings
* log level settings
* model bundle path settings

It should not own any feature-contract logic.

It should not own any ELT schema logic.

## Resource and runtime settings to preserve

The deployment defaults already point to the right runtime idea:

* sane replica CPU defaults
* strict ONNX Runtime thread sizing
* explicit exporter endpoint configuration
* stable service identity

Those settings should remain configuration-only.

# 10) `src/workflows/deploy/telemetry.py`

This file should remain the telemetry bootstrapper.

It should continue to:

* configure traces
* configure metrics
* configure logs
* attach resource identity
* export to the collector
* validate OTLP settings
* manage shutdown cleanly

No feature-contract logic belongs here.
No model schema logic belongs here.
No inference math belongs here.

# 11) What should not drift anywhere

There are a few invariants that must stay true across all files:

1. The 14 matrix columns are the contract.
2. `trip_training_matrix` is the training and inference source of truth.
3. `trip_training_contracts` is provenance only.
4. Training uses `log1p` targets.
5. Deployment receives the frozen matrix and returns seconds.
6. The bundle root is immutable and versioned.
7. `schema.json` and `manifest.json` are required.
8. `metadata.json` carries provenance and evaluation context.
9. No file should create a second feature pipeline.
10. No file should infer model behavior from anything other than the bundle contract.

# Final corrected summary

The correct system is not “raw serving features plus deployment-side feature generation.”

The correct system is:

* ELT writes a frozen matrix
* training reads the frozen matrix
* evaluation reads the same frozen matrix
* registration writes an immutable bundle root
* deployment loads the bundle root
* service validates matrix rows and returns predictions
* the model session wrapper converts log-space outputs back to seconds

That is the contract that matches the code you have and keeps ELT, training, evaluation, registration, and inference synchronized without introducing a second feature path.
