# Flyte Training Workflow

This repository implements a Flyte-based training pipeline for a frozen-matrix LightGBM model that exports an ONNX bundle for deployment and evaluation. The workflow is intentionally split into a narrow orchestration layer, a training task, and a post-training evaluation/registration task.

## Purpose

The workflow exists to produce a versioned, checksum-validated model bundle and register the resulting run with MLflow. It is designed to keep the training path, evaluation path, and deployment bundle contract aligned with the ELT-produced gold table.

The central contract is:

* ELT materializes `gold.trip_training_matrix`
* training consumes the frozen matrix from that table
* evaluation uses the same matrix contract
* training exports a deployable ONNX bundle
* registration logs the bundle and metrics to MLflow

## Workflow entry point

The main Flyte workflow is `train(...)` in `src/workflows/train/workflows/training_workflow.py`.

It orchestrates two tasks:

1. `train_model_task(...)`
2. `evaluate_and_register_task(...)`

The workflow itself does not train the model, validate data, or manipulate artifacts. It only passes inputs and outputs between the tasks.

## Launch plan

The launch plan module is `src/workflows/train/launch_plans.py`.

It provides the default runtime values for the workflow and exports the symbol expected by the registration path:

* `TRAIN_WORKFLOW_LP_NAME`
* `TRAIN_WORKFLOW_LP`

Default values are conservative so the workflow stays within resource quota:

* `train_num_threads = 2`
* `tuning_sample_rows = 100_000`
* `max_boost_rounds = 5_000`
* `mlflow_experiment_name = "trip_eta_lgbm"`
* `max_eval_rows = 100_000`

## Training task

The training task is `train_model_task(...)` in `src/workflows/train/tasks/train_model_task.py`.

It performs the full model-building sequence:

1. Loads the ELT contract from `gold.trip_training_contracts`
2. Loads the frozen matrix from `gold.trip_training_matrix`
3. Validates schema, values, and label constraints
4. Splits the data into train/eval/test by date
5. Performs an inner split for hyperparameter search
6. Caps the label and applies the log target transform
7. Searches candidate LightGBM configurations
8. Trains the final model
9. Evaluates inner validation and holdout metrics
10. Builds the artifact plan
11. Exports ONNX
12. Writes and uploads the bundle files
13. Returns a JSON string containing the full training result

### Training resources

The task is declared with Flyte resource limits that stay below the quota issue observed earlier:

* requests: `cpu="2"`, `mem="3Gi"`
* limits: `cpu="3"`, `mem="3Gi"`

The task also rejects overly large thread counts to avoid oversubscription.

## Training data contract

The model uses the frozen matrix feature order defined by the ELT output. The feature columns are:

* `pickup_hour`
* `pickup_dow`
* `pickup_month`
* `pickup_is_weekend`
* `pickup_borough_id`
* `pickup_zone_id`
* `pickup_service_zone_id`
* `dropoff_borough_id`
* `dropoff_zone_id`
* `dropoff_service_zone_id`
* `route_pair_id`
* `avg_duration_7d_zone_hour`
* `avg_fare_30d_zone`
* `trip_count_90d_zone_hour`

These columns are treated as the frozen serving and training contract. No feature re-derivation occurs inside the training task.

## Bundle output

The training task writes a versioned bundle with exactly these files:

* `model.onnx`
* `schema.json`
* `metadata.json`
* `manifest.json`

### `schema.json`

This describes the bundle contract, including:

* schema version
* feature version
* preprocessing version
* input name
* feature order
* output names
* whether extra features are allowed

### `metadata.json`

This stores the provenance and training metadata, including:

* ELT contract fields
* lineage
* model name and version
* category levels
* feature order hashes
* label cap
* train/test counts
* inner and holdout metrics
* selected candidate and candidate reports

### `manifest.json`

This stores the checksum record:

* `format_version`
* `source_uri`
* `model_version`
* `model_sha256`
* `schema_sha256`
* `metadata_sha256`

The bundle is validated locally before upload.

## Export parity check

Before upload, the training task checks that the exported ONNX model matches the native LightGBM model on a small sample. The comparison is done in log space and must stay within a tight tolerance.

This catches export drift before the bundle is published.

## Evaluation and registration task

The evaluation task is `evaluate_and_register_task(...)` in `src/workflows/train/tasks/evaluate_and_register_task.py`.

It performs the post-training handoff:

1. Reloads the ELT gold table
2. Validates the frozen matrix schema
3. Recomputes the holdout split
4. Optionally downsamples the evaluation set
5. Downloads the published bundle files from S3
6. Validates bundle checksums and metadata alignment
7. Creates the MLflow input signature
8. Logs metrics, parameters, and artifacts to MLflow
9. Logs a PyFunc wrapper around the ONNX model
10. Runs `mlflow.models.evaluate(...)`
11. Returns the run metadata as JSON

### Evaluation resources

The task is lighter than training:

* requests: `cpu="1"`, `mem="2Gi"`
* limits: `cpu="2"`, `mem="3Gi"`

## MLflow logging contract

The evaluation task logs:

* model identity parameters
* bundle URIs
* feature order hashes
* ELT schema hash
* source snapshot ID
* holdout metrics
* baseline metrics
* the serialized training result
* bundle contract and metadata
* evaluation artifacts

The MLflow PyFunc model uses the same frozen matrix order and returns the prediction column only.

## Error handling

The workflow is designed to fail fast.

Typical failure modes include:

* missing or invalid ELT contract
* schema mismatch in the gold table
* nulls or invalid values in required columns
* artifact checksum mismatch
* ONNX export mismatch
* unsupported bundle contract
* resource quota rejection at Flyte task submission time

## Type safety and task boundaries

The workflow boundary is intentionally kept simple. The training task returns a JSON string rather than a deeply nested Python object graph. This avoids Flyte schema extraction problems and keeps the task interface stable.

## Execution summary

At runtime, the pipeline behaves like this:

1. Flyte launches `train(...)`
2. `train_model_task(...)` builds the model and bundle
3. `evaluate_and_register_task(...)` downloads and validates the bundle
4. MLflow receives the metrics and artifacts
5. The run is registered with a stable, checksum-backed bundle contract

## Operational notes

* Use the launch plan for standard runs
* Keep `train_num_threads` low enough to fit the container budget
* Do not add dynamic `Any`-typed payloads to task-facing dataclasses
* Keep the frozen matrix column order unchanged unless deployment is updated at the same time

## Directory map

* `src/workflows/train/workflows/training_workflow.py`

  * workflow orchestration only
* `src/workflows/train/tasks/train_model_task.py`

  * model training, validation, export, bundle upload
* `src/workflows/train/tasks/evaluate_and_register_task.py`

  * bundle validation, MLflow logging, evaluation, registration
* `src/workflows/train/shared_utils.py`

  * shared contract types and utility functions
* `src/workflows/train/launch_plans.py`

  * defaults and exported launch plan symbol

## In short

This workflow is a contract-driven training system. The ELT layer defines the frozen feature matrix, the training task learns the model and emits a bundle, and the evaluation task validates and registers that bundle with MLflow.
