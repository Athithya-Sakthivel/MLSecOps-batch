# Flyte training workflow for trip duration ETA

This package trains a frozen Gold-contract regression model from Iceberg/S3, evaluates it, exports ONNX, and registers the run in MLflow.

The design is intentionally narrow:
- one training task
- one evaluate/register task
- one workflow
- one manual launch plan
- explicit scalar inputs only
- S3 URIs for durable artifacts
- no hidden dataset rewriting across task boundaries

## 1) Runtime model

```text
Flyte control plane
  ├── train_pipeline task
  │     ├── read Gold parquet from Iceberg/S3
  │     ├── validate/canonicalize contract
  │     ├── split chronologically
  │     ├── train LightGBM
  │     └── persist artifacts to S3
  │
  └── evaluate_register task
        ├── load training bundle
        ├── validate contract again
        ├── evaluate on validation split
        ├── export ONNX and parity check
        └── log/register in MLflow
````

Flyte orchestrates. Python performs the data work. Iceberg owns the Gold tables. S3 stores immutable artifacts. MLflow stores run metadata and registered model lineage.

## 2) File responsibilities

### `src/workflows/train/tasks/train_pipeline_helpers.py`

Single source of truth for:

* Gold feature contract
* dtype coercion
* schema/value validation
* parquet read/write helpers
* chronological splitting
* feature-frame preparation
* regression metrics
* contract/spec/manifest generation
* artifact path construction

This module is pure utility code. It should not contain Flyte decorators or MLflow calls.

### `src/workflows/train/tasks/train_pipeline.py`

Training task only:

* read Gold dataset
* validate and canonicalize
* split chronologically
* train the model
* write the model and JSON/Parquet artifacts to S3
* return a string-only bundle of URIs and scalar metadata

This task must not contain workflow orchestration.

### `src/workflows/train/tasks/evaluate_register_helpers.py`

Second-stage utility code for:

* parsing the training bundle
* loading training artifacts
* validating contract continuity
* computing evaluation metrics
* exporting ONNX
* computing ONNX parity
* building MLflow tags, params, and metrics
* logging and registering the model

This module should remain task-agnostic.

### `src/workflows/train/tasks/evaluate_register.py`

Second task only:

* consume the training bundle from `train_pipeline`
* evaluate
* export ONNX
* register the model in MLflow
* return a string-only bundle

No workflow decorator belongs here.

### `src/workflows/train/workflows/train.py`

Thin orchestration layer:

* calls `train_pipeline`
* then calls `evaluate_register`

This file should contain no data logic, no schema logic, and no MLflow implementation details.

### `src/workflows/train/launch_plans.py`

Manual execution entrypoint:

* resolves default inputs
* creates the manual launch plan
* keeps execution defaults in one place

## 3) Gold data contract

The Gold dataset is treated as a frozen model contract, not a free-form input.

Required canonical columns:

* `trip_id`
* `as_of_ts`
* `as_of_date`
* `schema_version`
* `feature_version`
* the fixed feature columns
* `label_trip_duration_seconds`

Contract rules:

* exact column order is enforced
* exact dtype expectations are enforced after coercion
* timestamp/date consistency is enforced
* duplicate IDs are rejected
* label values must be positive
* categorical IDs are non-negative with `0` reserved for unknown

The Gold table is the exact matrix used for both training and evaluation.

## 4) Artifact model

The training task writes a compact bundle of immutable artifacts to S3.

Typical outputs:

* `model.txt`
* `manifest.json`
* `contract.json`
* `feature_spec.json`
* `encoding_spec.json`
* `aggregate_spec.json`
* `label_spec.json`
* `quality_report.json`
* `validation_sample.parquet`
* `runtime_config.json`
* `training_summary.json`
* `best_config.json`
* `lightgbm_params.json`

The second-stage task reads those artifacts by URI and does not reconstruct state from local temp paths.

## 5) Workflow execution model

Recommended flow:

1. `train_pipeline`
2. `evaluate_register`

The second stage should depend on the bundle created by the first stage, not on re-reading or re-deriving training state from scratch.

That avoids:

* split drift
* schema drift
* local-path fragility
* duplicate contract logic
* hidden task coupling

## 6) Launch plan model

Use a single manual launch plan for the workflow.

The launch plan should provide defaults for:

* dataset URI
* bucket
* artifact prefix
* registered model name
* validation fraction
* random seed
* number of boosting rounds
* early stopping rounds
* model family
* training profile
* MLflow experiment name
* MLflow tracking URI
* ONNX opset
* validation sample size

The workflow can still accept overrides, but the launch plan should make the common path zero-config.

## 7) Environment variables

Keep env vars small and deployment-oriented.

Strong defaults are:

* `S3_BUCKET`
* `TRAIN_DATASET_URI`
* `TRAIN_PROFILE`
* `REGISTERED_MODEL_NAME`
* `ARTIFACT_ROOT_PREFIX`
* `TRAIN_VALIDATION_FRACTION`
* `TRAIN_RANDOM_SEED`
* `TRAIN_NUM_BOOST_ROUND`
* `TRAIN_EARLY_STOPPING_ROUNDS`
* `TRAIN_MODEL_FAMILY`
* `MLFLOW_EXPERIMENT_NAME`
* `MLFLOW_TRACKING_URI`

Prefer workflow inputs and launch-plan defaults over task-local environment reads.

## 8) What should not live in tasks

Do not put these in task code:

* workflow orchestration
* launch-plan wiring
* container build logic
* manual shell/bootstrap logic
* schema/version ownership
* duplicate Gold parsing logic
* MLflow server deployment details

Do not use task-local `/tmp` as a durable contract boundary.

## 9) Local operator model

The operator path stays separate from training logic:

* `run.py` for bootstrap, registration, execution, diagnosis, deletion, and cleanup
* `commands.sh` for shell convenience

Those files are not part of the training contract itself.

## 10) Invariants

These should always hold:

* Flyte orchestrates
* tasks do the work
* workflows only compose tasks
* launch plans supply defaults
* Iceberg owns Gold tables
* S3 stores immutable artifacts
* MLflow stores metadata and registry lineage
* the Gold contract is frozen
* evaluation uses the training bundle
* ONNX export is parity-checked
* runtime behavior is explicit and reproducible

## 11) Practical rule

If a file changes data or model behavior, it belongs in `tasks/`.

If a file changes step ordering, it belongs in `workflows/`.

If a file changes execution defaults, it belongs in `launch_plans.py`.

If a file changes operator behavior, it belongs in `run.py` or `commands.sh`.
