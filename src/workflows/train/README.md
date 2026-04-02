# Flyte Training Workflow (Gold → Validate → Train → Evaluate → Export ONNX → Register Model) + frozen Gold contract + Ray orchestration

## End-to-end runtime model

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Ray + Python) ────► Gold loading, validation, train/validation split, FLAML search, LightGBM training, metrics, ONNX export, MLflow registration
Storage (Iceberg Gold) ───────► frozen training dataset and contract tables
Object store (S3) ────────────► immutable parquet/model artifacts and sidecars
Catalog DB ───────────────────► Iceberg metadata/state
```

Flyte tasks run in Kubernetes pods, and the Ray task plugin provides the Ray runtime needed by the training task. The code is organized around registered workflows and launch plans: Flyte generates a default launch plan for each workflow when it is registered, and launch plans are the invocation mechanism for executions. Launch plans can also define fixed inputs, schedules, and other execution-time settings. ([Flyte][1])

## 1) Core contracts

### Single runtime image

One shared runtime image is used by all task files.

It contains:

* Python runtime
* Flytekit
* Ray
* FLAML
* LightGBM
* ONNX / ONNX Runtime / onnxmltools
* pandas / numpy
* MLflow
* libraries needed by the training, evaluation, ONNX export, and registry tasks

### Versioning

* Image version is separate from code version.
* Code lineage is tracked by git SHA.
* Execution lineage is tracked through Flyte execution IDs and registered workflow/launch-plan versions.
* Model lineage is tracked through Gold contract version, schema hash, split cutoff, and MLflow run metadata.

## 2) Final file-by-file plan

| File                                            | Final responsibility                                                                       | Must contain                                                                                                                    | Must not contain                                                               |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `src/workflows/train/launch_plans.py`           | Single source of truth for Flyte execution entrypoints.                                    | `LaunchPlan` definition for the training workflow; fixed defaults; exports.                                                     | Task code, workflow orchestration logic, operational shell logic.              |
| `src/workflows/train/tasks/common.py`           | Single source of truth for the training contract and shared helpers.                       | Frozen Gold contract constants; schema/dtype validation; sidecar helpers; split helpers; model feature helpers; JSON helpers.   | Workflow orchestration, Ray task code, MLflow logging, ONNX export logic.      |
| `src/workflows/train/tasks/load_gold.py`        | Gold ingestion and canonicalization task.                                                  | Contract validation; exact column/order checks; dtype coercion; deterministic parquet snapshot; contract sidecars.              | Feature engineering, training, evaluation, ONNX export, MLflow registry logic. |
| `src/workflows/train/tasks/validate_dataset.py` | Validation gate for the Gold dataset.                                                      | Contract validation; chronological split verification; canonical ordered snapshot; validation report sidecar.                   | Feature creation, training, evaluation, ONNX export, MLflow registry logic.    |
| `src/workflows/train/tasks/train_model.py`      | Ray + FLAML + LightGBM training task.                                                      | Chronological split; FLAML search; Ray LightGBM training; metrics; model artifact; manifest; parity sample; contract artifacts. | Gold loading logic, ONNX export, registry, manual debugging logic.             |
| `src/workflows/train/tasks/evaluate_model.py`   | Evaluation task.                                                                           | Same contract as training; exact feature order and categorical coercion; regression metrics on validation split.                | Training, ONNX export, MLflow registry logic.                                  |
| `src/workflows/train/tasks/export_onnx.py`      | ONNX conversion and parity task.                                                           | Contract hash checks; ONNX conversion; parity sample validation; ONNX parity metrics and manifest.                              | Training, data loading beyond parity sample, MLflow registry logic.            |
| `src/workflows/train/tasks/register_model.py`   | Final registry task.                                                                       | MLflow experiment/runs; tags; metrics; model/ONNX artifacts; contract artifacts.                                                | Training, evaluation, ONNX conversion, dataset validation.                     |
| `src/workflows/train/workflows/train.py`        | Thin training orchestration DAG.                                                           | The sequence `load_gold -> validate_dataset -> train_model -> evaluate_model -> export_onnx -> register_model`.                 | Business logic, schema validation internals, Ray internals, MLflow internals.  |
| `src/workflows/train/run.py`                    | Operator command for bootstrap, registration, execution, diagnosis, deletion, and cleanup. | Namespace bootstrap, Ray ServiceAccount/RBAC, registration of workflow/launch plan, execution submission, diagnosis, cleanup.   | Task code, workflow logic, model logic.                                        |
| `src/workflows/train/commands.sh`               | Operator convenience entrypoint.                                                           | Activation of environment; training profile selection; registration and execution commands.                                     | Task internals, workflow definitions, container build logic.                   |

### Canonical ownership

* Shared contract helpers own schema truth.
* Tasks own data work and model work.
* Workflows own orchestration only.
* Launch plans own execution contracts.
* Ops scripts own bootstrap, registration, execution triggering, diagnosis, and cleanup.

### Final rule

If a file changes **data or model behavior**, it belongs in `tasks/`.

If a file changes **step ordering**, it belongs in `workflows/`.

If a file changes **how/when Flyte runs things**, it belongs in `launch_plans.py`.

If a file changes **how operators interact with the system**, it belongs in `run.py` or `commands.sh`.

## 3) Data contract for the Gold training dataset

The source dataset is treated as a frozen model contract, not as an ad hoc training input.

The Gold table contains:

* a single trip-level label column
* a fixed metadata envelope
* a fixed feature order
* frozen categorical encodings
* point-in-time-safe aggregate features

Required canonical columns include:

* `trip_id`
* `as_of_ts`
* `as_of_date`
* `schema_version`
* `feature_version`
* feature columns in fixed order
* `label_trip_duration_seconds`

The Gold table must be the exact feature matrix that training and ONNX inference consume identically.

## 4) Exact behavior of each task

### `common.py`

Purpose: define the training contract and shared validation helpers.

Rules:

* hold the frozen Gold feature contract as the single source of truth
* define required columns, categorical features, numeric features, and label/timestamp columns
* validate exact column order and dtype expectations
* validate value contracts such as timestamp sanity, non-null rules, non-negative label, and categorical ID constraints
* provide deterministic split, feature-frame, and JSON/sidecar helpers
* provide schema hash and contract summary helpers used by all downstream tasks

This file should be the first place updated whenever the Gold contract changes.

### `load_gold.py`

Purpose: read the Gold dataset and canonicalize it into a validated parquet snapshot.

Rules:

* read the Gold dataset from Iceberg/S3
* validate exact column set and exact order against the frozen contract
* coerce dtypes only after contract validation passes
* keep `as_of_ts` and `as_of_date` intact
* sort deterministically by `as_of_ts`
* write the canonical parquet snapshot
* write contract sidecars alongside the snapshot

This task must fail fast if the Gold schema has drifted.

### `validate_dataset.py`

Purpose: validate the Gold snapshot before training.

Rules:

* read only from the canonical Gold snapshot produced by `load_gold`
* validate exact contract and value rules again
* verify that a chronological split is possible
* materialize a deterministic, timestamp-ordered snapshot
* write validation metadata sidecars including schema hash, feature version, schema version, split cutoff, and row counts

This task is a validation gate, not a feature engineering step.

### `train_model.py`

Purpose: train the regression model against the frozen Gold contract.

Rules:

* read only from the validated Gold snapshot
* use the Gold contract as the source of truth
* split chronologically on `as_of_ts`
* keep the model input feature order locked
* use the same categorical feature handling in every training and inference path
* run FLAML on a sampled subset of the training split
* train LightGBM via Ray using the documented `LightGBMTrainer` pattern
* consume per-worker data through `ray.train.get_dataset_shard(name)`
* use `RayTrainReportCallback` for checkpoint/reporting
* save model, manifest, metrics, best config, runtime config, contract summary, and validation sample
* persist `feature_version`, `schema_version`, `schema_hash`, `gold_table`, `source_silver_table`, and split cutoff for exact reproducibility

This task is the primary training runtime and must be deterministic on contract and split behavior.

### `evaluate_model.py`

Purpose: evaluate the trained model on the validation split.

Rules:

* evaluate against the same Gold contract used by training
* use the same chronological split basis
* use the same feature list and categorical coercions as training
* compare predictions against the validation label
* emit regression metrics only; do not retrain or alter artifacts
* fail if manifest or feature spec does not match the loaded Gold frame

This task is strictly an evaluation gate.

### `export_onnx.py`

Purpose: convert the trained LightGBM model into ONNX and verify parity.

Rules:

* verify the saved feature spec and contract hash before export
* refuse export if the current Gold contract has drifted
* use the exact feature order used in training
* preserve the same categorical coercions and parity sample handling as training
* convert to ONNX
* compare ONNX predictions against LightGBM predictions on the validation sample
* write ONNX model, parity metrics, and ONNX manifest sidecars

This task is a contract enforcement step, not a model training step.

### `register_model.py`

Purpose: register the trained model and artifacts in MLflow.

Rules:

* log the real contract identifiers as MLflow tags
* include `feature_version`, `schema_version`, `schema_hash`, `gold_table`, `source_silver_table`, split cutoff, and profile tags
* log training metrics, evaluation metrics, and ONNX parity metrics
* log the model, contract, and parity artifacts together
* keep registry logging separate from training, validation, and export logic

This task is the final lineage and registry step.

### `train.py`

Purpose: be the thin orchestration layer for the training hot path.

Recommended flow:

1. `load_gold`
2. `validate_dataset`
3. `train_model`
4. `evaluate_model`
5. `export_onnx`
6. `register_model`

If any contract drift is detected, the workflow should fail before registry.

### `launch_plans.py`

Purpose: centralize launch-plan definitions.

Recommended launch plan:

* one manual launch plan for `train`

If scheduled training is needed later, add it here without changing task logic.

## 5) Runtime and execution model

Use the Ray task pattern Flyte documents:

* task config carries Ray settings
* the Ray plugin provides the Ray infrastructure the task needs by the time the task function runs
* the training task uses Ray only for distributed model fitting, not for orchestration

That gives this runtime shape:

```text
Flyte workflow
  -> Gold load task pod
  -> Validation task pod
  -> Ray training task pod
  -> Evaluation task pod
  -> ONNX export task pod
  -> MLflow registry task pod
```

The training task itself may launch Ray workers inside the Flyte-managed Ray job, but Flyte remains the control plane.

## 6) Bottleneck rules

* Only `load_gold.py` talks to the Gold input dataset.
* Only `validate_dataset.py` verifies the canonical Gold snapshot for train readiness.
* Only `train_model.py` performs FLAML search and LightGBM training.
* Only `evaluate_model.py` computes evaluation metrics.
* Only `export_onnx.py` performs ONNX export and parity checks.
* Only `register_model.py` performs MLflow logging.
* The workflow file must stay thin.
* Rebuild the shared image only when task dependencies change, not for workflow wiring changes.
* Keep the Ray job bounded and task-scoped.
* Do not move model training or ONNX export into the workflow file.
* Do not let evaluation mutate training artifacts.
* Do not let registry logic infer schema; it must consume the saved contract.

## 7) Final invariants

These should always hold:

* Flyte orchestrates.
* Ray executes the training workload.
* Iceberg owns the Gold table and contract tables.
* S3 stores immutable artifacts.
* Gold is the frozen model contract.
* Validation is a gate, not a feature creator.
* Training is chronological and reproducible.
* Evaluation matches the training contract.
* ONNX export is parity-checked.
* Registry logs the final lineage.
* The runtime image is shared.
* Application code is external and versioned by git SHA.

## 8) Local operator model

The local operator model is split across two files:

* `run.py` for bootstrap, registration, execution, diagnosis, deletion, and cleanup
* `commands.sh` for the minimal shell wrapper used by operators

This is the training-side replacement for the old single-script pattern.

## 9) Two separate execution modes

* Training workflow

  * manual or on a business schedule
  * Gold load → validation → training → evaluation → ONNX export → registry

* Debug and lifecycle mode

  * register
  * run
  * diagnose
  * delete
  * reset

This keeps the training path deterministic while preserving a clean operator workflow for local and cluster use.

[1]: https://docs-legacy.flyte.org/en/latest/user_guide/basics/launch_plans.html "Launch plans"
