## 1) Medallion data contract

### Bronze

Raw landing only.

* Pull external sources.
* Minimal normalization only.
* Write raw Iceberg Bronze tables.
* Attach lineage metadata only:

  * `run_id`
  * `ingestion_ts`
  * `source_uri`
  * `source_revision`
  * `source_kind`
  * `source_file`
* No joins, feature engineering, aggregations, labels, or maintenance.

### Silver

Canonical trip fact only.

* Read only Bronze.
* Validate schema.
* Normalize types.
* Join trips to taxi zones.
* Produce one canonical row per trip.
* Keep deterministic keys and lineage.
* No model-specific features, rollups, label creation, or external source access.

### Gold

Training-ready ML dataset only.

* Read only Silver.
* Build leakage-safe features.
* Keep target explicit.
* Version by use case / model.
* Freeze feature schema, order, types, and encoding contract.
* No ingestion logic or generic cleanup.

---

## 2) ML problem contract

Current finalized use case:

* **Pre-trip ETA / trip duration regression**

Label:

* `label_trip_duration_seconds`

Features:

* pickup time encodings
* pickup context
* optional destination context only if known at prediction time
* point-in-time-safe historical aggregates

Core rule:

* Do not mix pre-trip features with post-trip information.

---

## 3) ELT ↔ training boundary

### Large datasets

* Pass **URI strings**, not files.
* Flyte should not move large Parquet data between tasks.

### FlyteFile / FlyteDirectory

Use only for **artifacts**, mainly after training:

* model files
* ONNX bundles
* manifests
* metrics
* validation samples
* checkpoints

### Validation task

* Fail fast.
* Read only a bounded sample.
* No rewrite.
* Return the same dataset URI unchanged.

### Training task

* Reads the same dataset URI that validation checked.
* Performs the full train/holdout split.
* Produces artifact bundles.

---

## 4) Training workflow invariants

### `load_gold.py`

* Boundary check only.
* Validate schema and a small sample.
* Do not write data.
* Return the same URI.

### `validate_dataset.py`

* Sample-based validation only.
* No full rewrite.
* No output dataset copy.
* Return the same URI.

### `train_model.py`

* Uses the validated URI.
* Loads Gold data.
* Applies chronological split.
* Runs FLAML search.
* Trains LightGBM through Ray Train.
* Writes a model artifact bundle.
* Returns `FlyteDirectory`.

### `evaluate_model.py`

* Evaluates the trained artifact bundle.
* Does not retrain.
* Does not read the full Gold dataset again.
* Uses stored validation sample / artifact metadata.

### `export_onnx.py`

* Converts trained LightGBM model to ONNX.
* Runs parity check against LightGBM predictions.
* Writes ONNX bundle.
* Returns `FlyteDirectory`.

### `register_model.py`

* Final side-effect step only.
* Logs metrics and artifacts to MLflow.
* No training, no feature logic.

### `workflows/train.py`

* Orchestration only.
* No business logic.
* No data transformation.
* Just wires the task graph.

---

## 5) Determinism invariants

* Pinned package versions.
* Frozen feature list.
* Frozen feature order.
* Stable categorical integer encoding.
* Reserved unknown category value: `0`.
* Chronological split only.
* Fixed random seed.
* No random train/validation split.
* Same input URI must produce the same contract checks and the same split boundary.

---

## 6) Performance and scaling invariants

* Do not copy large datasets through Flyte artifacts.
* Do not load full large datasets into pandas if Ray Data can read them directly.
* Avoid full rewrites in validation.
* Keep validation cheap.
* Use Ray Train only for training compute.
* Use Ray Data only when dataset scale requires it.

---

## 7) Artifact invariants

Training must produce a bundle containing at least:

* `model.txt`
* `model.onnx`
* `feature_spec.json`
* `schema.json`
* `manifest.json`
* `metrics.json`
* `best_config.json`
* `lightgbm_params.json`
* `validation_sample.parquet`
* `onnx_parity.json`
* `onnx_manifest.json`

Artifacts are versioned outputs, not mutable working files.

---

## 8) ONNX invariants

* ONNX export happens after LightGBM training.
* ONNX runtime must match LightGBM predictions within tolerance.
* ONNX bundle is validated before registration.
* ONNX is for inference portability, not for replacing the feature contract.

---

## 9) Flyte infrastructure invariants

### Task image

* Separate training image from ELT image.
* No application code baked into the image.
* Rebuild only when requirements change.
* Use a date+git immutable tag, for example:

  * `2026-03-26-abcdef0`

### ENTRYPOINT / CMD

* Use `ENTRYPOINT` for `tini`.
* Do not rely on `CMD` for Flyte task execution.

### Caching

* `cache=False` for these tasks.
* No task-level caching for training, ONNX export, or registration.

---

## 10) Package/runtime invariants

* Local submit environment can be smaller than the task image.
* Task image contains the full runtime stack.
* Local environment only needs what is required to import the workflow modules and submit them.
* Ray plugin is required where Ray task configs are imported.

---

## 11) Operational invariant

The whole design follows one rule:

> **Flyte orchestrates. Storage holds data. Tasks compute. Artifacts are tracked. Large datasets are never shuttled through Flyte.**
