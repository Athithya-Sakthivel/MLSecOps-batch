## Finalized invariants by ELT layers

### Platform / execution

* **Flyte orchestrates; Spark executes; Iceberg owns table state; S3 stores immutable files.**
* **Workflow code is packaged and submitted locally, but runs remotely in Kubernetes pods.**
* **The task runtime image is separate from the local `.venv_elt`; local packages are only for submission-time import/packaging.**
* **Task pods must have the correct Spark/Iceberg/AWS runtime dependencies; local tooling does not substitute for that.**

### Repo / submission

* **`run.sh` is the operator entrypoint for ad hoc submission, linting, diagnosis, and deletion.**
* **Ruff runs locally before submit, with safe-only fixes.**
* **Execution names are deterministic from git SHA + timestamp.**
* **`pyflyte run --remote` is used for manual executions; schedules are handled by launch plans.**

### Bronze

* **Bronze is raw landing only.**
* **Only Bronze touches external raw datasets.**
* **Bronze preserves lineage metadata (`run_id`, timestamps, source identity).**
* **Bronze writes idempotently into Iceberg.**
* **Bronze only does minimal normalization and type cleanup.**
* **Bronze must fail fast if source access or schema assumptions break.**

### Silver

* **Silver is the canonical cleaned trip fact layer.**
* **Silver reads only from Bronze.**
* **Silver resolves the taxi-zone joins deterministically.**
* **Silver produces one stable row per trip.**
* **Silver is not the final training matrix.**
* **Silver must remain schema-stable and reproducible.**

### Gold

* **Gold is the frozen, model-ready training matrix.**
* **Gold is the feature-store-like contract layer.**
* **Gold must have a fixed schema, fixed column order, fixed dtypes, and fixed null policy.**
* **Gold features must be point-in-time safe using `as_of_ts`.**
* **Gold must not leak post-trip information into pre-trip features.**
* **Gold must use stable integer encodings for categoricals, with reserved unknown values.**
* **Gold must be versioned with schema, encodings, label spec, and feature spec.**
* **Gold is the only layer that should prepare training-ready features and labels.**

### Training contract

* **Training must reject Gold if the schema hash changes.**
* **Training consumes the frozen matrix; it does not repair ELT.**
* **Training owns model fitting, validation, tuning, and export.**
* **ONNX export assumes the Gold feature contract is already frozen.**

### Maintenance

* **Iceberg maintenance is a separate workflow from ELT.**
* **Expiration and orphan cleanup are regular, low-risk maintenance.**
* **Compaction is optional, table-specific, and predicate-gated.**
* **Maintenance must not block ingestion or feature generation.**
* **Maintenance should run on a separate cadence via launch plans.**

### Workflows / launch plans

* **ELT workflow = Bronze → Silver → Gold.**
* **Maintenance workflow = Iceberg hygiene only.**
* **Launch plans define schedule and fixed runtime behavior.**
* **Daily maintenance covers expiry + orphan cleanup.**
* **Weekly maintenance can enable compaction for selected tables only.**

### Architectural identity

* **This is a Medallion Lakehouse with an ML-first Gold contract.**
* **It behaves like an offline feature store, without running a dedicated feature store service.**
* **The design goal is deterministic, reproducible, leakage-safe training and inference parity.**

### Non-negotiable operational rules

* **No ad hoc feature logic in notebooks or training scripts.**
* **No maintenance in the hot ELT path.**
* **No silent schema drift.**
* **No ambiguous category mappings.**
* **No random splits for temporally drifting trip data.**
* **No reliance on local environment for runtime execution.**

