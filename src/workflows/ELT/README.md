## End-to-end runtime model (control plane vs data plane)

The system is intentionally split into independent layers with strict responsibilities:

```text
Control plane (Flyte) ───────► orchestration only
Data plane (Spark)   ───────► execution only
Storage (Iceberg)    ───────► state + transactions
Object store (S3)    ───────► data files
Metadata DB          ───────► catalog state
```

---

# 1) Core architectural contracts

## Single-image contract

* Exactly **one runtime image** is used: `ELT_TASK_IMAGE`
* This image contains:

  * Flytekit + Spark plugin
  * PySpark runtime
  * Iceberg + Hadoop AWS + AWS SDK jars
* This image **does NOT contain application code**

## Code delivery contract

* Code is **not baked into the image**
* `pyflyte run --remote`:

  * packages the current repo state
  * uploads it
  * executes that snapshot

```text
(image = runtime) + (git checkout = code) → execution
```

## Reproducibility contract

* Image → versioned via tag or digest
* Code → versioned via Git SHA
* Both must be tracked externally

---

# 2) Trigger → workflow start

* Workflow triggered manually or scheduled
* Flyte Admin:

  * records execution
  * assigns execution ID
* Flyte Propeller:

  * begins orchestration

No data processing occurs at this stage.

---

# 3) Task execution lifecycle

Each task follows this pattern:

```text
Flyte → K8s Pod (ELT_TASK_IMAGE) → SparkApplication → exit
```

Inside the Flyte task pod:

1. Container starts (runtime-only image)
2. Flyte downloads packaged user code
3. Python task executes
4. `Spark` plugin creates SparkApplication spec
5. Submits CRD to Kubernetes

At this point:

```text
Flyte task pod = control only
```

It does not process data.

---

# 4) Spark operator lifecycle

* Spark Operator watches for `SparkApplication`
* Creates:

  * Driver pod
  * Executor pods

```text
Driver → coordinates
Executors → process data
```

All pods use the same image (`ELT_TASK_IMAGE`).

---

# 5) Data plane execution (Spark)

All data work happens inside Spark.

### Extract + Load (current workflow)

```text
Read → S3 (raw parquet)
Transform → normalize + enrich
Write → Iceberg (partitioned)
```

Future layers follow same pattern:

| Layer  | Operation type               |
| ------ | ---------------------------- |
| Bronze | append / partition overwrite |
| Silver | merge/upsert                 |
| Gold   | aggregate                    |
| Maint  | compaction / cleanup         |

---

# 6) Iceberg interaction model

Write path:

```text
Executors → write parquet files → S3
Driver    → commit via REST → Iceberg catalog
Catalog   → update metadata → Postgres
```

Sequence:

1. Data files written to S3
2. Manifests generated
3. REST catalog commit
4. Snapshot published

Property:

```text
Atomic visibility at snapshot level
```

---

# 7) Metadata flow

```text
Spark → Iceberg REST → Postgres
```

### Stored in Postgres

* table metadata
* snapshots
* manifests
* version history

### Stored in S3

* parquet data files
* manifest files

---

# 8) Task completion

* Spark job finishes
* Driver pod exits
* SparkApplication marked complete
* Flyte marks task success

---

# 9) Retry and idempotency

Failures are isolated per layer.

Flyte retry:

* re-runs entire task
* re-submits Spark job

Correctness depends on idempotent writes:

| Layer  | Safety mechanism        |
| ------ | ----------------------- |
| Bronze | partition overwrite     |
| Silver | MERGE semantics         |
| Gold   | deterministic overwrite |
| Maint  | safe re-execution       |

---

# 10) End-to-end flow

```text
Trigger
  ↓
Flyte workflow
  ↓
Flyte task pod (control only)
  ↓
SparkApplication (CRD)
  ↓
Spark driver + executors
  ↓
S3 (data files)
  ↓
Iceberg REST
  ↓
Postgres (metadata)
  ↓
Task completes
```

---

# 11) Role of the Docker image

The single image is used in:

```text
Flyte task pod
Spark driver pod
Spark executor pods
```

It provides:

* Python runtime
* Spark runtime
* Iceberg + S3 connectivity

It does **not**:

* contain business logic code
* store state
* manage metadata

---

# 12) Failure boundaries

| Layer          | Impact                          |
| -------------- | ------------------------------- |
| Flyte task pod | job not submitted               |
| Spark driver   | task fails                      |
| Spark executor | retried internally by Spark     |
| Iceberg commit | atomic failure, safe retry      |
| S3             | partial writes hidden by commit |

---

# 13) System invariants

These must always hold:

* Flyte does not process data
* Spark is the only data execution engine
* Iceberg is the only table state authority
* S3 stores only immutable data files
* Postgres is only accessed via Iceberg REST
* Task image contains runtime, not code

---

# 14) Mental model

```text
Flyte    → scheduler / orchestrator
Spark    → execution engine
Iceberg  → transactional table layer
S3       → storage layer
Postgres → metadata store
Image    → runtime environment
Git SHA  → code version
```

This separation is the core design constraint and should not be violated.


