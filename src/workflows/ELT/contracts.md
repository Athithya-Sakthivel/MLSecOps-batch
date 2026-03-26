# Hard Contract Boundary: Gold Layer as the ELT–ML Interface

### 1) Schema contract

Gold must output a **fixed set of columns** with:

* exact column names
* exact order
* exact dtypes
* exact null policy
* exact units

Example:

```text
pickup_hour                int
pickup_dow                 int
pickup_month               int
pickup_is_weekend          int
pickup_borough_id          int
pickup_zone_id             int
pickup_service_zone_id     int
route_pair_id              int
avg_duration_7d_zone_hour  float
avg_fare_30d_zone           float
trip_count_90d_zone_hour    float
label_trip_duration_seconds float
```

Training must reject the dataset if the schema hash differs from the version it was trained against. This is a recommendation, but it is the correct production boundary for a model that will later be exported to ONNX, because ONNX only receives the model graph, not your upstream business logic. ONNX conversion examples and pipeline conversion docs show that the exported model depends on the input feature specification supplied at conversion time. ([onnx.ai][2])

### 2) Time contract

Every row in Gold needs an **as-of timestamp** or equivalent event time, and every feature must be computable using only information available at or before that timestamp.

Concrete rule:

* `avg_duration_7d_zone_hour` must use trips with `trip_start_ts < as_of_ts`
* no feature may peek at the label window
* no post-trip fields may leak into pre-trip prediction

This is the main accuracy contract. If you violate it, offline metrics will look good and production will degrade.

### 3) Label contract

The label must be explicit, singular, and frozen per dataset version.

For trip ETA:

* `label_trip_duration_seconds`

Optional other datasets:

* `label_total_amount`
* `label_fare_amount`

Do not mix labels in one training table unless you are intentionally doing multi-task learning. Keep the target definition in the dataset name and metadata, not in tribal knowledge.

### 4) Categorical encoding contract

If you want a clean ELT→ONNX path, categorical values must be converted to **stable integer IDs** in Gold. LightGBM’s docs explicitly support integer-encoded categorical features and recommend specifying categorical features; they also note categorical values should be non-negative integers, preferably contiguous from zero. ([lightgbm.readthedocs.io][1])

Concrete rule:

* `pickup_borough = "Manhattan"` becomes `pickup_borough_id = 1`
* the mapping lives in a versioned artifact
* the same string must always map to the same integer for that model version
* unknowns must map to a reserved value, usually `0`

This is not optional if you want deterministic training and inference. If the mapping changes, the model sees different semantics for the same number.

### 5) Category-domain contract

For each encoded categorical column, Gold or the training manifest must define:

* the full allowed category list
* the reserved unknown bucket
* whether the column is treated as categorical by LightGBM
* whether the integer is an identity code or a hashed bucket

Example:

```json
pickup_borough_id:
  domain: [0,1,2,3,4,5]
  meaning:
    0: unknown
    1: Manhattan
    2: Queens
    3: Brooklyn
    4: Bronx
    5: Staten Island
  categorical_feature: true
```

This matters because LightGBM treats categorical splits differently from ordinary numeric splits. ([lightgbm.readthedocs.io][1])

### 6) Aggregate contract

All historical aggregates in Gold must be **point-in-time safe** and computed with one fixed recipe.

For each aggregate, define:

* source table
* filter predicate
* window length
* grouping keys
* minimum history rule
* null fallback

Example:

```text
avg_duration_7d_zone_hour
= mean(trip_duration_seconds)
over trips where
  trip_start_ts in [as_of_ts - 7d, as_of_ts)
grouped by pickup_zone_id, pickup_hour
```

This contract must be identical in training and backfill generation. If you later rebuild Gold, the same row keys must produce the same features.

### 7) Join contract

Any enrichment join in Silver must be deterministic and key-safe before Gold builds features.

For your case:

* `trip_id` must be stable
* taxi zone lookup join must be one-to-one or many-to-one by declared key
* no duplicate expansion is allowed unless explicitly modeled

Gold assumes Silver already resolved business keys into a canonical trip row.

### 8) Row identity contract

Every training row must have:

* a stable primary key, usually `trip_id`
* a stable event timestamp, usually `trip_start_ts`
* a stable dataset version

This allows exact lineage from Gold row back to Bronze source files via the lineage fields you already defined.

### 9) Split contract

Training/validation/test splits must be declared in advance and reproducible.

For trip prediction, the safest default is:

* time-based split by `trip_start_ts` or ingestion date
* never random split if the data has strong temporal drift
* avoid the same entity appearing across train and validation if that causes leakage

This is an accuracy contract, not just an evaluation preference. Time leakage will inflate metrics.

### 10) Transformation contract

Gold may do feature engineering, but only transformations that are:

* deterministic
* versioned
* reproducible from the same Silver rows
* available at prediction time

Do not put ad hoc notebook logic in training. Anything needed at inference must be representable as a saved contract artifact.

### 11) Artifact contract

Training must persist these artifacts together:

* model file: `model.onnx`
* feature schema: `schema.json`
* categorical map: `encodings.json`
* label definition: `label_spec.json`
* training config: `best_config.json`
* feature list with categorical flags: `feature_spec.json`

FLAML’s `best_config` can be used to initialize the original model after converting FLAML-specific parameters, which is relevant if you want to reproduce the exact trained LightGBM object before export. ([microsoft.github.io][3])

### 12) Conversion contract

ONNX conversion should happen only after the feature matrix is frozen.

Important implication:

* ONNX captures the model computation
* it does not magically recreate your ELT transforms
* any preprocessing that is not embedded in the exported graph must be duplicated exactly elsewhere

ONNX Runtime supports traditional ML models such as LightGBM, and the sklearn-onnx / ONNX tooling examples show that pipelines and LightGBM models can be converted for runtime inference. ([onnxruntime.ai][4])

### 13) Numeric precision contract

Pick one numeric policy and keep it consistent:

* `float32` or `float64`
* missing-value representation
* rounding rules for derived features

ONNX conversion and runtime execution can introduce small numeric differences, so your validation should tolerate tiny deviations and compare predictions within a defined epsilon, not exact string equality. ONNX Runtime documents compatibility across opsets and model versions, but not bitwise identity across all environments. ([onnxruntime.ai][5])

### 14) Missing-value contract

Define, per feature:

* whether nulls are allowed
* what fill value is used
* whether missingness is informative

Example:

* `pickup_zone_id = 0` for unknown
* `avg_duration_7d_zone_hour = global_mean_duration` or explicit `NaN`, but choose one policy and keep it fixed

LightGBM handles missing values natively, but your ELT must still be deterministic about how missingness is represented. ([lightgbm.readthedocs.io][6])

### 15) Metric contract

Training must optimize a metric aligned with the business use case.

For ETA:

* MAE is usually the primary metric
* RMSE is secondary
* maybe median AE if you care about robustness

The metric must be written into the dataset/model version, or you will compare incompatible models later.

---

## The practical minimum you need to enforce

If you only enforce five things, enforce these:

1. **Frozen Gold schema**
2. **Point-in-time-safe features**
3. **Stable integer encoding for categoricals**
4. **Exact train/infer feature order and dtypes**
5. **Versioned artifacts: schema, encodings, model, config**

That is the core contract that makes FLAML + Ray Train + LightGBM + ONNX operationally safe and accurate. FLAML gives you a tuned LightGBM configuration, Ray Train can scale the training run, LightGBM handles integer-coded categoricals, and ONNX Runtime can serve the exported model. ([microsoft.github.io][3])

## The strict line between ELT and training

**ELT owns**

* row construction
* canonical joins
* feature engineering
* categorical encoding
* point-in-time aggregates
* dataset versioning

**Training owns**

* hyperparameter search
* model fitting
* validation
* calibration if used
* export to ONNX
* artifact packaging

Training should not “fix” bad ELT. If training has to invent missing columns, reorder columns manually, or guess category mappings, the contract has already failed.

[1]: https://lightgbm.readthedocs.io/en/latest/Advanced-Topics.html "Advanced Topics — LightGBM 4.6.0.99 documentation"
[2]: https://onnx.ai/sklearn-onnx/pipeline.html "Convert a pipeline - sklearn-onnx 1.20.0 documentation"
[3]: https://microsoft.github.io/FLAML/docs/reference/automl/automl/ "automl.automl | FLAML"
[4]: https://onnxruntime.ai/docs/tutorials/traditional-ml.html "Deploy traditional ML models"
[5]: https://onnxruntime.ai/docs/reference/compatibility.html "ONNX Runtime compatibility"
[6]: https://lightgbm.readthedocs.io/en/latest/pythonapi/lightgbm.Dataset.html "lightgbm.Dataset — LightGBM 4.6.0.99 documentation"
