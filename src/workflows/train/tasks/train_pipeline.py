# src/workflows/train/tasks/train_pipeline.py
from __future__ import annotations

import os

from flytekit import Resources, task, workflow

from workflows.train.tasks.train_pipeline_helpers import (
    ARTIFACT_ROOT_S3,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_RANDOM_SEED,
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
    GOLD_TRAINING_TABLE,
    INFERENCE_RUNTIME,
    LABEL_COLUMN,
    MODEL_FAMILY,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    TRAIN_PROFILE,
    VALIDATION_MODE,
    VALIDATION_SAMPLE_FRACTION,
    VALIDATION_SAMPLE_MAX_ROWS,
    build_quality_report,
    build_run_id,
    build_task_environment,
    load_gold_frame,
    log_json,
    persist_training_artifacts,
    split_by_time,
    train_lightgbm_model,
    validate_and_canonicalize_gold_frame,
)

TRAIN_TASK_CPU = os.environ.get("TRAIN_TASK_CPU", "500m" if TRAIN_PROFILE == "staging" else "1000m")
TRAIN_TASK_MEM = os.environ.get("TRAIN_TASK_MEM", "768Mi" if TRAIN_PROFILE == "staging" else "1024Mi")
TRAIN_TASK_RETRIES = int(os.environ.get("TRAIN_TASK_RETRIES", "1"))


def _to_string_bundle(payload: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in payload.items()}


@task(
    cache=False,
    environment=build_task_environment(),
    retries=TRAIN_TASK_RETRIES,
    limits=Resources(cpu=TRAIN_TASK_CPU, mem=TRAIN_TASK_MEM),
)
def train_pipeline(
    dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    artifact_root_s3: str = ARTIFACT_ROOT_S3,
    validation_mode: str = VALIDATION_MODE,
    validation_sample_fraction: float = VALIDATION_SAMPLE_FRACTION,
    validation_sample_max_rows: int = VALIDATION_SAMPLE_MAX_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, str]:
    """
    Read Gold from Iceberg/S3, validate once, train once, and persist durable
    artifacts to S3. The return value is a compact string-only manifest for the
    downstream evaluation/register stage.
    """
    run_id = build_run_id()

    log_json(
        msg="train_pipeline_start",
        run_id=run_id,
        dataset_uri=dataset_uri,
        validation_fraction=validation_fraction,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        artifact_root_s3=artifact_root_s3,
        validation_mode=validation_mode,
        validation_sample_fraction=validation_sample_fraction,
        validation_sample_max_rows=validation_sample_max_rows,
        random_seed=random_seed,
        train_profile=TRAIN_PROFILE,
        gold_feature_version=GOLD_FEATURE_VERSION,
        gold_schema_version=GOLD_SCHEMA_VERSION,
        gold_training_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
        model_family=MODEL_FAMILY,
        inference_runtime=INFERENCE_RUNTIME,
    )

    raw_df = load_gold_frame(dataset_uri)
    canonical_df = validate_and_canonicalize_gold_frame(raw_df)

    if canonical_df.empty:
        raise ValueError(f"Canonical Gold frame is empty for dataset_uri={dataset_uri}")

    split = split_by_time(canonical_df, validation_fraction=validation_fraction)
    if split.train_df.empty or split.valid_df.empty:
        raise ValueError(
            f"Chronological split produced empty partitions: "
            f"train_rows={len(split.train_df)}, valid_rows={len(split.valid_df)}"
        )

    quality_report = build_quality_report(
        canonical_df,
        split=split,
        validation_mode=validation_mode,
        validation_sample_fraction=validation_sample_fraction,
        validation_sample_max_rows=validation_sample_max_rows,
        random_seed=random_seed,
    )

    log_json(
        msg="train_pipeline_split_ready",
        run_id=run_id,
        train_rows=len(split.train_df),
        valid_rows=len(split.valid_df),
        cutoff_ts=split.cutoff_ts,
        validation_mode=validation_mode,
    )

    booster, metrics, trainer_meta = train_lightgbm_model(
        split.train_df,
        split.valid_df,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        seed=random_seed,
    )

    runtime_config: dict[str, object] = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "validation_fraction": validation_fraction,
        "num_boost_round": num_boost_round,
        "early_stopping_rounds": early_stopping_rounds,
        "random_seed": random_seed,
        "train_rows": len(split.train_df),
        "valid_rows": len(split.valid_df),
        "cutoff_ts": split.cutoff_ts,
        "best_iteration": trainer_meta.get("best_iteration", 0),
        "current_iteration": trainer_meta.get("current_iteration", 0),
        "lightgbm_params": trainer_meta.get("lightgbm_params", {}),
        "validation_mode": validation_mode,
        "validation_sample_fraction": validation_sample_fraction,
        "validation_sample_max_rows": validation_sample_max_rows,
    }

    best_config: dict[str, object] = {
        "source": "direct_lightgbm",
        "seed": random_seed,
        "model_family": MODEL_FAMILY,
        "inference_runtime": INFERENCE_RUNTIME,
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": [
            "pickup_borough_id",
            "pickup_zone_id",
            "pickup_service_zone_id",
            "dropoff_borough_id",
            "dropoff_zone_id",
            "dropoff_service_zone_id",
            "route_pair_id",
        ],
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
    }

    artifacts = persist_training_artifacts(
        run_id=run_id,
        dataset_uri=dataset_uri,
        canonical_df=canonical_df,
        split=split,
        booster=booster,
        metrics=metrics,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        validation_fraction=validation_fraction,
        artifact_root_s3=artifact_root_s3,
        validation_sample_rows=min(2048, len(split.valid_df)),
        quality_report=quality_report,
        best_config=best_config,
        lightgbm_params=trainer_meta.get("lightgbm_params", {}),
        runtime_config=runtime_config,
    )

    log_json(
        msg="train_pipeline_artifacts_persisted",
        run_id=run_id,
        artifact_root_s3=artifacts["artifact_root_s3"],
        model_uri=artifacts["model_uri"],
        manifest_uri=artifacts["manifest_uri"],
        feature_spec_uri=artifacts["feature_spec_uri"],
        contract_uri=artifacts["contract_uri"],
        quality_report_uri=artifacts["quality_report_uri"],
        validation_sample_uri=artifacts["validation_sample_uri"],
        metrics_uri=artifacts["metrics_uri"],
        train_rows=len(split.train_df),
        valid_rows=len(split.valid_df),
        cutoff_ts=split.cutoff_ts,
        best_iteration=trainer_meta.get("best_iteration", 0),
        mae=metrics["mae"],
        rmse=metrics["rmse"],
        r2=metrics["r2"],
    )

    result: dict[str, object] = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "artifact_root_s3": artifacts["artifact_root_s3"],
        "model_uri": artifacts["model_uri"],
        "manifest_uri": artifacts["manifest_uri"],
        "feature_spec_uri": artifacts["feature_spec_uri"],
        "encoding_spec_uri": artifacts["encoding_spec_uri"],
        "aggregate_spec_uri": artifacts["aggregate_spec_uri"],
        "label_spec_uri": artifacts["label_spec_uri"],
        "contract_uri": artifacts["contract_uri"],
        "validation_sample_uri": artifacts["validation_sample_uri"],
        "quality_report_uri": artifacts["quality_report_uri"],
        "best_config_uri": artifacts["best_config_uri"],
        "lightgbm_params_uri": artifacts["lightgbm_params_uri"],
        "runtime_config_uri": artifacts["runtime_config_uri"],
        "training_summary_uri": artifacts["training_summary_uri"],
        "metrics_uri": artifacts["metrics_uri"],
        "schema_hash": artifacts["schema_hash"],
        "feature_version": artifacts["feature_version"],
        "schema_version": artifacts["schema_version"],
        "gold_table": GOLD_TRAINING_TABLE,
        "source_silver_table": SOURCE_SILVER_TABLE,
        "model_family": MODEL_FAMILY,
        "inference_runtime": INFERENCE_RUNTIME,
        "cutoff_ts": split.cutoff_ts,
        "train_rows": len(split.train_df),
        "valid_rows": len(split.valid_df),
        "best_iteration": trainer_meta.get("best_iteration", 0),
        "validation_mode": validation_mode,
        "validation_sample_fraction": validation_sample_fraction,
        "validation_sample_max_rows": validation_sample_max_rows,
        "validation_fraction": validation_fraction,
    }

    log_json(
        msg="train_pipeline_success",
        run_id=run_id,
        dataset_uri=dataset_uri,
        artifact_root_s3=artifacts["artifact_root_s3"],
        model_uri=artifacts["model_uri"],
        schema_hash=artifacts["schema_hash"],
        feature_version=artifacts["feature_version"],
        schema_version=artifacts["schema_version"],
        train_rows=len(split.train_df),
        valid_rows=len(split.valid_df),
        cutoff_ts=split.cutoff_ts,
        best_iteration=trainer_meta.get("best_iteration", 0),
    )
    return _to_string_bundle(result)


@workflow
def train_pipeline_workflow(
    dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    artifact_root_s3: str = ARTIFACT_ROOT_S3,
    validation_mode: str = VALIDATION_MODE,
    validation_sample_fraction: float = VALIDATION_SAMPLE_FRACTION,
    validation_sample_max_rows: int = VALIDATION_SAMPLE_MAX_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, str]:
    return train_pipeline(
        dataset_uri=dataset_uri,
        validation_fraction=validation_fraction,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        artifact_root_s3=artifact_root_s3,
        validation_mode=validation_mode,
        validation_sample_fraction=validation_sample_fraction,
        validation_sample_max_rows=validation_sample_max_rows,
        random_seed=random_seed,
    )