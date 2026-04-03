from __future__ import annotations

from flytekit import Resources, task

from workflows.train.tasks.train_pipeline_helpers import (
    DEFAULT_ARTIFACT_ROOT_PREFIX,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_MODEL_FAMILY,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_RANDOM_SEED,
    DEFAULT_REGISTERED_MODEL_NAME,
    DEFAULT_TRAIN_PROFILE,
    DEFAULT_VALIDATION_FRACTION,
    INFERENCE_RUNTIME,
    artifact_uri_join,
    build_quality_report,
    load_gold_frame,
    log_json,
    make_run_id,
    persist_training_artifacts,
    split_by_time,
    train_lightgbm_model,
    validate_and_canonicalize_gold_frame,
)

TRAIN_TASK_LIMITS = Resources(cpu="500m", mem="768Mi")


def _stringify_bundle(payload: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in payload.items()}


def _validate_non_empty(name: str, value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{name} must not be empty")
    return cleaned


@task(
    cache=False,
    retries=1,
    limits=TRAIN_TASK_LIMITS,
)
def train_pipeline(
    dataset_uri: str,
    s3_bucket: str,
    artifact_root_prefix: str = DEFAULT_ARTIFACT_ROOT_PREFIX,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    random_seed: int = DEFAULT_RANDOM_SEED,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    model_family: str = DEFAULT_MODEL_FAMILY,
    train_profile: str = DEFAULT_TRAIN_PROFILE,
) -> dict[str, str]:
    dataset_uri = _validate_non_empty("dataset_uri", dataset_uri)
    s3_bucket = _validate_non_empty("s3_bucket", s3_bucket)
    artifact_root_prefix = _validate_non_empty("artifact_root_prefix", artifact_root_prefix)

    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("validation_fraction must be > 0 and < 0.5")
    if random_seed < 0:
        raise ValueError("random_seed must be >= 0")
    if num_boost_round <= 0:
        raise ValueError("num_boost_round must be > 0")
    if early_stopping_rounds <= 0:
        raise ValueError("early_stopping_rounds must be > 0")
    if model_family != DEFAULT_MODEL_FAMILY:
        raise ValueError(f"Unsupported model_family={model_family!r}; expected {DEFAULT_MODEL_FAMILY!r}")
    if train_profile not in {"staging", "prod"}:
        raise ValueError("train_profile must be 'staging' or 'prod'")

    run_id = make_run_id()
    artifact_root_s3 = artifact_uri_join(f"s3://{s3_bucket}", artifact_root_prefix, run_id)

    log_json(
        msg="train_pipeline_start",
        run_id=run_id,
        dataset_uri=dataset_uri,
        artifact_root_s3=artifact_root_s3,
        validation_fraction=validation_fraction,
        random_seed=random_seed,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        model_family=model_family,
        train_profile=train_profile,
        inference_runtime=INFERENCE_RUNTIME,
    )

    raw_df = load_gold_frame(dataset_uri)
    canonical_df = validate_and_canonicalize_gold_frame(raw_df)
    split_result = split_by_time(canonical_df, validation_fraction=validation_fraction)

    if split_result.train_df.empty or split_result.valid_df.empty:
        raise ValueError(
            "Chronological split produced empty partitions: "
            f"train_rows={len(split_result.train_df)}, valid_rows={len(split_result.valid_df)}"
        )

    quality_report = build_quality_report(
        canonical_df,
        split=split_result,
        validation_mode="sample",
        validation_sample_fraction=0.10,
        validation_sample_max_rows=100000,
        random_seed=random_seed,
    )
    quality_report["train_profile"] = train_profile

    booster, metrics, trainer_meta = train_lightgbm_model(
        split_result.train_df,
        split_result.valid_df,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        seed=random_seed,
    )

    runtime_config = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "artifact_root_s3": artifact_root_s3,
        "validation_fraction": validation_fraction,
        "random_seed": random_seed,
        "num_boost_round": num_boost_round,
        "early_stopping_rounds": early_stopping_rounds,
        "train_profile": train_profile,
        "model_family": model_family,
        "inference_runtime": INFERENCE_RUNTIME,
        "best_iteration": trainer_meta["best_iteration"],
        "current_iteration": trainer_meta["current_iteration"],
        "lightgbm_params": trainer_meta["lightgbm_params"],
    }

    best_config = {
        "model_family": model_family,
        "train_profile": train_profile,
        "seed": random_seed,
        "validation_fraction": validation_fraction,
        "num_boost_round": num_boost_round,
        "early_stopping_rounds": early_stopping_rounds,
        "feature_columns": trainer_meta.get("feature_columns", []),
        "categorical_features": trainer_meta.get("categorical_features", []),
    }

    artifacts = persist_training_artifacts(
        run_id=run_id,
        dataset_uri=dataset_uri,
        canonical_df=canonical_df,
        split=split_result,
        booster=booster,
        metrics=metrics,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        validation_fraction=validation_fraction,
        artifact_root_s3=artifact_root_s3,
        validation_sample_rows=min(2048, len(split_result.valid_df)),
        quality_report=quality_report,
        best_config=best_config,
        lightgbm_params=trainer_meta["lightgbm_params"],
        runtime_config=runtime_config,
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
        "gold_table": artifacts["gold_table"],
        "source_silver_table": artifacts["source_silver_table"],
        "model_family": artifacts["model_family"],
        "inference_runtime": artifacts["inference_runtime"],
        "train_profile": artifacts["train_profile"],
        "validation_fraction": str(artifacts["validation_fraction"]),
        "validation_mode": artifacts["validation_mode"],
        "validation_sample_fraction": str(artifacts["validation_sample_fraction"]),
        "validation_sample_max_rows": str(artifacts["validation_sample_max_rows"]),
        "train_rows": str(artifacts["train_rows"]),
        "valid_rows": str(artifacts["valid_rows"]),
        "cutoff_ts": str(artifacts["cutoff_ts"]),
        "best_iteration": str(artifacts["best_iteration"]),
        "current_iteration": str(artifacts["current_iteration"]),
        "num_boost_round": str(artifacts["num_boost_round"]),
        "early_stopping_rounds": str(artifacts["early_stopping_rounds"]),
        "random_seed": str(random_seed),
        "registered_model_name": DEFAULT_REGISTERED_MODEL_NAME,
    }

    log_json(
        msg="train_pipeline_success",
        run_id=run_id,
        schema_hash=artifacts["schema_hash"],
        feature_version=artifacts["feature_version"],
        schema_version=artifacts["schema_version"],
        train_rows=artifacts["train_rows"],
        valid_rows=artifacts["valid_rows"],
        cutoff_ts=str(artifacts["cutoff_ts"]),
        artifact_root_s3=artifacts["artifact_root_s3"],
    )
    return _stringify_bundle(result)