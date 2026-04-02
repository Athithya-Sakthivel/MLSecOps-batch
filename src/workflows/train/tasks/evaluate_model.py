from __future__ import annotations

from pathlib import Path
from typing import Any

from flytekit import task
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    artifact_sidecar_path,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    compute_regression_metrics,
    load_gold_frame,
    log_json,
    prepare_model_input_frame,
    read_json,
    read_json_if_exists,
    split_by_time,
    validate_gold_contract,
    validate_value_contracts,
)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def evaluate_model(
    train_artifacts_dir: FlyteDirectory,
    gold_dataset: FlyteFile,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
) -> dict[str, Any]:
    """
    Evaluate the fitted LightGBM booster on the exact chronological validation split.

    This task is strict about contract drift:
    - the training artifact feature spec must match the current Gold contract,
    - the saved contract hash must match the current Gold contract hash,
    - the feature column order and categorical contract must match,
    - the validation split cutoff must match the training run cutoff when present.
    """
    from lightgbm import Booster

    artifact_dir = Path(str(train_artifacts_dir))
    manifest = read_json(artifact_dir / "manifest.json")
    artifact_feature_spec = read_json(artifact_dir / "feature_spec.json")
    artifact_contract = read_json_if_exists(artifact_dir / "contract.json") or manifest

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    if artifact_feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if artifact_contract.get("schema_hash") != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if list(manifest.get("feature_columns", [])) != FEATURE_COLUMNS:
        raise ValueError("Training feature column order does not match the current Gold contract")
    if list(manifest.get("categorical_features", [])) != [
        "pickup_borough_id",
        "pickup_zone_id",
        "pickup_service_zone_id",
        "dropoff_borough_id",
        "dropoff_zone_id",
        "dropoff_service_zone_id",
        "route_pair_id",
    ]:
        raise ValueError("Training categorical feature contract does not match the current Gold contract")

    gold_uri = str(gold_dataset)
    log_json(
        msg="evaluate_model_start",
        train_artifacts_dir=str(artifact_dir),
        gold_dataset=gold_uri,
        validation_fraction=validation_fraction,
        schema_hash=current_schema_hash,
        feature_version=current_feature_spec["feature_version"],
        schema_version=current_feature_spec["schema_version"],
    )

    booster_path = artifact_dir / "model.txt"
    if not booster_path.is_file():
        raise FileNotFoundError(f"Missing LightGBM model artifact: {booster_path}")

    booster = Booster(model_file=str(booster_path))

    df = load_gold_frame(gold_uri)
    validate_gold_contract(df, strict_dtypes=False, label="Gold input frame")
    df = coerce_contract_dtypes(df)
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)
    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    effective_validation_fraction = float(manifest.get("validation_fraction", validation_fraction))
    split = split_by_time(df, validation_fraction=effective_validation_fraction)
    valid_df = split.valid_df

    manifest_cutoff = manifest.get("cutoff_ts")
    if manifest_cutoff is not None and str(manifest_cutoff) != str(split.cutoff_ts):
        raise ValueError(
            f"Validation split cutoff drifted from training: "
            f"training_cutoff={manifest_cutoff}, current_cutoff={split.cutoff_ts}"
        )

    # If the current Gold dataset has a sidecar contract, it must also match.
    current_dataset_contract = read_json_if_exists(artifact_sidecar_path(gold_uri, ".contract.json"))
    if current_dataset_contract is not None:
        if current_dataset_contract.get("schema_hash") != current_schema_hash:
            raise ValueError("Current Gold dataset contract hash does not match the training contract")
        if current_dataset_contract.get("feature_spec_json") not in {None, current_feature_spec}:
            raise ValueError("Current Gold dataset feature spec does not match the training contract")

    features = prepare_model_input_frame(valid_df)
    y_true = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")
    y_pred = booster.predict(features, num_iteration=booster.best_iteration or None)

    metrics: dict[str, Any] = compute_regression_metrics(y_true, y_pred)
    metrics.update(
        {
            "validation_rows": len(valid_df),
            "train_rows": len(split.train_df),
            "manifest_best_iteration": int(manifest.get("boost_rounds", 0)),
            "schema_hash": current_schema_hash,
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "cutoff_ts": split.cutoff_ts,
            "gold_table": manifest.get("gold_table", ""),
            "source_silver_table": manifest.get("source_silver_table", SOURCE_SILVER_TABLE),
        }
    )

    log_json(msg="evaluate_model_success", **metrics)
    return metrics