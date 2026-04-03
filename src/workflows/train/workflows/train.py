# src/workflows/train/workflows/train.py
from __future__ import annotations

from flytekit import workflow

from workflows.train.tasks.evaluate_register import evaluate_register
from workflows.train.tasks.train_pipeline import train_pipeline
from workflows.train.tasks.train_pipeline_helpers import (
    ARTIFACT_ROOT_S3,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_VALIDATION_FRACTION,
    VALIDATION_MODE,
    VALIDATION_SAMPLE_FRACTION,
    VALIDATION_SAMPLE_MAX_ROWS,
)

__all__ = ["train"]


@workflow
def train(
    gold_dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    artifact_root_s3: str = ARTIFACT_ROOT_S3,
    validation_mode: str = VALIDATION_MODE,
    validation_sample_fraction: float = VALIDATION_SAMPLE_FRACTION,
    validation_sample_max_rows: int = VALIDATION_SAMPLE_MAX_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
    registered_model_name: str = "trip_duration_eta_lgbm",
    mlflow_experiment_name: str = "trip_duration_eta_lgbm",
    mlflow_tracking_uri: str = "",
) -> dict[str, str]:
    """
    End-to-end workflow:
    1) read and train on the Gold Iceberg table
    2) evaluate, export ONNX, and register in MLflow

    The only cross-stage payload is the compact training bundle emitted by train_pipeline.
    """
    training_bundle = train_pipeline(
        dataset_uri=gold_dataset_uri,
        validation_fraction=validation_fraction,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        artifact_root_s3=artifact_root_s3,
        validation_mode=validation_mode,
        validation_sample_fraction=validation_sample_fraction,
        validation_sample_max_rows=validation_sample_max_rows,
        random_seed=random_seed,
    )

    final_bundle = evaluate_register(
        training_bundle=training_bundle,
        gold_dataset_uri=gold_dataset_uri,
        validation_fraction=validation_fraction,
        onnx_opset=onnx_opset,
        validation_sample_rows=validation_sample_rows,
        random_seed=random_seed,
        registered_model_name=registered_model_name,
        mlflow_experiment_name=mlflow_experiment_name,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    return final_bundle