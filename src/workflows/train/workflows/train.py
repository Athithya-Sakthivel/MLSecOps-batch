# src/workflows/train/workflows/train.py
from __future__ import annotations

from flytekit import workflow

from workflows.train.tasks.evaluate_register import evaluate_register
from workflows.train.tasks.train_pipeline import train_pipeline
from workflows.train.tasks.train_pipeline_helpers import (
    DEFAULT_ARTIFACT_ROOT_PREFIX,
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_MODEL_FAMILY,
    DEFAULT_NUM_BOOST_ROUND,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_REGISTERED_MODEL_NAME,
    DEFAULT_TRAIN_PROFILE,
    DEFAULT_VALIDATION_FRACTION,
)

DEFAULT_DATASET_URI = "s3://e2e-mlops-data-681802563986/iceberg/warehouse/gold/trip_training_matrix"
DEFAULT_S3_BUCKET = "e2e-mlops-data-681802563986"
DEFAULT_MLFLOW_TRACKING_URI = "http://mlflow.mlflow.svc.cluster.local:5000"

__all__ = ["train"]


@workflow
def train(
    dataset_uri: str = DEFAULT_DATASET_URI,
    s3_bucket: str = DEFAULT_S3_BUCKET,
    artifact_root_prefix: str = DEFAULT_ARTIFACT_ROOT_PREFIX,
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    random_seed: int = DEFAULT_RANDOM_SEED,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    model_family: str = DEFAULT_MODEL_FAMILY,
    train_profile: str = DEFAULT_TRAIN_PROFILE,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = DEFAULT_MLFLOW_TRACKING_URI,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
) -> dict[str, str]:
    train_bundle = train_pipeline(
        dataset_uri=dataset_uri,
        s3_bucket=s3_bucket,
        artifact_root_prefix=artifact_root_prefix,
        validation_fraction=validation_fraction,
        random_seed=random_seed,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        model_family=model_family,
        train_profile=train_profile,
    )

    return evaluate_register(
        train_bundle=train_bundle,
        registered_model_name=registered_model_name,
        model_family=model_family,
        train_profile=train_profile,
        mlflow_experiment_name=mlflow_experiment_name,
        mlflow_tracking_uri=mlflow_tracking_uri,
        onnx_opset=onnx_opset,
        validation_sample_rows=validation_sample_rows,
        random_seed=random_seed,
    )