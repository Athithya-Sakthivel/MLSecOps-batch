# src/workflows/train/launch_plans.py
from __future__ import annotations

import os

from flytekit import LaunchPlan

from workflows.train.workflows.train import train

__all__ = [
    "TRAIN_WORKFLOW_LP",
    "TRAIN_WORKFLOW_LP_NAME",
]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value if value else default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name, "").strip()
    return float(value) if value else default


DEFAULT_S3_BUCKET = "e2e-mlops-data-681802563986"
DEFAULT_DATASET_URI = f"s3://{DEFAULT_S3_BUCKET}/iceberg/warehouse/gold/trip_training_matrix"
DEFAULT_ARTIFACT_ROOT_PREFIX = "artifacts/train"
DEFAULT_REGISTERED_MODEL_NAME = "trip_eta"
DEFAULT_TRAIN_PROFILE = "staging"
DEFAULT_MLFLOW_EXPERIMENT = "trip_duration_eta_lgbm"
DEFAULT_MLFLOW_TRACKING_URI = "http://mlflow.mlflow.svc.cluster.local:5000"

TRAIN_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=train,
    name="train_manual_lp",
    default_inputs={
        "dataset_uri": _env("TRAIN_DATASET_URI", DEFAULT_DATASET_URI),
        "s3_bucket": _env("S3_BUCKET", DEFAULT_S3_BUCKET),
        "artifact_root_prefix": _env("ARTIFACT_ROOT_PREFIX", DEFAULT_ARTIFACT_ROOT_PREFIX),
        "registered_model_name": _env("REGISTERED_MODEL_NAME", DEFAULT_REGISTERED_MODEL_NAME),
        "validation_fraction": _env_float("TRAIN_VALIDATION_FRACTION", 0.15),
        "random_seed": _env_int("TRAIN_RANDOM_SEED", 42),
        "num_boost_round": _env_int("TRAIN_NUM_BOOST_ROUND", 1500),
        "early_stopping_rounds": _env_int("TRAIN_EARLY_STOPPING_ROUNDS", 100),
        "model_family": _env("TRAIN_MODEL_FAMILY", "lightgbm"),
        "train_profile": _env("TRAIN_PROFILE", DEFAULT_TRAIN_PROFILE),
        "mlflow_experiment_name": _env("MLFLOW_EXPERIMENT_NAME", DEFAULT_MLFLOW_EXPERIMENT),
        "mlflow_tracking_uri": _env("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_TRACKING_URI),
        "onnx_opset": _env_int("TRAIN_ONNX_OPSET", 17),
        "validation_sample_rows": _env_int("TRAIN_VALIDATION_SAMPLE_ROWS", 2048),
    },
)

TRAIN_WORKFLOW_LP_NAME = TRAIN_WORKFLOW_LP.name