# src/workflows/train/launch_plans.py
from __future__ import annotations

import os

from flytekit import LaunchPlan
from workflows.train.train import train

__all__ = [
    "TRAIN_WORKFLOW_LP",
    "TRAIN_WORKFLOW_LP_NAME",
]

DEFAULT_MLFLOW_EXPERIMENT_NAME = "trip_eta_lgbm_production"
DEFAULT_TUNING_SAMPLE_ROWS = 50_000
DEFAULT_MAX_EVAL_ROWS = 250_000
DEFAULT_MAX_BOOST_ROUNDS = 20_000

MODEL_ARTIFACTS_S3_BUCKET = os.environ.get(
    "MODEL_ARTIFACTS_S3_BUCKET",
    "s3://e2e-mlops-data-681802563986/model-artifacts",
)

def _default_train_num_threads() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count - 1)


TRAIN_WORKFLOW_LP_NAME = "train_manual_lp"

TRAIN_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=train,
    name=TRAIN_WORKFLOW_LP_NAME,
    default_inputs={
        "mlflow_experiment_name": DEFAULT_MLFLOW_EXPERIMENT_NAME,
        "train_num_threads": _default_train_num_threads(),
        "tuning_sample_rows": DEFAULT_TUNING_SAMPLE_ROWS,
        "max_eval_rows": DEFAULT_MAX_EVAL_ROWS,
        "max_boost_rounds": DEFAULT_MAX_BOOST_ROUNDS,
    },
)