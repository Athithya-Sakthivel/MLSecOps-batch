# src/workflows/train/workflows/training_workflow.py
from __future__ import annotations

from flytekit import workflow

from workflows.train.tasks.evaluate_and_register_task import evaluate_and_register_task
from workflows.train.tasks.train_model_task import train_model_task


@workflow
def train(
    train_num_threads: int = 2,
    tuning_sample_rows: int = 100_000,
    max_boost_rounds: int = 5_000,
    mlflow_experiment_name: str = "trip_eta_lgbm",
    max_eval_rows: int = 100_000,
) -> str:
    """
    Train, evaluate, and register the frozen-matrix model bundle.

    The workflow is orchestration only:
    1. train_model_task() fits the model and publishes the bundle
    2. evaluate_and_register_task() evaluates the published bundle and logs registration metadata
    """
    training_result = train_model_task(
        train_num_threads=train_num_threads,
        tuning_sample_rows=tuning_sample_rows,
        max_boost_rounds=max_boost_rounds,
    )
    return evaluate_and_register_task(
        training_result=training_result,
        mlflow_experiment_name=mlflow_experiment_name,
        max_eval_rows=max_eval_rows,
    )