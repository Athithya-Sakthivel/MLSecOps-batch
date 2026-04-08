#!/usr/bin/env python3
from __future__ import annotations

from flytekit import workflow

from workflows.train.tasks.evaluate_and_register_task import evaluate_and_register_task
from workflows.train.tasks.train_model_task import train_model_task

__all__ = [
    "train",
    "train_and_register_workflow",
]


@workflow
def train(
    mlflow_experiment_name: str,
    train_num_threads: int,
    tuning_sample_rows: int,
    max_eval_rows: int,
    max_boost_rounds: int,
) -> str:
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


train_and_register_workflow = train