# src/workflows/train/launch_plans.py
from __future__ import annotations

import os

from flytekit import LaunchPlan

from workflows.train.workflows.train import train

__all__ = [
    "TRAIN_GOLD_DATASET_URI",
    "TRAIN_WORKFLOW_LP",
    "TRAIN_WORKFLOW_LP_NAME",
]


def _resolve_gold_dataset_uri() -> str:
    explicit = os.environ.get("TRAIN_GOLD_DATASET_URI", "").strip()
    if explicit:
        return explicit

    bucket = os.environ.get("S3_BUCKET", "").strip()
    if bucket:
        return f"s3://{bucket}/iceberg/warehouse/gold/trip_training_matrix"

    raise RuntimeError(
        "TRAIN_GOLD_DATASET_URI is not set and S3_BUCKET is empty. "
        "Set TRAIN_GOLD_DATASET_URI explicitly, or set S3_BUCKET so the default Gold dataset URI can be derived."
    )


TRAIN_GOLD_DATASET_URI = _resolve_gold_dataset_uri()

# Manual entrypoint for the training workflow.
# gold_dataset_uri is defaulted here so executions do not launch with an empty input.
TRAIN_WORKFLOW_LP = LaunchPlan.get_or_create(
    workflow=train,
    name="train_manual_lp",
    default_inputs={
        "gold_dataset_uri": TRAIN_GOLD_DATASET_URI,
    },
)
TRAIN_WORKFLOW_LP_NAME = TRAIN_WORKFLOW_LP.name