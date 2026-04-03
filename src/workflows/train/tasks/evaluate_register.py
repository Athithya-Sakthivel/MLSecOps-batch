# src/workflows/train/tasks/evaluate_register.py
from __future__ import annotations

import os
from collections.abc import Mapping

from flytekit import Resources, task, workflow

from workflows.train.tasks.evaluate_register_helpers import (
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_VALIDATION_FRACTION,
    REGISTERED_MODEL_NAME,
    build_evaluation_context,
    build_registry_payload,
    build_task_environment,
    evaluate_model_from_context,
    evaluation_output_uris,
    export_onnx_and_parity_from_context,
    load_training_bundle,
    log_and_register_mlflow,
    log_json,
    write_json_uri,
)

EVALUATE_REGISTER_TASK_CPU = os.environ.get("EVALUATE_REGISTER_TASK_CPU", "500m")
EVALUATE_REGISTER_TASK_MEM = os.environ.get("EVALUATE_REGISTER_TASK_MEM", "768Mi")
EVALUATE_REGISTER_TASK_RETRIES = int(os.environ.get("EVALUATE_REGISTER_TASK_RETRIES", "1"))


def _stringify_bundle(payload: Mapping[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in payload.items()}


@task(
    cache=False,
    environment=build_task_environment(),
    retries=EVALUATE_REGISTER_TASK_RETRIES,
    limits=Resources(cpu=EVALUATE_REGISTER_TASK_CPU, mem=EVALUATE_REGISTER_TASK_MEM),
)
def evaluate_register(
    training_bundle: dict[str, str],
    gold_dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
    random_seed: int = DEFAULT_RANDOM_SEED,
    registered_model_name: str = REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "").strip(),
) -> dict[str, str]:
    """
    Evaluate the trained model, export ONNX, and register the final model in MLflow.

    Expects the string-only bundle returned by train_pipeline.
    """
    normalized_bundle = load_training_bundle(training_bundle)

    log_json(
        msg="evaluate_register_start",
        run_id=normalized_bundle["run_id"],
        gold_dataset_uri=gold_dataset_uri,
        validation_fraction=validation_fraction,
        onnx_opset=onnx_opset,
        validation_sample_rows=validation_sample_rows,
        random_seed=random_seed,
        registered_model_name=registered_model_name,
        mlflow_experiment_name=mlflow_experiment_name,
        mlflow_tracking_uri=mlflow_tracking_uri,
        schema_hash=normalized_bundle["schema_hash"],
        feature_version=normalized_bundle["feature_version"],
        schema_version=normalized_bundle["schema_version"],
    )

    context = build_evaluation_context(
        normalized_bundle,
        gold_dataset_uri,
        validation_fraction=validation_fraction,
        random_seed=random_seed,
        validation_sample_rows=validation_sample_rows,
    )

    evaluation_metrics = evaluate_model_from_context(context)
    onnx_bundle = export_onnx_and_parity_from_context(
        context,
        onnx_opset=onnx_opset,
        validation_sample_rows=validation_sample_rows,
        random_seed=random_seed,
        evaluation_metrics=evaluation_metrics,
    )

    output_uris = evaluation_output_uris(normalized_bundle)
    registry_payload = build_registry_payload(normalized_bundle, onnx_bundle)
    write_json_uri(registry_payload, output_uris["registry_payload_uri"])

    mlflow_bundle = log_and_register_mlflow(
        normalized_bundle,
        onnx_bundle,
        registered_model_name=registered_model_name,
        mlflow_experiment_name=mlflow_experiment_name,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    result: dict[str, object] = {
        **normalized_bundle,
        **onnx_bundle,
        **mlflow_bundle,
        "registry_payload_uri": output_uris["registry_payload_uri"],
        "evaluation_metrics": evaluation_metrics,
        "registry_payload": registry_payload,
    }

    log_json(
        msg="evaluate_register_success",
        run_id=normalized_bundle["run_id"],
        registered_model_name=mlflow_bundle.get("registered_model_name", registered_model_name),
        registered_model_version=mlflow_bundle.get("registered_model_version", ""),
        mlflow_run_id=mlflow_bundle.get("mlflow_run_id", ""),
        onnx_uri=onnx_bundle["onnx_uri"],
        onnx_manifest_uri=onnx_bundle["onnx_manifest_uri"],
        onnx_parity_uri=onnx_bundle["onnx_parity_uri"],
        evaluation_metrics_uri=onnx_bundle["evaluation_metrics_uri"],
        evaluation_summary_uri=onnx_bundle["evaluation_summary_uri"],
        registry_payload_uri=output_uris["registry_payload_uri"],
        schema_hash=normalized_bundle["schema_hash"],
        feature_version=normalized_bundle["feature_version"],
        schema_version=normalized_bundle["schema_version"],
    )
    return _stringify_bundle(result)


@workflow
def evaluate_register_workflow(
    training_bundle: dict[str, str],
    gold_dataset_uri: str,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
    random_seed: int = DEFAULT_RANDOM_SEED,
    registered_model_name: str = REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "").strip(),
) -> dict[str, str]:
    return evaluate_register(
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