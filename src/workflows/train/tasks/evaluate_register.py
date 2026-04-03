from __future__ import annotations

from flytekit import Resources, task

from workflows.train.tasks.evaluate_register_helpers import (
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_REGISTERED_MODEL_NAME,
    DEFAULT_TRAIN_PROFILE,
    build_evaluation_context,
    build_registry_payload,
    evaluate_model_from_context,
    evaluation_output_uris,
    export_onnx_and_parity_from_context,
    load_training_bundle,
    log_and_register_mlflow,
    log_json,
    write_json_uri,
)
from workflows.train.tasks.train_pipeline_helpers import DEFAULT_MODEL_FAMILY

EVALUATE_REGISTER_LIMITS = Resources(cpu="500m", mem="768Mi")


def _stringify_bundle(payload: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in payload.items()}


@task(
    cache=False,
    retries=1,
    limits=EVALUATE_REGISTER_LIMITS,
)
def evaluate_register(
    train_bundle: dict[str, str],
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    model_family: str = DEFAULT_MODEL_FAMILY,
    train_profile: str = DEFAULT_TRAIN_PROFILE,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = "",
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, str]:
    if not registered_model_name.strip():
        raise ValueError("registered_model_name must not be empty")
    if model_family != DEFAULT_MODEL_FAMILY:
        raise ValueError(f"Unsupported model_family={model_family!r}; expected {DEFAULT_MODEL_FAMILY!r}")
    if train_profile not in {"staging", "prod"}:
        raise ValueError("train_profile must be 'staging' or 'prod'")
    if onnx_opset <= 0:
        raise ValueError("onnx_opset must be > 0")
    if validation_sample_rows <= 0:
        raise ValueError("validation_sample_rows must be > 0")

    normalized_bundle = load_training_bundle(train_bundle)
    if str(normalized_bundle["model_family"]) != model_family:
        raise ValueError("train_bundle model_family does not match the requested model_family")
    if str(normalized_bundle["train_profile"]) != train_profile:
        raise ValueError("train_bundle train_profile does not match the requested train_profile")

    log_json(
        msg="evaluate_register_start",
        run_id=normalized_bundle["run_id"],
        schema_hash=normalized_bundle["schema_hash"],
        feature_version=normalized_bundle["feature_version"],
        schema_version=normalized_bundle["schema_version"],
        registered_model_name=registered_model_name,
        model_family=model_family,
        train_profile=train_profile,
        mlflow_experiment_name=mlflow_experiment_name,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    context = build_evaluation_context(
        normalized_bundle,
        validation_sample_rows=validation_sample_rows,
        random_seed=random_seed,
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
    registry_payload = build_registry_payload(
        normalized_bundle,
        onnx_bundle,
        registered_model_name=registered_model_name,
        mlflow_experiment_name=mlflow_experiment_name,
    )
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
        "evaluation_artifact_root_s3": output_uris["evaluation_artifact_root_s3"],
        "evaluation_metrics_uri": output_uris["evaluation_metrics_uri"],
        "evaluation_summary_uri": output_uris["evaluation_summary_uri"],
        "onnx_uri": output_uris["onnx_uri"],
        "onnx_manifest_uri": output_uris["onnx_manifest_uri"],
        "onnx_parity_uri": output_uris["onnx_parity_uri"],
        "registry_payload_uri": output_uris["registry_payload_uri"],
        "mlflow_tracking_uri": mlflow_bundle["mlflow_tracking_uri"],
        "mlflow_experiment_name": mlflow_bundle["mlflow_experiment_name"],
        "mlflow_run_id": mlflow_bundle["mlflow_run_id"],
        "registered_model_name": mlflow_bundle["registered_model_name"],
        "registered_model_version": mlflow_bundle["registered_model_version"],
        "registered_model_source_uri": mlflow_bundle["registered_model_source_uri"],
        "schema_hash": mlflow_bundle["schema_hash"],
        "feature_version": mlflow_bundle["feature_version"],
        "schema_version": mlflow_bundle["schema_version"],
        "gold_table": mlflow_bundle["gold_table"],
        "source_silver_table": mlflow_bundle["source_silver_table"],
        "train_cutoff_ts": mlflow_bundle["train_cutoff_ts"],
    }

    log_json(
        msg="evaluate_register_success",
        run_id=normalized_bundle["run_id"],
        registered_model_name=mlflow_bundle["registered_model_name"],
        registered_model_version=mlflow_bundle["registered_model_version"],
        mlflow_run_id=mlflow_bundle["mlflow_run_id"],
        evaluation_artifact_root_s3=output_uris["evaluation_artifact_root_s3"],
        registry_payload_uri=output_uris["registry_payload_uri"],
        schema_hash=mlflow_bundle["schema_hash"],
        feature_version=mlflow_bundle["feature_version"],
        schema_version=mlflow_bundle["schema_version"],
    )
    return _stringify_bundle(result)