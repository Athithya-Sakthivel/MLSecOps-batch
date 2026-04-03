# src/workflows/train/tasks/evaluate_register_helpers.py
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd

from workflows.train.tasks.train_pipeline_helpers import (
    CATEGORICAL_FEATURES,
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    INFERENCE_RUNTIME,
    LABEL_COLUMN,
    MODEL_FAMILY,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    TRAIN_PROFILE,
    VALIDATION_MODE,
    VALIDATION_SAMPLE_FRACTION,
    VALIDATION_SAMPLE_MAX_ROWS,
    SplitResult,
    align_feature_frame,
    artifact_uri_join,
    build_feature_spec,
    build_schema_hash,
    compute_regression_metrics,
    filesystem_and_path,
    load_gold_frame,
    prepare_model_input_frame,
    read_json_uri,
    read_parquet_frame,
    sample_validation_frame,
    split_by_cutoff,
    split_by_time,
    validate_and_canonicalize_gold_frame,
    validate_gold_contract,
    write_bytes_uri,
    write_json_uri,
)

EVALUATE_TASK_CPU = os.environ.get("EVALUATE_TASK_CPU", "500m")
EVALUATE_TASK_MEM = os.environ.get("EVALUATE_TASK_MEM", "768Mi")
EVALUATE_TASK_RETRIES = int(os.environ.get("EVALUATE_TASK_RETRIES", "1"))

REGISTER_TASK_CPU = os.environ.get("REGISTER_TASK_CPU", "500m")
REGISTER_TASK_MEM = os.environ.get("REGISTER_TASK_MEM", "768Mi")
REGISTER_TASK_RETRIES = int(os.environ.get("REGISTER_TASK_RETRIES", "1"))

REGISTERED_MODEL_NAME = os.environ.get(
    "MLFLOW_REGISTERED_MODEL_NAME",
    os.environ.get("MODEL_REGISTRY_NAME", "trip_duration_eta_lgbm"),
).strip()

TRAINING_BUNDLE_REQUIRED_KEYS: list[str] = [
    "run_id",
    "dataset_uri",
    "artifact_root_s3",
    "model_uri",
    "manifest_uri",
    "feature_spec_uri",
    "contract_uri",
    "validation_sample_uri",
    "quality_report_uri",
    "metrics_uri",
    "runtime_config_uri",
    "training_summary_uri",
    "schema_hash",
    "feature_version",
    "schema_version",
    "gold_table",
    "source_silver_table",
    "model_family",
    "inference_runtime",
    "cutoff_ts",
    "train_rows",
    "valid_rows",
    "validation_fraction",
    "validation_mode",
    "validation_sample_fraction",
    "validation_sample_max_rows",
]


def _as_str(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _json_default(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _require_bundle_keys(bundle: Mapping[str, object], required: Sequence[str], *, label: str) -> None:
    missing = [key for key in required if key not in bundle or bundle[key] in {None, ""}]
    if missing:
        raise ValueError(f"{label} is missing required keys: {missing}")


def _download_uri_to_local(uri: str, target: Path) -> Path:
    filesystem, path = filesystem_and_path(uri)
    target.parent.mkdir(parents=True, exist_ok=True)

    if uri.startswith("s3://"):
        with filesystem.open_input_file(path) as src, target.open("wb") as dst:
            dst.write(src.read())
        return target

    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(source)
    target.write_bytes(source.read_bytes())
    return target


def _read_json_uri_as_dict(uri: str) -> dict[str, object]:
    payload = read_json_uri(uri)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {uri}")
    return payload


def _ensure_timestamp_utc(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _load_booster_from_uri(model_uri: str) -> object:
    from lightgbm import Booster

    temp_dir = Path(tempfile.mkdtemp(prefix="train_eval_model_"))
    local_model_path = temp_dir / "model.txt"
    _download_uri_to_local(model_uri, local_model_path)
    return Booster(model_file=str(local_model_path))


def _training_feature_contract_ok(
    training_feature_spec: dict[str, object],
    training_contract: dict[str, object],
    training_manifest: dict[str, object],
) -> tuple[dict[str, object], str]:
    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    if training_feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if _as_str(training_contract.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if _as_str(training_manifest.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training manifest hash does not match the current Gold contract")
    if training_manifest.get("feature_columns") != list(FEATURE_COLUMNS):
        raise ValueError("Training feature column order does not match the current Gold contract")
    if training_manifest.get("categorical_features") != list(CATEGORICAL_FEATURES):
        raise ValueError("Training categorical feature contract does not match the current Gold contract")
    if _as_str(training_manifest.get("schema_version")) != current_feature_spec["schema_version"]:
        raise ValueError("Training schema version does not match the current Gold contract")
    if _as_str(training_manifest.get("feature_version")) != current_feature_spec["feature_version"]:
        raise ValueError("Training feature version does not match the current Gold contract")

    return current_feature_spec, current_schema_hash


def load_training_bundle(training_bundle: Mapping[str, object]) -> dict[str, str]:
    """
    Normalize and validate the bundle emitted by train_pipeline.
    Returns a string-only mapping so downstream code stays stable.
    """
    _require_bundle_keys(training_bundle, TRAINING_BUNDLE_REQUIRED_KEYS, label="training_bundle")
    bundle = {str(key): _as_str(value) for key, value in training_bundle.items()}

    if bundle["validation_mode"] not in {"full", "sample"}:
        raise ValueError("training_bundle.validation_mode must be 'full' or 'sample'")

    return bundle


def evaluation_artifact_root(training_bundle: Mapping[str, object]) -> str:
    bundle = load_training_bundle(training_bundle)
    return artifact_uri_join(bundle["artifact_root_s3"], bundle["run_id"], "evaluation")


def evaluation_output_uris(training_bundle: Mapping[str, object]) -> dict[str, str]:
    root = evaluation_artifact_root(training_bundle)
    return {
        "evaluation_artifact_root_s3": root,
        "evaluation_metrics_uri": artifact_uri_join(root, "metrics", "evaluation_metrics.json"),
        "evaluation_summary_uri": artifact_uri_join(root, "summary", "evaluation_summary.json"),
        "onnx_uri": artifact_uri_join(root, "onnx", "model.onnx"),
        "onnx_manifest_uri": artifact_uri_join(root, "onnx", "onnx_manifest.json"),
        "onnx_parity_uri": artifact_uri_join(root, "onnx", "onnx_parity.json"),
        "registry_payload_uri": artifact_uri_join(root, "registry", "registry_payload.json"),
    }


def _load_context_manifest_artifacts(bundle: dict[str, str]) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    manifest = _read_json_uri_as_dict(bundle["manifest_uri"])
    feature_spec = _read_json_uri_as_dict(bundle["feature_spec_uri"])
    contract = _read_json_uri_as_dict(bundle["contract_uri"])
    return manifest, feature_spec, contract


def _load_gold_dataframe_for_evaluation(gold_dataset_uri: str) -> pd.DataFrame:
    raw_df = load_gold_frame(gold_dataset_uri)
    validate_gold_contract(raw_df, strict_dtypes=False, label="Gold input frame")
    canonical_df = validate_and_canonicalize_gold_frame(raw_df)
    return canonical_df


def build_evaluation_context(
    training_bundle: Mapping[str, object],
    gold_dataset_uri: str,
    *,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    random_seed: int = DEFAULT_RANDOM_SEED,
    validation_sample_rows: int = 2048,
) -> dict[str, object]:
    """
    Build a single evaluation context once, with all contract checks centralized.
    The downstream functions should consume this context rather than reloading state.
    """
    bundle = load_training_bundle(training_bundle)

    if bundle["dataset_uri"] and bundle["dataset_uri"] != gold_dataset_uri:
        raise ValueError(
            f"training_bundle.dataset_uri does not match gold_dataset_uri: "
            f"{bundle['dataset_uri']} != {gold_dataset_uri}"
        )

    manifest, training_feature_spec, training_contract = _load_context_manifest_artifacts(bundle)
    current_feature_spec, current_schema_hash = _training_feature_contract_ok(
        training_feature_spec=training_feature_spec,
        training_contract=training_contract,
        training_manifest=manifest,
    )

    booster = _load_booster_from_uri(bundle["model_uri"])
    canonical_df = _load_gold_dataframe_for_evaluation(gold_dataset_uri)

    manifest_cutoff = manifest.get("cutoff_ts") or bundle.get("cutoff_ts")
    if manifest_cutoff:
        split = split_by_cutoff(canonical_df, _ensure_timestamp_utc(manifest_cutoff))
    else:
        effective_validation_fraction = float(manifest.get("validation_fraction", validation_fraction))
        split = split_by_time(canonical_df, validation_fraction=effective_validation_fraction)

    if split.train_df.empty or split.valid_df.empty:
        raise ValueError(
            f"Evaluation split produced empty partitions: train_rows={len(split.train_df)}, "
            f"valid_rows={len(split.valid_df)}"
        )

    sample_uri = bundle.get("validation_sample_uri", "")
    sample_df: pd.DataFrame | None = None
    if sample_uri:
        try:
            sample_df = read_parquet_frame(sample_uri)
        except FileNotFoundError:
            sample_df = None

    if sample_df is None or sample_df.empty:
        sample_df = sample_validation_frame(
            split.valid_df,
            mode=str(bundle.get("validation_mode", VALIDATION_MODE)),
            fraction=float(bundle.get("validation_sample_fraction", VALIDATION_SAMPLE_FRACTION)),
            max_rows=min(validation_sample_rows, int(bundle.get("validation_sample_max_rows", VALIDATION_SAMPLE_MAX_ROWS))),
            seed=random_seed,
        )

    if sample_df.empty:
        raise ValueError("Validation parity sample is empty")

    sample_df = validate_and_canonicalize_gold_frame(sample_df)

    context: dict[str, object] = {
        "training_bundle": bundle,
        "gold_dataset_uri": gold_dataset_uri,
        "manifest": manifest,
        "training_feature_spec": training_feature_spec,
        "training_contract": training_contract,
        "current_feature_spec": current_feature_spec,
        "current_schema_hash": current_schema_hash,
        "booster": booster,
        "canonical_df": canonical_df,
        "split": split,
        "validation_sample_df": sample_df,
        "validation_fraction": validation_fraction,
        "validation_sample_rows": validation_sample_rows,
        "random_seed": random_seed,
    }
    return context


def evaluate_model_from_context(context: Mapping[str, object]) -> dict[str, object]:
    bundle = cast(dict[str, str], context["training_bundle"])
    manifest = cast(dict[str, object], context["manifest"])
    current_feature_spec = cast(dict[str, object], context["current_feature_spec"])
    current_schema_hash = _as_str(context["current_schema_hash"])
    split = cast(SplitResult, context["split"])
    booster = context["booster"]

    valid_df = split.valid_df
    if valid_df.empty:
        raise ValueError("Validation frame is empty")

    features = prepare_model_input_frame(valid_df)
    y_true = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")

    best_iteration = int(_as_str(manifest.get("best_iteration", 0)) or 0)
    if best_iteration > 0:
        y_pred = booster.predict(features, num_iteration=best_iteration)
    else:
        y_pred = booster.predict(features)

    metrics: dict[str, object] = compute_regression_metrics(
        y_true,
        np.asarray(y_pred, dtype="float64"),
    )
    metrics.update(
        {
            "run_id": bundle["run_id"],
            "dataset_uri": bundle["dataset_uri"],
            "schema_hash": current_schema_hash,
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "train_rows": len(split.train_df),
            "valid_rows": len(valid_df),
            "cutoff_ts": str(split.cutoff_ts),
            "gold_table": _as_str(manifest.get("gold_table", "")),
            "source_silver_table": _as_str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
            "model_family": _as_str(manifest.get("model_family", MODEL_FAMILY)),
            "inference_runtime": _as_str(manifest.get("inference_runtime", INFERENCE_RUNTIME)),
            "best_iteration": best_iteration,
        }
    )
    return metrics


def export_onnx_and_parity_from_context(
    context: Mapping[str, object],
    *,
    onnx_opset: int = DEFAULT_ONNX_OPSET,
    validation_sample_rows: int = 2048,
    random_seed: int = DEFAULT_RANDOM_SEED,
    evaluation_metrics: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """
    Export ONNX from the trained LightGBM booster and compute parity against the
    same validation sample used for the evaluation stage.
    """
    import onnxruntime as ort
    from onnxmltools.convert.common.data_types import FloatTensorType
    from onnxmltools.convert.lightgbm import convert as convert_lightgbm

    bundle = cast(dict[str, str], context["training_bundle"])
    manifest = cast(dict[str, object], context["manifest"])
    current_feature_spec = cast(dict[str, object], context["current_feature_spec"])
    current_schema_hash = _as_str(context["current_schema_hash"])
    split = cast(SplitResult, context["split"])
    booster = context["booster"]
    sample_df = cast(pd.DataFrame, context["validation_sample_df"])

    if sample_df.empty:
        raise ValueError("Validation sample is empty")

    sample_df = validate_and_canonicalize_gold_frame(sample_df)

    if len(sample_df) > validation_sample_rows:
        sample_df = sample_df.sample(n=validation_sample_rows, random_state=random_seed).copy()
        sample_df = sample_df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    booster_features = prepare_model_input_frame(sample_df)
    onnx_features = align_feature_frame(sample_df).to_numpy(dtype=np.float32, copy=False)

    onnx_model = convert_lightgbm(
        booster,
        initial_types=[("input", FloatTensorType([None, len(FEATURE_COLUMNS)]))],
        target_opset=onnx_opset,
        zipmap=False,
    )

    output_uris = evaluation_output_uris(bundle)
    with tempfile.TemporaryDirectory(prefix=f"{bundle['run_id']}_onnx_") as tmpdir:
        tmp = Path(tmpdir)
        onnx_local_path = tmp / "model.onnx"
        onnx_local_path.write_bytes(onnx_model.SerializeToString())

        session = ort.InferenceSession(str(onnx_local_path), providers=["CPUExecutionProvider"])
        input_name = session.get_inputs()[0].name
        onnx_pred = session.run(None, {input_name: onnx_features})[0]

    booster_best_iteration = int(_as_str(manifest.get("best_iteration", 0)) or 0)
    if booster_best_iteration > 0:
        booster_pred = booster.predict(booster_features, num_iteration=booster_best_iteration)
    else:
        booster_pred = booster.predict(booster_features)

    booster_pred = np.asarray(booster_pred, dtype="float64").reshape(-1)
    onnx_pred = np.asarray(onnx_pred, dtype="float64").reshape(-1)

    if booster_pred.shape != onnx_pred.shape:
        raise ValueError(f"Prediction shape mismatch: booster={booster_pred.shape}, onnx={onnx_pred.shape}")

    parity_metrics: dict[str, object] = compute_regression_metrics(booster_pred, onnx_pred)
    parity_metrics.update(
        {
            "run_id": bundle["run_id"],
            "dataset_uri": bundle["dataset_uri"],
            "schema_hash": current_schema_hash,
            "feature_version": current_feature_spec["feature_version"],
            "schema_version": current_feature_spec["schema_version"],
            "gold_table": _as_str(manifest.get("gold_table", "")),
            "source_silver_table": _as_str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
            "sample_rows": len(sample_df),
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "cutoff_ts": str(split.cutoff_ts),
            "onnx_opset": int(onnx_opset),
            "max_abs_error": float(np.max(np.abs(booster_pred - onnx_pred))),
            "validation_sample_rows": int(validation_sample_rows),
            "random_seed": int(random_seed),
        }
    )

    onnx_manifest: dict[str, object] = {
        "run_id": bundle["run_id"],
        "dataset_uri": bundle["dataset_uri"],
        "artifact_root_s3": output_uris["evaluation_artifact_root_s3"],
        "source_training_bundle": {
            "run_id": bundle["run_id"],
            "model_uri": bundle["model_uri"],
            "manifest_uri": bundle["manifest_uri"],
            "feature_spec_uri": bundle["feature_spec_uri"],
            "contract_uri": bundle["contract_uri"],
        },
        "manifest": manifest,
        "feature_spec": current_feature_spec,
        "schema_hash": current_schema_hash,
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
        "gold_table": _as_str(manifest.get("gold_table", "")),
        "source_silver_table": _as_str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "model_family": _as_str(manifest.get("model_family", MODEL_FAMILY)),
        "inference_runtime": _as_str(manifest.get("inference_runtime", INFERENCE_RUNTIME)),
        "cutoff_ts": str(split.cutoff_ts),
        "onnx_opset": int(onnx_opset),
    }

    evaluation_summary: dict[str, object] = {
        "run_id": bundle["run_id"],
        "dataset_uri": bundle["dataset_uri"],
        "model_uri": bundle["model_uri"],
        "manifest_uri": bundle["manifest_uri"],
        "feature_spec_uri": bundle["feature_spec_uri"],
        "contract_uri": bundle["contract_uri"],
        "validation_sample_uri": bundle["validation_sample_uri"],
        "artifact_root_s3": output_uris["evaluation_artifact_root_s3"],
        "schema_hash": current_schema_hash,
        "feature_version": current_feature_spec["feature_version"],
        "schema_version": current_feature_spec["schema_version"],
        "train_rows": len(split.train_df),
        "valid_rows": len(split.valid_df),
        "cutoff_ts": str(split.cutoff_ts),
        "onnx_uri": output_uris["onnx_uri"],
        "onnx_manifest_uri": output_uris["onnx_manifest_uri"],
        "onnx_parity_uri": output_uris["onnx_parity_uri"],
        "evaluation_metrics_uri": output_uris["evaluation_metrics_uri"],
        "validation_sample_rows": len(sample_df),
        "validation_sample_max_rows": int(validation_sample_rows),
        "random_seed": int(random_seed),
        "validation_mode": _as_str(bundle.get("validation_mode", VALIDATION_MODE)),
    }

    write_json_uri(evaluation_metrics or {}, output_uris["evaluation_metrics_uri"])
    write_json_uri(evaluation_summary, output_uris["evaluation_summary_uri"])
    write_bytes_uri(onnx_model.SerializeToString(), output_uris["onnx_uri"])
    write_json_uri(onnx_manifest, output_uris["onnx_manifest_uri"])
    write_json_uri(parity_metrics, output_uris["onnx_parity_uri"])

    result: dict[str, object] = {
        **output_uris,
        "evaluation_metrics": dict(evaluation_metrics or {}),
        "evaluation_summary": evaluation_summary,
        "onnx_manifest": onnx_manifest,
        "onnx_parity": parity_metrics,
    }
    return result


def build_mlflow_tags(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
) -> dict[str, str]:
    bundle = load_training_bundle(training_bundle)
    tags: dict[str, str] = {
        "problem_type": "trip_duration_regression",
        "prediction_timing": "pre_trip",
        "orchestration": "flyte",
        "model_family": _as_str(bundle.get("model_family", MODEL_FAMILY)),
        "inference_runtime": _as_str(bundle.get("inference_runtime", INFERENCE_RUNTIME)),
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "schema_hash": _as_str(bundle.get("schema_hash", "")),
        "gold_table": _as_str(bundle.get("gold_table", "")),
        "source_silver_table": _as_str(bundle.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "train_cutoff_ts": _as_str(bundle.get("cutoff_ts", "")),
        "validation_fraction": _as_str(bundle.get("validation_fraction", "")),
        "validation_mode": _as_str(bundle.get("validation_mode", "")),
        "train_profile": TRAIN_PROFILE,
        "registered_model_name": REGISTERED_MODEL_NAME,
        "run_id": _as_str(bundle.get("run_id", "")),
        "evaluation_artifact_root_s3": _as_str(evaluation_bundle.get("evaluation_artifact_root_s3", "")),
    }
    return tags


def build_mlflow_params(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
) -> dict[str, str]:
    bundle = load_training_bundle(training_bundle)
    params: dict[str, str] = {
        "run_id": _as_str(bundle.get("run_id", "")),
        "train_rows": _as_str(bundle.get("train_rows", "")),
        "valid_rows": _as_str(bundle.get("valid_rows", "")),
        "validation_fraction": _as_str(bundle.get("validation_fraction", "")),
        "validation_mode": _as_str(bundle.get("validation_mode", "")),
        "validation_sample_fraction": _as_str(bundle.get("validation_sample_fraction", "")),
        "validation_sample_max_rows": _as_str(bundle.get("validation_sample_max_rows", "")),
        "schema_hash": _as_str(bundle.get("schema_hash", "")),
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "artifact_root_s3": _as_str(bundle.get("artifact_root_s3", "")),
        "model_uri": _as_str(bundle.get("model_uri", "")),
        "manifest_uri": _as_str(bundle.get("manifest_uri", "")),
        "feature_spec_uri": _as_str(bundle.get("feature_spec_uri", "")),
        "contract_uri": _as_str(bundle.get("contract_uri", "")),
        "quality_report_uri": _as_str(bundle.get("quality_report_uri", "")),
        "onnx_uri": _as_str(evaluation_bundle.get("onnx_uri", "")),
        "onnx_manifest_uri": _as_str(evaluation_bundle.get("onnx_manifest_uri", "")),
        "onnx_parity_uri": _as_str(evaluation_bundle.get("onnx_parity_uri", "")),
        "evaluation_summary_uri": _as_str(evaluation_bundle.get("evaluation_summary_uri", "")),
    }
    return params


def build_mlflow_metrics(
    training_bundle: Mapping[str, object],
    evaluation_metrics: Mapping[str, object],
    parity_metrics: Mapping[str, object],
) -> dict[str, float]:
    bundle = load_training_bundle(training_bundle)

    metrics: dict[str, float] = {}
    for source in (evaluation_metrics, parity_metrics):
        for key, value in source.items():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float, np.integer, np.floating)):
                metrics[str(key)] = float(value)

    metrics["train_rows"] = float(int(bundle["train_rows"]))
    metrics["valid_rows"] = float(int(bundle["valid_rows"]))
    metrics["validation_fraction"] = float(bundle["validation_fraction"])
    metrics["validation_sample_fraction"] = float(bundle["validation_sample_fraction"])
    metrics["validation_sample_max_rows"] = float(bundle["validation_sample_max_rows"])
    return metrics


def _write_json_artifact_local(payload: object, directory: Path, filename: str) -> Path:
    path = directory / filename
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")
    return path


def log_and_register_mlflow(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
    *,
    registered_model_name: str = REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "").strip(),
) -> dict[str, object]:
    """
    Log the trained LightGBM model and its artifacts to MLflow, then register the model version.
    This function expects the MLflow tracking server HTTP URI, not the PostgreSQL backend URI.
    """
    import mlflow
    import mlflow.lightgbm
    from lightgbm import Booster

    if not registered_model_name:
        raise ValueError("registered_model_name must not be empty")
    if not mlflow_tracking_uri:
        raise ValueError("MLFLOW_TRACKING_URI must be set to the MLflow tracking server HTTP endpoint")
    if not mlflow_tracking_uri.startswith(("http://", "https://")):
        raise ValueError(
            f"MLFLOW_TRACKING_URI must be an HTTP(S) tracking server URI, got {mlflow_tracking_uri!r}"
        )

    bundle = load_training_bundle(training_bundle)
    _require_bundle_keys(
        evaluation_bundle,
        [
            "evaluation_metrics_uri",
            "evaluation_summary_uri",
            "onnx_uri",
            "onnx_manifest_uri",
            "onnx_parity_uri",
            "evaluation_artifact_root_s3",
        ],
        label="evaluation_bundle",
    )

    manifest = _read_json_uri_as_dict(bundle["manifest_uri"])
    feature_spec = _read_json_uri_as_dict(bundle["feature_spec_uri"])
    contract = _read_json_uri_as_dict(bundle["contract_uri"])
    quality_report = _read_json_uri_as_dict(bundle["quality_report_uri"])
    runtime_config = _read_json_uri_as_dict(bundle["runtime_config_uri"])
    training_summary = _read_json_uri_as_dict(bundle["training_summary_uri"])
    evaluation_metrics = _read_json_uri_as_dict(_as_str(evaluation_bundle["evaluation_metrics_uri"]))
    evaluation_summary = _read_json_uri_as_dict(_as_str(evaluation_bundle["evaluation_summary_uri"]))
    onnx_manifest = _read_json_uri_as_dict(_as_str(evaluation_bundle["onnx_manifest_uri"]))
    onnx_parity = _read_json_uri_as_dict(_as_str(evaluation_bundle["onnx_parity_uri"]))

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    if feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if _as_str(contract.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if _as_str(manifest.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training manifest hash does not match the current Gold contract")
    if _as_str(onnx_manifest.get("schema_hash")) != current_schema_hash:
        raise ValueError("ONNX manifest hash does not match the current Gold contract")
    if _as_str(onnx_manifest.get("run_id")) != bundle["run_id"]:
        raise ValueError("ONNX manifest run_id does not match the training run")
    if _as_str(evaluation_summary.get("schema_hash")) != current_schema_hash:
        raise ValueError("Evaluation summary hash does not match the current Gold contract")

    tracking_uri = mlflow_tracking_uri
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(mlflow_experiment_name)

    model_uri = bundle["model_uri"]
    model_local_dir = Path(tempfile.mkdtemp(prefix="mlflow_register_model_"))
    model_local_path = model_local_dir / "model.txt"
    _download_uri_to_local(model_uri, model_local_path)
    booster = Booster(model_file=str(model_local_path))

    with mlflow.start_run(run_name=bundle["run_id"]) as active_run:
        active_run_id = active_run.info.run_id
        model_info = mlflow.lightgbm.log_model(
            booster,
            artifact_path="model",
        )

        tags = build_mlflow_tags(bundle, evaluation_bundle)
        tags.update(
            {
                "mlflow_run_id": active_run_id,
                "registered_model_name": registered_model_name,
            }
        )
        mlflow.set_tags(tags)

        params = build_mlflow_params(bundle, evaluation_bundle)
        mlflow.log_params(params)
        mlflow.log_metrics(build_mlflow_metrics(bundle, cast(Mapping[str, object], evaluation_metrics), cast(Mapping[str, object], onnx_parity)))

        with tempfile.TemporaryDirectory(prefix="mlflow_artifacts_") as tmpdir:
            tmp = Path(tmpdir)
            manifest_path = _write_json_artifact_local(manifest, tmp, "manifest.json")
            feature_spec_path = _write_json_artifact_local(feature_spec, tmp, "feature_spec.json")
            contract_path = _write_json_artifact_local(contract, tmp, "contract.json")
            quality_report_path = _write_json_artifact_local(quality_report, tmp, "quality_report.json")
            runtime_config_path = _write_json_artifact_local(runtime_config, tmp, "runtime_config.json")
            training_summary_path = _write_json_artifact_local(training_summary, tmp, "training_summary.json")
            evaluation_metrics_path = _write_json_artifact_local(evaluation_metrics, tmp, "evaluation_metrics.json")
            evaluation_summary_path = _write_json_artifact_local(evaluation_summary, tmp, "evaluation_summary.json")
            onnx_manifest_path = _write_json_artifact_local(onnx_manifest, tmp, "onnx_manifest.json")
            onnx_parity_path = _write_json_artifact_local(onnx_parity, tmp, "onnx_parity.json")

            mlflow.log_artifact(str(manifest_path), artifact_path="metadata")
            mlflow.log_artifact(str(feature_spec_path), artifact_path="metadata")
            mlflow.log_artifact(str(contract_path), artifact_path="metadata")
            mlflow.log_artifact(str(quality_report_path), artifact_path="validation")
            mlflow.log_artifact(str(runtime_config_path), artifact_path="metadata")
            mlflow.log_artifact(str(training_summary_path), artifact_path="metadata")
            mlflow.log_artifact(str(evaluation_metrics_path), artifact_path="evaluation")
            mlflow.log_artifact(str(evaluation_summary_path), artifact_path="evaluation")
            mlflow.log_artifact(str(onnx_manifest_path), artifact_path="onnx")
            mlflow.log_artifact(str(onnx_parity_path), artifact_path="onnx")

        registered_model = mlflow.register_model(model_info.model_uri, registered_model_name)

    result: dict[str, object] = {
        "mlflow_tracking_uri": tracking_uri,
        "mlflow_experiment_name": mlflow_experiment_name,
        "mlflow_run_id": active_run_id,
        "registered_model_name": registered_model_name,
        "registered_model_version": _as_str(getattr(registered_model, "version", "")),
        "registered_model_source_uri": model_info.model_uri,
        "schema_hash": current_schema_hash,
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "gold_table": _as_str(bundle.get("gold_table", "")),
        "source_silver_table": _as_str(bundle.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "train_cutoff_ts": _as_str(bundle.get("cutoff_ts", "")),
    }
    return result


def build_registry_payload(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
) -> dict[str, object]:
    bundle = load_training_bundle(training_bundle)
    return {
        "run_id": bundle["run_id"],
        "dataset_uri": bundle["dataset_uri"],
        "artifact_root_s3": bundle["artifact_root_s3"],
        "model_uri": bundle["model_uri"],
        "manifest_uri": bundle["manifest_uri"],
        "feature_spec_uri": bundle["feature_spec_uri"],
        "contract_uri": bundle["contract_uri"],
        "validation_sample_uri": bundle["validation_sample_uri"],
        "quality_report_uri": bundle["quality_report_uri"],
        "runtime_config_uri": bundle["runtime_config_uri"],
        "training_summary_uri": bundle["training_summary_uri"],
        "evaluation_artifact_root_s3": _as_str(evaluation_bundle.get("evaluation_artifact_root_s3", "")),
        "evaluation_metrics_uri": _as_str(evaluation_bundle.get("evaluation_metrics_uri", "")),
        "evaluation_summary_uri": _as_str(evaluation_bundle.get("evaluation_summary_uri", "")),
        "onnx_uri": _as_str(evaluation_bundle.get("onnx_uri", "")),
        "onnx_manifest_uri": _as_str(evaluation_bundle.get("onnx_manifest_uri", "")),
        "onnx_parity_uri": _as_str(evaluation_bundle.get("onnx_parity_uri", "")),
        "schema_hash": bundle["schema_hash"],
        "feature_version": bundle["feature_version"],
        "schema_version": bundle["schema_version"],
        "gold_table": bundle["gold_table"],
        "source_silver_table": bundle["source_silver_table"],
        "model_family": bundle["model_family"],
        "inference_runtime": bundle["inference_runtime"],
        "cutoff_ts": bundle["cutoff_ts"],
        "train_rows": bundle["train_rows"],
        "valid_rows": bundle["valid_rows"],
        "validation_fraction": bundle["validation_fraction"],
        "validation_mode": bundle["validation_mode"],
    }