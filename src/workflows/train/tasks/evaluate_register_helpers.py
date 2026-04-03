# src/workflows/train/tasks/evaluate_register_helpers.py
from __future__ import annotations

import ast
import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd

from workflows.train.tasks.train_pipeline_helpers import (
    CATEGORICAL_FEATURES,
    DEFAULT_INFERENCE_RUNTIME,
    DEFAULT_MLFLOW_EXPERIMENT,
    DEFAULT_MODEL_FAMILY,
    DEFAULT_ONNX_OPSET,
    DEFAULT_RANDOM_SEED,
    DEFAULT_REGISTERED_MODEL_NAME,
    DEFAULT_TRAIN_PROFILE,
    DEFAULT_VALIDATION_FRACTION,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
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
    log_json,
    prepare_model_input_frame,
    read_json_uri,
    read_parquet_frame,
    sample_validation_frame,
    split_by_time,
    validate_and_canonicalize_gold_frame,
    validate_gold_contract,
    write_bytes_uri,
    write_json_uri,
)

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
    "random_seed",
    "num_boost_round",
    "early_stopping_rounds",
    "train_profile",
]

INT_KEYS = {
    "train_rows",
    "valid_rows",
    "validation_sample_max_rows",
    "random_seed",
    "num_boost_round",
    "early_stopping_rounds",
}
FLOAT_KEYS = {"validation_fraction", "validation_sample_fraction"}
STRING_KEYS = {
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
    "validation_mode",
    "train_profile",
}


def _as_str(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _require_bundle_keys(bundle: Mapping[str, object], required: list[str], *, label: str) -> None:
    missing = [key for key in required if key not in bundle or bundle[key] in {None, ""}]
    if missing:
        raise ValueError(f"{label} is missing required keys: {missing}")


def _normalize_bundle_value(key: str, value: object) -> object:
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return ""

    if key in STRING_KEYS:
        return text
    if key in INT_KEYS:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, bool) or not isinstance(parsed, int):
            raise ValueError(f"Expected integer for {key}, got {type(parsed).__name__}")
        return parsed
    if key in FLOAT_KEYS:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, bool) or not isinstance(parsed, (int, float)):
            raise ValueError(f"Expected float for {key}, got {type(parsed).__name__}")
        return float(parsed)

    return text


def load_training_bundle(training_bundle: Mapping[str, object]) -> dict[str, object]:
    _require_bundle_keys(training_bundle, TRAINING_BUNDLE_REQUIRED_KEYS, label="training_bundle")
    normalized: dict[str, object] = {}
    for key, value in training_bundle.items():
        normalized[str(key)] = _normalize_bundle_value(str(key), value)

    if str(normalized["validation_mode"]) not in {"full", "sample"}:
        raise ValueError("training_bundle.validation_mode must be 'full' or 'sample'")
    if str(normalized["train_profile"]) not in {"staging", "prod"}:
        raise ValueError("training_bundle.train_profile must be 'staging' or 'prod'")

    return normalized


def evaluation_output_uris(training_bundle: Mapping[str, object]) -> dict[str, str]:
    bundle = load_training_bundle(training_bundle)
    root = artifact_uri_join(str(bundle["artifact_root_s3"]), "evaluation")
    return {
        "evaluation_artifact_root_s3": root,
        "evaluation_metrics_uri": artifact_uri_join(root, "metrics", "evaluation_metrics.json"),
        "evaluation_summary_uri": artifact_uri_join(root, "summary", "evaluation_summary.json"),
        "onnx_uri": artifact_uri_join(root, "onnx", "model.onnx"),
        "onnx_manifest_uri": artifact_uri_join(root, "onnx", "onnx_manifest.json"),
        "onnx_parity_uri": artifact_uri_join(root, "onnx", "onnx_parity.json"),
        "registry_payload_uri": artifact_uri_join(root, "registry", "registry_payload.json"),
    }


def _read_json_dict(uri: str, *, label: str) -> dict[str, object]:
    payload = read_json_uri(uri)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {uri}")
    return payload


def _load_booster_from_uri(model_uri: str) -> object:
    from lightgbm import Booster

    temp_dir = Path(tempfile.mkdtemp(prefix="train_eval_model_"))
    local_model_path = temp_dir / "model.txt"
    filesystem, path = filesystem_and_path(model_uri)
    if model_uri.startswith("s3://"):
        with filesystem.open_input_file(path) as src, local_model_path.open("wb") as dst:
            dst.write(src.read())
    else:
        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(source)
        local_model_path.write_bytes(source.read_bytes())
    return Booster(model_file=str(local_model_path))


def _load_validation_sample(bundle: Mapping[str, object], valid_df: pd.DataFrame) -> pd.DataFrame:
    sample_uri = str(bundle.get("validation_sample_uri", "")).strip()
    sample_df: pd.DataFrame | None = None

    if sample_uri:
        try:
            sample_df = read_parquet_frame(sample_uri)
        except Exception as exc:
            log_json(msg="validation_sample_read_failed", validation_sample_uri=sample_uri, error=str(exc))
            sample_df = None

    if sample_df is None or sample_df.empty:
        sample_df = sample_validation_frame(
            valid_df,
            mode=VALIDATION_MODE,
            fraction=VALIDATION_SAMPLE_FRACTION,
            max_rows=min(VALIDATION_SAMPLE_MAX_ROWS, len(valid_df)),
            seed=int(bundle.get("random_seed", DEFAULT_RANDOM_SEED)),
        )

    return validate_and_canonicalize_gold_frame(sample_df)


def build_evaluation_context(
    training_bundle: Mapping[str, object],
    *,
    validation_sample_rows: int = 2048,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, object]:
    bundle = load_training_bundle(training_bundle)

    current_feature_spec = build_feature_spec()
    current_schema_hash = build_schema_hash(current_feature_spec)

    manifest = _read_json_dict(str(bundle["manifest_uri"]), label="manifest")
    feature_spec = _read_json_dict(str(bundle["feature_spec_uri"]), label="feature_spec")
    contract = _read_json_dict(str(bundle["contract_uri"]), label="contract")

    if feature_spec != current_feature_spec:
        raise ValueError("Training feature_spec does not match the current Gold contract")
    if _as_str(contract.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training contract hash does not match the current Gold contract")
    if _as_str(manifest.get("schema_hash")) != current_schema_hash:
        raise ValueError("Training manifest hash does not match the current Gold contract")
    if manifest.get("feature_columns") != list(FEATURE_COLUMNS):
        raise ValueError("Training feature column order does not match the current Gold contract")
    if manifest.get("categorical_features") != list(CATEGORICAL_FEATURES):
        raise ValueError("Training categorical feature contract does not match the current Gold contract")
    if _as_str(manifest.get("schema_version")) != current_feature_spec["schema_version"]:
        raise ValueError("Training schema version does not match the current Gold contract")
    if _as_str(manifest.get("feature_version")) != current_feature_spec["feature_version"]:
        raise ValueError("Training feature version does not match the current Gold contract")
    if _as_str(manifest.get("run_id")) != _as_str(bundle["run_id"]):
        raise ValueError("Training manifest run_id does not match the training bundle")
    if _as_str(manifest.get("gold_table")) != _as_str(bundle["gold_table"]):
        raise ValueError("Training manifest gold_table does not match the training bundle")
    if _as_str(manifest.get("source_silver_table")) != _as_str(bundle["source_silver_table"]):
        raise ValueError("Training manifest source_silver_table does not match the training bundle")

    booster = _load_booster_from_uri(str(bundle["model_uri"]))

    gold_df = load_gold_frame(str(bundle["dataset_uri"]))
    validate_gold_contract(gold_df, strict_dtypes=False, label="Gold input frame")
    canonical_df = validate_and_canonicalize_gold_frame(gold_df)

    split = split_by_time(canonical_df, validation_fraction=float(bundle["validation_fraction"]))

    manifest_cutoff = _as_str(manifest.get("cutoff_ts"))
    if manifest_cutoff and str(split.cutoff_ts) != manifest_cutoff:
        raise ValueError(
            f"Validation split cutoff drifted from training: training_cutoff={manifest_cutoff}, current_cutoff={split.cutoff_ts}"
        )

    if split.train_df.empty or split.valid_df.empty:
        raise ValueError(
            f"Evaluation split produced empty partitions: train_rows={len(split.train_df)}, valid_rows={len(split.valid_df)}"
        )

    sample_df = _load_validation_sample(bundle, split.valid_df)
    if len(sample_df) > validation_sample_rows:
        sample_df = sample_df.sample(n=validation_sample_rows, random_state=random_seed).copy()
        sample_df = sample_df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    return {
        "training_bundle": bundle,
        "manifest": manifest,
        "feature_spec": feature_spec,
        "contract": contract,
        "schema_hash": current_schema_hash,
        "booster": booster,
        "canonical_df": canonical_df,
        "split": split,
        "validation_sample_df": sample_df,
        "current_feature_spec": current_feature_spec,
    }


def evaluate_model_from_context(context: Mapping[str, object]) -> dict[str, object]:
    bundle = cast(dict[str, object], context["training_bundle"])
    manifest = cast(dict[str, object], context["manifest"])
    feature_spec = cast(dict[str, object], context["feature_spec"])
    split = cast(SplitResult, context["split"])
    booster = context["booster"]

    valid_df = split.valid_df
    if valid_df.empty:
        raise ValueError("Validation frame is empty")

    features = prepare_model_input_frame(valid_df)
    y_true = valid_df[LABEL_COLUMN].to_numpy(dtype="float64")

    best_iteration = int(manifest.get("best_iteration", 0) or 0)
    if best_iteration > 0:
        y_pred = booster.predict(features, num_iteration=best_iteration)
    else:
        y_pred = booster.predict(features)

    metrics: dict[str, object] = compute_regression_metrics(y_true, np.asarray(y_pred, dtype="float64"))
    metrics.update(
        {
            "run_id": _as_str(bundle["run_id"]),
            "dataset_uri": _as_str(bundle["dataset_uri"]),
            "schema_hash": _as_str(manifest.get("schema_hash", "")),
            "feature_version": _as_str(feature_spec["feature_version"]),
            "schema_version": _as_str(feature_spec["schema_version"]),
            "train_rows": len(split.train_df),
            "valid_rows": len(valid_df),
            "cutoff_ts": str(split.cutoff_ts),
            "gold_table": _as_str(manifest.get("gold_table", "")),
            "source_silver_table": _as_str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
            "model_family": _as_str(manifest.get("model_family", DEFAULT_MODEL_FAMILY)),
            "inference_runtime": _as_str(manifest.get("inference_runtime", DEFAULT_INFERENCE_RUNTIME)),
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
    import onnxruntime as ort
    from onnxmltools.convert.common.data_types import FloatTensorType
    from onnxmltools.convert.lightgbm import convert as convert_lightgbm

    bundle = cast(dict[str, object], context["training_bundle"])
    manifest = cast(dict[str, object], context["manifest"])
    feature_spec = cast(dict[str, object], context["feature_spec"])
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
    with tempfile.TemporaryDirectory(prefix=f"{_as_str(bundle['run_id'])}_onnx_") as tmpdir:
        tmp_path = Path(tmpdir) / "model.onnx"
        tmp_path.write_bytes(onnx_model.SerializeToString())

        session = ort.InferenceSession(str(tmp_path), providers=["CPUExecutionProvider"])
        input_name = session.get_inputs()[0].name
        onnx_pred = session.run(None, {input_name: onnx_features})[0]

        write_bytes_uri(tmp_path.read_bytes(), output_uris["onnx_uri"])

    best_iteration = int(manifest.get("best_iteration", 0) or 0)
    if best_iteration > 0:
        booster_pred = booster.predict(booster_features, num_iteration=best_iteration)
    else:
        booster_pred = booster.predict(booster_features)

    booster_pred = np.asarray(booster_pred, dtype="float64").reshape(-1)
    onnx_pred = np.asarray(onnx_pred, dtype="float64").reshape(-1)

    if booster_pred.shape != onnx_pred.shape:
        raise ValueError(f"Prediction shape mismatch: booster={booster_pred.shape}, onnx={onnx_pred.shape}")

    parity_metrics: dict[str, object] = compute_regression_metrics(booster_pred, onnx_pred)
    parity_metrics.update(
        {
            "run_id": _as_str(bundle["run_id"]),
            "dataset_uri": _as_str(bundle["dataset_uri"]),
            "schema_hash": _as_str(manifest.get("schema_hash", "")),
            "feature_version": _as_str(feature_spec["feature_version"]),
            "schema_version": _as_str(feature_spec["schema_version"]),
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

    evaluation_summary: dict[str, object] = {
        "run_id": _as_str(bundle["run_id"]),
        "dataset_uri": _as_str(bundle["dataset_uri"]),
        "model_uri": _as_str(bundle["model_uri"]),
        "manifest_uri": _as_str(bundle["manifest_uri"]),
        "feature_spec_uri": _as_str(bundle["feature_spec_uri"]),
        "contract_uri": _as_str(bundle["contract_uri"]),
        "validation_sample_uri": _as_str(bundle["validation_sample_uri"]),
        "artifact_root_s3": _as_str(bundle["artifact_root_s3"]),
        "schema_hash": _as_str(manifest.get("schema_hash", "")),
        "feature_version": _as_str(feature_spec["feature_version"]),
        "schema_version": _as_str(feature_spec["schema_version"]),
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

    onnx_manifest: dict[str, object] = {
        "run_id": _as_str(bundle["run_id"]),
        "dataset_uri": _as_str(bundle["dataset_uri"]),
        "artifact_root_s3": output_uris["evaluation_artifact_root_s3"],
        "source_training_bundle": {
            "run_id": _as_str(bundle["run_id"]),
            "model_uri": _as_str(bundle["model_uri"]),
            "manifest_uri": _as_str(bundle["manifest_uri"]),
            "feature_spec_uri": _as_str(bundle["feature_spec_uri"]),
            "contract_uri": _as_str(bundle["contract_uri"]),
        },
        "manifest": manifest,
        "feature_spec": feature_spec,
        "schema_hash": _as_str(manifest.get("schema_hash", "")),
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
        "gold_table": _as_str(manifest.get("gold_table", "")),
        "source_silver_table": _as_str(manifest.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "model_family": _as_str(manifest.get("model_family", DEFAULT_MODEL_FAMILY)),
        "inference_runtime": _as_str(manifest.get("inference_runtime", DEFAULT_INFERENCE_RUNTIME)),
        "cutoff_ts": str(split.cutoff_ts),
        "onnx_opset": int(onnx_opset),
    }

    write_json_uri(dict(evaluation_metrics or {}), output_uris["evaluation_metrics_uri"])
    write_json_uri(evaluation_summary, output_uris["evaluation_summary_uri"])
    write_json_uri(onnx_manifest, output_uris["onnx_manifest_uri"])
    write_json_uri(parity_metrics, output_uris["onnx_parity_uri"])

    return {
        **output_uris,
        "evaluation_metrics": dict(evaluation_metrics or {}),
        "evaluation_summary": evaluation_summary,
        "onnx_manifest": onnx_manifest,
        "onnx_parity": parity_metrics,
    }


def build_mlflow_tags(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
    *,
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, str]:
    bundle = load_training_bundle(training_bundle)
    return {
        "problem_type": "trip_duration_regression",
        "prediction_timing": "pre_trip",
        "orchestration": "flyte",
        "model_family": _as_str(bundle.get("model_family", DEFAULT_MODEL_FAMILY)),
        "inference_runtime": _as_str(bundle.get("inference_runtime", DEFAULT_INFERENCE_RUNTIME)),
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "schema_hash": _as_str(bundle.get("schema_hash", "")),
        "gold_table": _as_str(bundle.get("gold_table", "")),
        "source_silver_table": _as_str(bundle.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "train_cutoff_ts": _as_str(bundle.get("cutoff_ts", "")),
        "validation_fraction": _as_str(bundle.get("validation_fraction", "")),
        "validation_mode": _as_str(bundle.get("validation_mode", "")),
        "train_profile": _as_str(bundle.get("train_profile", DEFAULT_TRAIN_PROFILE)),
        "registered_model_name": registered_model_name,
        "mlflow_experiment_name": mlflow_experiment_name,
        "run_id": _as_str(bundle.get("run_id", "")),
        "evaluation_artifact_root_s3": _as_str(evaluation_bundle.get("evaluation_artifact_root_s3", "")),
    }


def build_mlflow_params(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
    *,
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, str]:
    bundle = load_training_bundle(training_bundle)
    return {
        "run_id": _as_str(bundle.get("run_id", "")),
        "train_rows": _as_str(bundle.get("train_rows", "")),
        "valid_rows": _as_str(bundle.get("valid_rows", "")),
        "validation_fraction": _as_str(bundle.get("validation_fraction", "")),
        "validation_mode": _as_str(bundle.get("validation_mode", "")),
        "validation_sample_fraction": _as_str(bundle.get("validation_sample_fraction", "")),
        "validation_sample_max_rows": _as_str(bundle.get("validation_sample_max_rows", "")),
        "random_seed": _as_str(bundle.get("random_seed", "")),
        "num_boost_round": _as_str(bundle.get("num_boost_round", "")),
        "early_stopping_rounds": _as_str(bundle.get("early_stopping_rounds", "")),
        "schema_hash": _as_str(bundle.get("schema_hash", "")),
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "artifact_root_s3": _as_str(bundle.get("artifact_root_s3", "")),
        "model_uri": _as_str(bundle.get("model_uri", "")),
        "manifest_uri": _as_str(bundle.get("manifest_uri", "")),
        "feature_spec_uri": _as_str(bundle.get("feature_spec_uri", "")),
        "contract_uri": _as_str(bundle.get("contract_uri", "")),
        "quality_report_uri": _as_str(bundle.get("quality_report_uri", "")),
        "registered_model_name": registered_model_name,
        "mlflow_experiment_name": mlflow_experiment_name,
        "evaluation_artifact_root_s3": _as_str(evaluation_bundle.get("evaluation_artifact_root_s3", "")),
        "evaluation_metrics_uri": _as_str(evaluation_bundle.get("evaluation_metrics_uri", "")),
        "evaluation_summary_uri": _as_str(evaluation_bundle.get("evaluation_summary_uri", "")),
        "onnx_uri": _as_str(evaluation_bundle.get("onnx_uri", "")),
        "onnx_manifest_uri": _as_str(evaluation_bundle.get("onnx_manifest_uri", "")),
        "onnx_parity_uri": _as_str(evaluation_bundle.get("onnx_parity_uri", "")),
    }


def build_mlflow_metrics(
    training_bundle: Mapping[str, object],
    evaluation_metrics: Mapping[str, object],
    parity_metrics: Mapping[str, object],
) -> dict[str, float]:
    bundle = load_training_bundle(training_bundle)

    def prefix_metrics(prefix: str, source: Mapping[str, object]) -> dict[str, float]:
        out: dict[str, float] = {}
        for key, value in source.items():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float, np.integer, np.floating)):
                out[f"{prefix}{key}"] = float(value)
        return out

    metrics: dict[str, float] = {}
    metrics.update(prefix_metrics("eval_", evaluation_metrics))
    metrics.update(prefix_metrics("onnx_", parity_metrics))
    metrics["train_rows"] = float(int(bundle["train_rows"]))
    metrics["valid_rows"] = float(int(bundle["valid_rows"]))
    metrics["validation_fraction"] = float(bundle["validation_fraction"])
    metrics["validation_sample_fraction"] = float(bundle["validation_sample_fraction"])
    metrics["validation_sample_max_rows"] = float(bundle["validation_sample_max_rows"])
    return metrics


def _write_json_artifact_local(payload: object, directory: Path, filename: str) -> Path:
    path = directory / filename
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def log_and_register_mlflow(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
    *,
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
    mlflow_tracking_uri: str = "",
) -> dict[str, object]:
    import mlflow
    import mlflow.lightgbm
    from lightgbm import Booster

    if not registered_model_name:
        raise ValueError("registered_model_name must not be empty")
    if mlflow_tracking_uri and not mlflow_tracking_uri.startswith(("http://", "https://")):
        raise ValueError(f"MLFLOW_TRACKING_URI must be an HTTP(S) tracking server URI, got {mlflow_tracking_uri!r}")

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

    manifest = _read_json_dict(str(bundle["manifest_uri"]), label="manifest")
    feature_spec = _read_json_dict(str(bundle["feature_spec_uri"]), label="feature_spec")
    contract = _read_json_dict(str(bundle["contract_uri"]), label="contract")
    quality_report = _read_json_dict(str(bundle["quality_report_uri"]), label="quality_report")
    runtime_config = _read_json_dict(str(bundle["runtime_config_uri"]), label="runtime_config")
    training_summary = _read_json_dict(str(bundle["training_summary_uri"]), label="training_summary")
    evaluation_metrics = _read_json_dict(str(evaluation_bundle["evaluation_metrics_uri"]), label="evaluation_metrics")
    evaluation_summary = _read_json_dict(str(evaluation_bundle["evaluation_summary_uri"]), label="evaluation_summary")
    onnx_manifest = _read_json_dict(str(evaluation_bundle["onnx_manifest_uri"]), label="onnx_manifest")
    onnx_parity = _read_json_dict(str(evaluation_bundle["onnx_parity_uri"]), label="onnx_parity")

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
    if _as_str(onnx_manifest.get("run_id")) != _as_str(bundle["run_id"]):
        raise ValueError("ONNX manifest run_id does not match the training run")
    if _as_str(evaluation_summary.get("schema_hash")) != current_schema_hash:
        raise ValueError("Evaluation summary hash does not match the current Gold contract")

    if mlflow_tracking_uri:
        mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(mlflow_experiment_name)

    model_uri = _as_str(bundle["model_uri"])
    model_local_dir = Path(tempfile.mkdtemp(prefix="mlflow_register_model_"))
    model_local_path = model_local_dir / "model.txt"
    filesystem, path = filesystem_and_path(model_uri)
    if model_uri.startswith("s3://"):
        with filesystem.open_input_file(path) as src, model_local_path.open("wb") as dst:
            dst.write(src.read())
    else:
        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(source)
        model_local_path.write_bytes(source.read_bytes())

    booster = Booster(model_file=str(model_local_path))

    with mlflow.start_run(run_name=_as_str(bundle["run_id"])) as active_run:
        active_run_id = active_run.info.run_id
        model_info = mlflow.lightgbm.log_model(booster, artifact_path="model")

        mlflow.set_tags(
            build_mlflow_tags(
                bundle,
                evaluation_bundle,
                registered_model_name=registered_model_name,
                mlflow_experiment_name=mlflow_experiment_name,
            )
        )
        mlflow.log_params(
            build_mlflow_params(
                bundle,
                evaluation_bundle,
                registered_model_name=registered_model_name,
                mlflow_experiment_name=mlflow_experiment_name,
            )
        )
        mlflow.log_metrics(build_mlflow_metrics(bundle, evaluation_metrics, onnx_parity))

        with tempfile.TemporaryDirectory(prefix="mlflow_artifacts_") as tmpdir:
            tmp = Path(tmpdir)
            artifacts = [
                _write_json_artifact_local(manifest, tmp, "manifest.json"),
                _write_json_artifact_local(feature_spec, tmp, "feature_spec.json"),
                _write_json_artifact_local(contract, tmp, "contract.json"),
                _write_json_artifact_local(quality_report, tmp, "quality_report.json"),
                _write_json_artifact_local(runtime_config, tmp, "runtime_config.json"),
                _write_json_artifact_local(training_summary, tmp, "training_summary.json"),
                _write_json_artifact_local(evaluation_metrics, tmp, "evaluation_metrics.json"),
                _write_json_artifact_local(evaluation_summary, tmp, "evaluation_summary.json"),
                _write_json_artifact_local(onnx_manifest, tmp, "onnx_manifest.json"),
                _write_json_artifact_local(onnx_parity, tmp, "onnx_parity.json"),
            ]
            for artifact_path in artifacts[:3]:
                mlflow.log_artifact(str(artifact_path), artifact_path="metadata")
            for artifact_path in artifacts[3:6]:
                mlflow.log_artifact(str(artifact_path), artifact_path="validation")
            for artifact_path in artifacts[6:8]:
                mlflow.log_artifact(str(artifact_path), artifact_path="evaluation")
            for artifact_path in artifacts[8:]:
                mlflow.log_artifact(str(artifact_path), artifact_path="onnx")

        registered_model = mlflow.register_model(model_info.model_uri, registered_model_name)

    return {
        "mlflow_tracking_uri": mlflow_tracking_uri,
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


def build_registry_payload(
    training_bundle: Mapping[str, object],
    evaluation_bundle: Mapping[str, object],
    *,
    registered_model_name: str = DEFAULT_REGISTERED_MODEL_NAME,
    mlflow_experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, object]:
    bundle = load_training_bundle(training_bundle)
    return {
        "run_id": _as_str(bundle.get("run_id", "")),
        "dataset_uri": _as_str(bundle.get("dataset_uri", "")),
        "artifact_root_s3": _as_str(bundle.get("artifact_root_s3", "")),
        "model_uri": _as_str(bundle.get("model_uri", "")),
        "manifest_uri": _as_str(bundle.get("manifest_uri", "")),
        "feature_spec_uri": _as_str(bundle.get("feature_spec_uri", "")),
        "contract_uri": _as_str(bundle.get("contract_uri", "")),
        "validation_sample_uri": _as_str(bundle.get("validation_sample_uri", "")),
        "quality_report_uri": _as_str(bundle.get("quality_report_uri", "")),
        "runtime_config_uri": _as_str(bundle.get("runtime_config_uri", "")),
        "training_summary_uri": _as_str(bundle.get("training_summary_uri", "")),
        "evaluation_artifact_root_s3": _as_str(evaluation_bundle.get("evaluation_artifact_root_s3", "")),
        "evaluation_metrics_uri": _as_str(evaluation_bundle.get("evaluation_metrics_uri", "")),
        "evaluation_summary_uri": _as_str(evaluation_bundle.get("evaluation_summary_uri", "")),
        "onnx_uri": _as_str(evaluation_bundle.get("onnx_uri", "")),
        "onnx_manifest_uri": _as_str(evaluation_bundle.get("onnx_manifest_uri", "")),
        "onnx_parity_uri": _as_str(evaluation_bundle.get("onnx_parity_uri", "")),
        "schema_hash": _as_str(bundle.get("schema_hash", "")),
        "feature_version": _as_str(bundle.get("feature_version", "")),
        "schema_version": _as_str(bundle.get("schema_version", "")),
        "gold_table": _as_str(bundle.get("gold_table", "")),
        "source_silver_table": _as_str(bundle.get("source_silver_table", SOURCE_SILVER_TABLE)),
        "model_family": _as_str(bundle.get("model_family", DEFAULT_MODEL_FAMILY)),
        "inference_runtime": _as_str(bundle.get("inference_runtime", DEFAULT_INFERENCE_RUNTIME)),
        "cutoff_ts": _as_str(bundle.get("cutoff_ts", "")),
        "train_rows": int(bundle.get("train_rows", 0) or 0),
        "valid_rows": int(bundle.get("valid_rows", 0) or 0),
        "validation_fraction": float(bundle.get("validation_fraction", DEFAULT_VALIDATION_FRACTION)),
        "validation_mode": _as_str(bundle.get("validation_mode", VALIDATION_MODE)),
        "train_profile": _as_str(bundle.get("train_profile", DEFAULT_TRAIN_PROFILE)),
        "registered_model_name": registered_model_name,
        "mlflow_experiment_name": mlflow_experiment_name,
    }