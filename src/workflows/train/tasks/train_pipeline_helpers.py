from __future__ import annotations

import hashlib
import json
import tempfile
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from pyarrow import fs as pa_fs
from pyarrow import parquet as pq

DEFAULT_MLFLOW_EXPERIMENT = "trip_duration_eta_lgbm"
DEFAULT_MODEL_FAMILY = "lightgbm"
DEFAULT_INFERENCE_RUNTIME = "onnxruntime"
DEFAULT_TRAIN_PROFILE = "staging"
DEFAULT_ARTIFACT_ROOT_PREFIX = "artifacts/train"
DEFAULT_REGISTERED_MODEL_NAME = "trip_eta"

DEFAULT_VALIDATION_FRACTION = 0.15
DEFAULT_RANDOM_SEED = 42
DEFAULT_NUM_BOOST_ROUND = 1500
DEFAULT_EARLY_STOPPING_ROUNDS = 100
DEFAULT_ONNX_OPSET = 17

VALIDATION_MODE = "sample"
VALIDATION_SAMPLE_FRACTION = 0.10
VALIDATION_SAMPLE_MAX_ROWS = 100000

LABEL_COLUMN = "label_trip_duration_seconds"
TIMESTAMP_COLUMN = "as_of_ts"
IDENTIFIER_COLUMNS = ["trip_id"]

GOLD_FEATURE_VERSION = "trip_eta_lgbm_v1"
GOLD_SCHEMA_VERSION = "trip_eta_frozen_matrix_v1"
GOLD_TRAINING_TABLE = "iceberg.gold.trip_training_matrix"
GOLD_CONTRACT_TABLE = "iceberg.gold.trip_training_contracts"
SOURCE_SILVER_TABLE = "iceberg.silver.trip_canonical"

ROUTE_PAIR_BUCKETS = 4096
ROUTE_PAIR_HASH_SALT = "trip_eta_route_pair_v1"
SERVICE_ZONE_VALUES = ("airports", "boro zone", "yellow zone")

FEATURE_COLUMNS: list[str] = [
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "pickup_is_weekend",
    "pickup_borough_id",
    "pickup_zone_id",
    "pickup_service_zone_id",
    "dropoff_borough_id",
    "dropoff_zone_id",
    "dropoff_service_zone_id",
    "route_pair_id",
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

CATEGORICAL_FEATURES: list[str] = [
    "pickup_borough_id",
    "pickup_zone_id",
    "pickup_service_zone_id",
    "dropoff_borough_id",
    "dropoff_zone_id",
    "dropoff_service_zone_id",
    "route_pair_id",
]

TIME_INTEGER_FEATURES: list[str] = [
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "pickup_is_weekend",
]

FLOAT_FEATURES: list[str] = [
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

NUMERIC_FEATURES: list[str] = [column for column in FEATURE_COLUMNS if column not in CATEGORICAL_FEATURES]

REQUIRED_COLUMNS: list[str] = [
    *IDENTIFIER_COLUMNS,
    TIMESTAMP_COLUMN,
    "as_of_date",
    "schema_version",
    "feature_version",
    *FEATURE_COLUMNS,
    LABEL_COLUMN,
]

CANONICAL_GOLD_DTYPE_MAP: dict[str, str] = {
    "trip_id": "string",
    "as_of_ts": "datetime64[ns, UTC]",
    "as_of_date": "object",
    "schema_version": "string",
    "feature_version": "string",
    "pickup_hour": "int32",
    "pickup_dow": "int32",
    "pickup_month": "int32",
    "pickup_is_weekend": "int32",
    "pickup_borough_id": "int32",
    "pickup_zone_id": "int32",
    "pickup_service_zone_id": "int32",
    "dropoff_borough_id": "int32",
    "dropoff_zone_id": "int32",
    "dropoff_service_zone_id": "int32",
    "route_pair_id": "int32",
    "avg_duration_7d_zone_hour": "float64",
    "avg_fare_30d_zone": "float64",
    "trip_count_90d_zone_hour": "float64",
    LABEL_COLUMN: "float64",
}

# Compatibility aliases expected by downstream modules.
MLFLOW_EXPERIMENT = DEFAULT_MLFLOW_EXPERIMENT
MODEL_FAMILY = DEFAULT_MODEL_FAMILY
INFERENCE_RUNTIME = DEFAULT_INFERENCE_RUNTIME
TRAIN_PROFILE = DEFAULT_TRAIN_PROFILE
ARTIFACT_ROOT_PREFIX = DEFAULT_ARTIFACT_ROOT_PREFIX
REGISTERED_MODEL_NAME = DEFAULT_REGISTERED_MODEL_NAME
VALIDATION_FRACTION = DEFAULT_VALIDATION_FRACTION
RANDOM_SEED = DEFAULT_RANDOM_SEED
NUM_BOOST_ROUND = DEFAULT_NUM_BOOST_ROUND
EARLY_STOPPING_ROUNDS = DEFAULT_EARLY_STOPPING_ROUNDS
ONNX_OPSET = DEFAULT_ONNX_OPSET

__all__ = [
    "ARTIFACT_ROOT_PREFIX",
    "CANONICAL_GOLD_DTYPE_MAP",
    "CATEGORICAL_FEATURES",
    "DEFAULT_ARTIFACT_ROOT_PREFIX",
    "DEFAULT_EARLY_STOPPING_ROUNDS",
    "DEFAULT_INFERENCE_RUNTIME",
    "DEFAULT_MLFLOW_EXPERIMENT",
    "DEFAULT_MODEL_FAMILY",
    "DEFAULT_NUM_BOOST_ROUND",
    "DEFAULT_ONNX_OPSET",
    "DEFAULT_RANDOM_SEED",
    "DEFAULT_REGISTERED_MODEL_NAME",
    "DEFAULT_TRAIN_PROFILE",
    "DEFAULT_VALIDATION_FRACTION",
    "EARLY_STOPPING_ROUNDS",
    "FEATURE_COLUMNS",
    "FLOAT_FEATURES",
    "GOLD_CONTRACT_TABLE",
    "GOLD_FEATURE_VERSION",
    "GOLD_SCHEMA_VERSION",
    "GOLD_TRAINING_TABLE",
    "IDENTIFIER_COLUMNS",
    "INFERENCE_RUNTIME",
    "LABEL_COLUMN",
    "MLFLOW_EXPERIMENT",
    "MODEL_FAMILY",
    "NUMERIC_FEATURES",
    "NUM_BOOST_ROUND",
    "ONNX_OPSET",
    "RANDOM_SEED",
    "REGISTERED_MODEL_NAME",
    "REQUIRED_COLUMNS",
    "ROUTE_PAIR_BUCKETS",
    "ROUTE_PAIR_HASH_SALT",
    "SERVICE_ZONE_VALUES",
    "SOURCE_SILVER_TABLE",
    "TIMESTAMP_COLUMN",
    "TIME_INTEGER_FEATURES",
    "TRAIN_PROFILE",
    "VALIDATION_FRACTION",
    "VALIDATION_MODE",
    "VALIDATION_SAMPLE_FRACTION",
    "VALIDATION_SAMPLE_MAX_ROWS",
    "SplitResult",
    "align_feature_frame",
    "artifact_uri_join",
    "assert_no_leakage_columns",
    "best_lightgbm_params",
    "build_aggregate_spec",
    "build_contract_summary",
    "build_encoding_spec",
    "build_feature_spec",
    "build_label_spec",
    "build_quality_report",
    "build_schema_hash",
    "coerce_contract_dtypes",
    "compute_regression_metrics",
    "dataframe_dtype_map",
    "discover_parquet_files",
    "ensure_directory",
    "filesystem_and_path",
    "load_gold_frame",
    "log_json",
    "make_run_id",
    "persist_training_artifacts",
    "prepare_model_input_frame",
    "prepare_training_frame",
    "read_json_uri",
    "read_parquet_frame",
    "sample_validation_frame",
    "split_by_time",
    "train_lightgbm_model",
    "upload_file_to_uri",
    "validate_and_canonicalize_gold_frame",
    "validate_gold_contract",
    "validate_required_columns",
    "validate_value_contracts",
    "write_bytes_uri",
    "write_json_uri",
    "write_parquet_frame_to_uri",
]


@dataclass(frozen=True)
class SplitResult:
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    cutoff_ts: pd.Timestamp


def _json_default(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def log_json(**payload: object) -> None:
    print(json.dumps(payload, default=_json_default, sort_keys=True))


def ensure_directory(path: str | Path) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def filesystem_and_path(uri: str) -> tuple[pa_fs.FileSystem, str]:
    value = (uri or "").strip()
    if not value:
        raise ValueError("uri must not be empty")
    filesystem, abstract_path = pa_fs.FileSystem.from_uri(value)
    return filesystem, filesystem.normalize_path(abstract_path)


def artifact_uri_join(root_uri: str, *parts: str) -> str:
    root = root_uri.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"{root}/{suffix}" if suffix else root


def make_run_id() -> str:
    return uuid.uuid4().hex


def _is_parquet_file(path: str) -> bool:
    name = Path(path).name
    return name.endswith(".parquet") and not name.startswith((".", "_"))


def discover_parquet_files(dataset_uri: str) -> list[str]:
    filesystem, base_path = filesystem_and_path(dataset_uri)
    info = filesystem.get_file_info(base_path)
    if info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(base_path)

    if info.type == pa_fs.FileType.File:
        return [base_path] if _is_parquet_file(base_path) else []

    selector = pa_fs.FileSelector(base_path, allow_not_found=False, recursive=True)
    files: list[str] = []
    for file_info in filesystem.get_file_info(selector):
        if file_info.type == pa_fs.FileType.File and _is_parquet_file(file_info.path):
            files.append(file_info.path)
    return sorted(files)


def _read_single_parquet_file(
    filesystem: pa_fs.FileSystem,
    file_path: str,
    columns: list[str] | None,
) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(file_path, filesystem=filesystem)
    table = parquet_file.read(columns=columns, use_threads=True)
    return table.to_pandas()


def _normalize_as_of_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if "as_of_date" not in df.columns:
        return df
    out = df.copy()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="raise").dt.date
    return out


def _normalize_utc_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="raise").astype("datetime64[ns, UTC]")


def read_parquet_frame(dataset_uri: str, columns: list[str] | None = None) -> pd.DataFrame:
    filesystem, base_path = filesystem_and_path(dataset_uri)
    info = filesystem.get_file_info(base_path)
    if info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(base_path)

    if info.type == pa_fs.FileType.File:
        if not _is_parquet_file(base_path):
            raise ValueError(f"{dataset_uri} is a file, but not a parquet file")
        frame = _read_single_parquet_file(filesystem, base_path, columns)
        return _normalize_as_of_date_column(frame)

    parquet_files = discover_parquet_files(dataset_uri)
    if not parquet_files:
        raise RuntimeError(f"No parquet files found at {dataset_uri}")

    frames: list[pd.DataFrame] = []
    for file_path in parquet_files:
        frame = _read_single_parquet_file(filesystem, file_path, columns)
        frames.append(_normalize_as_of_date_column(frame))

    final_df = pd.concat(frames, ignore_index=True, sort=False)
    return _normalize_as_of_date_column(final_df)


def load_gold_frame(dataset_uri: str) -> pd.DataFrame:
    frame = read_parquet_frame(dataset_uri)
    return validate_and_canonicalize_gold_frame(frame)


def read_json_uri(uri: str) -> object:
    filesystem, path = filesystem_and_path(uri)
    if uri.startswith("s3://"):
        with filesystem.open_input_file(path) as stream:
            return json.loads(stream.read().decode("utf-8"))
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json_uri(payload: object, uri: str) -> str:
    filesystem, path = filesystem_and_path(uri)
    text = json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n"

    if uri.startswith("s3://"):
        with filesystem.open_output_stream(path) as stream:
            stream.write(text.encode("utf-8"))
        return uri

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return uri


def write_bytes_uri(data: bytes, uri: str) -> str:
    filesystem, path = filesystem_and_path(uri)
    if uri.startswith("s3://"):
        with filesystem.open_output_stream(path) as stream:
            stream.write(data)
        return uri

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return uri


def upload_file_to_uri(local_path: str | Path, uri: str) -> str:
    source = Path(local_path)
    if not source.is_file():
        raise FileNotFoundError(source)
    return write_bytes_uri(source.read_bytes(), uri)


def write_parquet_frame_to_uri(df: pd.DataFrame, uri: str) -> str:
    _filesystem, path = filesystem_and_path(uri)
    if uri.startswith("s3://"):
        with tempfile.TemporaryDirectory(prefix="parquet_upload_") as tmpdir:
            tmp_path = Path(tmpdir) / "frame.parquet"
            df.to_parquet(tmp_path, index=False)
            return upload_file_to_uri(tmp_path, uri)

    local_path = Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, index=False)
    return uri


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str] = REQUIRED_COLUMNS) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def dataframe_dtype_map(df: pd.DataFrame) -> dict[str, str]:
    return {column: str(dtype) for column, dtype in df.dtypes.items()}


def validate_gold_contract(
    df: pd.DataFrame,
    *,
    expected_columns: Sequence[str] = REQUIRED_COLUMNS,
    strict_dtypes: bool = True,
    expected_dtypes: dict[str, str] | None = None,
    label: str = "Gold dataset",
) -> None:
    actual_columns = list(df.columns)
    expected_columns_list = list(expected_columns)

    if actual_columns != expected_columns_list:
        missing = [c for c in expected_columns_list if c not in actual_columns]
        extra = [c for c in actual_columns if c not in expected_columns_list]
        raise ValueError(
            f"{label} does not match the frozen Gold contract. "
            f"expected={expected_columns_list}, actual={actual_columns}, missing={missing}, extra={extra}"
        )

    if strict_dtypes:
        expected_dtypes = expected_dtypes or CANONICAL_GOLD_DTYPE_MAP
        actual_dtypes = dataframe_dtype_map(df)
        mismatched = {
            column: {"expected": expected_dtypes.get(column), "actual": actual_dtypes.get(column)}
            for column in expected_columns_list
            if expected_dtypes.get(column) != actual_dtypes.get(column)
        }
        if mismatched:
            raise ValueError(f"{label} has dtype drift: {json.dumps(mismatched, sort_keys=True, default=str)}")


def assert_no_leakage_columns(df: pd.DataFrame) -> None:
    forbidden_markers = ("post_trip", "actual_", "target_")
    leakage_like = [column for column in df.columns if any(marker in column for marker in forbidden_markers)]
    if leakage_like:
        raise ValueError(f"Potential leakage columns present in Gold: {leakage_like}")


def coerce_contract_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    validate_required_columns(df)
    out = df.copy()

    out["trip_id"] = out["trip_id"].astype("string")
    out[TIMESTAMP_COLUMN] = _normalize_utc_timestamp_series(out[TIMESTAMP_COLUMN])
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="raise").dt.date
    out["schema_version"] = out["schema_version"].astype("string")
    out["feature_version"] = out["feature_version"].astype("string")

    for column in TIME_INTEGER_FEATURES:
        out[column] = pd.to_numeric(out[column], errors="raise").astype("int32")

    for column in CATEGORICAL_FEATURES:
        out[column] = pd.to_numeric(out[column], errors="raise").astype("int32")

    for column in FLOAT_FEATURES:
        out[column] = pd.to_numeric(out[column], errors="raise").astype("float64")

    out[LABEL_COLUMN] = pd.to_numeric(out[LABEL_COLUMN], errors="raise").astype("float64")
    return out[REQUIRED_COLUMNS]


def validate_value_contracts(df: pd.DataFrame) -> None:
    if df["trip_id"].isna().any():
        raise ValueError("trip_id contains nulls")
    if df["trip_id"].duplicated().any():
        raise ValueError("duplicate trip_id rows are not allowed")

    if df[TIMESTAMP_COLUMN].isna().any():
        raise ValueError(f"{TIMESTAMP_COLUMN} contains nulls after parsing")
    if df["as_of_date"].isna().any():
        raise ValueError("as_of_date contains nulls after parsing")
    if not isinstance(df[TIMESTAMP_COLUMN].dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{TIMESTAMP_COLUMN} must be timezone-aware UTC")

    ts_date = pd.to_datetime(df[TIMESTAMP_COLUMN], utc=True, errors="raise").dt.date
    if not (pd.Series(df["as_of_date"]).reset_index(drop=True) == pd.Series(ts_date).reset_index(drop=True)).all():
        raise ValueError("as_of_date must match the date component of as_of_ts")

    if (df[LABEL_COLUMN] <= 0).any():
        raise ValueError(f"{LABEL_COLUMN} must be strictly positive for duration regression")

    if (df["pickup_hour"] < 0).any() or (df["pickup_hour"] > 23).any():
        raise ValueError("pickup_hour must be in [0, 23]")
    if (df["pickup_dow"] < 1).any() or (df["pickup_dow"] > 7).any():
        raise ValueError("pickup_dow must be in [1, 7]")
    if (df["pickup_month"] < 1).any() or (df["pickup_month"] > 12).any():
        raise ValueError("pickup_month must be in [1, 12]")
    if not set(df["pickup_is_weekend"].dropna().unique()).issubset({0, 1}):
        raise ValueError("pickup_is_weekend must be 0/1")

    for column in CATEGORICAL_FEATURES:
        if (df[column] < 0).any():
            raise ValueError(f"{column} contains negative category ids; reserve 0 for unknown")

    for column in FLOAT_FEATURES:
        non_null = df[column].dropna()
        if (non_null < 0).any():
            raise ValueError(f"{column} contains negative values")

    if (df["route_pair_id"] > ROUTE_PAIR_BUCKETS).any():
        raise ValueError(f"route_pair_id exceeds the configured bucket count {ROUTE_PAIR_BUCKETS}")


def validate_and_canonicalize_gold_frame(df: pd.DataFrame) -> pd.DataFrame:
    validate_required_columns(df)
    assert_no_leakage_columns(df)
    validate_gold_contract(df, strict_dtypes=False, label="Gold input frame")
    canonical = coerce_contract_dtypes(df)
    validate_gold_contract(canonical, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(canonical)
    canonical = canonical.sort_values(
        [TIMESTAMP_COLUMN, "trip_id", "as_of_date", "feature_version", "schema_version"],
        kind="mergesort",
    ).reset_index(drop=True)
    return canonical


def split_by_time(df: pd.DataFrame, validation_fraction: float) -> SplitResult:
    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("validation_fraction must be > 0 and < 0.5")
    if len(df) < 2:
        raise ValueError("at least 2 rows are required to split train/validation sets")

    ordered = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)
    split_idx = int(len(ordered) * (1.0 - validation_fraction))
    split_idx = min(max(split_idx, 1), len(ordered) - 1)

    train_df = ordered.iloc[:split_idx].copy()
    valid_df = ordered.iloc[split_idx:].copy()
    cutoff_ts = valid_df[TIMESTAMP_COLUMN].iloc[0]
    return SplitResult(train_df=train_df, valid_df=valid_df, cutoff_ts=cutoff_ts)


def sample_validation_frame(
    df: pd.DataFrame,
    *,
    mode: str = VALIDATION_MODE,
    fraction: float = VALIDATION_SAMPLE_FRACTION,
    max_rows: int = VALIDATION_SAMPLE_MAX_ROWS,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    if mode == "full" or len(df) <= max_rows:
        return df.copy()

    if not 0.0 < fraction <= 1.0:
        raise ValueError("validation sample fraction must be in (0, 1]")

    rng = np.random.default_rng(seed)

    if "as_of_date" in df.columns and df["as_of_date"].nunique(dropna=False) > 1:
        pieces: list[pd.DataFrame] = []
        grouped = df.groupby("as_of_date", dropna=False, sort=True)
        for _, group in grouped:
            n = max(1, round(len(group) * fraction))
            n = min(n, len(group))
            if n == len(group):
                pieces.append(group.copy())
            else:
                indices = rng.choice(group.index.to_numpy(), size=n, replace=False)
                pieces.append(group.loc[np.sort(indices)].copy())
        sampled = pd.concat(pieces, ignore_index=True, sort=False)
    else:
        n = max(1, round(len(df) * fraction))
        n = min(n, len(df))
        sampled = df.sample(n=n, random_state=seed).copy()

    if len(sampled) > max_rows:
        sampled = sampled.sample(n=max_rows, random_state=seed).copy()

    return sampled.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)


def align_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.loc[:, FEATURE_COLUMNS].copy()
    for column in CATEGORICAL_FEATURES:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("int32")
    for column in [c for c in FEATURE_COLUMNS if c not in CATEGORICAL_FEATURES]:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
    return frame


def prepare_model_input_frame(df: pd.DataFrame) -> pd.DataFrame:
    return align_feature_frame(df)


def prepare_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, [*FEATURE_COLUMNS, LABEL_COLUMN]].copy()


def compute_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    r2 = r2_score(y_true, y_pred)
    return {"mae": float(mae), "rmse": float(rmse), "r2": float(r2)}


def build_feature_spec() -> dict[str, object]:
    service_zone_domain = [0, *range(1, len(SERVICE_ZONE_VALUES) + 1)]
    route_pair_domain = [0, *range(1, ROUTE_PAIR_BUCKETS + 1)]
    borough_domain = [0, 1, 2, 3, 4, 5, 6]

    def col(
        name: str,
        role: str,
        dtype: str,
        nullable: bool,
        unit: str,
        missing_policy: str,
        **extra: object,
    ) -> dict[str, object]:
        result: dict[str, object] = {
            "name": name,
            "role": role,
            "dtype": dtype,
            "nullable": nullable,
            "unit": unit,
            "missing_policy": missing_policy,
        }
        result.update(extra)
        return result

    return {
        "feature_version": GOLD_FEATURE_VERSION,
        "schema_version": GOLD_SCHEMA_VERSION,
        "label_column": LABEL_COLUMN,
        "timestamp_column": TIMESTAMP_COLUMN,
        "identifier_columns": IDENTIFIER_COLUMNS,
        "feature_columns": FEATURE_COLUMNS,
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "feature_order_locked": True,
        "prediction_problem": "trip_duration_regression",
        "prediction_timing": "pre_trip",
        "output_columns": [
            col("trip_id", "metadata", "string", False, "identifier", "required"),
            col("as_of_ts", "metadata", "timestamp", False, "timestamp_utc", "required"),
            col("as_of_date", "metadata", "date", False, "date_utc", "required"),
            col("schema_version", "metadata", "string", False, "version_tag", "required"),
            col("feature_version", "metadata", "string", False, "version_tag", "required"),
            col("pickup_hour", "feature", "int32", False, "hour_0_23", "required"),
            col("pickup_dow", "feature", "int32", False, "dayofweek_1_7", "required"),
            col("pickup_month", "feature", "int32", False, "month_1_12", "required"),
            col("pickup_is_weekend", "feature", "int32", False, "boolean_0_1", "required"),
            col(
                "pickup_borough_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=borough_domain,
            ),
            col(
                "pickup_zone_id",
                "feature",
                "int32",
                False,
                "taxi_zone_location_id",
                "0_unknown",
                categorical_feature=True,
                domain="positive_location_ids_and_0_unknown",
            ),
            col(
                "pickup_service_zone_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=service_zone_domain,
            ),
            col(
                "dropoff_borough_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=borough_domain,
            ),
            col(
                "dropoff_zone_id",
                "feature",
                "int32",
                False,
                "taxi_zone_location_id",
                "0_unknown",
                categorical_feature=True,
                domain="positive_location_ids_and_0_unknown",
            ),
            col(
                "dropoff_service_zone_id",
                "feature",
                "int32",
                False,
                "categorical_id",
                "0_unknown",
                categorical_feature=True,
                domain=service_zone_domain,
            ),
            col(
                "route_pair_id",
                "feature",
                "int32",
                False,
                "hashed_bucket",
                "0_unknown",
                categorical_feature=True,
                domain=route_pair_domain,
                hash_algorithm="sha256",
                hash_salt=ROUTE_PAIR_HASH_SALT,
                bucket_count=ROUTE_PAIR_BUCKETS,
            ),
            col("avg_duration_7d_zone_hour", "feature", "float64", True, "seconds", "nan_on_cold_start"),
            col("avg_fare_30d_zone", "feature", "float64", True, "currency_amount", "nan_on_cold_start"),
            col("trip_count_90d_zone_hour", "feature", "float64", False, "count", "0_on_cold_start"),
            col(LABEL_COLUMN, "label", "float64", False, "seconds", "drop_row_if_null", target_metric="mae"),
        ],
    }


def build_encoding_spec() -> dict[str, object]:
    service_zone_values = list(SERVICE_ZONE_VALUES)
    service_zone_lookup = {idx + 1: value for idx, value in enumerate(service_zone_values)}

    borough_values = {
        1: "Manhattan",
        2: "Queens",
        3: "Brooklyn",
        4: "Bronx",
        5: "Staten Island",
        6: "EWR",
    }

    return {
        "pickup_borough_id": {"type": "fixed_enum", "unknown": 0, "values": borough_values},
        "dropoff_borough_id": {"type": "fixed_enum", "unknown": 0, "values": borough_values},
        "pickup_zone_id": {
            "type": "identity_code",
            "unknown": 0,
            "source": "silver.pickup_location_id",
            "note": "stable taxi zone location IDs",
        },
        "dropoff_zone_id": {
            "type": "identity_code",
            "unknown": 0,
            "source": "silver.dropoff_location_id",
            "note": "stable taxi zone location IDs",
        },
        "pickup_service_zone_id": {
            "type": "versioned_lookup",
            "unknown": 0,
            "source": "silver.pickup_service_zone",
            "values": service_zone_lookup,
        },
        "dropoff_service_zone_id": {
            "type": "versioned_lookup",
            "unknown": 0,
            "source": "silver.dropoff_service_zone",
            "values": service_zone_lookup,
        },
        "route_pair_id": {
            "type": "hashed_bucket",
            "unknown": 0,
            "hash_algorithm": "sha256",
            "hash_salt": ROUTE_PAIR_HASH_SALT,
            "bucket_count": ROUTE_PAIR_BUCKETS,
        },
    }


def build_aggregate_spec(source_silver_table: str = SOURCE_SILVER_TABLE) -> list[dict[str, object]]:
    return [
        {
            "name": "avg_duration_7d_zone_hour",
            "source_table": source_silver_table,
            "source_column": "label_trip_duration_seconds",
            "filter_predicate": "pickup_ts in [as_of_ts - 7d, as_of_ts)",
            "window_length": "7d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "avg_fare_30d_zone",
            "source_table": source_silver_table,
            "source_column": "fare_amount",
            "filter_predicate": "pickup_ts in [as_of_ts - 30d, as_of_ts)",
            "window_length": "30d",
            "grouping_keys": ["pickup_zone_id"],
            "minimum_history_rule": "no prior rows => NaN",
            "null_fallback": "NaN",
        },
        {
            "name": "trip_count_90d_zone_hour",
            "source_table": source_silver_table,
            "source_column": "count(*)",
            "filter_predicate": "pickup_ts in [as_of_ts - 90d, as_of_ts)",
            "window_length": "90d",
            "grouping_keys": ["pickup_zone_id", "pickup_hour"],
            "minimum_history_rule": "no prior rows => 0",
            "null_fallback": "0",
        },
    ]


def build_label_spec(source_silver_table: str = SOURCE_SILVER_TABLE) -> dict[str, object]:
    return {
        "name": LABEL_COLUMN,
        "dtype": "float64",
        "unit": "seconds",
        "source_table": source_silver_table,
        "source_column": "trip_duration_seconds",
        "null_policy": "drop_row_if_null",
        "primary_metric": "mae",
        "secondary_metric": "rmse",
        "target_family": "eta",
    }


def build_schema_hash(feature_spec: dict[str, object] | None = None) -> str:
    spec = feature_spec or build_feature_spec()
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_contract_summary(
    *,
    dataset_uri: str,
    row_count: int,
    dataframe: pd.DataFrame | None = None,
    gold_table: str = GOLD_TRAINING_TABLE,
    source_silver_table: str = SOURCE_SILVER_TABLE,
    run_id: str | None = None,
    cutoff_ts: object | None = None,
    created_ts: datetime | None = None,
    model_family: str = DEFAULT_MODEL_FAMILY,
    inference_runtime: str = DEFAULT_INFERENCE_RUNTIME,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    feature_spec = build_feature_spec()
    encoding_spec = build_encoding_spec()
    aggregate_spec = build_aggregate_spec(source_silver_table=source_silver_table)
    label_spec = build_label_spec(source_silver_table=source_silver_table)

    summary: dict[str, object] = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "row_count": int(row_count),
        "gold_table": gold_table,
        "source_silver_table": source_silver_table,
        "feature_version": GOLD_FEATURE_VERSION,
        "schema_version": GOLD_SCHEMA_VERSION,
        "schema_hash": build_schema_hash(feature_spec),
        "model_family": model_family,
        "inference_runtime": inference_runtime,
        "output_columns_json": json.dumps(
            [row["name"] for row in cast(list[dict[str, object]], feature_spec["output_columns"])],
            separators=(",", ":"),
        ),
        "feature_spec_json": json.dumps(feature_spec, sort_keys=True, separators=(",", ":"), default=_json_default),
        "encoding_spec_json": json.dumps(encoding_spec, sort_keys=True, separators=(",", ":"), default=_json_default),
        "aggregate_spec_json": json.dumps(aggregate_spec, sort_keys=True, separators=(",", ":"), default=_json_default),
        "label_spec_json": json.dumps(label_spec, sort_keys=True, separators=(",", ":"), default=_json_default),
        "created_ts": created_ts or datetime.now(UTC),
    }
    if cutoff_ts is not None:
        summary["cutoff_ts"] = cutoff_ts
    if dataframe is not None:
        summary["pandas_dtypes"] = dataframe_dtype_map(dataframe)
    if extra:
        summary.update(extra)
    return summary


def build_quality_report(
    df: pd.DataFrame,
    *,
    split: SplitResult | None = None,
    validation_mode: str = VALIDATION_MODE,
    validation_sample_fraction: float = VALIDATION_SAMPLE_FRACTION,
    validation_sample_max_rows: int = VALIDATION_SAMPLE_MAX_ROWS,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, object]:
    sample_df = sample_validation_frame(
        df,
        mode=validation_mode,
        fraction=validation_sample_fraction,
        max_rows=validation_sample_max_rows,
        seed=random_seed,
    )
    report: dict[str, object] = {
        "validation_mode": validation_mode,
        "validation_sample_fraction": validation_sample_fraction,
        "validation_sample_max_rows": validation_sample_max_rows,
        "random_seed": random_seed,
        "sampled_rows": len(sample_df),
        "full_rows": len(df),
        "columns": list(df.columns),
        "null_counts_sample": sample_df.isna().sum().to_dict(),
        "min_timestamp_sample": sample_df[TIMESTAMP_COLUMN].min() if TIMESTAMP_COLUMN in sample_df.columns else None,
        "max_timestamp_sample": sample_df[TIMESTAMP_COLUMN].max() if TIMESTAMP_COLUMN in sample_df.columns else None,
        "min_date_sample": sample_df["as_of_date"].min() if "as_of_date" in sample_df.columns else None,
        "max_date_sample": sample_df["as_of_date"].max() if "as_of_date" in sample_df.columns else None,
    }
    if split is not None:
        report["train_rows"] = len(split.train_df)
        report["valid_rows"] = len(split.valid_df)
        report["cutoff_ts"] = split.cutoff_ts
    return report


def best_lightgbm_params(seed: int = DEFAULT_RANDOM_SEED) -> dict[str, object]:
    return {
        "objective": "regression",
        "metric": ["l1", "rmse"],
        "boosting_type": "gbdt",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "feature_fraction": 0.90,
        "bagging_fraction": 0.90,
        "bagging_freq": 1,
        "lambda_l1": 0.0,
        "lambda_l2": 0.0,
        "verbosity": -1,
        "seed": int(seed),
        "feature_fraction_seed": int(seed),
        "bagging_seed": int(seed),
        "data_random_seed": int(seed),
        "deterministic": True,
        "force_col_wise": True,
    }


def _require_training_columns(frame: pd.DataFrame, *, label: str) -> None:
    missing = [column for column in [*FEATURE_COLUMNS, LABEL_COLUMN, TIMESTAMP_COLUMN] if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def train_lightgbm_model(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    *,
    num_boost_round: int = DEFAULT_NUM_BOOST_ROUND,
    early_stopping_rounds: int = DEFAULT_EARLY_STOPPING_ROUNDS,
    seed: int = DEFAULT_RANDOM_SEED,
) -> tuple[object, dict[str, float], dict[str, object]]:
    import lightgbm as lgb

    _require_training_columns(train_df, label="train_df")
    _require_training_columns(valid_df, label="valid_df")

    X_train = align_feature_frame(train_df)
    y_train = train_df[LABEL_COLUMN].astype("float64")
    X_valid = align_feature_frame(valid_df)
    y_valid = valid_df[LABEL_COLUMN].astype("float64")

    train_set = lgb.Dataset(
        X_train,
        label=y_train,
        categorical_feature=list(CATEGORICAL_FEATURES),
        free_raw_data=False,
    )
    valid_set = lgb.Dataset(
        X_valid,
        label=y_valid,
        categorical_feature=list(CATEGORICAL_FEATURES),
        free_raw_data=False,
    )

    params = best_lightgbm_params(seed=seed)
    booster = lgb.train(
        params,
        train_set,
        num_boost_round=int(num_boost_round),
        valid_sets=[valid_set],
        valid_names=["validation"],
        callbacks=[
            lgb.early_stopping(int(early_stopping_rounds), verbose=False),
            lgb.log_evaluation(period=0),
        ],
    )

    best_iteration = int(getattr(booster, "best_iteration", 0) or 0)
    preds = booster.predict(X_valid, num_iteration=best_iteration or None)
    metrics = compute_regression_metrics(y_valid.to_numpy(dtype="float64"), np.asarray(preds, dtype="float64"))
    extras = {
        "best_iteration": best_iteration,
        "current_iteration": int(booster.current_iteration()),
        "num_boost_round": int(num_boost_round),
        "early_stopping_rounds": int(early_stopping_rounds),
        "lightgbm_params": params,
    }
    return booster, metrics, extras


def persist_training_artifacts(
    *,
    run_id: str,
    dataset_uri: str,
    canonical_df: pd.DataFrame,
    split: SplitResult,
    booster: object,
    metrics: Mapping[str, float],
    num_boost_round: int,
    early_stopping_rounds: int,
    validation_fraction: float,
    artifact_root_s3: str,
    validation_sample_rows: int = 2048,
    quality_report: Mapping[str, object] | None = None,
    best_config: Mapping[str, object] | None = None,
    lightgbm_params: Mapping[str, object] | None = None,
    runtime_config: Mapping[str, object] | None = None,
) -> dict[str, object]:
    feature_spec = build_feature_spec()
    encoding_spec = build_encoding_spec()
    aggregate_spec = build_aggregate_spec()
    label_spec = build_label_spec()
    schema_hash = build_schema_hash(feature_spec)

    artifact_root = artifact_root_s3.rstrip("/")
    runtime_payload: dict[str, object] = dict(runtime_config or {})
    best_iteration = int(runtime_payload.get("best_iteration", 0) or 0)

    outputs: dict[str, object] = {
        "run_id": run_id,
        "artifact_root_s3": artifact_root,
        "model_uri": artifact_uri_join(artifact_root, "model", "model.txt"),
        "validation_sample_uri": artifact_uri_join(artifact_root, "validation", "validation_sample.parquet"),
        "metrics_uri": artifact_uri_join(artifact_root, "metrics", "metrics.json"),
        "contract_uri": artifact_uri_join(artifact_root, "contract", "contract.json"),
        "feature_spec_uri": artifact_uri_join(artifact_root, "contract", "feature_spec.json"),
        "encoding_spec_uri": artifact_uri_join(artifact_root, "contract", "encoding_spec.json"),
        "aggregate_spec_uri": artifact_uri_join(artifact_root, "contract", "aggregate_spec.json"),
        "label_spec_uri": artifact_uri_join(artifact_root, "contract", "label_spec.json"),
        "manifest_uri": artifact_uri_join(artifact_root, "manifest", "manifest.json"),
        "quality_report_uri": artifact_uri_join(artifact_root, "validation", "quality_report.json"),
        "best_config_uri": artifact_uri_join(artifact_root, "summary", "best_config.json"),
        "lightgbm_params_uri": artifact_uri_join(artifact_root, "summary", "lightgbm_params.json"),
        "runtime_config_uri": artifact_uri_join(artifact_root, "summary", "runtime_config.json"),
        "training_summary_uri": artifact_uri_join(artifact_root, "summary", "training_summary.json"),
    }

    with tempfile.TemporaryDirectory(prefix=f"{run_id}_train_artifacts_") as tmpdir:
        tmp = Path(tmpdir)

        model_path = tmp / "model.txt"
        save_model = getattr(booster, "save_model", None)
        if not callable(save_model):
            raise TypeError("booster must expose a save_model() method")
        save_model(str(model_path))

        validation_sample = split.valid_df.sample(
            n=min(validation_sample_rows, len(split.valid_df)),
            random_state=DEFAULT_RANDOM_SEED,
        ).copy()
        validation_sample = validation_sample.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

        quality_payload = dict(
            quality_report
            or build_quality_report(
                canonical_df,
                split=split,
                validation_mode=VALIDATION_MODE,
                validation_sample_fraction=VALIDATION_SAMPLE_FRACTION,
                validation_sample_max_rows=VALIDATION_SAMPLE_MAX_ROWS,
                random_seed=DEFAULT_RANDOM_SEED,
            )
        )
        quality_payload.update(
            {
                "run_id": run_id,
                "dataset_uri": dataset_uri,
                "schema_hash": schema_hash,
                "feature_version": feature_spec["feature_version"],
                "schema_version": feature_spec["schema_version"],
            }
        )

        runtime_payload.update(
            {
                "run_id": run_id,
                "dataset_uri": dataset_uri,
                "validation_fraction": validation_fraction,
                "train_rows": len(split.train_df),
                "valid_rows": len(split.valid_df),
                "cutoff_ts": split.cutoff_ts,
                "best_iteration": best_iteration,
                "schema_hash": schema_hash,
                "model_family": DEFAULT_MODEL_FAMILY,
                "inference_runtime": DEFAULT_INFERENCE_RUNTIME,
                "train_profile": runtime_payload.get("train_profile", DEFAULT_TRAIN_PROFILE),
            }
        )

        manifest = build_contract_summary(
            dataset_uri=dataset_uri,
            row_count=len(canonical_df),
            dataframe=canonical_df,
            run_id=run_id,
            cutoff_ts=split.cutoff_ts,
            extra={
                "task": "train_pipeline",
                "validation_fraction": validation_fraction,
                "train_rows": len(split.train_df),
                "valid_rows": len(split.valid_df),
                "validation_mode": VALIDATION_MODE,
                "validation_sample_fraction": VALIDATION_SAMPLE_FRACTION,
                "validation_sample_max_rows": VALIDATION_SAMPLE_MAX_ROWS,
                "feature_version": feature_spec["feature_version"],
                "schema_version": feature_spec["schema_version"],
                "schema_hash": schema_hash,
                "best_iteration": best_iteration,
                "num_boost_round": num_boost_round,
                "early_stopping_rounds": early_stopping_rounds,
                "train_profile": runtime_payload["train_profile"],
            },
        )
        manifest["metrics"] = dict(metrics)

        best_config_payload = dict(best_config or {})
        best_config_payload.update(
            {
                "source": "direct_lightgbm",
                "seed": DEFAULT_RANDOM_SEED,
                "model_family": DEFAULT_MODEL_FAMILY,
                "inference_runtime": DEFAULT_INFERENCE_RUNTIME,
                "feature_columns": list(FEATURE_COLUMNS),
                "categorical_features": list(CATEGORICAL_FEATURES),
                "label_column": LABEL_COLUMN,
                "timestamp_column": TIMESTAMP_COLUMN,
            }
        )

        lightgbm_params_payload = dict(lightgbm_params or best_lightgbm_params())
        lightgbm_params_payload.setdefault("seed", DEFAULT_RANDOM_SEED)

        write_bytes_uri(model_path.read_bytes(), str(outputs["model_uri"]))
        write_parquet_frame_to_uri(validation_sample, str(outputs["validation_sample_uri"]))
        write_json_uri(dict(metrics), str(outputs["metrics_uri"]))
        write_json_uri(manifest, str(outputs["manifest_uri"]))
        write_json_uri(
            build_contract_summary(
                dataset_uri=dataset_uri,
                row_count=len(canonical_df),
                dataframe=canonical_df,
                run_id=run_id,
                cutoff_ts=split.cutoff_ts,
            ),
            str(outputs["contract_uri"]),
        )
        write_json_uri(feature_spec, str(outputs["feature_spec_uri"]))
        write_json_uri(encoding_spec, str(outputs["encoding_spec_uri"]))
        write_json_uri(aggregate_spec, str(outputs["aggregate_spec_uri"]))
        write_json_uri(label_spec, str(outputs["label_spec_uri"]))
        write_json_uri(quality_payload, str(outputs["quality_report_uri"]))
        write_json_uri(best_config_payload, str(outputs["best_config_uri"]))
        write_json_uri(lightgbm_params_payload, str(outputs["lightgbm_params_uri"]))
        write_json_uri(runtime_payload, str(outputs["runtime_config_uri"]))
        write_json_uri(
            {
                "run_id": run_id,
                "dataset_uri": dataset_uri,
                "artifact_root_s3": artifact_root,
                "cutoff_ts": split.cutoff_ts,
                "train_rows": len(split.train_df),
                "valid_rows": len(split.valid_df),
                "validation_fraction": validation_fraction,
                "validation_mode": VALIDATION_MODE,
                "validation_sample_fraction": VALIDATION_SAMPLE_FRACTION,
                "validation_sample_max_rows": VALIDATION_SAMPLE_MAX_ROWS,
                "best_iteration": best_iteration,
                "current_iteration": int(runtime_payload.get("current_iteration", 0) or 0),
                "num_boost_round": num_boost_round,
                "early_stopping_rounds": early_stopping_rounds,
                "schema_hash": schema_hash,
                "feature_version": feature_spec["feature_version"],
                "schema_version": feature_spec["schema_version"],
                "model_family": DEFAULT_MODEL_FAMILY,
                "inference_runtime": DEFAULT_INFERENCE_RUNTIME,
                "train_profile": runtime_payload["train_profile"],
            },
            str(outputs["training_summary_uri"]),
        )

    outputs.update(
        {
            "schema_hash": schema_hash,
            "feature_version": feature_spec["feature_version"],
            "schema_version": feature_spec["schema_version"],
            "gold_table": GOLD_TRAINING_TABLE,
            "source_silver_table": SOURCE_SILVER_TABLE,
            "model_family": DEFAULT_MODEL_FAMILY,
            "inference_runtime": DEFAULT_INFERENCE_RUNTIME,
            "train_profile": runtime_payload["train_profile"],
            "validation_fraction": validation_fraction,
            "validation_mode": VALIDATION_MODE,
            "validation_sample_fraction": VALIDATION_SAMPLE_FRACTION,
            "validation_sample_max_rows": VALIDATION_SAMPLE_MAX_ROWS,
            "train_rows": len(split.train_df),
            "valid_rows": len(split.valid_df),
            "cutoff_ts": split.cutoff_ts,
            "best_iteration": best_iteration,
            "current_iteration": int(runtime_payload.get("current_iteration", 0) or 0),
            "num_boost_round": num_boost_round,
            "early_stopping_rounds": early_stopping_rounds,
        }
    )
    return outputs