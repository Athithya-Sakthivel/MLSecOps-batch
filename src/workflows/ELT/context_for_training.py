#!/usr/bin/env python3
# export S3_BUCKET=
# python3 src/workflows/ELT/context_for_training.py
# S3-native inspector that reads Iceberg gold tables safely and prints ML-ready context—schema, sample rows, stats, and contract alignment—for training validation.

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from io import StringIO
from typing import Any
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs as pa_fs

DEFAULT_GOLD_PREFIX = "iceberg/warehouse/gold"
DEFAULT_SAMPLE_ROWS = 2
DEFAULT_MAX_DATA_FILES = 20

# Output guardrails
MAX_STRING_PREVIEW = 180
MAX_LIST_PREVIEW = 10
MAX_DICT_PREVIEW = 12
MAX_TOP_VALUES = 5
MAX_SAMPLE_FILES = 5


@dataclass(frozen=True)
class TableSpec:
    name: str
    prefix: str


def eprint(msg: str = "") -> None:
    print(msg, flush=True)


def env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return max(int(raw), minimum)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Set {name}")
    return value


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"invalid S3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def build_root_uri() -> str:
    bucket = require_env("S3_BUCKET")
    prefix = os.environ.get("ICEBERG_GOLD_PREFIX", DEFAULT_GOLD_PREFIX).strip().strip("/")
    return f"s3://{bucket}/{prefix}"


def build_boto3_client() -> Any:
    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")).strip() or None
    session = boto3.session.Session(region_name=region)
    return session.client("s3")


def build_arrow_fs() -> pa_fs.S3FileSystem:
    kwargs: dict[str, Any] = {}

    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")).strip() or None
    if region:
        kwargs["region"] = region

    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()
    if access_key and secret_key:
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key
        if session_token:
            kwargs["session_token"] = session_token

    endpoint_override = os.environ.get("S3_ENDPOINT", "").strip()
    if endpoint_override:
        kwargs["endpoint_override"] = endpoint_override.rstrip("/")

    kwargs["scheme"] = os.environ.get("S3_SCHEME", "https").strip() or "https"
    return pa_fs.S3FileSystem(**kwargs)


def list_objects(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    paginator = client.get_paginator("list_objects_v2")
    items: list[dict[str, Any]] = []
    prefix = prefix.strip("/")
    effective_prefix = f"{prefix}/" if prefix else ""
    for page in paginator.paginate(Bucket=bucket, Prefix=effective_prefix):
        items.extend(page.get("Contents", []))
    return items


def list_parquet_objects(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    return [obj for obj in list_objects(client, bucket, prefix) if obj["Key"].endswith(".parquet")]


def list_metadata_objects(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    metadata_prefix = f"{prefix.strip('/')}/metadata"
    return [obj for obj in list_objects(client, bucket, metadata_prefix) if obj["Key"].endswith(".metadata.json")]


def latest_object(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    return max(items, key=lambda item: item.get("LastModified") or 0)


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def read_json_s3(client: Any, bucket: str, key: str) -> dict[str, Any]:
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return json.loads(body)


def print_header(title: str) -> None:
    eprint("")
    eprint("=" * 88)
    eprint(title)
    eprint("=" * 88)


def print_kv(label: str, value: Any) -> None:
    eprint(f"{label}: {value}")


def partition_summary(keys: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in keys:
        for part in key.split("/"):
            if "=" in part:
                counts[part] = counts.get(part, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: kv[0]))


def is_missing_scalar(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def truncate_text(text: str, limit: int = MAX_STRING_PREVIEW) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def summarize_int_list(values: list[int]) -> str:
    if not values:
        return "[]"
    if len(values) == 1:
        return f"[{values[0]}]"
    consecutive = all(values[i] == values[0] + i for i in range(len(values)))
    if consecutive and len(values) > MAX_LIST_PREVIEW:
        return f"int_range({values[0]}..{values[-1]}) [len={len(values)}]"
    if len(values) > MAX_LIST_PREVIEW:
        head = ", ".join(map(str, values[:5]))
        tail = ", ".join(map(str, values[-3:]))
        return f"[{head}, ..., {tail}] [len={len(values)}]"
    return "[" + ", ".join(map(str, values)) + "]"


def summarize_value(value: Any) -> Any:
    """
    Convert large / nested values into compact, human-readable previews.
    """
    if is_missing_scalar(value):
        return value

    if isinstance(value, str):
        return truncate_text(value)

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value

    if isinstance(value, dict):
        items = list(value.items())
        parts: list[str] = []
        for k, v in items[:MAX_DICT_PREVIEW]:
            parts.append(f"{k}={summarize_value(v)}")
        if len(items) > MAX_DICT_PREVIEW:
            parts.append(f"... +{len(items) - MAX_DICT_PREVIEW} more")
        return "{ " + ", ".join(parts) + " }"

    if isinstance(value, (list, tuple)):
        if all(isinstance(x, int) for x in value):
            return summarize_int_list(list(value))
        if len(value) > MAX_LIST_PREVIEW:
            head = ", ".join(repr(summarize_value(x)) for x in value[:5])
            tail = ", ".join(repr(summarize_value(x)) for x in value[-3:])
            return f"[{head}, ..., {tail}] [len={len(value)}]"
        return [summarize_value(x) for x in value]

    return value


def compact_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a display-safe copy where string/object cells are truncated and
    nested values are summarized.
    """
    preview = df.copy()

    for col in preview.columns:
        dtype = preview[col].dtype
        if (
            pd.api.types.is_object_dtype(dtype)
            or pd.api.types.is_string_dtype(dtype)
            or isinstance(dtype, pd.CategoricalDtype)
            or pd.api.types.is_bool_dtype(dtype)
        ):
            preview[col] = preview[col].map(summarize_value)

    return preview


def print_metadata_summary(metadata: dict[str, Any]) -> None:
    print_header("ICEBERG METADATA")
    for key in [
        "format-version",
        "table-uuid",
        "location",
        "last-updated-ms",
        "current-schema-id",
        "current-snapshot-id",
        "default-spec-id",
    ]:
        if key in metadata:
            print_kv(key, metadata[key])

    schemas = metadata.get("schemas") or []
    if schemas:
        current_schema_id = metadata.get("current-schema-id")
        chosen = next((s for s in schemas if s.get("schema-id") == current_schema_id), schemas[-1])

        eprint("")
        eprint("SCHEMA")
        for field in chosen.get("fields", []):
            extras: list[str] = []
            if field.get("doc"):
                extras.append(f"doc={truncate_text(str(field['doc']))}")
            if field.get("initial-default") is not None:
                extras.append(f"initial_default={summarize_value(field['initial-default'])}")
            if field.get("write-default") is not None:
                extras.append(f"write_default={summarize_value(field['write-default'])}")
            extra_txt = f" | {' | '.join(extras)}" if extras else ""
            eprint(
                f"  - id={field.get('id')} | name={field.get('name')} | "
                f"type={field.get('type')} | required={field.get('required')}{extra_txt}"
            )

    specs = metadata.get("partition-specs") or []
    if specs:
        eprint("")
        eprint("PARTITION SPEC")
        for spec in specs:
            eprint(f"  spec-id={spec.get('spec-id')}")
            for field in spec.get("fields", []):
                eprint(
                    f"    - source-id={field.get('source-id')} | field-id={field.get('field-id')} | "
                    f"name={field.get('name')} | transform={field.get('transform')}"
                )

    snapshots = metadata.get("snapshots") or []
    if snapshots:
        eprint("")
        eprint("LATEST SNAPSHOTS")
        for snap in snapshots[-3:]:
            eprint(
                f"  - snapshot-id={snap.get('snapshot-id')} | committed-at={snap.get('committed-at')} | "
                f"operation={snap.get('operation')} | manifest-list={snap.get('manifest-list')}"
            )


def read_parquet_file(arrow_fs: pa_fs.S3FileSystem, bucket: str, key: str) -> pd.DataFrame:
    path = f"{bucket}/{key}"
    with arrow_fs.open_input_file(path) as handle:
        pf = pq.ParquetFile(handle)
        table = pf.read()
    # Read a single file only. Never use pq.read_table(list_of_files), which can
    # trigger schema merging and fail on dictionary-vs-string physical encodings.
    return table.to_pandas()


def safe_read_parquet_file(arrow_fs: pa_fs.S3FileSystem, bucket: str, key: str) -> pd.DataFrame | None:
    try:
        return read_parquet_file(arrow_fs, bucket, key)
    except Exception as exc:
        eprint(f"PARQUET READ ERROR for {s3_uri(bucket, key)}: {exc}")
        return None


def print_pandas_info(df: pd.DataFrame) -> None:
    eprint("")
    eprint("PANDAS INFO")
    buf = StringIO()
    df.info(buf=buf, memory_usage="deep", show_counts=True)
    for line in buf.getvalue().splitlines():
        eprint("  " + line)


def print_head(df: pd.DataFrame, n: int) -> None:
    eprint("")
    eprint(f"FIRST {n} ROWS")
    if df.empty:
        eprint("  <empty>")
        return
    preview = compact_df_for_display(df.head(n))
    eprint(preview.to_string(index=False))


def print_nulls(df: pd.DataFrame, max_cols: int = 12) -> None:
    eprint("")
    eprint("NULL SUMMARY")
    if df.empty:
        eprint("  <empty>")
        return

    null_counts = df.isna().sum().sort_values(ascending=False)
    null_counts = null_counts[null_counts > 0]
    if null_counts.empty:
        eprint("  No missing values detected in the sampled rows.")
        return

    for col, count in null_counts.head(max_cols).items():
        pct = (count / len(df)) * 100 if len(df) else 0.0
        eprint(f"  - {col}: {count} ({pct:.1f}%)")


def print_numeric_summary(df: pd.DataFrame) -> None:
    numeric = df.select_dtypes(include=["number"])
    if numeric.empty:
        return

    eprint("")
    eprint("NUMERIC SUMMARY")
    desc = numeric.describe().T
    eprint(desc.to_string())


def is_categorical_series(series: pd.Series) -> bool:
    dtype = series.dtype
    return (
        pd.api.types.is_object_dtype(dtype)
        or pd.api.types.is_string_dtype(dtype)
        or isinstance(dtype, pd.CategoricalDtype)
        or pd.api.types.is_bool_dtype(dtype)
    )


def print_categorical_summary(df: pd.DataFrame, max_columns: int = 12) -> None:
    columns = [col for col in df.columns if is_categorical_series(df[col])]
    if not columns:
        return

    eprint("")
    eprint("CATEGORICAL SUMMARY")
    for col in columns[:max_columns]:
        series = df[col]
        eprint(f"  - {col}: unique={series.nunique(dropna=True)}")
        top = series.value_counts(dropna=False).head(MAX_TOP_VALUES)
        for value, count in top.items():
            eprint(f"      * {summarize_value(value)!r}: {count}")


def print_date_range(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        return
    values = pd.to_datetime(df[column], errors="coerce")
    values = values.dropna()
    if values.empty:
        return
    eprint("")
    eprint(f"DATE RANGE: {column}")
    eprint(f"  min={values.min()}")
    eprint(f"  max={values.max()}")


def parse_json_field(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def print_compact_mapping(mapping: dict[str, Any], indent: str = "      ", limit: int = 10) -> None:
    items = list(mapping.items())
    for key, value in items[:limit]:
        eprint(f"{indent}{key!r} -> {summarize_value(value)!r}")
    if len(items) > limit:
        eprint(f"{indent}... +{len(items) - limit} more")


def inspect_contract_table(
    client: Any,
    arrow_fs: pa_fs.S3FileSystem,
    bucket: str,
    prefix: str,
) -> dict[str, Any]:
    print_header("TABLE: trip_training_contracts")
    print_kv("table root", s3_uri(bucket, prefix))

    data_objects = list_parquet_objects(client, bucket, prefix)
    metadata_objects = list_metadata_objects(client, bucket, prefix)
    print_kv("data file count", len(data_objects))
    print_kv("metadata file count", len(metadata_objects))

    partitions = partition_summary([obj["Key"] for obj in data_objects])
    if partitions:
        eprint("")
        eprint("PARTITIONS DISCOVERED")
        for part, count in partitions.items():
            eprint(f"  - {part}: {count} files")

    if data_objects:
        eprint("")
        eprint("SAMPLE DATA FILES")
        for obj in data_objects[:MAX_SAMPLE_FILES]:
            eprint(f"  - {s3_uri(bucket, obj['Key'])}")

    latest_meta = latest_object(metadata_objects)
    metadata: dict[str, Any] = {}
    if latest_meta:
        print_kv("latest metadata", s3_uri(bucket, latest_meta["Key"]))
        try:
            metadata = read_json_s3(client, bucket, latest_meta["Key"])
            print_metadata_summary(metadata)
        except Exception as exc:
            eprint(f"METADATA READ ERROR: {exc}")
    else:
        eprint("No metadata file found.")

    if not data_objects:
        eprint("No contract parquet file found.")
        return {"metadata": metadata}

    contract_df = safe_read_parquet_file(arrow_fs, bucket, data_objects[0]["Key"])
    if contract_df is None:
        return {"metadata": metadata}

    eprint("")
    eprint("PARQUET SCHEMA")
    eprint(contract_df.dtypes.astype(str).to_string())

    print_pandas_info(contract_df)
    print_head(contract_df, 1)

    if contract_df.empty:
        return {"metadata": metadata}

    row = contract_df.iloc[0].to_dict()
    eprint("")
    eprint("CONTRACT FIELDS")
    for key in [
        "run_id",
        "feature_version",
        "schema_version",
        "schema_hash",
        "model_family",
        "inference_runtime",
        "gold_table",
        "source_silver_table",
        "created_ts",
    ]:
        if key in row:
            print_kv(key, row[key])

    output_columns = parse_json_field(row.get("output_columns_json")) or []
    feature_spec = parse_json_field(row.get("feature_spec_json")) or {}
    encoding_spec = parse_json_field(row.get("encoding_spec_json")) or {}
    aggregate_spec = parse_json_field(row.get("aggregate_spec_json")) or []
    label_spec = parse_json_field(row.get("label_spec_json")) or {}

    if output_columns:
        eprint("")
        eprint("OUTPUT COLUMNS")
        eprint("  " + ", ".join(output_columns))

    if feature_spec:
        eprint("")
        eprint("FEATURE SPEC")
        rows = feature_spec.get("output_columns", [])
        spec_df = pd.DataFrame(rows)
        cols = [c for c in ["role", "name", "dtype", "nullable", "missing_policy"] if c in spec_df.columns]
        if not spec_df.empty and cols:
            eprint(spec_df[cols].to_string(index=False))

    if encoding_spec:
        eprint("")
        eprint("ENCODING SPEC")
        for name, spec in encoding_spec.items():
            eprint(f"  - {name}:")
            for k in ["type", "unknown", "source", "hash_algorithm", "hash_salt", "bucket_count"]:
                if k in spec:
                    eprint(f"      {k}={summarize_value(spec[k])}")
            if "domain" in spec:
                eprint(f"      domain={summarize_value(spec['domain'])!r}")
            values = spec.get("values")
            if isinstance(values, dict):
                print_compact_mapping(values, indent="      ", limit=10)

    if aggregate_spec:
        eprint("")
        eprint("AGGREGATE SPEC")
        for item in aggregate_spec:
            eprint(
                "  - "
                f"{item.get('name')}: source_column={summarize_value(item.get('source_column'))}, "
                f"window={summarize_value(item.get('window_length'))}, grouping_keys={summarize_value(item.get('grouping_keys'))}, "
                f"null_fallback={summarize_value(item.get('null_fallback'))}"
            )

    if label_spec:
        eprint("")
        eprint("LABEL SPEC")
        for k, v in label_spec.items():
            eprint(f"  - {k}: {summarize_value(v)}")

    return {
        "expected_columns": output_columns,
        "feature_version": row.get("feature_version"),
        "schema_version": row.get("schema_version"),
        "schema_hash": row.get("schema_hash"),
        "metadata": metadata,
    }


def inspect_matrix_table(
    client: Any,
    arrow_fs: pa_fs.S3FileSystem,
    bucket: str,
    prefix: str,
    sample_rows: int,
    max_data_files: int,
    contract_context: dict[str, Any] | None,
) -> None:
    print_header("TABLE: trip_training_matrix")
    print_kv("table root", s3_uri(bucket, prefix))

    data_objects = list_parquet_objects(client, bucket, prefix)
    metadata_objects = list_metadata_objects(client, bucket, prefix)
    print_kv("data file count", len(data_objects))
    print_kv("metadata file count", len(metadata_objects))

    partitions = partition_summary([obj["Key"] for obj in data_objects])
    if partitions:
        eprint("")
        eprint("PARTITIONS DISCOVERED")
        for part, count in partitions.items():
            eprint(f"  - {part}: {count} files")

    if data_objects:
        eprint("")
        eprint("SAMPLE DATA FILES")
        for obj in data_objects[:MAX_SAMPLE_FILES]:
            eprint(f"  - {s3_uri(bucket, obj['Key'])}")

    latest_meta = latest_object(metadata_objects)
    metadata: dict[str, Any] = {}
    if latest_meta:
        print_kv("latest metadata", s3_uri(bucket, latest_meta["Key"]))
        try:
            metadata = read_json_s3(client, bucket, latest_meta["Key"])
            print_metadata_summary(metadata)
        except Exception as exc:
            eprint(f"METADATA READ ERROR: {exc}")
    else:
        eprint("No metadata file found.")

    if not data_objects:
        eprint("No matrix parquet files found.")
        return

    selected = data_objects[:max_data_files]
    if len(data_objects) > max_data_files:
        eprint("")
        eprint(f"Reading only the first {max_data_files} data files to avoid schema-merge failures.")

    dfs: list[pd.DataFrame] = []
    for obj in selected:
        df = safe_read_parquet_file(arrow_fs, bucket, obj["Key"])
        if df is not None:
            dfs.append(df)

    if not dfs:
        eprint("No matrix rows could be read.")
        return

    df = pd.concat(dfs, ignore_index=True, sort=False)
    print_pandas_info(df)

    eprint("")
    eprint("SCHEMA / COLUMNS")
    for col, dtype in df.dtypes.items():
        eprint(f"  - {col}: {dtype}")

    print_head(df, sample_rows)
    print_nulls(df)
    print_numeric_summary(df)
    print_categorical_summary(df)

    if "as_of_date" in df.columns:
        print_date_range(df, "as_of_date")
    if "as_of_ts" in df.columns:
        print_date_range(df, "as_of_ts")
    if "pickup_ts" in df.columns:
        print_date_range(df, "pickup_ts")

    if "label_trip_duration_seconds" in df.columns:
        label = pd.to_numeric(df["label_trip_duration_seconds"], errors="coerce")
        eprint("")
        eprint("LABEL PROFILE: label_trip_duration_seconds")
        eprint(f"  non_null={int(label.notna().sum())}")
        eprint(f"  nulls={int(label.isna().sum())}")
        if not label.dropna().empty:
            eprint(f"  min={label.min()}")
            eprint(f"  mean={label.mean()}")
            eprint(f"  median={label.median()}")
            eprint(f"  p95={label.quantile(0.95)}")
            eprint(f"  max={label.max()}")

    if "trip_id" in df.columns:
        eprint("")
        eprint("IDENTITY CHECK")
        eprint(f"  duplicate trip_id rows: {int(df['trip_id'].duplicated().sum())}")

    expected = (contract_context or {}).get("expected_columns") or []
    if expected:
        actual = list(df.columns)
        eprint("")
        eprint("CONTRACT CHECK")
        eprint(f"  expected column count: {len(expected)}")
        eprint(f"  actual column count:   {len(actual)}")
        eprint(f"  exact order match:     {expected == actual}")
        if expected != actual:
            missing = [c for c in expected if c not in actual]
            extra = [c for c in actual if c not in expected]
            if missing:
                eprint(f"  missing columns: {missing}")
            if extra:
                eprint(f"  extra columns:   {extra}")

    if "as_of_date" in df.columns:
        eprint("")
        eprint("PARTITION ROW COUNTS (sampled rows)")
        counts = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date.value_counts().sort_index()
        for idx, value in counts.items():
            eprint(f"  - {idx}: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Print readable ML context for gold Iceberg tables in S3")
    parser.add_argument("--sample-rows", type=int, default=env_int("SAMPLE_ROWS", DEFAULT_SAMPLE_ROWS, minimum=1))
    parser.add_argument("--max-data-files", type=int, default=env_int("MAX_DATA_FILES", DEFAULT_MAX_DATA_FILES, minimum=1))
    parser.add_argument(
        "--table",
        choices=["both", "matrix", "contracts"],
        default=os.environ.get("GOLD_TABLE", "both"),
        help="Which gold table(s) to inspect",
    )
    args = parser.parse_args()

    root_uri = build_root_uri()
    bucket, prefix = parse_s3_uri(root_uri)

    client = build_boto3_client()
    arrow_fs = build_arrow_fs()

    contract_context: dict[str, Any] = {}
    tables = [
        TableSpec("trip_training_contracts", f"{prefix}/trip_training_contracts"),
        TableSpec("trip_training_matrix", f"{prefix}/trip_training_matrix"),
    ]

    if args.table == "contracts":
        tables = [tables[0]]
    elif args.table == "matrix":
        tables = [tables[1]]

    eprint(f"ICEBERG GOLD ROOT: {root_uri}")
    eprint(f"SAMPLE ROWS: {args.sample_rows}")
    eprint(f"MAX DATA FILES USED FOR PROFILE: {args.max_data_files}")

    for table in tables:
        if table.name == "trip_training_contracts":
            contract_context = inspect_contract_table(client, arrow_fs, bucket, table.prefix)

    for table in tables:
        if table.name == "trip_training_matrix":
            inspect_matrix_table(
                client=client,
                arrow_fs=arrow_fs,
                bucket=bucket,
                prefix=table.prefix,
                sample_rows=args.sample_rows,
                max_data_files=args.max_data_files,
                contract_context=contract_context if contract_context else None,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())