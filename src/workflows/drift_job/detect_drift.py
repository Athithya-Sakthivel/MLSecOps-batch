#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

import boto3
import polars as pl
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset

from workflows.train.shared_utils import (
    EXPECTED_FEATURE_VERSION,
    EXPECTED_SCHEMA_VERSION,
    LABEL_COLUMN,
    MATRIX_FEATURE_COLUMNS,
    load_elt_contract,
    load_iceberg_table,
    read_table_as_dataframe,
    table_snapshot_lineage,
)

LOGGER = logging.getLogger("drift_job")

ID_COLUMN = "trip_id"
PICKUP_TIMESTAMP_COLUMN = "pickup_ts"
EVENT_TIMESTAMP_COLUMN = "as_of_ts"
DATE_COLUMN = "as_of_date"
SCHEMA_VERSION_COLUMN = "schema_version"
FEATURE_VERSION_COLUMN = "feature_version"

CATEGORICAL_COLUMNS = [
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
]

NUMERICAL_COLUMNS = [
    "avg_duration_7d_zone_hour",
    "avg_fare_30d_zone",
    "trip_count_90d_zone_hour",
]

BASE_REQUIRED_COLUMNS = [
    ID_COLUMN,
    PICKUP_TIMESTAMP_COLUMN,
    EVENT_TIMESTAMP_COLUMN,
    DATE_COLUMN,
    SCHEMA_VERSION_COLUMN,
    FEATURE_VERSION_COLUMN,
    *MATRIX_FEATURE_COLUMNS,
    LABEL_COLUMN,
]

DEFAULT_DRIFT_METHOD = "psi"


@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str

    @classmethod
    def parse(cls, value: str, default_bucket: str | None = None) -> S3Location:
        raw = value.strip()
        if not raw:
            raise ValueError("S3 location cannot be empty.")

        if raw.startswith("s3://"):
            parsed = urlparse(raw)
            bucket = parsed.netloc.strip()
            key = parsed.path.lstrip("/").strip()
            if not bucket or not key:
                raise ValueError(f"Invalid S3 URI: {value}")
            return cls(bucket=bucket, key=key)

        if default_bucket:
            key = raw.lstrip("/").strip()
            if not key:
                raise ValueError("S3 key cannot be empty.")
            return cls(bucket=default_bucket.strip(), key=key)

        raise ValueError(
            f"Path '{value}' is not an S3 URI and no default bucket was provided."
        )


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def s3_client():
    session = boto3.session.Session(
        region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    )
    return session.client("s3")


def read_s3_bytes(loc: S3Location) -> bytes:
    resp = s3_client().get_object(Bucket=loc.bucket, Key=loc.key)
    return resp["Body"].read()


def write_s3_json(loc: S3Location, payload: dict[str, Any]) -> None:
    s3_client().put_object(
        Bucket=loc.bucket,
        Key=loc.key,
        Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def load_parquet_from_s3(loc: S3Location) -> pl.DataFrame:
    raw = read_s3_bytes(loc)
    try:
        return pl.read_parquet(BytesIO(raw))
    except Exception as exc:
        raise ValueError(f"Failed to read parquet from s3://{loc.bucket}/{loc.key}") from exc


def _snapshot_to_dict(snapshot: Any) -> dict[str, Any]:
    if hasattr(snapshot, "dict") and callable(snapshot.dict):
        raw = snapshot.dict()
    elif hasattr(snapshot, "model_dump") and callable(snapshot.model_dump):
        raw = snapshot.model_dump()
    else:
        raw = dict(snapshot)

    if not isinstance(raw, dict):
        raise TypeError("Evidently snapshot did not serialize to a dict")
    return raw


def validate_required_columns(df: pl.DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def validate_exact_column_order(df: pl.DataFrame, expected: list[str], name: str) -> None:
    actual = list(df.columns)
    if actual != expected:
        raise ValueError(
            f"{name} column order mismatch.\n"
            f"Expected: {expected}\n"
            f"Actual:   {actual}"
        )


def normalize_frame(df: pl.DataFrame) -> pl.DataFrame:
    keep = [c for c in BASE_REQUIRED_COLUMNS if c in df.columns]
    return df.select(keep)


def coerce_types(df: pl.DataFrame) -> pl.DataFrame:
    exprs: list[Any] = []

    if ID_COLUMN in df.columns:
        exprs.append(pl.col(ID_COLUMN).cast(pl.Utf8, strict=False))
    if PICKUP_TIMESTAMP_COLUMN in df.columns:
        exprs.append(
            pl.col(PICKUP_TIMESTAMP_COLUMN).cast(
                pl.Datetime(time_unit="us", time_zone="UTC"),
                strict=False,
            )
        )
    if EVENT_TIMESTAMP_COLUMN in df.columns:
        exprs.append(
            pl.col(EVENT_TIMESTAMP_COLUMN).cast(
                pl.Datetime(time_unit="us", time_zone="UTC"),
                strict=False,
            )
        )
    if DATE_COLUMN in df.columns:
        exprs.append(pl.col(DATE_COLUMN).cast(pl.Date, strict=False))
    if SCHEMA_VERSION_COLUMN in df.columns:
        exprs.append(pl.col(SCHEMA_VERSION_COLUMN).cast(pl.Utf8, strict=False))
    if FEATURE_VERSION_COLUMN in df.columns:
        exprs.append(pl.col(FEATURE_VERSION_COLUMN).cast(pl.Utf8, strict=False))

    for col in CATEGORICAL_COLUMNS:
        if col in df.columns:
            exprs.append(pl.col(col).cast(pl.Int64, strict=False))

    for col in [*NUMERICAL_COLUMNS, LABEL_COLUMN]:
        if col in df.columns:
            exprs.append(pl.col(col).cast(pl.Float64, strict=False))

    return df.with_columns(exprs) if exprs else df


def validate_contract_alignment(df: pl.DataFrame, name: str) -> None:
    validate_required_columns(df, BASE_REQUIRED_COLUMNS, name)
    validate_exact_column_order(df, BASE_REQUIRED_COLUMNS, name)

    schema_versions = df.select(pl.col(SCHEMA_VERSION_COLUMN).n_unique()).item()
    feature_versions = df.select(pl.col(FEATURE_VERSION_COLUMN).n_unique()).item()
    if schema_versions != 1:
        raise ValueError(f"{name} schema_version must be single-valued.")
    if feature_versions != 1:
        raise ValueError(f"{name} feature_version must be single-valued.")

    schema_version = df.select(pl.col(SCHEMA_VERSION_COLUMN).first()).item()
    feature_version = df.select(pl.col(FEATURE_VERSION_COLUMN).first()).item()

    if schema_version != EXPECTED_SCHEMA_VERSION:
        raise ValueError(
            f"{name} schema_version must be {EXPECTED_SCHEMA_VERSION!r}, got {schema_version!r}"
        )
    if feature_version != EXPECTED_FEATURE_VERSION:
        raise ValueError(
            f"{name} feature_version must be {EXPECTED_FEATURE_VERSION!r}, got {feature_version!r}"
        )

    if df.select(pl.col(ID_COLUMN).is_null().any()).item():
        raise ValueError(f"{name} id column contains nulls.")
    if df.select(pl.col(EVENT_TIMESTAMP_COLUMN).is_null().any()).item():
        raise ValueError(f"{name} timestamp column contains nulls.")

    for col in [*NUMERICAL_COLUMNS, LABEL_COLUMN]:
        if df.select(pl.col(col).is_null().any()).item():
            raise ValueError(f"{name} column {col!r} contains nulls.")
        if df.select(pl.col(col).cast(pl.Float64).is_infinite().any()).item():
            raise ValueError(f"{name} column {col!r} contains infinite values.")

    if df.select((pl.col(LABEL_COLUMN) <= 0).any()).item():
        raise ValueError(f"{name} label column must be strictly positive.")


def build_data_definition() -> DataDefinition:
    return DataDefinition(
        id_column=ID_COLUMN,
        timestamp=EVENT_TIMESTAMP_COLUMN,
        categorical_columns=list(CATEGORICAL_COLUMNS),
        numerical_columns=[*NUMERICAL_COLUMNS, LABEL_COLUMN],
    )


def to_evidently_dataset(df: pl.DataFrame, definition: DataDefinition) -> Dataset:
    return Dataset.from_pandas(df.to_pandas(), data_definition=definition)


def build_report(monitored_columns: list[str], drift_share: float, drift_method: str) -> Report:
    return Report(
        metrics=[
            DataDriftPreset(
                columns=monitored_columns,
                drift_share=drift_share,
                method=drift_method,
            ),
        ]
    )


def _extract_drift_share(obj: Any) -> float | None:
    if isinstance(obj, dict):
        for key in ("share", "drift_share", "dataset_drift_share"):
            value = obj.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
        for value in obj.values():
            found = _extract_drift_share(value)
            if found is not None:
                return found

    if isinstance(obj, list):
        for item in obj:
            found = _extract_drift_share(item)
            if found is not None:
                return found

    return None


def _extract_drifted_columns(obj: Any, out: list[str]) -> None:
    if isinstance(obj, dict):
        if obj.get("drift_detected") is True:
            name = (
                obj.get("column_name")
                or obj.get("column")
                or obj.get("name")
                or obj.get("column_name_display")
            )
            if isinstance(name, str) and name.strip():
                out.append(name.strip())

        for key in ("drift_by_columns", "columns", "drifted_columns"):
            items = obj.get(key)
            if isinstance(items, list):
                for item in items:
                    _extract_drifted_columns(item, out)

        for value in obj.values():
            _extract_drifted_columns(value, out)

    elif isinstance(obj, list):
        for item in obj:
            _extract_drifted_columns(item, out)


def build_compact_summary(
    snapshot: dict[str, Any],
    *,
    current_rows: int,
    reference_rows: int,
    monitored_columns: list[str],
    reference_table_uuid: str,
    reference_snapshot_id: int | None,
) -> dict[str, Any]:
    drifted: list[str] = []
    _extract_drifted_columns(snapshot, drifted)

    deduped: list[str] = []
    seen: set[str] = set()
    for col in drifted:
        if col not in seen:
            seen.add(col)
            deduped.append(col)

    drift_share = _extract_drift_share(snapshot)
    dataset_drift_detected = bool(deduped) if drift_share is not None else None

    return {
        "current_rows": current_rows,
        "reference_rows": reference_rows,
        "monitored_columns": monitored_columns,
        "drift_share": drift_share,
        "drifted_columns": deduped,
        "drifted_columns_count": len(deduped),
        "dataset_drift_detected": dataset_drift_detected,
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "feature_version": EXPECTED_FEATURE_VERSION,
        "reference_table_uuid": reference_table_uuid,
        "reference_snapshot_id": reference_snapshot_id,
    }


def _load_reference_from_iceberg(
    *,
    iceberg_catalog_name: str,
    iceberg_rest_uri: str,
    iceberg_warehouse: str,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    contract = load_elt_contract(
        iceberg_catalog_name,
        iceberg_rest_uri,
        iceberg_warehouse,
    )
    if contract.schema_version != EXPECTED_SCHEMA_VERSION:
        raise RuntimeError(
            f"ELT contract schema_version must be {EXPECTED_SCHEMA_VERSION!r}, got {contract.schema_version!r}"
        )
    if contract.feature_version != EXPECTED_FEATURE_VERSION:
        raise RuntimeError(
            f"ELT contract feature_version must be {EXPECTED_FEATURE_VERSION!r}, got {contract.feature_version!r}"
        )

    gold_table = load_iceberg_table(
        iceberg_catalog_name,
        iceberg_rest_uri,
        iceberg_warehouse,
    )
    reference_pdf = read_table_as_dataframe(gold_table)
    reference_df = pl.from_pandas(reference_pdf)
    reference_df = normalize_frame(reference_df)
    reference_df = coerce_types(reference_df)
    validate_contract_alignment(reference_df, "reference")

    lineage = table_snapshot_lineage(gold_table)
    reference_meta = {
        "elt_contract": contract.as_dict(),
        "lineage": lineage.as_dict(),
    }
    return reference_df, reference_meta


def run_drift_job(
    current_input: str,
    output_prefix: str,
    drift_share: float,
    drift_method: str,
) -> None:
    s3_bucket = os.environ["S3_BUCKET"].strip()
    if not s3_bucket:
        raise ValueError("S3_BUCKET must be set.")

    current_loc = S3Location.parse(current_input, default_bucket=s3_bucket)
    output_loc = S3Location.parse(output_prefix, default_bucket=s3_bucket)

    iceberg_catalog_name = os.environ.get("ICEBERG_CATALOG_NAME", "default")
    iceberg_rest_uri = os.environ.get(
        "ICEBERG_REST_URI",
        "http://iceberg-rest.default.svc.cluster.local:8181",
    )
    iceberg_warehouse = os.environ.get(
        "ICEBERG_WAREHOUSE",
        "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
    )

    current_df = load_parquet_from_s3(current_loc)
    current_df = normalize_frame(current_df)
    current_df = coerce_types(current_df)
    validate_contract_alignment(current_df, "current")

    reference_df, reference_meta = _load_reference_from_iceberg(
        iceberg_catalog_name=iceberg_catalog_name,
        iceberg_rest_uri=iceberg_rest_uri,
        iceberg_warehouse=iceberg_warehouse,
    )

    monitored_columns = [*MATRIX_FEATURE_COLUMNS, LABEL_COLUMN]

    definition = build_data_definition()
    current_ds = to_evidently_dataset(current_df, definition)
    reference_ds = to_evidently_dataset(reference_df, definition)

    report = build_report(monitored_columns, drift_share=drift_share, drift_method=drift_method)
    snapshot = report.run(current_ds, reference_ds)

    raw_snapshot = _snapshot_to_dict(snapshot)
    ref_lineage = reference_meta["lineage"]
    summary = build_compact_summary(
        raw_snapshot,
        current_rows=current_df.height,
        reference_rows=reference_df.height,
        monitored_columns=monitored_columns,
        reference_table_uuid=str(ref_lineage.get("table_uuid", "")),
        reference_snapshot_id=(
            int(ref_lineage["current_snapshot_id"])
            if ref_lineage.get("current_snapshot_id") is not None
            else None
        ),
    )

    base_key = output_loc.key.rstrip("/")
    write_s3_json(
        S3Location(output_loc.bucket, f"{base_key}/evidently_drift_snapshot.json"),
        {
            "current_input": f"s3://{current_loc.bucket}/{current_loc.key}",
            "reference_source": "iceberg://gold/trip_training_matrix",
            "reference_contract": reference_meta["elt_contract"],
            "reference_lineage": reference_meta["lineage"],
            "snapshot": raw_snapshot,
        },
    )
    write_s3_json(
        S3Location(output_loc.bucket, f"{base_key}/evidently_drift_summary.json"),
        summary,
    )

    LOGGER.info(
        "Completed drift job current=s3://%s/%s reference=iceberg://gold/trip_training_matrix output=s3://%s/%s",
        current_loc.bucket,
        current_loc.key,
        output_loc.bucket,
        base_key,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trip ETA Evidently drift job")
    parser.add_argument(
        "--current-input",
        required=True,
        help="s3://bucket/key.parquet or key under S3_BUCKET",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="s3://bucket/prefix or key under S3_BUCKET",
    )
    parser.add_argument(
        "--drift-share",
        type=float,
        default=float(os.getenv("DRIFT_SHARE", "0.5")),
    )
    parser.add_argument(
        "--drift-method",
        default=os.getenv("DRIFT_METHOD", DEFAULT_DRIFT_METHOD),
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    run_drift_job(
        current_input=args.current_input,
        output_prefix=args.output_prefix,
        drift_share=args.drift_share,
        drift_method=args.drift_method,
    )


if __name__ == "__main__":
    main()