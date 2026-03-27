from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable, Sequence, Tuple

from flytekit import Resources, current_context, task
from flytekitplugins.spark import Spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOG = logging.getLogger("elt_bronze_ingest")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
LOG.handlers[:] = [_handler]
LOG.propagate = False

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "iceberg")
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3://e2e-mlops-data-681802563986/iceberg/warehouse/",
)
ICEBERG_REST_URI = os.environ.get(
    "ICEBERG_REST_URI",
    "http://iceberg-rest.default.svc.cluster.local:9001/iceberg",
)
ICEBERG_REST_AUTH_TYPE = os.environ.get("ICEBERG_REST_AUTH_TYPE", "")
ICEBERG_REST_USER = os.environ.get("ICEBERG_REST_USER", "")
ICEBERG_REST_PASSWORD = os.environ.get("ICEBERG_REST_PASSWORD", "")

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")
S3_PATH_STYLE_ACCESS = os.environ.get("S3_PATH_STYLE_ACCESS", "false")

BRONZE_NAMESPACE = os.environ.get("BRONZE_NAMESPACE", "bronze")
SILVER_NAMESPACE = os.environ.get("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.environ.get("GOLD_NAMESPACE", "gold")

BRONZE_TRIPS_TABLE = os.environ.get(
    "BRONZE_TRIPS_TABLE",
    f"{CATALOG_NAME}.bronze.trips_raw",
)
BRONZE_TAXI_ZONE_TABLE = os.environ.get(
    "BRONZE_TAXI_ZONE_TABLE",
    f"{CATALOG_NAME}.bronze.taxi_zone_lookup_raw",
)
SILVER_TRIPS_TABLE = os.environ.get(
    "SILVER_TRIPS_TABLE",
    f"{CATALOG_NAME}.silver.trip_canonical",
)
GOLD_TRAINING_TABLE = os.environ.get(
    "GOLD_TRAINING_TABLE",
    f"{CATALOG_NAME}.gold.trip_training_matrix",
)
GOLD_CONTRACT_TABLE = os.environ.get(
    "GOLD_CONTRACT_TABLE",
    f"{CATALOG_NAME}.gold.trip_training_contracts",
)

TRIPS_DATASET_ID = os.environ.get("TRIPS_DATASET_ID", "koorukuroo/yellow_tripdata")
TRIPS_DATASET_SPLIT = os.environ.get("TRIPS_DATASET_SPLIT", "train")
TRIPS_DATASET_REVISION = os.environ.get(
    "TRIPS_DATASET_REVISION",
    "ef7653853df26ba2cd9ccbae6db2f4094c2d63b0",
)
TAXI_ZONE_LOOKUP_URL = os.environ.get(
    "TAXI_ZONE_LOOKUP_URL",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
)
HF_TOKEN = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN") or None

MAX_ROWS_TO_EXTRACT_FROM_DATASETS = int(os.environ.get("MAX_ROWS_TO_EXTRACT_FROM_DATASETS", "0"))
BRONZE_CHUNK_SIZE = int(os.environ.get("BRONZE_CHUNK_SIZE", "2000"))
BRONZE_ROWS_PER_PARTITION = int(os.environ.get("BRONZE_ROWS_PER_PARTITION", "25000"))

ICEBERG_TARGET_FILE_SIZE_BYTES = os.environ.get("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")

# Conservative Spark defaults to reduce memory pressure in the Spark driver/executor pods.
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "512m")
SPARK_EXECUTOR_MEMORY_OVERHEAD = os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "512m")
SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")
SPARK_EXECUTOR_INSTANCES = os.environ.get("SPARK_EXECUTOR_INSTANCES", "1")
SPARK_DRIVER_CORES = os.environ.get("SPARK_DRIVER_CORES", "1")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
SPARK_MAX_PARTITION_BYTES = os.environ.get("SPARK_MAX_PARTITION_BYTES", "134217728")
SPARK_MAX_RESULT_SIZE = os.environ.get("SPARK_MAX_RESULT_SIZE", "256m")

PARQUET_COMPRESSION = os.environ.get("PARQUET_COMPRESSION", "snappy")

ICEBERG_EXPIRE_DAYS = int(os.environ.get("ICEBERG_EXPIRE_DAYS", "7"))
ICEBERG_ORPHAN_DAYS = int(os.environ.get("ICEBERG_ORPHAN_DAYS", "3"))
ICEBERG_RETAIN_LAST = int(os.environ.get("ICEBERG_RETAIN_LAST", "3"))
MAINTENANCE_REWRITE_DAYS = int(os.environ.get("MAINTENANCE_REWRITE_DAYS", "30"))

ALLOW_LOCAL_SPARK_FALLBACK = os.environ.get("FLYTE_ALLOW_LOCAL_SPARK_FALLBACK", "false").lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:1.0.9",
).strip()
if not TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must be set before importing bronze_ingest.py")


@dataclass(frozen=True)
class BronzeIngestResult:
    run_id: str
    trips_table: str
    taxi_zone_table: str
    trips_rows: int
    taxi_zone_rows: int
    trips_source_ref: str
    taxi_zone_source_ref: str
    trips_write_mode: str
    taxi_zone_write_mode: str


def log_json(**payload) -> None:
    LOG.info(json.dumps(payload, default=str, sort_keys=True))


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_").lower()
    if not normalized:
        raise ValueError(f"invalid column name after normalization: {name!r}")
    return normalized


def normalize_record(row: dict) -> dict:
    out: dict = {}
    for key, value in row.items():
        normalized_key = normalize_column_name(str(key))
        if normalized_key in out:
            raise ValueError(
                f"column collision after normalization: {key!r} -> {normalized_key!r}"
            )
        out[normalized_key] = value
    return out


def first_existing(columns: Sequence[str], candidates: Sequence[str]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise KeyError(f"none of the candidate columns exist: {list(candidates)}")


def qualify_table_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) == 3:
        return table_id
    if len(parts) == 2:
        return f"{CATALOG_NAME}.{table_id}"
    raise ValueError(
        f"expected table id in the form catalog.namespace.table or namespace.table, got {table_id!r}"
    )


def parse_table_id(table_id: str) -> Tuple[str, str, str]:
    qualified = qualify_table_id(table_id)
    parts = qualified.split(".")
    return parts[0], parts[1], parts[2]


def ensure_namespace(spark: SparkSession, catalog_name: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")


def table_exists(spark: SparkSession, table_id: str) -> bool:
    catalog_name, namespace, table_name = parse_table_id(table_id)
    rows = spark.sql(
        f"SHOW TABLES IN {catalog_name}.{namespace} LIKE '{table_name}'"
    ).limit(1).collect()
    return len(rows) > 0


def build_spark_conf() -> dict[str, str]:
    conf = {
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": ICEBERG_REST_URI,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": ICEBERG_WAREHOUSE,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.shuffle.partitions": SPARK_SHUFFLE_PARTITIONS,
        "spark.sql.files.maxPartitionBytes": SPARK_MAX_PARTITION_BYTES,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.driver.memory": SPARK_DRIVER_MEMORY,
        "spark.driver.memoryOverhead": SPARK_DRIVER_MEMORY_OVERHEAD,
        "spark.executor.memory": SPARK_EXECUTOR_MEMORY,
        "spark.executor.memoryOverhead": SPARK_EXECUTOR_MEMORY_OVERHEAD,
        "spark.executor.cores": SPARK_EXECUTOR_CORES,
        "spark.executor.instances": SPARK_EXECUTOR_INSTANCES,
        "spark.driver.cores": SPARK_DRIVER_CORES,
        "spark.driver.maxResultSize": SPARK_MAX_RESULT_SIZE,
        "spark.kubernetes.authenticate.driver.serviceAccountName": SPARK_SERVICE_ACCOUNT,
        "spark.kubernetes.authenticate.executor.serviceAccountName": SPARK_SERVICE_ACCOUNT,
    }
    if ICEBERG_REST_AUTH_TYPE:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type"] = ICEBERG_REST_AUTH_TYPE
    if ICEBERG_REST_USER:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.username"] = ICEBERG_REST_USER
    if ICEBERG_REST_PASSWORD:
        conf[f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.basic.password"] = ICEBERG_REST_PASSWORD
    return conf


def build_hadoop_conf() -> dict[str, str]:
    conf = {
        "fs.s3a.endpoint.region": AWS_REGION,
    }
    if S3_ENDPOINT:
        conf["fs.s3a.endpoint"] = S3_ENDPOINT
        conf["fs.s3a.path.style.access"] = S3_PATH_STYLE_ACCESS
    else:
        conf["fs.s3a.path.style.access"] = "false"

    # Prefer the ambient credential chain when workload identity / IRSA is in use.
    # Only force static credentials when they are explicitly present and the pod is
    # not already configured for web identity / role-based auth.
    has_web_identity = bool(os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE"))
    has_role_based_auth = bool(AWS_ROLE_ARN)

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and not has_web_identity and not has_role_based_auth:
        conf["fs.s3a.access.key"] = AWS_ACCESS_KEY_ID
        conf["fs.s3a.secret.key"] = AWS_SECRET_ACCESS_KEY
        conf["fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            if AWS_SESSION_TOKEN
            else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        if AWS_SESSION_TOKEN:
            conf["fs.s3a.session.token"] = AWS_SESSION_TOKEN

    return conf


def load_streaming_dataset(
    source: str,
    *,
    split: str = "train",
    data_files: str | dict | None = None,
    revision: str | None = None,
    token: str | bool | None = None,
):
    from datasets import load_dataset

    kwargs: dict[str, Any] = {"split": split, "streaming": True}
    if data_files is not None:
        kwargs["data_files"] = data_files
    if revision:
        kwargs["revision"] = revision
    if token:
        kwargs["token"] = token
    return load_dataset(source, **kwargs)


def get_spark_session() -> SparkSession:
    try:
        spark = current_context().spark_session
    except Exception:
        spark = None

    if spark is not None:
        return spark

    if not ALLOW_LOCAL_SPARK_FALLBACK:
        raise RuntimeError(
            "Flyte did not provide a Spark session for this task. "
            "This usually means the Spark plugin was not active. "
            "Set FLYTE_ALLOW_LOCAL_SPARK_FALLBACK=true only for local testing."
        )

    builder = SparkSession.builder.appName("bronze_ingest_local_fallback")
    for k, v in build_spark_conf().items():
        builder = builder.config(k, v)
    for k, v in build_hadoop_conf().items():
        builder = builder.config(f"spark.hadoop.{k}", v)
    return builder.getOrCreate()


def iter_preview_rows(stream: Iterable[dict], n: int = 2) -> tuple[list[dict], Iterable[dict]]:
    it = iter(stream)
    preview = list(islice(it, n))
    return preview, it


def stream_to_dataframe(
    spark: SparkSession,
    rows: Iterable[dict],
    *,
    label: str,
    chunk_size: int = BRONZE_CHUNK_SIZE,
) -> tuple[DataFrame, int]:
    accumulated_df: DataFrame | None = None
    schema = None
    total_rows = 0
    batch: list[dict] = []

    def flush_batch(current_batch: list[dict], current_schema) -> tuple[DataFrame, Any]:
        if not current_batch:
            raise RuntimeError(f"attempted to flush an empty batch for {label}")
        batch_df = spark.createDataFrame(current_batch, schema=current_schema)
        return batch_df, batch_df.schema

    for row in rows:
        batch.append(normalize_record(dict(row)))
        total_rows += 1
        if len(batch) >= chunk_size:
            batch_df, schema = flush_batch(batch, schema)
            accumulated_df = batch_df if accumulated_df is None else accumulated_df.unionByName(
                batch_df,
                allowMissingColumns=True,
            )
            log_json(
                msg="materialized_batch",
                label=label,
                rows=total_rows,
                batch_rows=len(batch),
            )
            batch.clear()

    if batch:
        batch_df, schema = flush_batch(batch, schema)
        accumulated_df = batch_df if accumulated_df is None else accumulated_df.unionByName(
            batch_df,
            allowMissingColumns=True,
        )
        log_json(
            msg="materialized_final_batch",
            label=label,
            rows=total_rows,
            batch_rows=len(batch),
        )
        batch.clear()

    if accumulated_df is None or total_rows == 0:
        raise RuntimeError(f"no rows read from source {label!r}")

    return accumulated_df, total_rows


def cast_if_present(df: DataFrame, column: str, spark_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, F.col(column).cast(spark_type))
    return df


def add_trip_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    pickup_col = first_existing(df.columns, ("lpep_pickup_datetime", "tpep_pickup_datetime", "pickup_ts"))
    dropoff_col = first_existing(df.columns, ("lpep_dropoff_datetime", "tpep_dropoff_datetime", "dropoff_ts"))
    pickup_location_col = first_existing(df.columns, ("pulocation_id", "pickup_location_id"))
    dropoff_location_col = first_existing(df.columns, ("dolocation_id", "dropoff_location_id"))

    df = (
        df.withColumn("pickup_ts", F.to_timestamp(F.col(pickup_col)))
        .withColumn("dropoff_ts", F.to_timestamp(F.col(dropoff_col)))
        .withColumn("pickup_location_id", F.col(pickup_location_col).cast("long"))
        .withColumn("dropoff_location_id", F.col(dropoff_location_col).cast("long"))
        .withColumn("event_date", F.to_date(F.coalesce(F.col("pickup_ts"), F.col("dropoff_ts"))))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_uri", F.lit(source_ref))
        .withColumn("source_revision", F.lit(TRIPS_DATASET_REVISION))
        .withColumn("source_kind", F.lit("huggingface_dataset"))
        .withColumn("source_file", F.lit(TRIPS_DATASET_ID))
        .filter(F.col("event_date").isNotNull())
    )

    numeric_double_cols = (
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "cbd_congestion_fee",
    )
    integer_cols = (
        "vendor_id",
        "ratecode_id",
        "passenger_count",
        "payment_type",
        "trip_type",
    )

    for col_name in numeric_double_cols:
        df = cast_if_present(df, col_name, "double")
    for col_name in integer_cols:
        df = cast_if_present(df, col_name, "long")
    if "store_and_fwd_flag" in df.columns:
        df = df.withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast("string"))

    required = {
        "pickup_ts",
        "dropoff_ts",
        "pickup_location_id",
        "dropoff_location_id",
        "event_date",
        "trip_distance",
        "fare_amount",
        "total_amount",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"trips bronze dataframe is missing required columns: {sorted(missing)}")
    return df


def add_zone_bronze_columns(df: DataFrame, *, run_id: str, source_ref: str) -> DataFrame:
    df = (
        df.select(
            F.col("location_id").cast("long").alias("location_id"),
            F.col("borough").cast("string").alias("borough"),
            F.col("zone").cast("string").alias("zone"),
            F.col("service_zone").cast("string").alias("service_zone"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_uri", F.lit(source_ref))
        .withColumn("source_revision", F.lit(""))
        .withColumn("source_kind", F.lit("http_csv"))
        .withColumn("source_file", F.lit(TAXI_ZONE_LOOKUP_URL))
        .dropDuplicates(["location_id"])
    )

    required = {"location_id", "borough", "zone", "service_zone"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"taxi zone bronze dataframe is missing required columns: {sorted(missing)}")
    return df


def write_partitioned_iceberg_table(df: DataFrame, table_id: str, partition_column: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwritePartitions()
        return "overwrite_partitions"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .partitionedBy(F.col(partition_column))
        .create()
    )
    return "create"


def write_replace_iceberg_table(df: DataFrame, table_id: str) -> str:
    table_id = qualify_table_id(table_id)
    if table_exists(df.sparkSession, table_id):
        df.writeTo(table_id).overwrite(F.lit(True))
        return "overwrite"

    (
        df.writeTo(table_id)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.target-file-size-bytes", ICEBERG_TARGET_FILE_SIZE_BYTES)
        .create()
    )
    return "create"


@task(
    task_config=Spark(
        spark_conf=build_spark_conf(),
        hadoop_conf=build_hadoop_conf(),
        executor_path="/opt/venv/bin/python",
    ),
    container_image=TASK_IMAGE,
    retries=0,
    limits=Resources(cpu="1000m", mem="3500M"),
)
def bronze_ingest() -> BronzeIngestResult:
    run_id = os.environ.get("RUN_ID") or os.environ.get("FLYTE_INTERNAL_EXECUTION_ID") or uuid.uuid4().hex

    trips_source_ref = (
        f"{TRIPS_DATASET_ID}@{TRIPS_DATASET_REVISION}"
        if TRIPS_DATASET_REVISION
        else TRIPS_DATASET_ID
    )
    taxi_zone_source_ref = TAXI_ZONE_LOOKUP_URL

    log_json(
        msg="bronze_ingest_start",
        run_id=run_id,
        trips_source=trips_source_ref,
        taxi_zone_source=taxi_zone_source_ref,
        max_rows=MAX_ROWS_TO_EXTRACT_FROM_DATASETS,
    )

    trips_stream = load_streaming_dataset(
        TRIPS_DATASET_ID,
        split=TRIPS_DATASET_SPLIT,
        revision=TRIPS_DATASET_REVISION,
        token=HF_TOKEN,
    )
    taxi_zone_stream = load_streaming_dataset(
        "csv",
        split="train",
        data_files={"train": TAXI_ZONE_LOOKUP_URL},
        token=HF_TOKEN,
    )

    trips_preview, trips_iter = iter_preview_rows(trips_stream, 2)
    taxi_preview, taxi_iter = iter_preview_rows(taxi_zone_stream, 2)

    for i, row in enumerate(trips_preview, start=1):
        log_json(msg="trip_preview_row", row=i, data=normalize_record(dict(row)))
    for i, row in enumerate(taxi_preview, start=1):
        log_json(msg="taxi_zone_preview_row", row=i, data=normalize_record(dict(row)))

    if MAX_ROWS_TO_EXTRACT_FROM_DATASETS > 0:
        trips_iter = islice(trips_iter, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)
        taxi_iter = islice(taxi_iter, MAX_ROWS_TO_EXTRACT_FROM_DATASETS)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    ensure_namespace(spark, CATALOG_NAME, BRONZE_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, SILVER_NAMESPACE)
    ensure_namespace(spark, CATALOG_NAME, GOLD_NAMESPACE)

    trips_raw_df, trips_rows = stream_to_dataframe(
        spark,
        trips_iter,
        label="trips",
        chunk_size=BRONZE_CHUNK_SIZE,
    )
    trips_df = add_trip_bronze_columns(trips_raw_df, run_id=run_id, source_ref=trips_source_ref)
    trips_partitions = max(1, math.ceil(trips_rows / BRONZE_ROWS_PER_PARTITION))
    trips_df = trips_df.repartition(trips_partitions, F.col("event_date"))

    taxi_zone_raw_df, taxi_zone_rows = stream_to_dataframe(
        spark,
        taxi_iter,
        label="taxi_zone_lookup",
        chunk_size=min(BRONZE_CHUNK_SIZE, 1000),
    )
    taxi_zone_df = add_zone_bronze_columns(
        taxi_zone_raw_df,
        run_id=run_id,
        source_ref=taxi_zone_source_ref,
    ).coalesce(1)

    trips_write_mode = write_partitioned_iceberg_table(
        trips_df,
        BRONZE_TRIPS_TABLE,
        "event_date",
    )
    taxi_zone_write_mode = write_replace_iceberg_table(
        taxi_zone_df,
        BRONZE_TAXI_ZONE_TABLE,
    )

    result = BronzeIngestResult(
        run_id=run_id,
        trips_table=qualify_table_id(BRONZE_TRIPS_TABLE),
        taxi_zone_table=qualify_table_id(BRONZE_TAXI_ZONE_TABLE),
        trips_rows=trips_rows,
        taxi_zone_rows=taxi_zone_rows,
        trips_source_ref=trips_source_ref,
        taxi_zone_source_ref=taxi_zone_source_ref,
        trips_write_mode=trips_write_mode,
        taxi_zone_write_mode=taxi_zone_write_mode,
    )
    log_json(msg="bronze_ingest_success", **result.__dict__)
    return result