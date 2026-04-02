from __future__ import annotations

from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile

from workflows.train.tasks.common import (
    GOLD_TRAINING_TABLE,
    LIGHT_TASK_LIMITS,
    LIGHT_TASK_RETRIES,
    SOURCE_SILVER_TABLE,
    TIMESTAMP_COLUMN,
    artifact_sidecar_path,
    build_contract_summary,
    build_feature_spec,
    build_schema_hash,
    build_task_environment,
    coerce_contract_dtypes,
    ensure_directory,
    load_gold_frame,
    log_json,
    validate_gold_contract,
    validate_value_contracts,
    write_json,
)


@task(
    cache=False,
    environment=build_task_environment(),
    retries=LIGHT_TASK_RETRIES,
    limits=LIGHT_TASK_LIMITS,
)
def load_gold(dataset_uri: str, output_path: str = "/tmp/gold_canonical.parquet") -> FlyteFile:
    """
    Read the Gold dataset, validate the frozen contract, canonicalize dtypes,
    and materialize a deterministic parquet snapshot with sidecar contract files.

    This task is intentionally narrow:
    - it validates the Gold contract first,
    - it canonicalizes dtypes only after the contract check,
    - it sorts deterministically by as_of_ts,
    - and it writes the validated snapshot plus contract metadata.
    """
    log_json(msg="load_gold_start", dataset_uri=dataset_uri, output_path=output_path)

    raw_df = load_gold_frame(dataset_uri)
    validate_gold_contract(raw_df, strict_dtypes=False, label="Gold input frame")

    df = coerce_contract_dtypes(raw_df)
    validate_gold_contract(df, strict_dtypes=True, label="Gold canonical frame")
    validate_value_contracts(df)

    df = df.sort_values(TIMESTAMP_COLUMN, kind="mergesort").reset_index(drop=True)

    out_path = Path(output_path)
    ensure_directory(out_path.parent)
    df.to_parquet(out_path, index=False)

    feature_spec = build_feature_spec()
    schema_hash = build_schema_hash(feature_spec)
    contract = build_contract_summary(
        dataset_uri=dataset_uri,
        row_count=len(df),
        dataframe=df,
        gold_table=GOLD_TRAINING_TABLE,
        source_silver_table=SOURCE_SILVER_TABLE,
        extra={
            "output_path": str(out_path),
            "task": "load_gold",
            "validated_columns": list(df.columns),
        },
    )

    write_json(artifact_sidecar_path(out_path, ".feature_spec.json"), feature_spec)
    write_json(artifact_sidecar_path(out_path, ".contract.json"), contract)

    log_json(
        msg="load_gold_success",
        dataset_uri=dataset_uri,
        output_path=str(out_path),
        row_count=len(df),
        schema_hash=schema_hash,
        feature_version=feature_spec["feature_version"],
        schema_version=feature_spec["schema_version"],
        contract_sidecar=str(artifact_sidecar_path(out_path, ".contract.json")),
        feature_spec_sidecar=str(artifact_sidecar_path(out_path, ".feature_spec.json")),
    )

    return FlyteFile(path=str(out_path))