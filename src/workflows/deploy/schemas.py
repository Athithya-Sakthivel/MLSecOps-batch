from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import numpy as np


def coerce_instances(payload: Any) -> list[dict[str, Any]]:
    """
    Accept:
      - {"instances": [...]}  preferred
      - {"inputs": [...]}     backward-compatible alias
      - a raw list of objects
      - a single object dict
    """
    if isinstance(payload, dict):
        if "instances" in payload:
            instances = payload["instances"]
        elif "inputs" in payload:
            instances = payload["inputs"]
        else:
            instances = [payload]
    else:
        instances = payload

    if not isinstance(instances, list) or not instances:
        raise ValueError("Body must contain one or more instances")

    normalized: list[dict[str, Any]] = []
    for idx, row in enumerate(instances):
        if not isinstance(row, dict):
            raise ValueError(f"Instance at index {idx} must be an object")
        normalized.append(row)

    return normalized


def _to_python(value: Any) -> Any:
    """
    Convert numpy / array-like values into plain Python values suitable for JSON.
    """
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


def build_feature_matrix(
    instances: Sequence[dict[str, Any]],
    feature_order: Sequence[str],
    *,
    allow_extra_features: bool = False,
    dtype: Any = np.float32,
) -> np.ndarray:
    feature_set = set(feature_order)
    rows: list[list[float]] = []

    for row_idx, row in enumerate(instances):
        missing = [name for name in feature_order if name not in row]
        if missing:
            raise ValueError(
                f"Missing required features at row {row_idx}: {', '.join(missing)}"
            )

        if not allow_extra_features:
            extra = [name for name in row if name not in feature_set]
            if extra:
                raise ValueError(
                    f"Unexpected features at row {row_idx}: {', '.join(sorted(extra))}"
                )

        values: list[float] = []
        for name in feature_order:
            value = row[name]
            if value is None:
                raise ValueError(f"Feature '{name}' is null at row {row_idx}")

            if isinstance(value, bool):
                raise ValueError(f"Feature '{name}' must be numeric at row {row_idx}")

            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Feature '{name}' must be numeric at row {row_idx}"
                ) from exc

            if not math.isfinite(numeric):
                raise ValueError(f"Feature '{name}' must be finite at row {row_idx}")

            values.append(numeric)

        rows.append(values)

    matrix = np.asarray(rows, dtype=dtype)

    if matrix.ndim != 2:
        try:
            matrix = matrix.reshape(len(instances), len(feature_order))
        except ValueError as exc:
            raise ValueError("Feature matrix could not be reshaped to 2D") from exc

    return matrix


def _normalize_output_values(output: Any, row_count: int) -> list[Any]:
    """
    Normalize one model output into a per-row python value list.

    Rules:
      - scalar output -> broadcast to all rows
      - length-1 output -> broadcast to all rows
      - batch-shaped output with first dimension == row_count -> one value per row
      - multi-dimensional row values are preserved as lists
    """
    arr = np.asarray(output)

    if row_count < 1:
        return []

    if arr.ndim == 0:
        scalar = _to_python(arr.item())
        return [scalar for _ in range(row_count)]

    if arr.size == 1:
        scalar = _to_python(arr.reshape(()).item())
        return [scalar for _ in range(row_count)]

    if arr.shape[0] != row_count:
        raise ValueError(
            f"Output batch dimension {arr.shape[0]} does not match row_count {row_count}"
        )

    values: list[Any] = []
    for idx in range(row_count):
        item = _to_python(arr[idx])

        if isinstance(item, list) and len(item) == 1:
            values.append(item[0])
            continue

        values.append(item)

    return values


def split_model_outputs(
    outputs: Sequence[Any],
    output_names: Sequence[str],
    row_count: int,
) -> list[dict[str, Any]]:
    if len(outputs) != len(output_names):
        raise ValueError("Output name count does not match model outputs")

    if row_count < 1:
        return []

    normalized: list[dict[str, Any]] = [dict() for _ in range(row_count)]

    for name, output in zip(output_names, outputs, strict=True):
        values = _normalize_output_values(output, row_count)
        if len(values) != row_count:
            raise ValueError(
                f"Output '{name}' produced {len(values)} row values, expected {row_count}"
            )

        for idx, value in enumerate(values):
            normalized[idx][name] = value

    return normalized