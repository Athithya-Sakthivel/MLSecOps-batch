from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import math
import os
import posixpath
import shutil
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from string import hexdigits
from typing import Any

import fsspec
import numpy as np
import onnxruntime as ort
from config import Settings

_BUNDLE_FORMAT_VERSION = 1
_BUNDLE_MODEL_NAME = "model.onnx"
_BUNDLE_SCHEMA_NAME = "schema.json"
_BUNDLE_METADATA_NAME = "metadata.json"
_BUNDLE_MANIFEST_NAME = "manifest.json"
_CHUNK_SIZE = 1024 * 1024
_MAX_SCHEMA_BYTES = 1_048_576
_MAX_METADATA_BYTES = 1_048_576
_LOCK_TIMEOUT_SECONDS = 300

TARGET_TRANSFORM = "log1p"
MAX_PREDICTION_SECONDS = 24.0 * 3600.0

logger = logging.getLogger(__name__)


def _stable_json_text(obj: dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _canonical_json_text(obj: dict[str, Any]) -> str:
    return _stable_json_text(obj)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _normalize_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} is required and must be a sha256 hex string")

    normalized = value.strip().lower()
    if len(normalized) != 64 or any(ch not in hexdigits.lower() for ch in normalized):
        raise RuntimeError(f"{field_name} must be a 64-character lowercase hex sha256 digest")
    return normalized


def _require_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty string when provided")
    return value.strip()


def _require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f"{field_name} must be a boolean")
    return value


def _require_float(value: Any, field_name: str) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise RuntimeError(f"{field_name} must be numeric") from exc


def _require_nonempty_str_list(values: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, list) or not values:
        raise RuntimeError(f"{field_name} must be a non-empty list")

    cleaned: list[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(values):
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"{field_name}[{idx}] must be a non-empty string")
        value = item.strip()
        if value in seen:
            raise RuntimeError(f"{field_name} contains duplicate value: {value}")
        seen.add(value)
        cleaned.append(value)
    return tuple(cleaned)


def _read_limited_text(src_fs: fsspec.AbstractFileSystem, src_path: str, max_bytes: int) -> str:
    with src_fs.open(src_path, "rb") as f:
        data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise RuntimeError(f"{src_path} exceeds the configured size limit of {max_bytes} bytes")
    return data.decode("utf-8")


def _read_json_object_from_fs(
    src_fs: fsspec.AbstractFileSystem,
    src_path: str,
    *,
    max_bytes: int,
) -> dict[str, Any]:
    raw = json.loads(_read_limited_text(src_fs, src_path, max_bytes))
    if not isinstance(raw, dict):
        raise RuntimeError(f"{src_path} must contain a JSON object")
    return raw


def _read_json_object_from_path(src_path: Path) -> dict[str, Any]:
    raw = json.loads(src_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"{src_path} must contain a JSON object")
    return raw


def _atomic_write_text(dst_path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{dst_path.name}.",
        suffix=".tmp",
        dir=str(dst_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(text, encoding="utf-8")
        tmp_path.replace(dst_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_json(dst_path: Path, obj: dict[str, Any]) -> None:
    _atomic_write_text(dst_path, _canonical_json_text(obj))


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _copy_with_hash(src_fs: fsspec.AbstractFileSystem, src_path: str, dst_path: Path) -> str:
    hasher = hashlib.sha256()
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{dst_path.name}.",
        suffix=".tmp",
        dir=str(dst_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)

    try:
        with src_fs.open(src_path, "rb") as src, tmp_path.open("wb") as dst:
            while True:
                chunk = src.read(_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
                dst.write(chunk)
        tmp_path.replace(dst_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return hasher.hexdigest()


@contextlib.contextmanager
def _acquire_lock(lock_dir: Path, timeout_seconds: int = _LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            lock_dir.mkdir(parents=False, exist_ok=False)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for bundle lock: {lock_dir}") from None
            time.sleep(0.2)

    try:
        yield
    finally:
        shutil.rmtree(lock_dir, ignore_errors=True)


def _bundle_source_paths(root_uri: str) -> tuple[str, str, str, str]:
    root = root_uri.rstrip("/")
    if root.endswith(".onnx"):
        raise RuntimeError(
            "MODEL_URI must point to the bundle root directory, not directly to model.onnx"
        )

    model_src = posixpath.join(root, _BUNDLE_MODEL_NAME)
    schema_src = posixpath.join(root, _BUNDLE_SCHEMA_NAME)
    metadata_src = posixpath.join(root, _BUNDLE_METADATA_NAME)
    manifest_src = posixpath.join(root, _BUNDLE_MANIFEST_NAME)
    return model_src, schema_src, metadata_src, manifest_src


@dataclass(frozen=True, slots=True)
class ModelSchema:
    schema_version: str
    feature_version: str
    target_transform: str
    feature_order: tuple[str, ...]
    input_name: str
    output_names: tuple[str, ...]
    allow_extra_features: bool
    request_feature_order: tuple[str, ...]
    engineered_feature_order: tuple[str, ...]
    feature_order_hash: str
    request_feature_order_hash: str
    engineered_feature_order_hash: str

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "feature_version": self.feature_version,
            "target_transform": self.target_transform,
            "feature_order": list(self.feature_order),
            "input_name": self.input_name,
            "output_names": list(self.output_names),
            "allow_extra_features": self.allow_extra_features,
            "request_feature_order": list(self.request_feature_order),
            "engineered_feature_order": list(self.engineered_feature_order),
            "feature_order_hash": self.feature_order_hash,
            "request_feature_order_hash": self.request_feature_order_hash,
            "engineered_feature_order_hash": self.engineered_feature_order_hash,
        }


@dataclass(frozen=True, slots=True)
class ModelMetadata:
    model_name: str
    model_version: str
    schema_version: str
    feature_version: str
    preprocessing_version: str
    label_cap_seconds: float
    category_levels: dict[str, list[int]]
    request_feature_order: tuple[str, ...]
    engineered_feature_order: tuple[str, ...]
    bundle_contract: dict[str, Any] | None
    artifact_plan: dict[str, Any] | None
    raw: dict[str, Any]

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "schema_version": self.schema_version,
            "feature_version": self.feature_version,
            "preprocessing_version": self.preprocessing_version,
            "label_cap_seconds": self.label_cap_seconds,
            "category_levels": self.category_levels,
            "request_feature_order": list(self.request_feature_order),
            "engineered_feature_order": list(self.engineered_feature_order),
            "bundle_contract": self.bundle_contract,
            "artifact_plan": self.artifact_plan,
            "raw": self.raw,
        }


@dataclass(frozen=True, slots=True)
class ModelBundleManifest:
    format_version: int
    source_uri: str
    model_version: str
    model_sha256: str
    schema_sha256: str
    metadata_sha256: str

    def __getitem__(self, key: str):
        return getattr(self, key)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LoadedModel:
    model_path: Path
    cache_dir: Path
    schema: ModelSchema
    metadata: ModelMetadata
    manifest: ModelBundleManifest
    session: PredictionTransformingSession
    input_name: str
    output_names: tuple[str, ...]


def _cache_key(model_sha256: str, schema_sha256: str, model_version: str) -> str:
    h = hashlib.sha256()
    h.update(model_sha256.encode("utf-8"))
    h.update(b"\0")
    h.update(schema_sha256.encode("utf-8"))
    h.update(b"\0")
    h.update(model_version.encode("utf-8"))
    return h.hexdigest()[:16]


def _cache_dir(
    settings: Settings,
    model_sha256: str,
    schema_sha256: str,
    model_version: str,
) -> Path:
    cache_dir = Path(settings.model_cache_dir) / _cache_key(model_sha256, schema_sha256, model_version)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _parse_schema(raw: dict[str, Any]) -> ModelSchema:
    schema_version = _require_str(raw.get("schema_version"), "schema_version")
    feature_version = _require_str(raw.get("feature_version"), "feature_version")
    target_transform = _require_str(raw.get("target_transform"), "target_transform")
    if target_transform != TARGET_TRANSFORM:
        raise RuntimeError(
            f"schema.json target_transform must be {TARGET_TRANSFORM!r}, got {target_transform!r}"
        )

    feature_order = _require_nonempty_str_list(raw.get("feature_order"), "feature_order")
    request_feature_order_raw = raw.get("request_feature_order", list(feature_order))
    engineered_feature_order_raw = raw.get("engineered_feature_order", list(feature_order))
    request_feature_order = _require_nonempty_str_list(
        request_feature_order_raw, "request_feature_order"
    )
    engineered_feature_order = _require_nonempty_str_list(
        engineered_feature_order_raw, "engineered_feature_order"
    )

    if feature_order != request_feature_order:
        raise RuntimeError("schema.json feature_order and request_feature_order must match exactly")
    if feature_order != engineered_feature_order:
        raise RuntimeError("schema.json feature_order and engineered_feature_order must match exactly")

    input_name = _require_str(raw.get("input_name"), "input_name")
    if input_name != "input":
        raise RuntimeError(f"schema.json input_name must be 'input', got {input_name!r}")

    output_names = _require_nonempty_str_list(raw.get("output_names"), "output_names")

    allow_extra_features = _require_bool(raw.get("allow_extra_features"), "allow_extra_features")
    if allow_extra_features is not False:
        raise RuntimeError("schema.json allow_extra_features must be false")

    feature_order_hash = _require_str(raw.get("feature_order_hash"), "feature_order_hash")
    request_feature_order_hash = _require_str(
        raw.get("request_feature_order_hash"), "request_feature_order_hash"
    )
    engineered_feature_order_hash = _require_str(
        raw.get("engineered_feature_order_hash"), "engineered_feature_order_hash"
    )

    if feature_order_hash != _sha256_text("\n".join(feature_order)):
        raise RuntimeError("schema.json feature_order_hash mismatch")
    if request_feature_order_hash != _sha256_text("\n".join(request_feature_order)):
        raise RuntimeError("schema.json request_feature_order_hash mismatch")
    if engineered_feature_order_hash != _sha256_text("\n".join(engineered_feature_order)):
        raise RuntimeError("schema.json engineered_feature_order_hash mismatch")

    return ModelSchema(
        schema_version=schema_version,
        feature_version=feature_version,
        target_transform=target_transform,
        feature_order=tuple(feature_order),
        input_name=input_name,
        output_names=tuple(output_names),
        allow_extra_features=allow_extra_features,
        request_feature_order=tuple(request_feature_order),
        engineered_feature_order=tuple(engineered_feature_order),
        feature_order_hash=feature_order_hash,
        request_feature_order_hash=request_feature_order_hash,
        engineered_feature_order_hash=engineered_feature_order_hash,
    )


def _parse_metadata(raw: dict[str, Any]) -> ModelMetadata:
    model_name = _require_str(raw.get("model_name"), "model_name")
    model_version = _require_str(raw.get("model_version"), "model_version")
    schema_version = _require_str(raw.get("schema_version"), "schema_version")
    feature_version = _require_str(raw.get("feature_version"), "feature_version")
    preprocessing_version = _require_str(raw.get("preprocessing_version"), "preprocessing_version")
    label_cap_seconds = _require_float(raw.get("label_cap_seconds"), "label_cap_seconds")
    if label_cap_seconds <= 0:
        raise RuntimeError("label_cap_seconds must be > 0")

    request_feature_order = _require_nonempty_str_list(
        raw.get("request_feature_order"), "request_feature_order"
    )
    engineered_feature_order = _require_nonempty_str_list(
        raw.get("engineered_feature_order"), "engineered_feature_order"
    )

    category_levels_raw = raw.get("category_levels", {})
    if not isinstance(category_levels_raw, dict):
        raise RuntimeError("category_levels must be a JSON object")
    category_levels: dict[str, list[int]] = {}
    for key, values in category_levels_raw.items():
        if not isinstance(key, str) or not key.strip():
            raise RuntimeError("category_levels contains an invalid key")
        if not isinstance(values, list):
            raise RuntimeError(f"category_levels[{key!r}] must be a list")
        cleaned: list[int] = []
        for idx, item in enumerate(values):
            if isinstance(item, bool):
                raise RuntimeError(f"category_levels[{key!r}][{idx}] must be an integer")
            try:
                cleaned.append(int(item))
            except Exception as exc:
                raise RuntimeError(f"category_levels[{key!r}][{idx}] must be an integer") from exc
        category_levels[key] = cleaned

    bundle_contract_raw = raw.get("bundle_contract")
    bundle_contract = bundle_contract_raw if isinstance(bundle_contract_raw, dict) else None

    artifact_plan_raw = raw.get("artifact_plan")
    artifact_plan = artifact_plan_raw if isinstance(artifact_plan_raw, dict) else None

    if request_feature_order != engineered_feature_order:
        raise RuntimeError("metadata request_feature_order and engineered_feature_order must match")
    if schema_version != raw.get("schema_version"):
        raise RuntimeError("metadata schema_version mismatch")
    if feature_version != raw.get("feature_version"):
        raise RuntimeError("metadata feature_version mismatch")
    if preprocessing_version != raw.get("preprocessing_version"):
        raise RuntimeError("metadata preprocessing_version mismatch")

    return ModelMetadata(
        model_name=model_name,
        model_version=model_version,
        schema_version=schema_version,
        feature_version=feature_version,
        preprocessing_version=preprocessing_version,
        label_cap_seconds=label_cap_seconds,
        category_levels=category_levels,
        request_feature_order=tuple(request_feature_order),
        engineered_feature_order=tuple(engineered_feature_order),
        bundle_contract=bundle_contract,
        artifact_plan=artifact_plan,
        raw=raw,
    )


def _parse_manifest(raw: dict[str, Any]) -> ModelBundleManifest:
    format_version = raw.get("format_version")
    if format_version != _BUNDLE_FORMAT_VERSION:
        raise RuntimeError(f"Unsupported bundle format version: {format_version!r}")

    source_uri = _require_str(raw.get("source_uri"), "source_uri")
    model_version = _require_str(raw.get("model_version"), "model_version")
    model_sha256 = _normalize_sha256(raw.get("model_sha256"), "model_sha256")
    schema_sha256 = _normalize_sha256(raw.get("schema_sha256"), "schema_sha256")
    metadata_sha256 = _normalize_sha256(raw.get("metadata_sha256"), "metadata_sha256")

    return ModelBundleManifest(
        format_version=int(format_version),
        source_uri=source_uri,
        model_version=model_version,
        model_sha256=model_sha256,
        schema_sha256=schema_sha256,
        metadata_sha256=metadata_sha256,
    )


def _validate_bundle_consistency(
    *,
    schema: ModelSchema,
    metadata: ModelMetadata,
    manifest: ModelBundleManifest,
    settings: Settings,
) -> None:
    expected_source_uri = str(settings.model_uri).rstrip("/")
    if manifest.source_uri.rstrip("/") != expected_source_uri:
        raise RuntimeError(
            f"Manifest source_uri mismatch: expected {expected_source_uri!r}, got {manifest.source_uri!r}"
        )

    expected_model_version = _require_str(settings.model_version, "settings.model_version")
    if manifest.model_version != expected_model_version:
        raise RuntimeError(
            f"Manifest model_version mismatch: expected {expected_model_version!r}, got {manifest.model_version!r}"
        )
    if metadata.model_version != expected_model_version:
        raise RuntimeError(
            f"Metadata model_version mismatch: expected {expected_model_version!r}, got {metadata.model_version!r}"
        )

    if schema.schema_version != metadata.schema_version:
        raise RuntimeError("Schema and metadata schema_version mismatch")
    if schema.feature_version != metadata.feature_version:
        raise RuntimeError("Schema and metadata feature_version mismatch")
    if schema.target_transform != TARGET_TRANSFORM:
        raise RuntimeError(f"Schema target_transform must be {TARGET_TRANSFORM!r}")
    if schema.feature_order != metadata.request_feature_order:
        raise RuntimeError("Schema feature_order and metadata request_feature_order mismatch")
    if schema.feature_order != metadata.engineered_feature_order:
        raise RuntimeError("Schema feature_order and metadata engineered_feature_order mismatch")

    if metadata.preprocessing_version != "matrix_identity_v1":
        raise RuntimeError(
            f"Metadata preprocessing_version must be 'matrix_identity_v1', got {metadata.preprocessing_version!r}"
        )

    if settings.model_sha256:
        expected_model_sha256 = _normalize_sha256(settings.model_sha256, "settings.model_sha256")
        if manifest.model_sha256 != expected_model_sha256:
            raise RuntimeError(
                f"Manifest model_sha256 mismatch: expected {expected_model_sha256!r}, got {manifest.model_sha256!r}"
            )


def _validate_bundle_files(cache_dir: Path) -> None:
    required = (
        cache_dir / _BUNDLE_MODEL_NAME,
        cache_dir / _BUNDLE_SCHEMA_NAME,
        cache_dir / _BUNDLE_METADATA_NAME,
        cache_dir / _BUNDLE_MANIFEST_NAME,
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Incomplete cached bundle: {missing}")


def _cleanup_bundle(cache_dir: Path) -> None:
    shutil.rmtree(cache_dir, ignore_errors=True)


def _manifest_cache_key(manifest: ModelBundleManifest) -> str:
    return _cache_key(manifest.model_sha256, manifest.schema_sha256, manifest.model_version)


def _transform_prediction_outputs(
    outputs: list[Any],
    *,
    prediction_cap_seconds: float,
) -> list[Any]:
    transformed: list[Any] = []

    for output in outputs:
        arr = np.asarray(output)

        if arr.dtype.kind not in {"f", "i", "u"}:
            transformed.append(output)
            continue

        if arr.ndim == 2 and arr.shape[1] == 1:
            arr = arr.reshape(-1)

        arr = arr.astype(np.float32, copy=False)
        arr = np.expm1(arr)
        arr = np.clip(arr, 0.0, prediction_cap_seconds).astype(np.float32, copy=False)
        transformed.append(arr)

    return transformed


class PredictionTransformingSession:
    def __init__(
        self,
        raw_session: ort.InferenceSession,
        *,
        target_transform: str,
        prediction_cap_seconds: float,
    ) -> None:
        self._raw_session = raw_session
        self._target_transform = target_transform
        self._prediction_cap_seconds = prediction_cap_seconds

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw_session, name)

    def get_inputs(self):
        return self._raw_session.get_inputs()

    def get_outputs(self):
        return self._raw_session.get_outputs()

    def run(self, output_names, input_feed, run_options=None):
        outputs = self._raw_session.run(output_names, input_feed, run_options)
        if self._target_transform == TARGET_TRANSFORM:
            return _transform_prediction_outputs(
                outputs,
                prediction_cap_seconds=self._prediction_cap_seconds,
            )
        return outputs


def load_model_bundle(settings: Settings) -> tuple[Path, ModelSchema, ModelMetadata, ModelBundleManifest]:
    """
    Materialize an immutable local bundle:
      bundle/
        model.onnx
        schema.json
        metadata.json
        manifest.json
    """
    root_uri = _require_str(settings.model_uri, "settings.model_uri")
    model_src, schema_src, metadata_src, manifest_src = _bundle_source_paths(root_uri)

    src_fs, _ = fsspec.core.url_to_fs(root_uri)
    if not src_fs.exists(model_src):
        raise FileNotFoundError(f"Model artifact not found: {model_src}")
    if not src_fs.exists(schema_src):
        raise FileNotFoundError(f"schema.json is required next to the model artifact: {schema_src}")
    if not src_fs.exists(metadata_src):
        raise FileNotFoundError(f"metadata.json is required next to the model artifact: {metadata_src}")
    if not src_fs.exists(manifest_src):
        raise FileNotFoundError(f"manifest.json is required next to the model artifact: {manifest_src}")

    manifest_raw = _read_json_object_from_fs(src_fs, manifest_src, max_bytes=_MAX_SCHEMA_BYTES)
    manifest = _parse_manifest(manifest_raw)

    schema_raw = _read_json_object_from_fs(src_fs, schema_src, max_bytes=_MAX_SCHEMA_BYTES)
    metadata_raw = _read_json_object_from_fs(src_fs, metadata_src, max_bytes=_MAX_METADATA_BYTES)

    schema_canonical = _canonical_json_text(schema_raw)
    metadata_canonical = _canonical_json_text(metadata_raw)

    schema_sha256 = _sha256_text(schema_canonical)
    metadata_sha256 = _sha256_text(metadata_canonical)

    if schema_sha256 != manifest.schema_sha256:
        raise RuntimeError(
            f"schema.json checksum mismatch for {root_uri}: expected {manifest.schema_sha256}, got {schema_sha256}"
        )
    if metadata_sha256 != manifest.metadata_sha256:
        raise RuntimeError(
            f"metadata.json checksum mismatch for {root_uri}: expected {manifest.metadata_sha256}, got {metadata_sha256}"
        )

    schema = _parse_schema(schema_raw)
    metadata = _parse_metadata(metadata_raw)
    _validate_bundle_consistency(
        schema=schema,
        metadata=metadata,
        manifest=manifest,
        settings=settings,
    )

    cache_dir = _cache_dir(settings, manifest.model_sha256, schema_sha256, manifest.model_version)
    lock_dir = cache_dir.parent / f".{cache_dir.name}.lock"
    model_path = cache_dir / _BUNDLE_MODEL_NAME
    schema_path = cache_dir / _BUNDLE_SCHEMA_NAME
    metadata_path = cache_dir / _BUNDLE_METADATA_NAME
    manifest_path = cache_dir / _BUNDLE_MANIFEST_NAME

    with _acquire_lock(lock_dir):
        if cache_dir.exists():
            try:
                _validate_bundle_files(cache_dir)
                cached_manifest = _parse_manifest(_read_json_object_from_path(manifest_path))
                cached_schema_raw = _read_json_object_from_path(schema_path)
                cached_metadata_raw = _read_json_object_from_path(metadata_path)

                cached_schema_canonical = _canonical_json_text(cached_schema_raw)
                cached_metadata_canonical = _canonical_json_text(cached_metadata_raw)

                cached_schema_sha256 = _sha256_text(cached_schema_canonical)
                cached_metadata_sha256 = _sha256_text(cached_metadata_canonical)
                cached_model_sha256 = _hash_file(model_path)

                if cached_model_sha256 != cached_manifest.model_sha256:
                    raise RuntimeError(
                        f"Cached model checksum mismatch for {root_uri}: "
                        f"expected {cached_manifest.model_sha256}, got {cached_model_sha256}"
                    )
                if cached_schema_sha256 != cached_manifest.schema_sha256:
                    raise RuntimeError(
                        f"Cached schema checksum mismatch for {root_uri}: "
                        f"expected {cached_manifest.schema_sha256}, got {cached_schema_sha256}"
                    )
                if cached_metadata_sha256 != cached_manifest.metadata_sha256:
                    raise RuntimeError(
                        f"Cached metadata checksum mismatch for {root_uri}: "
                        f"expected {cached_manifest.metadata_sha256}, got {cached_metadata_sha256}"
                    )

                cached_schema = _parse_schema(cached_schema_raw)
                cached_metadata = _parse_metadata(cached_metadata_raw)
                _validate_bundle_consistency(
                    schema=cached_schema,
                    metadata=cached_metadata,
                    manifest=cached_manifest,
                    settings=settings,
                )

                logger.info(
                    "bundle.load.cache_hit",
                    extra={
                        "event": "bundle.load.cache_hit",
                        "cache_dir": str(cache_dir),
                        "model_sha256": cached_manifest.model_sha256,
                        "schema_sha256": cached_manifest.schema_sha256,
                    },
                )
                return model_path, cached_schema, cached_metadata, cached_manifest
            except Exception:
                logger.exception(
                    "bundle.load.cache_invalid",
                    extra={
                        "event": "bundle.load.cache_invalid",
                        "cache_dir": str(cache_dir),
                        "model_uri": root_uri,
                    },
                )
                _cleanup_bundle(cache_dir)

        cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "bundle.load.materialize_start",
            extra={
                "event": "bundle.load.materialize_start",
                "cache_dir": str(cache_dir),
                "model_uri": root_uri,
                "model_sha256": manifest.model_sha256,
                "schema_sha256": manifest.schema_sha256,
            },
        )

        try:
            actual_model_sha256 = _copy_with_hash(src_fs, model_src, model_path)
            if actual_model_sha256 != manifest.model_sha256:
                logger.error(
                    "bundle.load.checksum_mismatch",
                    extra={
                        "event": "bundle.load.checksum_mismatch",
                        "cache_dir": str(cache_dir),
                        "model_uri": root_uri,
                        "expected_sha256": manifest.model_sha256,
                        "actual_sha256": actual_model_sha256,
                    },
                )
                _cleanup_bundle(cache_dir)
                raise RuntimeError(
                    f"MODEL_SHA256 mismatch for {root_uri}: expected {manifest.model_sha256}, got {actual_model_sha256}"
                )

            _atomic_write_json(schema_path, schema_raw)
            _atomic_write_json(metadata_path, metadata_raw)
            _atomic_write_json(manifest_path, manifest.as_dict())

            logger.info(
                "bundle.load.materialize_complete",
                extra={
                    "event": "bundle.load.materialize_complete",
                    "cache_dir": str(cache_dir),
                    "model_sha256": manifest.model_sha256,
                    "schema_sha256": manifest.schema_sha256,
                    "metadata_present": True,
                },
            )
            return model_path, schema, metadata, manifest
        except Exception:
            logger.exception(
                "bundle.load.materialize_failed",
                extra={
                    "event": "bundle.load.materialize_failed",
                    "cache_dir": str(cache_dir),
                    "model_uri": root_uri,
                },
            )
            _cleanup_bundle(cache_dir)
            raise


def _normalize_providers(settings: Settings) -> list[Any]:
    raw_providers = list(getattr(settings, "ort_providers", None) or ["CPUExecutionProvider"])
    available = set(ort.get_available_providers())

    normalized: list[Any] = []
    for provider in raw_providers:
        if isinstance(provider, tuple):
            if len(provider) != 2:
                raise RuntimeError("Provider tuples must be of the form (name, options_dict)")
            name, options = provider
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError("Provider name must be a non-empty string")
            if not isinstance(options, dict):
                raise RuntimeError(f"Provider options for {name} must be a dict")
            if name not in available:
                raise RuntimeError(
                    f"Requested provider '{name}' is not available in this onnxruntime build. "
                    f"Available providers: {tuple(ort.get_available_providers())}"
                )
            normalized.append((name, options))
        else:
            name = str(provider).strip()
            if not name:
                raise RuntimeError("Provider name must be a non-empty string")
            if name not in available:
                raise RuntimeError(
                    f"Requested provider '{name}' is not available in this onnxruntime build. "
                    f"Available providers: {tuple(ort.get_available_providers())}"
                )
            normalized.append(name)

    return normalized


def build_onnx_session(model_path: Path, settings: Settings) -> ort.InferenceSession:
    sess_options = ort.SessionOptions()
    sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    sess_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

    intra = int(getattr(settings, "ort_intra_op_num_threads", 0) or 0)
    if intra <= 0:
        replica_cpus = float(getattr(settings, "replica_num_cpus", 1))
        intra = max(1, math.ceil(replica_cpus))

    inter = int(getattr(settings, "ort_inter_op_num_threads", 0) or 0)
    if inter <= 0:
        inter = 1

    sess_options.intra_op_num_threads = intra
    sess_options.inter_op_num_threads = inter
    sess_options.log_severity_level = int(getattr(settings, "ort_log_severity_level", 2))

    providers = _normalize_providers(settings)

    logger.info(
        "onnx.session.create_start",
        extra={
            "event": "onnx.session.create_start",
            "model_path": str(model_path),
            "providers": providers,
            "intra_op_num_threads": intra,
            "inter_op_num_threads": inter,
            "log_severity_level": sess_options.log_severity_level,
        },
    )

    try:
        session = ort.InferenceSession(
            str(model_path),
            sess_options=sess_options,
            providers=providers,
        )
    except Exception:
        logger.exception(
            "onnx.session.create_failed",
            extra={
                "event": "onnx.session.create_failed",
                "model_path": str(model_path),
                "providers": providers,
            },
        )
        raise

    logger.info(
        "onnx.session.create_complete",
        extra={
            "event": "onnx.session.create_complete",
            "model_path": str(model_path),
            "providers": providers,
            "input_count": len(session.get_inputs()),
            "output_count": len(session.get_outputs()),
        },
    )
    return session


def _resolve_session_io(
    session: ort.InferenceSession,
    schema: ModelSchema,
) -> tuple[str, tuple[str, ...]]:
    session_inputs = tuple(inp.name for inp in session.get_inputs())
    session_outputs = tuple(out.name for out in session.get_outputs())

    if not session_inputs:
        logger.error("onnx.session.no_inputs", extra={"event": "onnx.session.no_inputs"})
        raise RuntimeError("The ONNX model declares no inputs")

    if not session_outputs:
        logger.error("onnx.session.no_outputs", extra={"event": "onnx.session.no_outputs"})
        raise RuntimeError("The ONNX model declares no outputs")

    if schema.input_name not in session_inputs:
        logger.error(
            "onnx.schema.input_missing",
            extra={
                "event": "onnx.schema.input_missing",
                "schema_input_name": schema.input_name,
                "session_inputs": session_inputs,
            },
        )
        raise RuntimeError(
            f"Schema input_name '{schema.input_name}' not found in model inputs: {session_inputs}"
        )

    missing = [name for name in schema.output_names if name not in session_outputs]
    if missing:
        logger.error(
            "onnx.schema.output_missing",
            extra={
                "event": "onnx.schema.output_missing",
                "schema_output_names": schema.output_names,
                "missing_outputs": missing,
                "session_outputs": session_outputs,
            },
        )
        raise RuntimeError(
            f"Schema output_names not found in model outputs: {missing}. Available outputs: {session_outputs}"
        )

    logger.info(
        "onnx.session.io_resolved",
        extra={
            "event": "onnx.session.io_resolved",
            "input_name": schema.input_name,
            "output_names": schema.output_names,
        },
    )
    return schema.input_name, schema.output_names


def load_loaded_model(settings: Settings) -> LoadedModel:
    start = time.perf_counter()
    model_path, schema, metadata, manifest = load_model_bundle(settings)
    raw_session = build_onnx_session(model_path, settings)
    input_name, output_names = _resolve_session_io(raw_session, schema)

    prediction_cap_seconds = min(float(metadata.label_cap_seconds), MAX_PREDICTION_SECONDS)
    session = PredictionTransformingSession(
        raw_session,
        target_transform=schema.target_transform,
        prediction_cap_seconds=prediction_cap_seconds,
    )

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    logger.info(
        "model.ready",
        extra={
            "event": "model.ready",
            "model_path": str(model_path),
            "cache_dir": str(model_path.parent),
            "model_name": metadata.model_name,
            "model_version": metadata.model_version,
            "schema_version": schema.schema_version,
            "feature_version": schema.feature_version,
            "elapsed_ms": elapsed_ms,
            "input_name": input_name,
            "output_names": output_names,
            "label_cap_seconds": metadata.label_cap_seconds,
            "prediction_cap_seconds": prediction_cap_seconds,
            "manifest_model_sha256": manifest.model_sha256,
        },
    )

    return LoadedModel(
        model_path=model_path,
        cache_dir=model_path.parent,
        schema=schema,
        metadata=metadata,
        manifest=manifest,
        session=session,
        input_name=input_name,
        output_names=output_names,
    )