
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from json import JSONDecodeError
from typing import Any, ClassVar

import httpx
import numpy as np
from config import get_settings
from model_store import LoadedModel, load_loaded_model
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind, Status, StatusCode, get_current_span
from ray import serve
from ray.serve.handle import DeploymentHandle
from schemas import build_feature_matrix, coerce_instances, split_model_outputs
from starlette.requests import Request
from starlette.responses import JSONResponse
from telemetry import initialize_telemetry

SETTINGS = get_settings()

BACKEND_DEPLOYMENT_NAME = f"{SETTINGS.serve_deployment_name}_backend"
INGRESS_DEPLOYMENT_NAME = SETTINGS.serve_deployment_name

INGRESS_NUM_CPUS = 0.0
BACKEND_NUM_CPUS = float(SETTINGS.replica_num_cpus)

INGRESS_MAX_ONGOING_REQUESTS = max(int(SETTINGS.max_ongoing_requests), 32)
BACKEND_MAX_ONGOING_REQUESTS = max(int(SETTINGS.max_ongoing_requests), int(SETTINGS.batch_max_size))

REQUEST_ID_HEADER = "X-Request-Id"
DEFAULT_AUTH_VALIDATE_URL = "http://auth-svc.inference.svc.cluster.local:8000/me"
AUTH_VALIDATE_URL = os.getenv("AUTH_VALIDATE_URL", DEFAULT_AUTH_VALIDATE_URL).strip() or DEFAULT_AUTH_VALIDATE_URL
AUTH_TIMEOUT_SECONDS = float(os.getenv("AUTH_TIMEOUT_SECONDS", "2.0"))

logger = logging.getLogger("tabular-inference")
BASE_LOG_FIELDS: dict[str, Any] = {
    "service.name": SETTINGS.service_name,
    "service.version": SETTINGS.service_version,
    "deployment.name": SETTINGS.serve_deployment_name,
    "deployment.environment": SETTINGS.deployment_environment,
    "k8s.cluster.name": SETTINGS.cluster_name,
    "service.instance.id": SETTINGS.instance_id,
    "model.uri": SETTINGS.model_uri,
}


class JsonFormatter(logging.Formatter):
    _standard_attrs: ClassVar[set[str]] = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in self._standard_attrs or key.startswith("_") or value is None:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _resolve_log_level(level_name: str) -> int:
    level = level_name.strip().upper()
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return mapping.get(level, logging.INFO)


def configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(_resolve_log_level(SETTINGS.log_level))

    formatter = JsonFormatter()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            with suppress(Exception):
                handler.setFormatter(formatter)

    for name in ("asyncio", "uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)


configure_logging()


def _current_span() -> trace.Span | None:
    try:
        span = get_current_span()
        ctx = span.get_span_context()
        if ctx is None or not ctx.is_valid:
            return None
        return span
    except Exception:
        return None


def _span_event(event: str, **attrs: Any) -> None:
    span = _current_span()
    if span is not None:
        span.add_event(event, attributes={k: v for k, v in attrs.items() if v is not None})


def _span_error(exc: BaseException, **attrs: Any) -> None:
    span = _current_span()
    if span is not None:
        for key, value in attrs.items():
            if value is not None:
                span.set_attribute(key, value)
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))


def _log(logger_obj: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger_obj.log(level, event, extra={**BASE_LOG_FIELDS, **fields})


def _log_exception(logger_obj: logging.Logger, event: str, **fields: Any) -> None:
    logger_obj.exception(event, extra={**BASE_LOG_FIELDS, **fields})


def _json_response(payload: Any, status_code: int, request_id: str | None = None) -> JSONResponse:
    headers = {REQUEST_ID_HEADER: request_id} if request_id else None
    return JSONResponse(payload, status_code=status_code, headers=headers)


def _route_key(path: str) -> str:
    cleaned = path.rstrip("/")
    return cleaned if cleaned else "/"


def _autoscaling_config() -> dict[str, Any]:
    return {
        "min_replicas": SETTINGS.min_replicas,
        "initial_replicas": SETTINGS.initial_replicas,
        "max_replicas": SETTINGS.max_replicas,
        "target_ongoing_requests": SETTINGS.target_ongoing_requests,
        "upscale_delay_s": SETTINGS.upscale_delay_s,
        "downscale_delay_s": SETTINGS.downscale_delay_s,
    }


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
            continue
        if value != "":
            return value
    return None


def _merge_auth_payload(payload: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    merged.update(payload)
    for key in ("user", "identity", "session"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            merged.update(nested)
    return merged


def _coerce_auth_context(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("authentication response must be a JSON object")

    merged = _merge_auth_payload(payload)
    user_id = _first_non_empty(
        merged.get("user_id"),
        merged.get("id"),
        merged.get("sub"),
        merged.get("userId"),
    )
    if not user_id:
        raise ValueError("authentication response is missing a user identifier")

    return {
        "user_id": str(user_id),
        "session_id": _first_non_empty(
            merged.get("session_id"),
            merged.get("sid"),
            merged.get("sessionId"),
        ),
        "provider": _first_non_empty(merged.get("provider"), merged.get("auth_provider")),
        "email": _first_non_empty(
            merged.get("email"),
            merged.get("primary_email"),
            merged.get("user_email"),
        ),
        "name": _first_non_empty(
            merged.get("name"),
            merged.get("display_name"),
            merged.get("user_name"),
        ),
        "expires_at": _first_non_empty(
            merged.get("expires_at"),
            merged.get("session_expires_at"),
        ),
        "raw": merged,
    }


class AuthValidationError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class InferenceBackend:
    def __init__(self) -> None:
        self.settings = SETTINGS
        self.logger = logging.getLogger("tabular-inference.backend")
        self.tracer = trace.get_tracer("tabular-inference-backend")
        self.meter = metrics.get_meter("tabular-inference-backend")

        try:
            self._telemetry = initialize_telemetry(self.settings)
        except Exception as exc:
            self._telemetry = None
            _log_exception(
                self.logger,
                "telemetry_initialize_failed",
                error_type=exc.__class__.__name__,
            )

        self.inference_request_counter = self.meter.create_counter(
            name="inference.requests",
            description="Total inference requests",
            unit="1",
        )
        self.inference_error_counter = self.meter.create_counter(
            name="inference.errors",
            description="Total inference failures",
            unit="1",
        )
        self.inference_duration_histogram = self.meter.create_histogram(
            name="inference.duration",
            description="Model execution latency",
            unit="ms",
        )
        self.inference_batch_size_histogram = self.meter.create_histogram(
            name="inference.batch_size",
            description="Number of instances per inference request",
            unit="1",
        )

        self.loaded_model: LoadedModel = load_loaded_model(self.settings)
        self.session = self.loaded_model.session
        self.schema = self.loaded_model.schema
        self.metadata = self.loaded_model.metadata
        self.manifest = self.loaded_model.manifest

        self.effective_model_version = self.metadata.model_version or self.settings.model_version
        self.model_name = self.metadata.model_name or "model"
        self.input_name = self.loaded_model.input_name
        self.output_names = tuple(self.loaded_model.output_names)
        self.feature_order = tuple(self.schema.feature_order)
        self.allow_extra_features = bool(self.schema.allow_extra_features)
        self.ort_providers = tuple(self.settings.ort_providers)
        self.prediction_cap_seconds = min(float(self.metadata.label_cap_seconds), 24.0 * 3600.0)

        _log(
            self.logger,
            logging.INFO,
            "model_loaded",
            model_name=self.model_name,
            model_version=self.effective_model_version,
            schema_version=self.schema.schema_version,
            feature_version=self.schema.feature_version,
            model_path=str(self.loaded_model.model_path),
            input_name=self.input_name,
            output_names=list(self.output_names),
            feature_count=len(self.feature_order),
            ort_providers=list(self.ort_providers),
        )

    def check_health(self) -> None:
        if self.session is None:
            raise RuntimeError("model session is not initialized")
        if not self.input_name:
            raise RuntimeError("model input name is missing")
        if not self.output_names:
            raise RuntimeError("model output names are missing")
        if not self.feature_order:
            raise RuntimeError("feature order is missing")

    async def ready_summary(self) -> dict[str, Any]:
        self.check_health()
        return {
            "status": "ok",
            "service_name": self.settings.service_name,
            "model_name": self.model_name,
            "model_version": self.effective_model_version,
            "schema_version": self.schema.schema_version,
            "feature_version": self.schema.feature_version,
            "model_uri": self.settings.model_uri,
            "model_path": str(self.loaded_model.model_path),
            "cache_dir": str(self.loaded_model.cache_dir),
            "feature_order": list(self.feature_order),
            "allow_extra_features": self.allow_extra_features,
            "prediction_cap_seconds": self.prediction_cap_seconds,
            "manifest_model_sha256": self.manifest.model_sha256,
        }

    @serve.batch(
        max_batch_size=SETTINGS.batch_max_size,
        batch_wait_timeout_s=SETTINGS.batch_wait_timeout_s,
    )
    async def predict_batch(self, feature_matrices: list[np.ndarray]) -> list[list[dict[str, Any]]]:
        if not feature_matrices:
            return []

        batch_start = time.perf_counter()
        request_count = len(feature_matrices)
        feature_count = len(self.feature_order)

        matrices: list[np.ndarray] = []
        row_counts: list[int] = []

        for idx, matrix in enumerate(feature_matrices):
            arr = np.asarray(matrix, dtype=np.float32)
            if arr.ndim != 2:
                raise ValueError(f"feature matrix at index {idx} must be 2D")
            if arr.shape[1] != feature_count:
                raise ValueError(
                    f"feature matrix at index {idx} has {arr.shape[1]} features, expected {feature_count}"
                )
            if arr.shape[0] < 1:
                raise ValueError(f"feature matrix at index {idx} must contain at least one row")

            matrices.append(arr)
            row_counts.append(int(arr.shape[0]))

        batch_instance_count = sum(row_counts)
        if batch_instance_count == 0:
            return [[] for _ in feature_matrices]

        combined = np.concatenate(matrices, axis=0)

        with self.tracer.start_as_current_span("onnx_inference") as span:
            span.set_attribute("batch.request_count", request_count)
            span.set_attribute("batch.size", batch_instance_count)
            span.set_attribute("model.name", self.model_name)
            span.set_attribute("model.version", self.effective_model_version)
            span.set_attribute("onnx.input.name", self.input_name)
            span.set_attribute("onnx.provider", ",".join(self.ort_providers))

            try:
                outputs = await asyncio.to_thread(
                    self.session.run,
                    self.output_names,
                    {self.input_name: combined},
                )
            except Exception as exc:
                self.inference_error_counter.add(
                    request_count,
                    attributes={
                        "model.name": self.model_name,
                        "model.version": self.effective_model_version,
                    },
                )
                _span_error(
                    exc,
                    **{
                        "batch.request_count": request_count,
                        "batch.size": batch_instance_count,
                        "model.name": self.model_name,
                        "model.version": self.effective_model_version,
                        "error.type": exc.__class__.__name__,
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_batch_failed",
                    model_name=self.model_name,
                    model_version=self.effective_model_version,
                    batch_requests=request_count,
                    batch_size=batch_instance_count,
                    error_type=exc.__class__.__name__,
                )
                raise

        elapsed_ms = (time.perf_counter() - batch_start) * 1000.0
        self.inference_request_counter.add(
            request_count,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )
        self.inference_duration_histogram.record(
            elapsed_ms,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )
        self.inference_batch_size_histogram.record(
            batch_instance_count,
            attributes={
                "model.name": self.model_name,
                "model.version": self.effective_model_version,
            },
        )

        all_predictions = split_model_outputs(
            outputs,
            self.output_names,
            row_count=batch_instance_count,
        )

        partitioned: list[list[dict[str, Any]]] = []
        offset = 0
        for row_count in row_counts:
            partitioned.append(all_predictions[offset : offset + row_count])
            offset += row_count

        return partitioned


class TabularInferenceApp:
    def __init__(self, backend: DeploymentHandle) -> None:
        self.settings = SETTINGS
        self.backend = backend
        self.logger = logger
        self.tracer = trace.get_tracer("tabular-inference-http")
        self.meter = metrics.get_meter("tabular-inference-http")
        try:
            self._telemetry = initialize_telemetry(self.settings)
        except Exception as exc:
            self._telemetry = None
            _log_exception(
                self.logger,
                "telemetry_initialize_failed",
                error_type=exc.__class__.__name__,
            )

        self._backend_info_cache: dict[str, Any] | None = None
        self._auth_client = httpx.AsyncClient(
            timeout=httpx.Timeout(AUTH_TIMEOUT_SECONDS),
            follow_redirects=False,
        )

        self.http_request_counter = self.meter.create_counter(
            name="http.server.request_count",
            description="Total HTTP requests",
            unit="1",
        )
        self.http_error_counter = self.meter.create_counter(
            name="http.server.errors",
            description="Total failed HTTP requests",
            unit="1",
        )
        self.http_duration_histogram = self.meter.create_histogram(
            name="http.server.duration",
            description="HTTP request latency",
            unit="ms",
        )
        self.http_active_requests = self.meter.create_up_down_counter(
            name="active.requests",
            description="Number of in-flight HTTP requests",
            unit="1",
        )

    async def _get_backend_info(self, refresh: bool = False) -> dict[str, Any]:
        if self._backend_info_cache is not None and not refresh:
            return self._backend_info_cache
        raw = await self.backend.ready_summary.remote()
        if not isinstance(raw, dict):
            raise RuntimeError("backend ready_summary returned an invalid payload")
        self._backend_info_cache = raw
        return raw

    def _build_auth_headers(self, request: Request, request_id: str) -> dict[str, str]:
        headers: dict[str, str] = {
            "Accept": "application/json",
            "X-Request-Id": request_id,
        }
        cookie = request.headers.get("cookie")
        if cookie:
            headers["Cookie"] = cookie
        authorization = request.headers.get("authorization")
        if authorization:
            headers["Authorization"] = authorization
        inject(headers)
        return headers

    async def _validate_auth(self, request: Request, request_id: str) -> dict[str, Any]:
        if not request.headers.get("cookie") and not request.headers.get("authorization"):
            raise AuthValidationError(401, "Unauthorized")

        headers = self._build_auth_headers(request, request_id)
        auth_started = time.perf_counter()

        with self.tracer.start_as_current_span("auth_validation") as span:
            span.set_attribute("http.route", _route_key(request.url.path))
            span.set_attribute("http.method", request.method.upper())
            span.set_attribute("request.id", request_id)
            span.set_attribute("auth.upstream", AUTH_VALIDATE_URL)

            try:
                response = await self._auth_client.get(AUTH_VALIDATE_URL, headers=headers)
            except asyncio.CancelledError:
                raise
            except httpx.TimeoutException as exc:
                _span_error(
                    exc,
                    **{
                        "request.id": request_id,
                        "auth.upstream": AUTH_VALIDATE_URL,
                        "error.type": "timeout",
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_auth_timeout",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                )
                raise AuthValidationError(503, "Authentication service timeout") from exc
            except httpx.RequestError as exc:
                _span_error(
                    exc,
                    **{
                        "request.id": request_id,
                        "auth.upstream": AUTH_VALIDATE_URL,
                        "error.type": exc.__class__.__name__,
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_auth_unreachable",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                    error_type=exc.__class__.__name__,
                )
                raise AuthValidationError(503, "Authentication service unavailable") from exc

            elapsed_ms = (time.perf_counter() - auth_started) * 1000.0
            span.set_attribute("auth.latency_ms", round(elapsed_ms, 3))
            span.set_attribute("auth.status_code", response.status_code)

            if response.status_code in {401, 403}:
                _span_event(
                    "predict_auth_unauthorized",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                    auth_status_code=response.status_code,
                )
                raise AuthValidationError(401, "Unauthorized")

            if response.status_code < 200 or response.status_code >= 300:
                _span_error(
                    RuntimeError(f"auth upstream returned {response.status_code}"),
                    **{
                        "request.id": request_id,
                        "auth.upstream": AUTH_VALIDATE_URL,
                        "auth.status_code": response.status_code,
                        "error.type": "bad_gateway",
                    },
                )
                _log(
                    self.logger,
                    logging.WARNING,
                    "predict_auth_bad_gateway",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                    auth_status_code=response.status_code,
                )
                raise AuthValidationError(502, "Authentication service returned an unexpected response")

            try:
                payload = response.json()
            except ValueError as exc:
                _span_error(
                    exc,
                    **{
                        "request.id": request_id,
                        "auth.upstream": AUTH_VALIDATE_URL,
                        "auth.status_code": response.status_code,
                        "error.type": "invalid_json",
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_auth_invalid_json",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                )
                raise AuthValidationError(502, "Authentication service returned invalid JSON") from exc

            try:
                context = _coerce_auth_context(payload)
            except ValueError as exc:
                _span_error(
                    exc,
                    **{
                        "request.id": request_id,
                        "auth.upstream": AUTH_VALIDATE_URL,
                        "error.type": "invalid_auth_payload",
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_auth_invalid_payload",
                    request_id=request_id,
                    auth_upstream=AUTH_VALIDATE_URL,
                )
                raise AuthValidationError(502, "Authentication service returned an invalid payload") from exc

            span.set_attribute("user.id", context["user_id"])
            if context.get("provider"):
                span.set_attribute("auth.provider", context["provider"])
            if context.get("session_id"):
                span.set_attribute("auth.session_id", context["session_id"])

            return context

    async def _handle_ready(self, request_id: str) -> JSONResponse:
        try:
            info = await self._get_backend_info(refresh=True)
        except Exception as exc:
            return _json_response({"detail": f"Backend unavailable: {exc}"}, 503, request_id=request_id)

        payload = {
            "status": "ok",
            "service_name": self.settings.service_name,
            "service_version": self.settings.service_version,
            "deployment": self.settings.serve_deployment_name,
            "model_name": info["model_name"],
            "model_version": info["model_version"],
            "schema_version": info["schema_version"],
            "feature_version": info["feature_version"],
            "model_uri": self.settings.model_uri if "model_uri" not in info else info["model_uri"],
            "model_path": info["model_path"],
            "feature_order": info["feature_order"],
            "allow_extra_features": info["allow_extra_features"],
            "prediction_cap_seconds": info["prediction_cap_seconds"],
        }
        return _json_response(payload, 200, request_id=request_id)

    async def _handle_predict(self, request: Request, request_id: str, request_start: float) -> JSONResponse:
        try:
            auth_context = await self._validate_auth(request, request_id)
        except AuthValidationError as exc:
            return _json_response({"detail": exc.detail}, exc.status_code, request_id=request_id)

        try:
            payload = await request.json()
        except (JSONDecodeError, ValueError) as exc:
            _span_event(
                "predict_invalid_json",
                **{
                    "http.route": "/predict",
                    "request.id": request_id,
                    "error.type": exc.__class__.__name__,
                    "user.id": auth_context["user_id"],
                },
            )
            _log(
                self.logger,
                logging.WARNING,
                "predict_invalid_json",
                route="/predict",
                request_id=request_id,
                error_type=exc.__class__.__name__,
                user_id=auth_context["user_id"],
            )
            return _json_response({"detail": "Invalid JSON body"}, 400, request_id=request_id)

        try:
            rows = coerce_instances(payload)
            n_instances = len(rows)

            if n_instances < 1:
                raise ValueError("At least one instance is required")
            if n_instances > self.settings.max_instances_per_request:
                raise ValueError(f"Too many instances: {n_instances} > {self.settings.max_instances_per_request}")

            try:
                backend_info = await self._get_backend_info(refresh=False)
            except Exception as exc:
                _span_event(
                    "backend_unavailable",
                    **{
                        "http.route": "/predict",
                        "request.id": request_id,
                        "error.type": exc.__class__.__name__,
                        "user.id": auth_context["user_id"],
                    },
                )
                return _json_response({"detail": f"Backend unavailable: {exc}"}, 503, request_id=request_id)

            with self.tracer.start_as_current_span("prepare_input") as span:
                span.set_attribute("batch.size", n_instances)
                span.set_attribute("model.name", backend_info["model_name"])
                span.set_attribute("model.version", backend_info["model_version"])
                span.set_attribute("feature.count", len(backend_info["feature_order"]))
                span.set_attribute("request.id", request_id)
                span.set_attribute("user.id", auth_context["user_id"])

            feature_matrix = build_feature_matrix(
                rows,
                backend_info["feature_order"],
                allow_extra_features=bool(backend_info["allow_extra_features"]),
            )

            try:
                predictions = await self.backend.predict_batch.remote(feature_matrix)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _span_error(
                    exc,
                    **{
                        "http.route": "/predict",
                        "request.id": request_id,
                        "model.name": backend_info["model_name"],
                        "model.version": backend_info["model_version"],
                        "error.type": exc.__class__.__name__,
                        "user.id": auth_context["user_id"],
                    },
                )
                _log_exception(
                    self.logger,
                    "predict_backend_failed",
                    route="/predict",
                    request_id=request_id,
                    model_name=backend_info["model_name"],
                    model_version=backend_info["model_version"],
                    error_type=exc.__class__.__name__,
                    user_id=auth_context["user_id"],
                )
                return _json_response({"detail": "Inference failed"}, 500, request_id=request_id)

            total_ms = (time.perf_counter() - request_start) * 1000.0
            if total_ms >= self.settings.slow_request_ms:
                _span_event(
                    "predict_slow_request",
                    **{
                        "http.route": "/predict",
                        "request.id": request_id,
                        "model.name": backend_info["model_name"],
                        "model.version": backend_info["model_version"],
                        "batch.size": n_instances,
                        "latency_ms": round(total_ms, 3),
                        "slow_request_threshold_ms": self.settings.slow_request_ms,
                        "user.id": auth_context["user_id"],
                    },
                )
                _log(
                    self.logger,
                    logging.WARNING,
                    "predict_slow_request",
                    route="/predict",
                    request_id=request_id,
                    model_name=backend_info["model_name"],
                    model_version=backend_info["model_version"],
                    n_instances=n_instances,
                    latency_ms=round(total_ms, 3),
                    slow_request_threshold_ms=self.settings.slow_request_ms,
                    user_id=auth_context["user_id"],
                )

            return _json_response(
                {
                    "model_version": backend_info["model_version"],
                    "n_instances": n_instances,
                    "predictions": predictions,
                },
                200,
                request_id=request_id,
            )

        except ValueError as exc:
            backend_info: dict[str, Any] | None = None
            with suppress(Exception):
                backend_info = await self._get_backend_info(refresh=False)

            model_name = backend_info["model_name"] if backend_info else "model"
            model_version = backend_info["model_version"] if backend_info else self.settings.model_version

            _span_event(
                "predict_validation_failed",
                **{
                    "http.route": "/predict",
                    "request.id": request_id,
                    "model.name": model_name,
                    "model.version": model_version,
                    "error.type": "validation_error",
                    "error.message": str(exc),
                    "user.id": auth_context["user_id"],
                },
            )
            _log(
                self.logger,
                logging.WARNING,
                "predict_validation_failed",
                route="/predict",
                request_id=request_id,
                model_name=model_name,
                model_version=model_version,
                error_type="validation_error",
                error_message=str(exc),
                user_id=auth_context["user_id"],
            )
            return _json_response({"detail": str(exc)}, 422, request_id=request_id)

        except Exception as exc:
            _span_error(
                exc,
                **{
                    "http.route": "/predict",
                    "request.id": request_id,
                    "error.type": exc.__class__.__name__,
                    "user.id": auth_context["user_id"],
                },
            )
            _log_exception(
                self.logger,
                "predict_internal_failure",
                route="/predict",
                request_id=request_id,
                error_type=exc.__class__.__name__,
                user_id=auth_context["user_id"],
            )
            return _json_response({"detail": "Inference failed"}, 500, request_id=request_id)

    async def __call__(self, request: Request) -> JSONResponse:
        path = _route_key(request.url.path)
        method = request.method.upper()
        request_id = (request.headers.get("x-request-id") or uuid.uuid4().hex).strip()
        request_start = time.perf_counter()
        status_code = 500

        ctx = extract(dict(request.headers))
        self.http_active_requests.add(1, attributes={"route": path})
        self.http_request_counter.add(1, attributes={"route": path, "method": method})

        with self.tracer.start_as_current_span(
            f"HTTP {method} {path}",
            context=ctx,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.route", path)
            span.set_attribute("http.request_id", request_id)
            span.set_attribute("service.name", self.settings.service_name)
            span.set_attribute("service.version", self.settings.service_version)
            span.set_attribute("deployment.environment", self.settings.deployment_environment)
            span.set_attribute("k8s.cluster.name", self.settings.cluster_name)
            span.set_attribute("service.instance.id", self.settings.instance_id)

            try:
                if method in {"GET", "HEAD"} and path in {"/", "/readyz", "/-/healthz"}:
                    response = await self._handle_ready(request_id=request_id)
                elif method in {"GET", "HEAD"} and path == "/healthz":
                    response = _json_response({"status": "ok"}, 200, request_id=request_id)
                elif method == "POST" and path in {"/", "/predict"}:
                    response = await self._handle_predict(request, request_id, request_start)
                else:
                    response = _json_response({"detail": "Not found"}, 404, request_id=request_id)

                status_code = response.status_code
                span.set_attribute("http.status_code", status_code)
                if status_code >= 500:
                    span.set_status(Status(StatusCode.ERROR))
                return response

            except asyncio.CancelledError:
                span.set_status(Status(StatusCode.ERROR))
                raise

            except Exception as exc:
                status_code = 500
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                _log_exception(
                    self.logger,
                    "request_failed",
                    route=path,
                    request_id=request_id,
                    error_type=exc.__class__.__name__,
                )
                return _json_response({"detail": "Internal server error"}, 500, request_id=request_id)

            finally:
                elapsed_ms = (time.perf_counter() - request_start) * 1000.0
                self.http_duration_histogram.record(
                    elapsed_ms,
                    attributes={
                        "route": path,
                        "method": method,
                        "status_code": status_code,
                    },
                )
                if status_code >= 400:
                    self.http_error_counter.add(
                        1,
                        attributes={
                            "route": path,
                            "method": method,
                            "status_code": status_code,
                        },
                    )
                self.http_active_requests.add(-1, attributes={"route": path})


@serve.deployment(
    name=BACKEND_DEPLOYMENT_NAME,
    num_replicas="auto",
    autoscaling_config=_autoscaling_config(),
    ray_actor_options={"num_cpus": BACKEND_NUM_CPUS},
    max_ongoing_requests=BACKEND_MAX_ONGOING_REQUESTS,
    health_check_period_s=SETTINGS.serve_health_check_period_s,
    health_check_timeout_s=SETTINGS.serve_health_check_timeout_s,
    graceful_shutdown_wait_loop_s=SETTINGS.serve_graceful_shutdown_wait_loop_s,
    graceful_shutdown_timeout_s=SETTINGS.serve_graceful_shutdown_timeout_s,
)
class InferenceBackendDeployment(InferenceBackend):
    pass


@serve.deployment(
    name=INGRESS_DEPLOYMENT_NAME,
    num_replicas=1,
    ray_actor_options={"num_cpus": INGRESS_NUM_CPUS},
    max_ongoing_requests=INGRESS_MAX_ONGOING_REQUESTS,
    health_check_period_s=SETTINGS.serve_health_check_period_s,
    health_check_timeout_s=SETTINGS.serve_health_check_timeout_s,
    graceful_shutdown_wait_loop_s=SETTINGS.serve_graceful_shutdown_wait_loop_s,
    graceful_shutdown_timeout_s=SETTINGS.serve_graceful_shutdown_timeout_s,
)
class TabularInferenceDeployment(TabularInferenceApp):
    pass


app = TabularInferenceDeployment.bind(InferenceBackendDeployment.bind())

if __name__ == "__main__":
    serve.run(app, route_prefix="/")
