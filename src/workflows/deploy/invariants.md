Here are the invariants planned, by layer.

## 1) Deployment / runtime

* **Production runs on KubeRay `RayService`**, not `serve.run(...)`.
* **The app file defines the service; Kubernetes deploys it**.
* **Ray Serve autoscaling is enabled**; fixed replica counts are only a starting point.
* **A custom production image** is preferred over runtime package setup.

## 2) Serving path

* **One FastAPI ingress** is the HTTP entrypoint.
* **One trace root per request**.
* **Ray Serve handles batching and replica scaling**, not the app code.
* **Dynamic batching is optional**, enabled only if load tests show benefit.

## 3) Model loading

* **Model URI is versioned and immutable**.
* **Model bundle is a directory root**, not just a mutable single object key.
* Bundle contents are:

  * `model.onnx`
  * `schema.json`
  * optional `metadata.json`
* **Checksum validation is required** before serving.
* **Model loads once per replica**, then is cached locally.

## 4) ONNX Runtime

* **Threading is bound to Ray replica CPU budget**.
* **Thread counts are set in code**, not by ad hoc process env vars.
* Default stance:

  * sequential execution
  * explicit intra-op / inter-op sizing
* **No uncontrolled thread oversubscription**.

## 5) Request schema

* **Feature order is fixed and explicit**.
* **Unknown or missing features are rejected**.
* **Input schema is strict**.
* **Tabular coercion is numeric and finite only**.
* **Max instances per request is bounded**.

## 6) Traces

* **FastAPI request span is the root**.
* **Model execution is wrapped in child spans**.
* **Trace context must survive across service boundaries**.
* **Ray execution should not fragment traces**.
* **Resource attributes are stable**:

  * `service.name`
  * `service.version`
  * `deployment.environment`
  * `k8s.cluster.name`
  * `service.instance.id`

## 7) Metrics

* **Minimal metrics only**:

  * request count
  * error count
  * inference latency histogram
  * active requests
  * batch size
* **Latency uses histogram, not counter**.
* **High-cardinality labels are avoided**.
* **Metrics are app-level; infra metrics live elsewhere**.

## 8) Logs

* **Logs are sparse and structured**.
* **Only important paths log**:

  * startup
  * errors
  * slow requests
* **Logs are trace-correlated**.
* **No payload dumps or noisy per-request logs**.

## 9) Telemetry pipeline

* **App exports via OTLP to the Collector**.
* **The Collector mediates batching, enrichment, and export**.
* **Self-hosted SigNoz means ClickHouse exporters, not Cloud ingestion**.
* **App telemetry and infra telemetry are separate planes**.

## 10) Infrastructure telemetry

* **K8s infra metrics come from K8s-Infra / Collector receivers**.
* **Node, pod, container, and cluster metrics are collected outside app code**.
* **Kubernetes metadata enrichment is required** for correlation.
* **Do not scrape infra in the service module**.

## 11) Environment

* **`LOG_LEVEL` controls app logs**.
* **`OTEL_LOG_LEVEL` controls OpenTelemetry internal logging**.
* **OTel env vars are minimal and deliberate**.
* **Service identity is fixed by env/resource attributes**.

## 12) File layout

* **Five Python files in one directory**:

  * `config.py`
  * `telemetry.py`
  * `model_store.py`
  * `schemas.py`
  * `service.py`
* **No monolithic serving script in production**.

## 13) Failure handling

* **Startup fails fast** if required config is missing.
* **Readiness is only true after model/session load**.
* **Artifact mismatch is a hard error**.
* **Request validation errors are explicit and bounded**.
* **Telemetry shutdown is deterministic**.

## 14) Production defaults

* **Self-hosted SigNoz is the backend**.
* **No Prometheus storage is required**.
* **K8s-Infra handles infra observability**.
* **Ray autoscaling + optional batching + strict schema + checksummed model bundle** is the production baseline.

The core invariant across everything is this:

**One request enters FastAPI, is traced end-to-end through Ray Serve and ONNX execution, emits sparse metrics and logs, and lands in SigNoz with stable identity and Kubernetes context.**
