# Observability and Deployment Architecture

## Scope

This document summarizes the end-to-end architecture of the tabular inference service, including:

- the application layer
- the deployment/infrastructure layer
- the observability stack
- the request and rollout lifecycles

The system is built around a Ray Serve inference application deployed as a KubeRay `RayService`, with SigNoz providing centralized telemetry collection and visualization.

---

## 1. System Overview

The stack has three major parts:

1. **Application layer**
   - HTTP ingress
   - inference backend
   - model loading
   - batching
   - validation
   - request-level telemetry

2. **Infrastructure layer**
   - manifest rendering
   - Kubernetes deployment generation
   - RayService lifecycle management
   - cluster authentication and security posture

3. **Observability layer**
   - OpenTelemetry traces, metrics, and logs
   - SigNoz deployment and readiness
   - OTLP endpoint wiring from the app to the collector

The overall flow is:

```text
Client request
  -> Ray Serve ingress
  -> input validation and feature preparation
  -> Ray backend batching
  -> ONNX Runtime inference
  -> response serialization

Deployment flow
  -> environment validation
  -> Kubernetes manifest rendering
  -> kubectl apply / delete
  -> RayService rollout

Telemetry flow
  -> OpenTelemetry SDK in app
  -> OTLP exporter
  -> SigNoz collector
  -> ClickHouse storage
  -> UI and dashboards
```

---

## 2. Configuration Layer

### 2.1 App configuration

The application configuration is loaded from environment variables and normalized into a frozen `Settings` dataclass.

Key properties:

- **Single deployment profile**
  - `DEPLOYMENT_PROFILE` is fixed to `prod`
  - non-prod profiles are rejected

- **Strong validation**
  - required fields must be present
  - numeric values are range-checked
  - lists are deduplicated where required
  - invalid telemetry settings fail fast

- **Stable defaults**
  - production defaults are centralized
  - the service is configured for conservative CPU usage, bounded batching, and controlled autoscaling

### 2.2 Infra configuration

The infra deploy script also uses a frozen configuration model:

- namespace
- RayService name
- Ray image and version
- head and worker CPU/memory
- worker replica bounds
- storage paths
- non-root security settings
- AWS credential mode

The infra configuration is also validated strictly, with the same general principle: fail early and avoid implicit behavior.

---

## 3. Application Layer

## 3.1 Runtime structure

The application is implemented as two Ray Serve deployments:

- **Ingress deployment**: handles HTTP requests
- **Backend deployment**: performs model execution and batching

The deployment graph is:

```text
HTTP
  -> TabularInferenceDeployment
      -> InferenceBackendDeployment
```

This separation keeps request handling isolated from inference execution.

---

## 3.2 Ingress layer: HTTP handling

The ingress deployment is responsible for:

- request routing
- request ID assignment
- OpenTelemetry context extraction
- structured logging
- HTTP metrics
- error mapping
- response construction

### Routes

The service supports the following paths:

- `GET /`
- `GET /readyz`
- `GET /-/healthz`
- `GET /healthz`
- `POST /`
- `POST /predict`

Any other route returns `404`.

### Request context

For every request, the ingress layer:

1. normalizes the route
2. extracts or generates `X-Request-Id`
3. extracts trace context from request headers
4. increments active request and request counters
5. starts a server span for the request
6. records final latency and error metrics on completion

### Health and readiness

- `GET /healthz` returns a simple OK response
- `GET /readyz` and `GET /-/healthz` validate the backend model state and return model metadata when healthy

---

## 3.3 Predict path

The predict path is the main end-to-end inference flow.

### Step 1: Parse JSON

The ingress layer parses the request body as JSON.

Failure modes:

- invalid JSON -> `400 Bad Request`

### Step 2: Coerce and validate instances

The payload is coerced into structured instances.

Validation includes:

- at least one instance
- no more than `MAX_INSTANCES_PER_REQUEST`

Failure modes:

- semantic validation failure -> `422 Unprocessable Entity`

### Step 3: Fetch backend summary

The ingress queries the backend for model metadata and schema information.

This data is cached locally in the ingress for efficiency, but refreshes are allowed for readiness checks.

If the backend is not available:

- `503 Service Unavailable`

### Step 4: Build the feature matrix

The ingress converts user instances into a NumPy feature matrix in the exact model feature order.

The model schema controls:

- feature ordering
- whether extra features are allowed

### Step 5: Call the backend

The feature matrix is sent to the backend via Ray Serve remote invocation.

If backend execution fails:

- `500 Internal Server Error`

### Step 6: Return predictions

The response includes:

- `model_version`
- `n_instances`
- `predictions`

If the request is slow beyond the configured threshold, the service emits a slow-request event and log entry.

---

## 3.4 Backend layer: model execution

The backend deployment is responsible for:

- telemetry initialization
- model loading
- model health checks
- batched ONNX Runtime inference
- backend metrics
- output shaping

### Model loading

At startup, the backend loads a `LoadedModel` object, which provides:

- ONNX session
- schema
- metadata
- manifest

The backend validates that the session and model I/O metadata are usable before serving traffic.

### Health checks

The backend checks that:

- session exists
- input name exists
- output names exist
- feature order exists

A missing invariant is treated as a fatal readiness problem.

### Backend metadata

The backend exposes a summary containing:

- service and model identifiers
- schema and feature versions
- model URI and cached path
- feature order
- whether extra features are allowed
- the prediction cap
- manifest checksum metadata

This summary powers ingress readiness checks and lets the HTTP layer report a coherent service state.

---

## 3.5 Batched inference path

The backend uses Ray Serve batching.

### Batching behavior

- max batch size is configurable
- batch wait timeout is configurable
- the backend receives multiple feature matrices and merges them into a single ONNX invocation

### Batch validation

Each input matrix must be:

- 2D
- aligned to the configured feature count
- non-empty

Invalid shapes fail immediately.

### Inference execution

The backend:

1. concatenates the batch
2. starts an OpenTelemetry span for inference
3. calls `session.run(...)` in a worker thread
4. records request count, batch size, and latency metrics
5. splits model outputs back into per-request results

### Output shaping

The ONNX outputs are split into structured prediction dictionaries and then partitioned back into one result list per original request.

---

## 3.6 Error handling model

The application follows a strict error mapping strategy:

- invalid JSON -> `400`
- request validation errors -> `422`
- backend unavailable -> `503`
- inference failure -> `500`
- unexpected internal error -> `500`

All failures are:

- logged
- traced
- counted in metrics where appropriate

---

## 3.7 Application telemetry

The application emits:

### HTTP metrics
- request count
- error count
- latency
- active requests

### Inference metrics
- inference request count
- inference error count
- inference latency
- batch size

### Traces
- server span for each HTTP request
- input preparation span
- ONNX inference span

Trace context is propagated from incoming request headers and is also injected into logs where possible.

### Logs
- JSON formatted logs
- service metadata attached
- request ID attached
- trace and span identifiers included when available

---

## 4. Infrastructure Layer

## 4.1 Purpose

The infra deploy layer is a deterministic manifest generator and Kubernetes lifecycle manager for the Ray-based inference service.

It is responsible for:

- validating deployment settings
- generating KubeRay manifests
- handling AWS authentication mode
- producing deterministic YAML
- applying and deleting resources through `kubectl`

---

## 4.2 Configuration model

The infra layer uses two configuration groups:

### Deployment settings
These control the Kubernetes and Ray deployment shape:

- namespace
- RayService name
- service account
- image and version
- resource requests and limits
- worker replica bounds
- model cache volume configuration
- runtime user/group IDs
- IAM vs static AWS credentials

### Application environment
These are injected into the container environment:

- model URI, version, and SHA256
- model input and output names
- feature order
- Ray Serve tuning
- ONNX Runtime configuration
- OpenTelemetry configuration
- log level
- request-size thresholds

The app environment contains required placeholders that must be explicitly overridden before deployment.

---

## 4.3 Authentication modes

The deployment supports two mutually exclusive AWS access patterns:

### IAM mode
- enabled via `USE_IAM=true`
- uses a Kubernetes service account with an EKS IRSA annotation
- no static AWS credentials are injected

### Static credential mode
- enabled via `USE_IAM=false`
- requires `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- credentials are stored in a Kubernetes Secret

This is strictly validated before manifest generation.

---

## 4.4 Generated Kubernetes resources

The rollout generates:

1. **Namespace**
2. **ServiceAccount** when IAM is enabled
3. **Secret** when static AWS credentials are used
4. **RayService** containing the Ray cluster and Serve app

---

## 4.5 RayService specification

The RayService manifest defines:

- a head pod
- a worker pod group
- in-tree Ray autoscaling
- Ray Serve application configuration
- security hardening
- health checks
- shared ephemeral volumes

### Head and worker behavior

The head pod:
- runs Ray head services
- exposes Ray health checks

The worker pod:
- runs the inference workload
- validates both Ray health and application health

### Security posture

The containers are configured to:

- run as non-root
- drop all capabilities
- disallow privilege escalation
- use a read-only root filesystem

### Storage

The workload uses `emptyDir` volumes for:

- model cache
- temporary files

This keeps the deployment stateless at the Kubernetes storage layer.

---

## 4.6 Health probes

Health checks are deliberately layered.

### Head probes
- use `ray health-check`

### Worker probes
- use `ray health-check`
- verify `http://127.0.0.1:8000/healthz`
- confirm the application is healthy, not just the Ray process

This prevents a partially booted worker from becoming ready too early.

---

## 4.7 Ray Serve configuration

The embedded Serve configuration sets:

- `proxy_location: EveryNode`
- HTTP host: `0.0.0.0`
- HTTP port: `8000`
- application import path: `service:app`

This connects the Ray cluster directly to the application entrypoint.

---

## 4.8 Rollout lifecycle

The rollout flow is:

1. load and validate infra settings
2. load and validate app environment
3. build Kubernetes documents
4. render YAML deterministically
5. compute SHA256 of rendered YAML
6. apply Namespace
7. apply Secret if needed
8. apply ServiceAccount if needed
9. apply RayService
10. persist rendered state and hash

The stored state is used to support reproducible deletion and traceability.

---

## 4.9 Delete lifecycle

The delete flow is:

1. load settings
2. recover previously rendered RayService if available
3. reconstruct manifests if needed
4. delete RayService
5. delete ServiceAccount if present
6. delete Secret if present
7. delete Namespace
8. remove persisted state files

The delete path is intentionally defensive and can fall back to reconstruction if persisted state is missing.

---

## 5. Observability Layer

## 5.1 Telemetry initialization

Telemetry is initialized in both the ingress and backend processes.

The telemetry module is designed to be:

- idempotent
- thread-safe
- validated
- reusable across components

It sets up:

- tracer provider
- meter provider
- logger provider
- OpenTelemetry logging handler
- log record factory enrichment

---

## 5.2 Export transport

The service uses OTLP over gRPC.

The OTLP endpoint is validated to ensure it is compatible with gRPC configuration and not an HTTP collector endpoint.

The exporters are configured for:

- traces
- metrics
- logs

---

## 5.3 Resource attributes

Telemetry resources include:

- service name
- service version
- deployment environment
- Kubernetes cluster name
- service instance ID

These attributes allow telemetry streams to be grouped consistently in the backend observability system.

---

## 5.4 Sampling and log levels

Trace sampling is configurable with support for:

- always on
- always off
- trace id ratio
- parent-based variants

Log levels are normalized and validated, with support for standard logging levels and `WARN` aliasing to `WARNING`.

---

## 5.5 Logging integration

Logs are structured and include trace context when available.

The telemetry module:

- injects trace metadata into log records
- attaches a logging handler to the root logger
- prevents recursive OpenTelemetry logging loops
- restores the previous logging factory on shutdown

---

## 5.6 Shutdown behavior

Telemetry shutdown is orderly and idempotent:

1. remove the OTEL log handler
2. flush and close the handler
3. restore the previous log record factory
4. force flush and shut down logger, meter, and tracer providers

This reduces loss of telemetry during termination.

---

## 6. SigNoz Observability Stack

## 6.1 Purpose

SigNoz is deployed as the central observability backend for the inference service.

It provides:

- OTLP ingestion
- trace storage
- metric storage
- log storage
- UI for inspection and troubleshooting

---

## 6.2 Bootstrap script behavior

The SigNoz installer is a Bash-based rollout script that:

- validates prerequisites
- waits for DNS readiness
- checks storage class availability
- generates Helm values deterministically
- installs or upgrades the SigNoz release
- waits for workloads and services to become ready
- verifies ClickHouse health
- prints connection information
- supports forceful cleanup

---

## 6.3 Cluster profile support

The script supports two cluster profiles:

- `kind`
- `eks`

The profile drives:

- cluster name
- cloud metadata

---

## 6.4 Helm deployment

The script uses Helm to install:

- SigNoz
- internal ClickHouse
- Zookeeper
- OTLP collector components

The Helm deployment is executed with:

- version pinning
- atomic upgrades
- wait semantics
- long timeout budgets
- template validation before installation

---

## 6.5 Generated values file

The values file is generated on the fly and includes:

- global storage class
- cluster name and cloud
- ClickHouse configuration
- Zookeeper resources
- SigNoz resources
- SigNoz persistence
- inference namespace filter for logs
- security context settings

A ClickHouse password is either reused from an existing values file or generated securely.

---

## 6.6 Readiness checks

The script validates:

- DNS availability
- storage class presence
- Helm render success
- rollout completion for deployments and statefulsets
- service endpoint readiness
- ClickHouse query health

This ensures the observability stack is not only installed, but operational.

---

## 6.7 Service discovery and integration points

The script resolves service names dynamically by port and prints the key integration details:

- SigNoz UI port-forward command
- UI URL
- OTLP endpoint for application telemetry
- ClickHouse service endpoint

This is the direct integration point used by the application telemetry settings.

---

## 6.8 Cleanup behavior

Deletion is defensive and includes:

- Helm uninstall
- forced deletion of namespaced resources
- finalizer removal
- namespace finalization if needed
- generated values file cleanup

This avoids common Kubernetes termination deadlocks.

---

## 7. End-to-End Lifecycle Summary

## 7.1 Request lifecycle

```text
HTTP request
  -> ingress deployment
  -> JSON parsing
  -> instance validation
  -> backend metadata lookup
  -> feature matrix build
  -> Ray backend batching
  -> ONNX inference
  -> output splitting
  -> HTTP response
```

At each step:

- logs are emitted
- traces are annotated
- metrics are recorded
- errors are mapped explicitly

## 7.2 Deployment lifecycle

```text
environment variables
  -> validation
  -> manifest generation
  -> YAML rendering
  -> kubectl apply
  -> RayService startup
  -> readiness probes
  -> traffic handling
```

## 7.3 Observability lifecycle

```text
application telemetry
  -> OTLP exporter
  -> SigNoz collector
  -> ClickHouse storage
  -> SigNoz UI
```

---

## 8. Operational Characteristics

The system is intentionally designed around the following principles:

- deterministic configuration
- strict validation
- idempotent initialization
- explicit lifecycle control
- clear separation of application and infrastructure concerns
- hardened container security
- bounded batching and autoscaling
- full-stack observability

---

## 9. Summary

This platform is a production-oriented Ray Serve inference service with a layered design:

- the **application layer** handles HTTP requests, validation, batching, inference, and telemetry
- the **infra layer** renders and manages the RayService deployment in Kubernetes
- the **observability layer** exports traces, metrics, and logs to SigNoz through OTLP

The result is a deterministic, inspectable, and operationally controlled deployment pipeline with coherent end-to-end observability.
