# Production Metrics Contract

This document defines the **minimal, production-grade metrics standard** for an inference service (FastAPI + Ray Serve).

---

## 1. Principles (Strict)

- Metrics are for **system health**, not debugging
- Must be **low-cardinality**
- Must be **aggregatable**
- Must be **stable over time**

### Non-goals

- No per-request visibility  
- No user-level dimensions  
- No dynamic labels  

---

## 2. Required Metrics

## 2.1 HTTP / Request Layer

| Name                        | Type      | Description           |
|-----------------------------|-----------|------------------------|
| `http.server.duration`      | Histogram | Request latency        |
| `http.server.request_count` | Counter   | Total requests         |
| `http.server.errors`        | Counter   | Total failed requests  |

---

## 2.2 Inference Layer

| Name                 | Type      | Description            |
|----------------------|-----------|------------------------|
| `inference.duration` | Histogram | Model execution latency|
| `inference.requests` | Counter   | Inference calls        |
| `inference.errors`   | Counter   | Inference failures     |

---

## 2.3 Optional (Use Only If Needed)

- `inflight.requests` → current concurrency  
- `queue.depth` → Ray backpressure  

Do not add unless required for operational decisions.

---

## 3. Labels (Strict)

### Allowed Labels

- `service.name`
- `http.method`
- `http.status_code`
- `model.name`

### Forbidden Labels

- `user_id`
- `request_id`
- `session_id`
- any dynamic or high-cardinality value

---

## 4. Histogram Configuration

### Buckets (Required)

```text
[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
```

### Rationale

* Matches typical inference latency ranges
* Enables accurate p50 / p95 / p99
* Avoids unnecessary storage overhead

---

## 5. Metric Semantics

### Counters

* Monotonic increasing
* Reset only on restart

Examples:

* request count
* error count

---

### Histograms

* Used for latency distributions
* Required for SLOs (p50 / p95 / p99)

---

## 6. Minimal Instrumentation Pattern

```python
start = time.time()

try:
    result = run_inference(data)
    inference_requests.add(1, {"model.name": "fraud"})
except Exception:
    inference_errors.add(1, {"model.name": "fraud"})
    raise
finally:
    duration = time.time() - start
    inference_duration.record(duration, {"model.name": "fraud"})
```

---

## 7. Cardinality Constraints (Critical)

### Hard Rules

* Total label combinations must remain **bounded**
* No unbounded dimensions
* No per-request labeling

### Failure Mode

High cardinality leads to:

* memory blowups
* slow queries
* unusable dashboards

---

## 8. Volume Expectations

* Metrics scale with **aggregation**, not requests
* Safe at high throughput
* Low storage cost relative to logs

---

## 9. Validation Checklist

### Required

* [ ] Latency percentiles (p50, p95) visible
* [ ] Error rate visible
* [ ] Request throughput visible
* [ ] No cardinality explosion

---

## Final Contract

```text
Purpose        → system health
Types          → counters + histograms only
Labels         → fixed, low-cardinality
Histograms     → required for latency
Volume         → aggregation-based (not per request)
```

---

## Bottom Line

Metrics answer:

> **Is the system healthy and within expected bounds?**

They do not explain *why* — that is the role of traces and logs.
