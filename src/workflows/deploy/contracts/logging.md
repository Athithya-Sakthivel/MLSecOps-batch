# Production Logging Contract

This document defines the **strict logging standards** for a production-grade system.

---

## 1. Log Policy (Strict)

### Log Levels

- **Production:** `WARNING`
- **Development:** `DEBUG`

### Allowed Levels in Production

- `WARNING` → anomalies only  
- `ERROR` → failures  
- `CRITICAL` → severe failures  

All other levels are suppressed in production.

---

## 2. What to Log

### A. Exceptions (Mandatory)

```python
logger.exception("inference_failed")
```

### B. External / System Failures

```python
logger.error("downstream_timeout", extra={"service": "feature-store"})
```

### C. Rare Anomalies

```python
logger.warning("latency_spike", extra={"latency": latency})
```

---

## 3. What NOT to Log

* Per-request success events
* Request lifecycle steps
* Payloads (input/output)
* Debug traces in production
* Large objects or blobs

---

## 4. Log Structure (Non-Negotiable)

All logs must be **structured JSON**:

```json
{
  "timestamp": "...",
  "level": "ERROR",
  "message": "inference_failed",
  "service.name": "inference-api",
  "trace_id": "...",
  "span_id": "...",
  "attributes": {
    "model.name": "fraud"
  }
}
```

---

## 5. Trace Correlation (Required)

Each log must include:

* `trace_id`
* `span_id`

Enable via:

```bash
OTEL_PYTHON_LOG_CORRELATION=true
```

### Result

* Logs can be directly linked to traces
* Debugging becomes deterministic

---

## 6. Cardinality Control

### Allowed Fields

* `model.name`
* `service.name`
* Small enumerations (e.g., `"service": "feature-store"`)

### Forbidden Fields

* `user_id`
* `request_id`
* Emails
* Any dynamic or high-cardinality values

---

## 7. Volume Constraints

* Maximum log size: **≤ 10KB**
* Near-zero logs on success path
* Logging must scale with **failures**, not traffic volume

---

## 8. Minimal Implementation Pattern

```python
try:
    result = run_inference(data)
except Exception:
    logger.exception("inference_failed")
    raise

if latency > threshold:
    logger.warning("latency_spike", extra={"latency": latency})
```

---

## 9. Mental Model

* **0 logs** → normal request
* **1 log** → anomaly
* **>1 logs** → system issue

---

## Final Contract

```text
Sparse           → only failures and anomalies
Structured       → JSON format
Correlated       → includes trace_id and span_id
Low-cardinality  → fixed, controlled fields only
Log Level        → WARNING (prod), DEBUG (dev)
```

---

## Bottom Line

Logs are not for observing normal execution.

They exist to:

> **Explain why the system deviated from expected behavior.**

All normal execution visibility should come from traces and metrics.

