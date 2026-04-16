// src/frontend/app.js

(() => {
  const APP_CONFIG = window.__APP_CONFIG__ ?? {};

  function deriveRootDomain(hostname) {
    const parts = hostname.split(".");
    if (parts.length >= 3 && ["app", "auth", "predict"].includes(parts[0])) {
      return parts.slice(1).join(".");
    }
    return hostname;
  }

  const ROOT_DOMAIN = APP_CONFIG.rootDomain ?? deriveRootDomain(window.location.hostname);
  const AUTH_BASE = APP_CONFIG.authBase ?? `https://auth.api.${ROOT_DOMAIN}`;
  const PREDICT_BASE = APP_CONFIG.predictBase ?? `https://predict.api.${ROOT_DOMAIN}`;
  const REQUEST_ID_HEADER = "X-Request-Id";

  const state = {
    user: null,
    ready: null,
    mode: "form",
    instances: [makeEmptyInstance()],
    loadingReady: false,
    lastRequestId: null,
    lastResponse: null,
    lastRawRequest: null,
  };

  const el = {};

  function byId(id) {
    const node = document.getElementById(id);
    if (!node) throw new Error(`Missing element: ${id}`);
    return node;
  }

  function escapeHtml(text) {
    return String(text)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatLabel(name) {
    return String(name)
      .replaceAll("_", " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());
  }

  function makeEmptyInstance() {
    return {};
  }

  function setText(node, value) {
    node.textContent = value == null || value === "" ? "—" : String(value);
  }

  function setPill(node, text, variant) {
    node.textContent = text;
    node.className = `pill ${variant}`;
  }

  function safeJsonParse(text) {
    return JSON.parse(text);
  }

  function toPrettyJson(value) {
    if (typeof value === "string") return value;
    return JSON.stringify(value, null, 2);
  }

  function featureOrder() {
    return Array.isArray(state.ready?.feature_order) ? state.ready.feature_order : [];
  }

  function samplePayload() {
    const features = featureOrder();
    const instance = {};
    for (const feature of features) {
      instance[feature] = 0;
    }
    return {
      instances: [instance],
    };
  }

  function setMode(mode) {
    state.mode = mode;

    const formMode = el.formMode;
    const jsonMode = el.jsonMode;
    const formBtn = el.modeFormBtn;
    const jsonBtn = el.modeJsonBtn;

    const active = "mode-btn mode-btn-active";
    const inactive = "mode-btn";

    if (mode === "form") {
      formMode.classList.remove("hidden");
      jsonMode.classList.add("hidden");
      formBtn.className = active;
      jsonBtn.className = inactive;
    } else {
      formMode.classList.add("hidden");
      jsonMode.classList.remove("hidden");
      formBtn.className = inactive;
      jsonBtn.className = active;
    }
  }

  function showError(message) {
    if (!message) {
      el.errorBanner.classList.add("hidden");
      el.errorBanner.textContent = "";
      return;
    }
    el.errorBanner.textContent = message;
    el.errorBanner.classList.remove("hidden");
  }

  function renderSession(user) {
    if (!user) {
      setPill(el.sessionPill, "Session: signed out", "pill-neutral");
      el.logoutBtn.classList.add("hidden");
      el.userPanel.classList.add("hidden");
      return;
    }

    const name = user.name || user.email || user.sub || "Signed in";
    const provider = user.provider || user.iss || "provider";
    setPill(el.sessionPill, "Session: active", "pill-good");
    el.logoutBtn.classList.remove("hidden");
    el.userPanel.classList.remove("hidden");
    setText(el.userName, name);
    setText(el.userProvider, provider);
    setText(
      el.userMeta,
      [
        user.email ? `Email: ${user.email}` : null,
        user.sub ? `Sub: ${user.sub}` : null,
        user.tenant_id ? `Tenant: ${user.tenant_id}` : null,
      ]
        .filter(Boolean)
        .join(" • "),
    );
  }

  function renderReady(info) {
    if (!info) {
      setPill(el.readyPill, "Model: unavailable", "pill-bad");
      setText(el.readyStatus, "Unavailable");
      return;
    }

    setPill(
      el.readyPill,
      `${info.model_name || "model"} · ${info.model_version || "unknown"}`,
      "pill-info",
    );
    setText(el.readyStatus, info.status || "ok");
    setText(el.readyModel, info.model_name);
    setText(el.readyModelVersion, info.model_version);
    setText(el.readySchemaVersion, info.schema_version);
    setText(el.readyFeatureVersion, info.feature_version);
    setText(el.readyCap, `${info.prediction_cap_seconds ?? "—"} s`);

    el.featureOrder.innerHTML = "";
    for (const feature of featureOrder()) {
      const span = document.createElement("span");
      span.className = "pill pill-neutral";
      span.textContent = feature;
      el.featureOrder.appendChild(span);
    }
  }

  function renderInstanceCards() {
    const features = featureOrder();
    if (!features.length) {
      el.instancesContainer.innerHTML = `
        <div class="rounded-2xl border border-slate-800 bg-slate-900/60 p-4 text-sm text-slate-300">
          Waiting for <code class="mono">/readyz</code> to publish the feature schema.
        </div>
      `;
      return;
    }

    const cards = state.instances.map((instance, index) => {
      const fields = features
        .map((feature) => {
          const value = instance[feature] ?? 0;
          return `
            <label class="block">
              <span class="mb-1 block text-xs font-medium tracking-[0.12em] text-slate-400">
                ${escapeHtml(formatLabel(feature))}
              </span>
              <input
                type="number"
                step="any"
                inputmode="decimal"
                data-instance-index="${index}"
                data-feature="${escapeHtml(feature)}"
                value="${escapeHtml(value)}"
                class="input-area w-full py-2.5"
              />
            </label>
          `;
        })
        .join("");

      return `
        <section class="rounded-3xl border border-slate-800 bg-slate-900/70 p-4">
          <div class="mb-4 flex items-center justify-between gap-3">
            <div>
              <div class="text-sm uppercase tracking-[0.18em] text-slate-500">Instance ${index + 1}</div>
              <div class="text-sm text-slate-300">One row in <code class="mono">instances</code></div>
            </div>
            ${
              state.instances.length > 1
                ? `<button type="button" class="btn btn-secondary" data-remove-instance="${index}">Remove</button>`
                : ""
            }
          </div>
          <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-3">${fields}</div>
        </section>
      `;
    });

    el.instancesContainer.innerHTML = cards.join("");

    el.instancesContainer.querySelectorAll("[data-remove-instance]").forEach((button) => {
      button.addEventListener("click", () => {
        const index = Number(button.getAttribute("data-remove-instance"));
        state.instances.splice(index, 1);
        if (state.instances.length === 0) state.instances.push(makeEmptyInstance());
        renderInstanceCards();
      });
    });

    el.instancesContainer.querySelectorAll("input[data-instance-index]").forEach((input) => {
      input.addEventListener("input", () => {
        const index = Number(input.getAttribute("data-instance-index"));
        const feature = input.getAttribute("data-feature");
        const raw = input.value;
        state.instances[index][feature] = raw === "" ? 0 : Number(raw);
      });
    });
  }

  function collectInstancesFromForm() {
    const features = featureOrder();
    return state.instances.map((_, index) => {
      const obj = {};
      for (const feature of features) {
        const input = el.instancesContainer.querySelector(
          `input[data-instance-index="${index}"][data-feature="${CSS.escape(feature)}"]`,
        );
        const raw = input?.value ?? "0";
        obj[feature] = raw === "" ? 0 : Number(raw);
      }
      return obj;
    });
  }

  function setBusy(busy) {
    for (const button of [el.predictBtnForm, el.predictBtnJson, el.refreshReadyBtn, el.logoutBtn]) {
      if (button) button.disabled = busy;
    }
    el.addInstanceBtn.disabled = busy;
    el.loadSampleBtn.disabled = busy;
  }

  function renderDebug({ requestId, status, latencyMs, requestBody, responseBody, responseHeaders }) {
    setText(el.httpStatus, status);
    setText(el.requestId, requestId);
    setText(el.latency, `${latencyMs.toFixed(2)} ms`);
    setText(el.instanceCount, Array.isArray(requestBody?.instances) ? requestBody.instances.length : "—");

    el.rawRequest.textContent = typeof requestBody === "string" ? requestBody : toPrettyJson(requestBody);
    el.rawResponse.textContent = typeof responseBody === "string" ? responseBody : toPrettyJson(responseBody);

    state.lastRequestId = requestId;
    state.lastResponse = responseBody;
    state.lastRawRequest = requestBody;
    state.lastResponseHeaders = responseHeaders;
  }

  async function loadSession() {
    try {
      const res = await fetch(`${AUTH_BASE}/me`, {
        method: "GET",
        credentials: "include",
        mode: "cors",
      });

      if (!res.ok) {
        state.user = null;
        renderSession(null);
        return;
      }

      state.user = await res.json();
      renderSession(state.user);
    } catch {
      state.user = null;
      renderSession(null);
    }
  }

  async function loadReady() {
    if (state.loadingReady) return;
    state.loadingReady = true;
    showError("");

    try {
      const res = await fetch(`${PREDICT_BASE}/readyz`, {
        method: "GET",
        credentials: "include",
        mode: "cors",
      });

      const data = res.ok ? await res.json() : null;
      state.ready = data;

      if (data) {
        renderReady(data);
        if (!featureOrder().length) {
          state.instances = [makeEmptyInstance()];
        }
        renderInstanceCards();
      } else {
        setPill(el.readyPill, "Model: unavailable", "pill-bad");
        setText(el.readyStatus, "Unavailable");
      }
    } catch (err) {
      state.ready = null;
      setPill(el.readyPill, "Model: unavailable", "pill-bad");
      setText(el.readyStatus, "Unavailable");
      showError(`Failed to load readiness: ${err?.message ?? err}`);
    } finally {
      state.loadingReady = false;
    }
  }

  async function login(provider) {
    const next = `${window.location.pathname}${window.location.search}`;
    window.location.href = `${AUTH_BASE}/login?provider=${encodeURIComponent(provider)}&next=${encodeURIComponent(next)}`;
  }

  async function logout() {
    try {
      await fetch(`${AUTH_BASE}/logout`, {
        method: "POST",
        credentials: "include",
        mode: "cors",
      });
    } catch {
      // Ignore network failures; the local session state is cleared below.
    }

    state.user = null;
    renderSession(null);
    showError("");
    await loadSession();
  }

  function buildJsonFromUi() {
    return {
      instances: collectInstancesFromForm(),
    };
  }

  async function runPredict(payload) {
    showError("");
    const requestId = crypto.randomUUID();
    const startedAt = performance.now();

    setBusy(true);
    setText(el.predictionOutput, "Running prediction...");
    setText(el.rawRequest, toPrettyJson(payload));
    setText(el.rawResponse, "Waiting for backend response...");

    try {
      const res = await fetch(`${PREDICT_BASE}/predict`, {
        method: "POST",
        credentials: "include",
        mode: "cors",
        headers: {
          "Content-Type": "application/json",
          [REQUEST_ID_HEADER]: requestId,
        },
        body: JSON.stringify(payload),
      });

      const latencyMs = performance.now() - startedAt;
      const responseText = await res.text();

      let responseBody;
      try {
        responseBody = responseText ? JSON.parse(responseText) : {};
      } catch {
        responseBody = responseText;
      }

      const responseRequestId = res.headers.get("x-request-id") || requestId;

      renderDebug({
        requestId: responseRequestId,
        status: res.status,
        latencyMs,
        requestBody: payload,
        responseBody,
        responseHeaders: Object.fromEntries(res.headers.entries()),
      });

      if (!res.ok) {
        const detail =
          typeof responseBody === "object" && responseBody
            ? responseBody.detail || responseBody.message || JSON.stringify(responseBody)
            : String(responseBody);

        setText(el.predictionOutput, `HTTP ${res.status}\n${detail}`);
        showError(`Request failed: ${detail}`);
        return;
      }

      setText(el.predictionOutput, toPrettyJson(responseBody));
    } catch (err) {
      const latencyMs = performance.now() - startedAt;
      renderDebug({
        requestId,
        status: 0,
        latencyMs,
        requestBody: payload,
        responseBody: { error: String(err) },
        responseHeaders: {},
      });
      setText(el.predictionOutput, `Request failed: ${err?.message ?? err}`);
      showError(`Request failed: ${err?.message ?? err}`);
    } finally {
      setBusy(false);
    }
  }

  function bindEvents() {
    document.querySelectorAll("[data-provider]").forEach((button) => {
      button.addEventListener("click", () => login(button.getAttribute("data-provider")));
    });

    el.logoutBtn.addEventListener("click", logout);
    el.refreshReadyBtn.addEventListener("click", async () => {
      await loadReady();
    });

    el.modeFormBtn.addEventListener("click", () => setMode("form"));
    el.modeJsonBtn.addEventListener("click", () => setMode("json"));

    el.addInstanceBtn.addEventListener("click", () => {
      state.instances.push(makeEmptyInstance());
      renderInstanceCards();
    });

    el.loadSampleBtn.addEventListener("click", () => {
      const sample = samplePayload();
      el.jsonInput.value = JSON.stringify(sample, null, 2);
    });

    el.predictBtnForm.addEventListener("click", async () => {
      const payload = buildJsonFromUi();
      await runPredict(payload);
    });

    el.predictBtnJson.addEventListener("click", async () => {
      let payload;
      try {
        payload = safeJsonParse(el.jsonInput.value);
      } catch (err) {
        showError(`Invalid JSON: ${err?.message ?? err}`);
        return;
      }
      await runPredict(payload);
    });

    el.copyResponseBtn.addEventListener("click", async () => {
      const text = el.predictionOutput.textContent || "";
      try {
        await navigator.clipboard.writeText(text);
      } catch {
        showError("Clipboard copy failed.");
      }
    });
  }

  function initializeDomRefs() {
    el.sessionPill = byId("session-pill");
    el.readyPill = byId("ready-pill");
    el.logoutBtn = byId("logout-btn");

    el.authPanel = byId("auth-panel");
    el.userPanel = byId("user-panel");
    el.userName = byId("user-name");
    el.userProvider = byId("user-provider");
    el.userMeta = byId("user-meta");

    el.refreshReadyBtn = byId("refresh-ready-btn");
    el.readyStatus = byId("ready-status");
    el.readyModel = byId("ready-model");
    el.readyModelVersion = byId("ready-model-version");
    el.readySchemaVersion = byId("ready-schema-version");
    el.readyFeatureVersion = byId("ready-feature-version");
    el.readyCap = byId("ready-cap");
    el.featureOrder = byId("feature-order");

    el.modeFormBtn = byId("mode-form-btn");
    el.modeJsonBtn = byId("mode-json-btn");
    el.formMode = byId("form-mode");
    el.jsonMode = byId("json-mode");

    el.instancesContainer = byId("instances-container");
    el.addInstanceBtn = byId("add-instance-btn");
    el.predictBtnForm = byId("predict-btn-form");
    el.predictBtnJson = byId("predict-btn-json");
    el.loadSampleBtn = byId("load-sample-btn");
    el.jsonInput = byId("json-input");

    el.httpStatus = byId("http-status");
    el.requestId = byId("request-id");
    el.latency = byId("latency");
    el.instanceCount = byId("instance-count");
    el.predictionOutput = byId("prediction-output");
    el.rawRequest = byId("raw-request");
    el.rawResponse = byId("raw-response");
    el.copyResponseBtn = byId("copy-response-btn");
    el.errorBanner = byId("error-banner");
  }

  async function init() {
    initializeDomRefs();
    bindEvents();

    setMode("form");
    setPill(el.sessionPill, "Session: checking…", "pill-neutral");
    setPill(el.readyPill, "Model: loading…", "pill-neutral");
    renderInstanceCards();
    await loadSession();
    await loadReady();

    if (!el.jsonInput.value.trim()) {
      el.jsonInput.value = JSON.stringify(samplePayload(), null, 2);
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();