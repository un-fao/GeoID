// processes/processes_browser.js — Processes Browser page with OGC execute support.
//
// Extends the read-only Processes/Jobs tabs with a "Run" panel that:
//  - Lets the user pick a scope (Platform | Catalog | Collection)
//  - Fetches scoped processes, renders the selected process description
//  - Auto-generates an input form from the OGC Process `inputs[name].schema`
//    using the shared schema-form.js engine
//  - POSTs to the scope-appropriate OGC execution endpoint with
//    Prefer: respond-async or respond-sync
//  - Polls job status for async (201), shows result for sync (200)
//  - Surfaces 403/4xx errors clearly
//
// Auth: api.js attaches the Bearer token; server-side IamMiddleware enforces authz.

import { getJSON, authHeader } from "../static/common/api.js";
import { apiUrl } from "../static/common/url.js";
import { mountSchemaForm } from "../static/common/schema-form.js";
import { register, t, lang } from "../static/common/i18n.js";

register({
  en: {
    "proc.loading": "Loading…",
    "proc.none": "Nothing to show",
    "proc.error": "Failed to load",
    "proc.run.no_process": "Select a process to run it.",
    "proc.run.submit": "Execute",
    "proc.run.submitting": "Executing…",
    "proc.run.polling": "Job running…",
    "proc.run.success": "Completed",
    "proc.run.failed": "Failed",
    "proc.run.forbidden": "You do not have permission to execute this process on the selected target.",
    "proc.run.validation": "Validation error from server:",
    "proc.run.error": "Execution error:",
  },
  fr: {
    "proc.loading": "Chargement…",
    "proc.none": "Rien à afficher",
    "proc.error": "Échec du chargement",
    "proc.run.no_process": "Sélectionnez un processus pour l'exécuter.",
    "proc.run.submit": "Exécuter",
    "proc.run.submitting": "Exécution…",
    "proc.run.polling": "Tâche en cours…",
    "proc.run.success": "Terminé",
    "proc.run.failed": "Échec",
    "proc.run.forbidden": "Vous n'avez pas la permission d'exécuter ce processus sur la cible sélectionnée.",
    "proc.run.validation": "Erreur de validation du serveur :",
    "proc.run.error": "Erreur d'exécution :",
  },
  es: {
    "proc.loading": "Cargando…",
    "proc.none": "Nada que mostrar",
    "proc.error": "Error al cargar",
    "proc.run.no_process": "Seleccione un proceso para ejecutarlo.",
    "proc.run.submit": "Ejecutar",
    "proc.run.submitting": "Ejecutando…",
    "proc.run.polling": "Trabajo en ejecución…",
    "proc.run.success": "Completado",
    "proc.run.failed": "Fallido",
    "proc.run.forbidden": "No tiene permiso para ejecutar este proceso en el destino seleccionado.",
    "proc.run.validation": "Error de validación del servidor:",
    "proc.run.error": "Error de ejecución:",
  },
});

// --- DOM refs ---
const navEl = document.getElementById("nav-list");
const bodyEl = document.getElementById("detail-body");
const tabProcesses = document.getElementById("tab-processes");
const tabJobs = document.getElementById("tab-jobs");

// --- Tab state ---
function setActive(tab) {
  for (const b of [tabProcesses, tabJobs]) b.classList.toggle("active", b === tab);
}

// --- Generic list renderer ---
function renderList(rows, labelOf, onClick) {
  navEl.replaceChildren();
  if (!rows || rows.length === 0) {
    const p = document.createElement("p");
    p.textContent = t("proc.none");
    navEl.appendChild(p);
    return;
  }
  const ul = document.createElement("ul");
  for (const r of rows) {
    const li = document.createElement("li");
    const b = document.createElement("button");
    b.textContent = labelOf(r);
    b.addEventListener("click", () => onClick(r));
    li.appendChild(b);
    ul.appendChild(li);
  }
  navEl.appendChild(ul);
}

// Key/value table renderer; objects/arrays land in a <pre> block.
function renderDetail(title, obj) {
  bodyEl.replaceChildren();
  const h = document.createElement("h3");
  h.textContent = title;
  bodyEl.appendChild(h);
  const table = document.createElement("table");
  for (const [k, v] of Object.entries(obj || {})) {
    const tr = document.createElement("tr");
    const td1 = document.createElement("td");
    td1.textContent = k;
    const td2 = document.createElement("td");
    if (v !== null && typeof v === "object") {
      const pre = document.createElement("pre");
      pre.textContent = JSON.stringify(v, null, 2);
      td2.appendChild(pre);
    } else {
      td2.textContent = String(v ?? "");
    }
    tr.appendChild(td1);
    tr.appendChild(td2);
    table.appendChild(tr);
  }
  bodyEl.appendChild(table);
}

// ============================================================
// Scope / target selector state
// ============================================================

let _scopeType = "platform"; // "platform" | "catalog" | "collection"
let _catalogId = null;
let _collectionId = null;
let _selectedProcessId = null;
let _selectedProcess = null; // full process description

/** Compute the base path segment for current scope (no leading /processes prefix). */
function _scopeBasePath() {
  if (_scopeType === "collection" && _catalogId && _collectionId) {
    return `/processes/catalogs/${encodeURIComponent(_catalogId)}/collections/${encodeURIComponent(_collectionId)}`;
  }
  if (_scopeType === "catalog" && _catalogId) {
    return `/processes/catalogs/${encodeURIComponent(_catalogId)}`;
  }
  return "/processes";
}

function _processListUrl() {
  return `${_scopeBasePath()}/processes?language=${lang()}`;
}

function _processDescUrl(processId) {
  return `/processes/processes/${encodeURIComponent(processId)}`;
}

function _executionUrl(processId) {
  return `${_scopeBasePath()}/processes/${encodeURIComponent(processId)}/execution`;
}

function _jobStatusUrl(jobId) {
  return `${_scopeBasePath()}/jobs/${encodeURIComponent(jobId)}`;
}

function _jobResultsUrl(jobId) {
  return `${_scopeBasePath()}/jobs/${encodeURIComponent(jobId)}/results`;
}

// ============================================================
// Run panel — scope selector + form + execution
// ============================================================

/** Build the scope/target selector bar and append to container. */
function buildScopeBar(container, onScopeChange) {
  const bar = document.createElement("div");
  bar.className = "scope-bar";

  // Scope type radio-like select
  const scopeLabel = document.createElement("label");
  scopeLabel.textContent = "Scope:";
  const scopeSel = document.createElement("select");
  scopeSel.id = "scope-type-sel";
  for (const [val, label] of [["platform", "Platform"], ["catalog", "Catalog"], ["collection", "Collection"]]) {
    const opt = document.createElement("option");
    opt.value = val;
    opt.textContent = label;
    scopeSel.appendChild(opt);
  }
  bar.appendChild(scopeLabel);
  bar.appendChild(scopeSel);

  // Catalog picker (hidden until scope != platform)
  const catLabel = document.createElement("label");
  catLabel.textContent = "Catalog:";
  const catSel = document.createElement("select");
  catSel.id = "scope-cat-sel";
  catSel.style.display = "none";
  catLabel.style.display = "none";
  bar.appendChild(catLabel);
  bar.appendChild(catSel);

  // Collection picker (hidden until scope == collection)
  const colLabel = document.createElement("label");
  colLabel.textContent = "Collection:";
  const colSel = document.createElement("select");
  colSel.id = "scope-col-sel";
  colSel.style.display = "none";
  colLabel.style.display = "none";
  bar.appendChild(colLabel);
  bar.appendChild(colSel);

  container.appendChild(bar);

  // Load catalogs and populate catSel
  async function loadCatalogs() {
    catSel.replaceChildren();
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "— select —";
    catSel.appendChild(placeholder);
    try {
      const res = await getJSON("/stac/catalogs");
      const catalogs = res.catalogs || res || [];
      for (const c of catalogs) {
        const id = c.id || c.catalog_id || c.name;
        if (!id) continue;
        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = c.title || id;
        catSel.appendChild(opt);
      }
    } catch (_) {
      const e = document.createElement("option");
      e.textContent = "Failed to load catalogs";
      catSel.appendChild(e);
    }
  }

  async function loadCollections(catalogId) {
    colSel.replaceChildren();
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "— select —";
    colSel.appendChild(placeholder);
    if (!catalogId) return;
    try {
      const res = await getJSON(`/stac/catalogs/${encodeURIComponent(catalogId)}/collections`);
      const cols = res.collections || res || [];
      for (const c of cols) {
        const id = c.id || c.collection_id;
        if (!id) continue;
        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = c.title || id;
        colSel.appendChild(opt);
      }
    } catch (_) {
      const e = document.createElement("option");
      e.textContent = "Failed to load collections";
      colSel.appendChild(e);
    }
  }

  function showHide() {
    const scope = scopeSel.value;
    const isCat = scope === "catalog" || scope === "collection";
    const isCol = scope === "collection";
    catLabel.style.display = isCat ? "" : "none";
    catSel.style.display = isCat ? "" : "none";
    colLabel.style.display = isCol ? "" : "none";
    colSel.style.display = isCol ? "" : "none";
  }

  scopeSel.addEventListener("change", async () => {
    _scopeType = scopeSel.value;
    _catalogId = null;
    _collectionId = null;
    showHide();
    if (_scopeType !== "platform") {
      await loadCatalogs();
    }
    if (_scopeType === "collection" && catSel.value) {
      _catalogId = catSel.value;
      await loadCollections(_catalogId);
    }
    onScopeChange();
  });

  catSel.addEventListener("change", async () => {
    _catalogId = catSel.value || null;
    _collectionId = null;
    colSel.replaceChildren();
    if (_scopeType === "collection" && _catalogId) {
      await loadCollections(_catalogId);
    }
    onScopeChange();
  });

  colSel.addEventListener("change", () => {
    _collectionId = colSel.value || null;
    onScopeChange();
  });
}

/** Async/sync mode toggle row. Returns {el, isAsync} getter. */
function buildAsyncToggle() {
  const wrap = document.createElement("div");
  wrap.className = "exec-toggle";
  const cb = document.createElement("input");
  cb.type = "checkbox";
  cb.id = "exec-async-toggle";
  cb.checked = true;
  const lbl = document.createElement("label");
  lbl.htmlFor = "exec-async-toggle";
  lbl.textContent = "Async execution (Prefer: respond-async)";
  wrap.appendChild(cb);
  wrap.appendChild(lbl);
  return { el: wrap, isAsync: () => cb.checked };
}

// ============================================================
// OGC execution with raw fetch (need Prefer header + 201 handling)
// ============================================================

async function executeProcess(url, body, preferAsync) {
  const target = apiUrl(url);
  const headers = {
    "Content-Type": "application/json",
    ...authHeader(),
    "Prefer": preferAsync ? "respond-async" : "respond-sync",
  };
  const r = await fetch(target, {
    method: "POST",
    credentials: "same-origin",
    headers,
    body: JSON.stringify(body),
  });
  return r; // caller inspects status
}

// ============================================================
// Job polling
// ============================================================

const TERMINAL_STATUSES = new Set(["successful", "failed", "dismissed"]);
const POLL_INTERVAL_MS = 2500;

async function pollJob(jobId, resultEl, onDone) {
  let attempts = 0;
  const statusUrl = _jobStatusUrl(jobId);

  async function tick() {
    attempts++;
    let statusInfo;
    try {
      statusInfo = await getJSON(statusUrl);
    } catch (e) {
      // Network error during polling — retry up to ~2 min
      if (attempts < 50) {
        setTimeout(tick, POLL_INTERVAL_MS);
      } else {
        setResult(resultEl, "error", `Polling failed after ${attempts} attempts: ${e.message}`);
        if (onDone) onDone(false);
      }
      return;
    }

    const status = statusInfo.status || "unknown";
    const progress = statusInfo.progress != null ? ` (${statusInfo.progress}%)` : "";
    renderPollRow(resultEl, jobId, status, progress);

    if (!TERMINAL_STATUSES.has(status)) {
      setTimeout(tick, POLL_INTERVAL_MS);
      return;
    }

    if (status === "successful") {
      try {
        const results = await getJSON(_jobResultsUrl(jobId));
        showJobResults(resultEl, jobId, results);
      } catch (e) {
        setResult(resultEl, "warn", `Job succeeded but could not fetch results: ${e.message}`);
      }
      if (onDone) onDone(true);
    } else {
      const msg = statusInfo.message || statusInfo.detail || "";
      setResult(resultEl, "error", `Job ${status}: ${msg}`);
      if (onDone) onDone(false);
    }
  }

  setTimeout(tick, POLL_INTERVAL_MS);
}

function renderPollRow(container, jobId, status, progress) {
  // Update or create a poll-row
  let row = container.querySelector(".poll-row");
  if (!row) {
    row = document.createElement("div");
    row.className = "poll-row";
    container.prepend(row);
  }
  row.replaceChildren();

  const lbl = document.createElement("span");
  lbl.textContent = `Job ${String(jobId).slice(0, 8)}…`;
  lbl.style.color = "#94a3b8";
  lbl.style.fontSize = "0.8rem";

  const badge = document.createElement("span");
  badge.className = `job-status-label ${status}`;
  badge.textContent = status + progress;

  row.appendChild(lbl);
  row.appendChild(badge);
}

function showJobResults(container, jobId, results) {
  // Show a results link + inline summary
  const div = document.createElement("div");
  div.style.marginTop = "0.5rem";

  const ok = document.createElement("p");
  ok.className = "result-ok";
  ok.textContent = `${t("proc.run.success")} — Job ${jobId}`;
  div.appendChild(ok);

  const pre = document.createElement("pre");
  pre.style.maxHeight = "300px";
  pre.style.overflow = "auto";
  pre.style.fontSize = "0.78rem";
  pre.textContent = JSON.stringify(results, null, 2);
  div.appendChild(pre);

  container.appendChild(div);
}

function setResult(container, level, text) {
  const p = document.createElement("p");
  p.className = level === "ok" ? "result-ok" : level === "warn" ? "result-warn" : "result-err";
  p.textContent = text;
  container.appendChild(p);
}

// ============================================================
// Build the run panel for a selected process
// ============================================================

/**
 * Build the run panel: scope bar + sync/async toggle + schema-driven form + submit.
 * Appended to bodyEl after the read-only detail table.
 */
function buildRunPanel(process) {
  _selectedProcess = process;
  _selectedProcessId = process.id;

  const panel = document.createElement("div");
  panel.id = "run-panel";

  const h4 = document.createElement("h4");
  h4.textContent = `Run: ${process.title || process.id}`;
  panel.appendChild(h4);

  // --- Scope bar ---
  buildScopeBar(panel, () => {
    // When scope changes, reload scoped process list in sidebar
    // (non-blocking — just refresh sidebar without losing detail pane)
    _loadScopedProcessListSilent();
  });

  // --- Async toggle ---
  const asyncToggle = buildAsyncToggle();
  panel.appendChild(asyncToggle.el);

  // --- Schema-driven form ---
  const formWrap = document.createElement("div");
  formWrap.id = "exec-form-wrap";
  panel.appendChild(formWrap);

  // Build a synthetic JSON Schema from the process inputs.
  // OGC ProcessInput has .schema (JSON Schema) per input name.
  // We render each input individually so we can control hidden fields.
  const inputNames = Object.keys(process.inputs || {});
  const hiddenInputs = new Set(["catalog_id", "collection_id"]);

  // Build per-input renderers using mountSchemaForm
  const inputRenderers = {}; // name → schemaForm result

  if (inputNames.length === 0) {
    const note = document.createElement("p");
    note.style.color = "#64748b";
    note.style.fontSize = "0.82rem";
    note.textContent = "This process takes no inputs.";
    formWrap.appendChild(note);
  }

  for (const name of inputNames) {
    const inputDef = process.inputs[name];
    const schema = inputDef.schema || {};
    const isHidden = hiddenInputs.has(name);

    const fieldDiv = document.createElement("div");
    fieldDiv.className = "field-row";
    if (isHidden) fieldDiv.style.display = "none";

    const label = document.createElement("label");
    label.textContent = inputDef.title || name;
    if (inputDef.description) {
      const desc = document.createElement("div");
      desc.className = "field-desc";
      desc.textContent = inputDef.description;
      fieldDiv.appendChild(label);
      fieldDiv.appendChild(desc);
    } else {
      fieldDiv.appendChild(label);
    }

    // Determine the initial value for hidden injected inputs
    let initialValue = undefined;
    if (name === "catalog_id") initialValue = _catalogId || "";
    if (name === "collection_id") initialValue = _collectionId || "";

    // Use mountSchemaForm for the input schema (wraps a single field)
    const container = document.createElement("div");
    const sf = mountSchemaForm(container, {
      schema: schema,
      resolved: initialValue !== undefined ? initialValue : (schema.default !== undefined ? schema.default : undefined),
      explicit: {},
      allowInherit: false,
      onDirty: () => {},
    });
    // Override: for simple primitives, mountSchemaForm wraps the full schema as
    // an object form if it has properties; for primitives it renders directly.
    // We access the value via sf.getPatch() which returns { fieldName: val } for
    // dirty fields only. Since we need ALL values, we use a different approach:
    // render with a wrapper object schema so we get fieldRenderers.
    fieldDiv.appendChild(container);
    formWrap.appendChild(fieldDiv);
    inputRenderers[name] = { sf, schema, isHidden, fieldDiv };
  }

  // --- Rebuild form when scope changes (update hidden fields) ---
  function refreshHiddenFields() {
    for (const [name, r] of Object.entries(inputRenderers)) {
      if (name === "catalog_id" && r.isHidden) {
        // update the hidden field value by re-mounting isn't ideal; skip silent refresh
        // (inject at submit time instead)
      }
    }
  }

  // --- Submit button ---
  const submitBtn = document.createElement("button");
  submitBtn.id = "exec-submit";
  submitBtn.textContent = t("proc.run.submit");
  panel.appendChild(submitBtn);

  // --- Result area ---
  const resultDiv = document.createElement("div");
  resultDiv.id = "exec-result";
  panel.appendChild(resultDiv);

  submitBtn.addEventListener("click", async () => {
    submitBtn.disabled = true;
    submitBtn.textContent = t("proc.run.submitting");
    resultDiv.replaceChildren();

    // Collect inputs from schema forms
    // For each input: build a per-field schema wrapper to get values
    // Since mountSchemaForm with a primitive schema doesn't expose fieldRenderers,
    // we re-collect via the DOM inputs directly for simplicity.
    const inputs = {};

    for (const [name, r] of Object.entries(inputRenderers)) {
      if (r.isHidden) {
        // Inject path-level values
        if (name === "catalog_id") inputs[name] = _catalogId || "";
        else if (name === "collection_id") inputs[name] = _collectionId || "";
        continue;
      }
      // For non-hidden fields, collect the raw DOM value
      // getPatch() returns only dirty fields; use raw DOM instead
      const inputEl = r.sf && r.sf._inputEl; // not available; use DOM query
      const domInput = r.fieldDiv.querySelector("input,select,textarea");
      if (domInput) {
        if (domInput.type === "checkbox") {
          inputs[name] = domInput.checked;
        } else if (domInput.type === "number") {
          const v = domInput.value;
          inputs[name] = v === "" ? null : (r.schema.type === "integer" ? parseInt(v, 10) : Number(v));
        } else if (domInput.dataset && domInput.dataset.widget === "json") {
          try { inputs[name] = JSON.parse(domInput.value); } catch { inputs[name] = domInput.value; }
        } else {
          inputs[name] = domInput.value;
        }
      }
    }

    const execBody = { inputs, response: "document" };
    const preferAsync = asyncToggle.isAsync();
    const url = _executionUrl(_selectedProcessId);

    try {
      const r = await executeProcess(url, execBody, preferAsync);

      if (r.status === 403) {
        setResult(resultDiv, "error", t("proc.run.forbidden"));
      } else if (r.status === 400 || r.status === 422) {
        let detail = "";
        try { const j = await r.json(); detail = JSON.stringify(j, null, 2); } catch { detail = await r.text().catch(() => ""); }
        const msg = document.createElement("div");
        msg.className = "result-err";
        const title = document.createElement("p");
        title.textContent = t("proc.run.validation");
        msg.appendChild(title);
        const pre = document.createElement("pre");
        pre.textContent = detail.slice(0, 800);
        msg.appendChild(pre);
        resultDiv.appendChild(msg);
      } else if (r.status === 201) {
        // Async — read jobID from Location header or body
        let jobId = null;
        const location = r.headers.get("Location") || r.headers.get("location") || "";
        const locationMatch = location.match(/\/jobs\/([^/?#]+)/);
        if (locationMatch) jobId = locationMatch[1];
        if (!jobId) {
          try {
            const body = await r.json();
            jobId = body.jobID || body.job_id || body.id;
          } catch { /* ignore */ }
        }
        if (!jobId) {
          setResult(resultDiv, "warn", "Job submitted (201) but could not extract jobID from response.");
        } else {
          const notice = document.createElement("p");
          notice.style.color = "#60a5fa";
          notice.style.fontSize = "0.82rem";
          notice.textContent = `Job submitted — polling ${jobId.slice(0, 8)}…`;
          resultDiv.appendChild(notice);
          renderPollRow(resultDiv, jobId, "running", "");
          pollJob(jobId, resultDiv, (ok) => {
            submitBtn.disabled = false;
            submitBtn.textContent = t("proc.run.submit");
          });
          // Don't re-enable submit yet — polling is still active; re-enable on done.
          return;
        }
      } else if (r.status === 200) {
        // Sync result
        let result = null;
        try { result = await r.json(); } catch { result = await r.text().catch(() => null); }
        const ok = document.createElement("p");
        ok.className = "result-ok";
        ok.textContent = t("proc.run.success");
        resultDiv.appendChild(ok);
        const pre = document.createElement("pre");
        pre.style.maxHeight = "300px";
        pre.style.overflow = "auto";
        pre.style.fontSize = "0.78rem";
        pre.textContent = typeof result === "string" ? result : JSON.stringify(result, null, 2);
        resultDiv.appendChild(pre);
      } else {
        const text = await r.text().catch(() => "");
        setResult(resultDiv, "error", `${t("proc.run.error")} HTTP ${r.status}: ${text.slice(0, 300)}`);
      }
    } catch (e) {
      setResult(resultDiv, "error", `${t("proc.run.error")} ${e.message}`);
    }

    submitBtn.disabled = false;
    submitBtn.textContent = t("proc.run.submit");
  });

  return panel;
}

// ============================================================
// Processes tab
// ============================================================

/** Current active scope processes URL (may be scoped). */
function _processListUrlForCurrentScope() {
  return _processListUrl();
}

async function _loadScopedProcessListSilent() {
  // Reload sidebar list silently (don't overwrite detail pane)
  try {
    const res = await getJSON(_processListUrlForCurrentScope());
    const procs = res.processes || res;
    renderList(procs, (p) => p.title || p.id, onProcessClick);
  } catch (e) {
    // Non-critical; just leave existing list
  }
}

async function onProcessClick(p) {
  bodyEl.textContent = t("proc.loading");
  try {
    const detail = await getJSON(_processDescUrl(p.id));
    renderDetail(detail.title || detail.id, {
      id: detail.id,
      version: detail.version,
      description: detail.description,
      jobControlOptions: detail.jobControlOptions,
      inputs: detail.inputs,
      outputs: detail.outputs,
    });
    // Append the run panel below the detail table
    const panel = buildRunPanel(detail);
    bodyEl.appendChild(panel);
  } catch (e) {
    bodyEl.textContent = t("proc.error");
  }
}

async function showProcesses() {
  setActive(tabProcesses);
  navEl.textContent = t("proc.loading");
  try {
    const res = await getJSON(`/processes/processes?language=${lang()}`);
    const procs = res.processes || res;
    renderList(procs, (p) => p.title || p.id, onProcessClick);
  } catch (e) {
    navEl.textContent = t("proc.error");
  }
}

// ============================================================
// Jobs tab
// ============================================================

async function showJobs() {
  setActive(tabJobs);
  navEl.textContent = t("proc.loading");
  try {
    const res = await getJSON("/processes/jobs");
    const jobs = res.jobs || res;
    renderList(
      jobs,
      (j) => `${j.processID || j.type || "job"} · ${j.status || ""} · ${j.jobID || j.id}`,
      async (j) => {
        bodyEl.textContent = t("proc.loading");
        try {
          const id = j.jobID || j.id;
          const detail = await getJSON(`/processes/jobs/${encodeURIComponent(id)}`);
          renderDetail(`Job ${id}`, detail);
        } catch (e) { bodyEl.textContent = t("proc.error"); }
      }
    );
  } catch (e) {
    navEl.textContent = t("proc.error");
  }
}

// ============================================================
// Bootstrap
// ============================================================

tabProcesses.addEventListener("click", showProcesses);
tabJobs.addEventListener("click", showJobs);
showProcesses();
